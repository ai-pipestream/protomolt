package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import jakarta.persistence.EntityManager;
import jakarta.persistence.FlushModeType;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Inserts the immutable pre-write binding for an already validated batch. */
final class DocumentSchemaAdmissionBinding {
    private DocumentSchemaAdmissionBinding() {}

    /** Caller holds owner, policy, document, physical and batch artifact locks in that order. */
    static String insert(EntityManager em, RepositoryOperationLedger.Owner owner, DocumentSchemaBatch batch,
            DocumentCommitWriter.Candidate candidate, DocumentCommitParts.Bound parts, Runnable control) {
        Objects.requireNonNull(em); Objects.requireNonNull(owner); Objects.requireNonNull(batch);
        Objects.requireNonNull(candidate); Objects.requireNonNull(parts); Objects.requireNonNull(control);
        if (!em.getTransaction().isActive()) throw new IllegalStateException("Schema admission requires a transaction");
        try {
            active(control);
            if (!owner.key().account().equals(batch.command().intent().getAccountId())
                    || !owner.key().operationId().equals(batch.command().operationId()))
                throw new IllegalArgumentException("Schema admission owner differs from command");
            var member = batch.command().intent().getMembersList().stream()
                    .filter(value -> value.getMemberId().equals(candidate.member())).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown schema admission member"));
            if (!candidate.row().nodeId.equals(DocumentIds.nodeId(member.getDestination().getAddress()))
                    || !candidate.row().accountId.equals(owner.key().account())
                    || !Objects.equals(parts.selections().get(candidate.member()), candidate.selection()))
                throw new IllegalArgumentException("Schema admission candidate differs from selection");
            var manifest = DocumentSchemaManifest.prepare(batch, candidate.member(), parts, control);
            var snapshot = DocumentAdmissionSnapshot.prepare(em, candidate.row());
            var proof = batch.proofs().get(candidate.member());
            var container = proof == null ? null : proof.containerReference();
            active(control);
            int inserted = em.createNativeQuery("""
                    INSERT INTO document_revision_schema_admissions(revision_id,account_id,principal,operation_id,
                     owner_generation,member_id,node_id,selection_revision,command_sha256,policy_revision,policy_sha256,
                     decision,body,metadata,manifest,container_type_url_sha256,container_descriptor_sha256)
                    VALUES(:revision,:account,:principal,:operation,:generation,:member,:node,:selection,:command,
                     :policyRevision,:policy,:decision,CAST(:body AS jsonb),CAST(:metadata AS jsonb),CAST(:manifest AS jsonb),:containerUrl,:containerDescriptor)
                    """).setFlushMode(FlushModeType.COMMIT)
                    .setParameter("revision", candidate.revision()).setParameter("account", owner.key().account())
                    .setParameter("principal", owner.key().principal()).setParameter("operation", owner.key().operationId())
                    .setParameter("generation", owner.generation()).setParameter("member", candidate.member())
                    .setParameter("node", candidate.row().nodeId).setParameter("selection", candidate.selection())
                    .setParameter("command", HexFormat.of().parseHex(batch.command().sha256()))
                    .setParameter("policyRevision", batch.policy().revision())
                    .setParameter("policy", HexFormat.of().parseHex(batch.policy().policy().sha256()))
                    .setParameter("decision", manifest.decision()).setParameter("body", snapshot.body())
                    .setParameter("metadata", snapshot.metadata()).setParameter("manifest", manifest.json())
                    .setParameter("containerUrl", container == null ? null : HexFormat.of().parseHex(
                            DocumentPartCodec.sha256Hex(container.typeUrl().getBytes(StandardCharsets.UTF_8))))
                    .setParameter("containerDescriptor", container == null ? null : HexFormat.of().parseHex(container.descriptorSha256())).executeUpdate();
            if (inserted != 1) throw new IllegalStateException("Schema admission insert count differs");
            active(control);
            return manifest.decision();
        } catch (RuntimeException | Error failure) {
            try { em.getTransaction().setRollbackOnly(); }
            catch (RuntimeException markingFailure) { failure.addSuppressed(markingFailure); }
            throw failure;
        }
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Schema admission interrupted");
        control.run();
    }
}
