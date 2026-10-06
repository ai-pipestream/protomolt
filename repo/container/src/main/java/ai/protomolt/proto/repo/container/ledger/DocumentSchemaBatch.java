package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.persistence.EntityManager;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;

/** Complete command-level schema preparation. Neither staging nor this value publishes a revision. */
final class DocumentSchemaBatch {
    private final DocumentPublicationCommand command;
    private final DocumentSchemaPolicies.Selection policy;
    private final Map<String, DocumentSchemaAdmission.Proof> proofs;
    private final Map<String, ByteString> artifacts;

    private DocumentSchemaBatch(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Map<String, DocumentSchemaAdmission.Proof> proofs, Map<String, ByteString> artifacts) {
        this.command = command; this.policy = policy; this.proofs = Map.copyOf(proofs); this.artifacts = Map.copyOf(artifacts);
    }

    static DocumentSchemaBatch prepare(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Map<String, DocumentSchemaAdmission.Proof> supplied, Runnable control) throws InvalidProtocolBufferException {
        return prepareInternal(command, policy, supplied, null, control);
    }

    static DocumentSchemaBatch prepare(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Map<String, DocumentSchemaAdmission.Proof> supplied,
            ai.protomolt.proto.repo.admission.DocumentAdmissionReservations reservations, Runnable control)
            throws InvalidProtocolBufferException {
        return prepareInternal(command, policy, supplied, Objects.requireNonNull(reservations), control);
    }

    private static DocumentSchemaBatch prepareInternal(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Map<String, DocumentSchemaAdmission.Proof> supplied,
            ai.protomolt.proto.repo.admission.DocumentAdmissionReservations reservations, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(command); Objects.requireNonNull(policy); Objects.requireNonNull(supplied); active(control);
        command.requireExecutionSupported();
        if (!policy.account().equals(command.intent().getAccountId())) throw new IllegalArgumentException("Policy account differs from command");
        if (supplied.size() > command.intent().getMembersCount()) throw new IllegalArgumentException("Too many member schema proofs");
        var remaining = new HashMap<>(supplied);
        var accepted = new HashMap<String, DocumentSchemaAdmission.Proof>();
        var union = new DocumentSchemaUnion();
        var commandDigest = ByteString.copyFrom(HexFormat.of().parseHex(command.sha256()));
        for (var member : command.intent().getMembersList()) {
            active(control);
            boolean suppliedForMember = remaining.containsKey(member.getMemberId());
            var proof = remaining.remove(member.getMemberId());
            if (proof == null) {
                if (suppliedForMember || policy.policy().requiresTyped(member))
                    throw new IllegalArgumentException("Required member schema proof is absent");
                continue; // Explicitly permitted opaque member; no successful typed claim.
            }
            if (!proof.commandSha256().equals(commandDigest) || !proof.member().equals(member))
                throw new IllegalArgumentException("Schema proof differs from canonical command member");
            if (reservations == null) policy.policy().verifyProof(proof, () -> active(control));
            else policy.policy().verifyProof(proof, reservations, () -> active(control));
            union.add(proof.roots(), proof.artifacts(), () -> active(control));
            accepted.put(member.getMemberId(), proof);
        }
        if (!remaining.isEmpty()) throw new IllegalArgumentException("Schema proof names an unknown command member");
        active(control);
        return new DocumentSchemaBatch(command, policy, accepted, union.artifacts());
    }

    Map<String, DocumentSchemaAdmission.Proof> proofs() { return proofs; }
    Map<String, ByteString> artifacts() { return artifacts; }
    DocumentSchemaPolicies.Selection policy() { return policy; }
    DocumentPublicationCommand command() { return command; }

    /** Call before publication; this opens a separate owner-fenced staging transaction. */
    void stage(RepositorySchemaArtifacts storage, RepositoryOperationLedger.Owner owner, Runnable control) {
        Objects.requireNonNull(storage); requireOwner(owner); active(control);
        if (!artifacts.isEmpty()) storage.stage(owner, command, List.copyOf(new TreeMap<>(artifacts).values()), control);
    }

    /** Immediately after the operation fence and before document locks. */
    void lockPolicy(EntityManager em, RepositoryOperationLedger.Owner owner, Runnable control) {
        requireOwner(owner); active(control);
        RepositoryOperationLedger.requireCommand(em, owner.key(), command);
        DocumentSchemaPolicies.lockCurrent(em, policy, () -> active(control));
    }

    /** After physical publication locks, before writing revision references. No artifact bytes are loaded here. */
    void lockArtifacts(EntityManager em, RepositoryOperationLedger.Owner owner, Runnable control) {
        requireOwner(owner); active(control);
        em.createNativeQuery("SELECT require_repository_operation_write_fence(:account,:principal,:operation,:generation)")
                .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation()).getSingleResult();
        for (var digest : new java.util.TreeSet<>(artifacts.keySet())) {
            active(control);
            var rows = em.createNativeQuery("""
                    SELECT a.size_bytes FROM repository_schema_artifacts a JOIN repository_schema_artifact_claims c
                     ON c.account_id=a.account_id AND c.artifact_sha256=a.artifact_sha256
                    WHERE c.account_id=:account AND c.principal=:principal AND c.operation_id=:operation
                     AND c.owner_generation=:generation AND c.artifact_sha256=:sha FOR KEY SHARE OF a,c
                    """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                    .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                    .setParameter("sha", HexFormat.of().parseHex(digest)).getResultList();
            if (rows.size() != 1 || ((Number) rows.getFirst()).longValue() != artifacts.get(digest).size())
                throw new IllegalStateException("Schema artifact lacks an exact current owner claim");
        }
        active(control);
    }

    private void requireOwner(RepositoryOperationLedger.Owner owner) {
        Objects.requireNonNull(owner);
        if (!owner.key().account().equals(command.intent().getAccountId()) || !owner.key().operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Schema batch differs from operation owner scope");
    }
    private static void active(Runnable control) {
        Objects.requireNonNull(control);
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Schema batch interrupted");
        control.run();
    }
}
