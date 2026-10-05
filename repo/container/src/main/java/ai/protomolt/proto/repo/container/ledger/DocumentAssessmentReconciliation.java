package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Acknowledges an original stage; grants no publication, semantic-review or byte-read authority. */
final class DocumentAssessmentReconciliation {
    private final Tx tx;
    DocumentAssessmentReconciliation(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Empty means not observed under this fence, not proof of rollback or permission to recreate. */
    Optional<DocumentAssessmentCreation.Created> observe(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentSelectedAttemptLedger.Selected> selected,
            DocumentAssessmentEvidence evidence, UUID assessment, Instant deadline, PayloadBudget budget, Runnable control) {
        evidence.requireOwner(owner, control);
        var command = evidence.command(control);
        if (!command.operationId().equals(prepared.plan().command().operationId())
                || !command.canonical().equals(prepared.plan().command().canonical())) throw conflict();
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        var manifest = evidence.manifestBytes(control);
        var manifestSha = evidence.manifestSha256(control);
        var identity = new DocumentAssessmentSlotSnapshot.Identity(assessment, owner.key(), owner.generation(), command.sha256(), manifestSha, deadline);
        var selections = Map.copyOf(selected);
        int slots = command.intent().getMembersList().stream().mapToInt(member ->
                (int) member.getPartsList().stream().filter(part -> !part.hasEmpty()).count()).sum();
        // Only an exactly sized manifest is fetched. Snapshot reads reserve separately.
        try (var reading = budget.reserve(2L * manifest.size())) {
            return tx.inTransaction(em -> {
                evidence.check(control);
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
                var rows = em.createNativeQuery("""
                        SELECT assessment_id,command_codec,command_version,encode(command_sha256,'hex'),
                            sealed,release_xid IS NULL,retain_until=:deadline,retain_until>clock_timestamp(),
                            expected_slots,expected_artifacts,expected_roots,encode(manifest_sha256,'hex'),
                            CASE WHEN octet_length(manifest_bytes)=:size THEN manifest_bytes END
                        FROM document_assessment_owners WHERE account_id=:account AND principal=:principal
                            AND operation_id=:op AND owner_generation=:gen FOR UPDATE
                        """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                        .setParameter("op", owner.key().operationId()).setParameter("gen", owner.generation())
                        .setParameter("deadline", OffsetDateTime.ofInstant(deadline, ZoneOffset.UTC)).setParameter("size", manifest.size()).getResultList();
                if (rows.isEmpty()) {
                    RepositoryOperationLedger.fenceLiveOwner(em, owner); evidence.check(control);
                    return Optional.empty();
                }
                if (rows.size() != 1) throw conflict();
                var row = (Object[]) rows.getFirst();
                if (!assessment.equals(row[0]) || !DocumentPublicationCommand.CODEC.equals(row[1])
                        || ((Number) row[2]).intValue() != DocumentPublicationCommand.ENCODING_VERSION || !command.sha256().equals(row[3])
                        || !Boolean.TRUE.equals(row[4]) || !Boolean.TRUE.equals(row[5]) || !Boolean.TRUE.equals(row[6]) || !Boolean.TRUE.equals(row[7])
                        || ((Number) row[8]).intValue() != slots || ((Number) row[9]).intValue() != evidence.artifacts(control).size()
                        || ((Number) row[10]).intValue() != evidence.roots(control).size() || !manifestSha.equals(row[11])
                        || !(row[12] instanceof byte[] stored) || !manifest.asReadOnlyByteBuffer().equals(ByteBuffer.wrap(stored))) throw conflict();
                DocumentAssessmentRetainedSlots.verify(em, identity, prepared.plan(), selections, budget, control);
                verifyArtifacts(em, assessment, evidence.artifacts(control), control);
                verifyRoots(em, assessment, evidence, control);
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                if (!Boolean.TRUE.equals(em.createNativeQuery("""
                        SELECT retain_until>clock_timestamp() AND sealed AND release_xid IS NULL
                        FROM document_assessment_owners WHERE assessment_id=:id
                        """).setParameter("id", assessment).getSingleResult())) throw conflict();
                evidence.check(control);
                return Optional.of(new DocumentAssessmentCreation.Created(assessment, manifestSha, deadline));
            });
        }
    }

    private static void verifyArtifacts(EntityManager em, UUID assessment, Map<String,ByteString> expected, Runnable control) {
        var rows = em.createNativeQuery("""
                SELECT encode(r.artifact_sha256,'hex'),a.size_bytes,encode(sha256(a.artifact_bytes),'hex')
                FROM document_assessment_artifacts r JOIN repository_schema_artifacts a USING(account_id,artifact_sha256)
                WHERE r.assessment_id=:id LIMIT 65
                """).setParameter("id", assessment).getResultList();
        if (rows.size() != expected.size()) throw conflict();
        var seen = new HashSet<String>();
        for (Object value : rows) {
            control.run(); var row = (Object[]) value; String sha = (String) row[0]; var bytes = expected.get(sha);
            if (bytes == null || !seen.add(sha) || bytes.size() != ((Number) row[1]).intValue() || !sha.equals(row[2])) throw conflict();
        }
    }

    private record RootKey(String member, int ordinal, String locator) {}
    private static void verifyRoots(EntityManager em, UUID assessment, DocumentAssessmentEvidence evidence, Runnable control) {
        var expected = new HashMap<RootKey, DocumentAssessmentEvidence.Root>();
        for (var root : evidence.roots(control))
            if (expected.put(new RootKey(root.member(), root.ordinal(), root.locatorSha256()), root) != null) throw conflict();
        var rows = em.createNativeQuery("""
                SELECT member_id,revision_ordinal,encode(root_locator_sha256,'hex'),encode(fragment_sha256,'hex'),fragment_size,
                    evidence_codec,evidence_version,encode(evidence_sha256,'hex'),evidence_size,encode(sha256(evidence_bytes),'hex')
                FROM document_assessment_roots WHERE assessment_id=:id LIMIT 4097
                """).setParameter("id", assessment).getResultList();
        if (rows.size() != expected.size()) throw conflict();
        for (Object value : rows) {
            control.run(); var row = (Object[]) value;
            var root = expected.remove(new RootKey((String) row[0], ((Number) row[1]).intValue(), (String) row[2]));
            if (root == null || !root.fragmentSha256().equals(row[3]) || root.fragmentSize() != ((Number) row[4]).longValue()
                    || !root.codec().equals(row[5]) || root.version() != ((Number) row[6]).intValue() || !root.sha256().equals(row[7])
                    || root.bytes().size() != ((Number) row[8]).intValue() || !root.sha256().equals(row[9])) throw conflict();
        }
    }
    private static IllegalStateException conflict() { return new IllegalStateException("Retained assessment differs from requested original stage or is unavailable"); }
}
