package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Acknowledges an original stage; grants no publication, semantic-review or byte-read authority. */
final class DocumentAssessmentReconciliation {
    private final Tx tx;
    DocumentAssessmentReconciliation(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Same-process convenience; keeps the original observation scope checked through acknowledgement. */
    Optional<DocumentAssessmentCreation.Created> observe(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selected,
            DocumentAssessmentEvidence evidence, UUID assessment, Instant deadline, PayloadBudget budget, Runnable control) {
        evidence.requireOwner(owner, control);
        var observedCommand = evidence.command(control);
        if (!command.operationId().equals(observedCommand.operationId())
                || !command.canonical().equals(observedCommand.canonical())) throw conflict();
        return observeRetained(caller, owner, command, selected, assessment, evidence.manifestSha256(control), deadline, budget,
                () -> evidence.check(control));
    }

    /**
     * Uses only durable request identities and stored evidence. The original live
     * owner nonce must survive restart. Empty is not proof of rollback or authority
     * to recreate. This does not re-observe a runtime or validate candidate content.
     */
    Optional<DocumentAssessmentCreation.Created> observeRetained(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selected,
            UUID assessment, String manifestSha, Instant deadline, PayloadBudget budget, Runnable callerControl) {
        return observeRetained(caller, owner, command, selected, assessment, manifestSha, deadline, budget,
                callerControl, null, null, () -> {});
    }

    /** Verifies the complete stored evidence and creates its reader session in the same transaction. */
    DocumentAssessmentCreation.Created captureRetained(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selected,
            UUID assessment, String manifestSha, Instant deadline, PayloadBudget budget, Runnable callerControl,
            UUID reader, UUID session, Runnable beforeCommit) {
        Objects.requireNonNull(reader); Objects.requireNonNull(session); Objects.requireNonNull(beforeCommit);
        return observeRetained(caller, owner, command, selected, assessment, manifestSha, deadline, budget,
                callerControl, reader, session, beforeCommit).orElseThrow(DocumentAssessmentReconciliation::conflict);
    }

    private Optional<DocumentAssessmentCreation.Created> observeRetained(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selected,
            UUID assessment, String manifestSha, Instant deadline, PayloadBudget budget, Runnable callerControl,
            UUID reader, UUID session, Runnable beforeCommit) {
        Objects.requireNonNull(callerControl);
        Runnable control = () -> {
            if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Assessment acknowledgement interrupted");
            callerControl.run();
            if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Assessment acknowledgement interrupted");
        };
        control.run();
        if (!owner.key().account().equals(command.intent().getAccountId()) || !owner.key().operationId().equals(command.operationId())) throw conflict();
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        var identity = new DocumentAssessmentSlotSnapshot.Identity(assessment, owner.key(), owner.generation(), command.sha256(), manifestSha, deadline);
        var selections = Map.copyOf(selected);
        int slots = command.intent().getMembersList().stream().mapToInt(member ->
                (int) member.getPartsList().stream().filter(part -> !part.hasEmpty()).count()).sum();
        // JDBC payload, returned array and immutable copy. Decoding and snapshots reserve separately.
        try (var reading = budget.reserve(3L * DocumentAssessmentManifestCodec.MAX_BYTES)) {
            return tx.inTransaction(em -> {
                control.run();
                if (reader != null) em.createNativeQuery("SELECT require_active_repository_reader(:reader)")
                        .setParameter("reader", reader).getSingleResult();
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
                var rows = em.createNativeQuery("""
                        SELECT assessment_id,command_codec,command_version,encode(command_sha256,'hex'),
                            sealed,release_xid IS NULL,retain_until=:deadline,retain_until>clock_timestamp(),
                            expected_slots,expected_artifacts,expected_roots,encode(manifest_sha256,'hex'),
                            CASE WHEN octet_length(manifest_bytes) BETWEEN 1 AND 4194304 THEN manifest_bytes END
                        FROM document_assessment_owners WHERE account_id=:account AND principal=:principal
                            AND operation_id=:op AND owner_generation=:gen FOR UPDATE
                        """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                        .setParameter("op", owner.key().operationId()).setParameter("gen", owner.generation())
                        .setParameter("deadline", OffsetDateTime.ofInstant(deadline, ZoneOffset.UTC)).getResultList();
                if (rows.isEmpty()) {
                    RepositoryOperationLedger.fenceLiveOwner(em, owner); control.run();
                    return Optional.empty();
                }
                if (rows.size() != 1) throw conflict();
                var row = (Object[]) rows.getFirst();
                if (!assessment.equals(row[0]) || !DocumentPublicationCommand.CODEC.equals(row[1])
                        || ((Number) row[2]).intValue() != DocumentPublicationCommand.ENCODING_VERSION || !command.sha256().equals(row[3])
                        || !Boolean.TRUE.equals(row[4]) || !Boolean.TRUE.equals(row[5]) || !Boolean.TRUE.equals(row[6]) || !Boolean.TRUE.equals(row[7])
                        || ((Number) row[8]).intValue() != slots || !manifestSha.equals(row[11]) || !(row[12] instanceof byte[] stored)) throw conflict();
                DocumentAssessmentRetainedSlots.verify(em, identity, command, selections, budget, control);
                DocumentAssessmentRetainedEvidence.verify(em, identity, command, ByteString.copyFrom(stored),
                        ((Number) row[9]).intValue(), ((Number) row[10]).intValue(), budget, control);
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                if (!Boolean.TRUE.equals(em.createNativeQuery("""
                        SELECT retain_until>clock_timestamp() AND sealed AND release_xid IS NULL
                        FROM document_assessment_owners WHERE assessment_id=:id
                        """).setParameter("id", assessment).getSingleResult())) throw conflict();
                control.run();
                if (reader != null) {
                    em.createNativeQuery("""
                            INSERT INTO document_assessment_read_sessions(session_id,reader_incarnation,assessment_id)
                            VALUES(:session,:reader,:assessment)
                            """).setParameter("session", session).setParameter("reader", reader)
                            .setParameter("assessment", assessment).executeUpdate();
                    control.run();
                    beforeCommit.run();
                }
                return Optional.of(new DocumentAssessmentCreation.Created(assessment, manifestSha, deadline));
            });
        }
    }
    private static IllegalStateException conflict() { return new IllegalStateException("Retained assessment differs from requested original stage or is unavailable"); }
}
