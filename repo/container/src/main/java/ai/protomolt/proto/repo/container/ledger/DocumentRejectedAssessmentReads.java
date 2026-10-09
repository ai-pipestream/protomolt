package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Receipt-authorized historical evidence reads. Never establishes operation write authority. */
final class DocumentRejectedAssessmentReads {
    record Captured(DocumentAssessmentReadPlan plan, DocumentPublicationRejection receipt) {}
    private DocumentRejectedAssessmentReads() {}

    static Captured capture(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command, PayloadBudget budget,
            UUID reader, UUID session, RepositoryReadControl control, Consumer<DocumentAssessmentReadPlan> beforeCommit) {
        Objects.requireNonNull(command); Objects.requireNonNull(budget); Objects.requireNonNull(reader);
        Objects.requireNonNull(session); Objects.requireNonNull(beforeCommit); Objects.requireNonNull(control).check();
        // JDBC array and immutable copy are reserved before fetching the bounded manifest.
        try (var reading = budget.reserve(3L * DocumentAssessmentManifestCodec.MAX_BYTES)) {
            return tx.inTransaction(em -> {
                activeReader(em, reader);
                var receipt = receipt(em, caller, command);
                var identity = identity(receipt);
                lockStage(em, identity, control);
                var values = em.createNativeQuery("""
                        SELECT expected_slots,expected_artifacts,expected_roots,
                            CASE WHEN octet_length(manifest_bytes) BETWEEN 1 AND 4194304 THEN manifest_bytes END
                        FROM document_assessment_owners WHERE assessment_id=:id
                        """).setParameter("id", identity.assessment()).getResultList();
                if (values.size() != 1) throw corrupt();
                var row = (Object[]) values.getFirst();
                int slots = command.intent().getMembersList().stream()
                        .mapToInt(member -> (int) member.getPartsList().stream().filter(part -> !part.hasEmpty()).count()).sum();
                if (((Number) row[0]).intValue() != slots || !(row[3] instanceof byte[] bytes)) throw corrupt();
                var entries = DocumentAssessmentReadRows.capture(em, identity.assessment(), control::check);
                var selections = DocumentAssessmentRetainedSlots.retainedSelections(em, identity, control::check);
                DocumentAssessmentRetainedSlots.verify(em, identity, command, selections, budget, control::check);
                DocumentAssessmentRetainedEvidence.verify(em, identity, command, ByteString.copyFrom(bytes),
                        ((Number) row[1]).intValue(), ((Number) row[2]).intValue(), budget, control::check);
                requireUnexpired(em, identity);
                control.check();
                em.createNativeQuery("""
                        INSERT INTO document_assessment_read_sessions(session_id,reader_incarnation,assessment_id,authority)
                        VALUES(:session,:reader,:assessment,'ADMISSION_REJECTION')
                        """).setParameter("session", session).setParameter("reader", reader)
                        .setParameter("assessment", identity.assessment()).executeUpdate();
                var plan = new DocumentAssessmentReadPlan(new DocumentAssessmentCreation.Created(identity.assessment(),
                        identity.manifestSha256(), identity.retainUntil()), entries,
                        command.intent().getMembersList().stream().map(member -> member.getMemberId()).collect(Collectors.toSet()));
                control.check();
                beforeCommit.accept(plan);
                return new Captured(plan, receipt);
            });
        }
    }

    static void authorizeDelivery(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentPublicationRejection expected, DocumentAssessmentReadProtection<DocumentAssessmentReadPlan> captured,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        tx.inTransaction(em -> {
            activeReader(em, captured.reader());
            var receipt = receipt(em, caller, command);
            if (!receipt.equals(expected)) throw corrupt();
            var identity = identity(receipt); var stage = captured.plan().stage();
            if (!identity.assessment().equals(captured.assessment()) || !identity.assessment().equals(stage.assessment())
                    || !identity.manifestSha256().equals(stage.manifestSha256()) || !identity.retainUntil().equals(stage.retainUntil()))
                throw corrupt();
            lockStage(em, identity, control);
            var sessions = em.createNativeQuery("""
                    SELECT session_id FROM document_assessment_read_sessions WHERE session_id=:session
                        AND reader_incarnation=:reader AND assessment_id=:assessment AND authority='ADMISSION_REJECTION'
                        AND release_xid IS NULL FOR SHARE
                    """).setParameter("session", captured.session()).setParameter("reader", captured.reader())
                    .setParameter("assessment", identity.assessment()).getResultList();
            if (sessions.size() != 1) throw corrupt();
            requireUnexpired(em, identity);
            control.check();
        });
        control.check();
    }

    private static DocumentPublicationRejection receipt(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command) {
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED, "Authenticated repository caller is required");
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        // Shared operation-owner lock, exact command and complete current ACL set.
        // Replay validates the canonical body against every relational receipt field.
        var observed = DocumentPublicationReplay.observe(em, caller, command, key);
        if (observed.state() != DocumentPublicationReplay.State.TERMINATED
                || observed.rejection().orElseThrow().getReasonValue() != 2) throw unavailable();
        return observed.rejection().orElseThrow();
    }

    private static DocumentAssessmentSlotSnapshot.Identity identity(DocumentPublicationRejection receipt) {
        var binding = receipt.getAssessment(); long micros = binding.getRetainUntilEpochMicros();
        return new DocumentAssessmentSlotSnapshot.Identity(UUID.fromString(binding.getAssessmentId()),
                new RepositoryOperationLedger.Key(receipt.getAccountId(), receipt.getPrincipal(), UUID.fromString(receipt.getOperationId())),
                receipt.getOwnerGeneration(), receipt.getCommandSha256(), binding.getManifestSha256(),
                Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000));
    }

    private static void lockStage(EntityManager em, DocumentAssessmentSlotSnapshot.Identity identity, RepositoryReadControl control) {
        control.check();
        requireUnexpired(em, identity);
        var rows = em.createNativeQuery("""
                SELECT assessment_id FROM document_assessment_owners
                WHERE assessment_id=:id AND account_id=:account AND principal=:principal AND operation_id=:op
                    AND owner_generation=:gen AND command_codec=:codec AND command_version=:version
                    AND encode(command_sha256,'hex')=:command AND encode(manifest_sha256,'hex')=:manifest
                    AND sealed AND release_xid IS NULL AND retain_until=:deadline FOR SHARE
                """).setParameter("id", identity.assessment()).setParameter("account", identity.key().account())
                .setParameter("principal", identity.key().principal()).setParameter("op", identity.key().operationId())
                .setParameter("gen", identity.generation()).setParameter("codec", DocumentPublicationCommand.CODEC)
                .setParameter("version", DocumentPublicationCommand.ENCODING_VERSION).setParameter("command", identity.commandSha256())
                .setParameter("manifest", identity.manifestSha256()).setParameter("deadline", deadline(identity)).getResultList();
        // Check time after any row-lock wait, before classifying an absent row as corruption.
        requireUnexpired(em, identity);
        if (rows.size() != 1) throw corrupt();
        control.check();
    }

    private static void requireUnexpired(EntityManager em, DocumentAssessmentSlotSnapshot.Identity identity) {
        if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT CAST(:deadline AS timestamptz)>clock_timestamp()")
                .setParameter("deadline", deadline(identity)).getSingleResult())) throw unavailable();
    }
    private static OffsetDateTime deadline(DocumentAssessmentSlotSnapshot.Identity identity) {
        return OffsetDateTime.ofInstant(identity.retainUntil(), ZoneOffset.UTC);
    }
    private static void activeReader(EntityManager em, UUID reader) {
        em.createNativeQuery("SELECT require_active_repository_reader(:reader)").setParameter("reader", reader).getSingleResult();
    }
    private static RepositoryException unavailable() {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Retained rejection evidence is unavailable");
    }
    private static RepositoryException corrupt() {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Retained rejection evidence binding is incomplete or invalid");
    }
}
