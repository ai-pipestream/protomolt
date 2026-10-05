package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/** Real assessment COMMIT with lost JDBC acknowledgment, followed by shared-SQL reconciliation. */
public final class JournaledAssessmentProbe {
    static DocumentAssessmentCreation.Created createWithLostAcknowledgment(Tx tx, javax.sql.DataSource database,
            RepositoryCaller caller, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentSelectedAttemptLedger.Selected> selected,
            DocumentAssessmentEvidence evidence, PayloadBudget budget) {
        var journal = new DocumentAssessmentStartJournal(tx, budget);
        var start = journal.start(caller, owner, command, UUID.randomUUID(), Duration.ofMinutes(5), RepositoryReadControl.NONE);
        var fired = new AtomicBoolean();
        try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", AssessmentRejectionProbe.faultAfterAssessmentCommit(database, fired),
                        "hibernate.hbm2ddl.auto", "validate"))) {
            var faulted = new Tx(emf);
            try {
                new DocumentAssessmentCreation(faulted, new DriveLedger(faulted)).create(caller, owner, prepared,
                        selected, evidence, start.assessment(), start.retainUntil(), budget, () -> {});
                throw new AssertionError("Assessment commit acknowledgment was not lost");
            } catch (RuntimeException failure) {
                boolean jdbc = false;
                for (Throwable cause = failure; cause != null; cause = cause.getCause())
                    if (cause instanceof java.sql.SQLException sql && "08006".equals(sql.getSQLState())) jdbc = true;
                require(jdbc && fired.get(), "actual assessment COMMIT acknowledgment loss");
            }
        }
        // No CREATE retry. Fresh helpers recover the sticky identity and committed sealed stage.
        var recovered = new DocumentAssessmentStartJournal(tx, budget).load(caller, owner, command, RepositoryReadControl.NONE).orElseThrow();
        require(recovered.equals(start), "exact journal coordinates survive lost acknowledgment");
        var discovered = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
        require(discovered.stage().assessment().equals(recovered.assessment())
                && discovered.stage().retainUntil().equals(recovered.retainUntil()), "committed assessment matches sticky identity");
        require(discovered.selections().equals(DocumentAssessmentRetainedSlots.uploadSelections(selected)), "original attempts retained");
        long stages = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM document_assessment_owners WHERE operation_id=:op AND sealed AND release_xid IS NULL
                """).setParameter("op", command.operationId()).getSingleResult()).longValue());
        require(stages == 1, "one sealed original assessment");
        System.out.println("JOURNALED_ASSESSMENT_COMMIT_RECOVERY_OK");
        return discovered.stage();
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
