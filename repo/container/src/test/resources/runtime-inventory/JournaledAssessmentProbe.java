package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
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
        long beforeRestoration = budget.reservedBytes();
        try (var restored = DocumentPublicationRestoration.restore(tx, budget, caller, owner, RepositoryReadControl.NONE)) {
            require(budget.reservedBytes() > beforeRestoration, "restoration owns preparation bytes after committed assessment");
        }
        require(budget.reservedBytes() == beforeRestoration, "restoration releases preparation bytes");
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

    static void resume(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentAssessmentRuntimeObserver.Observation observation,
            ai.protomolt.proto.repo.codec.DocumentRevisionAssembly.Limits limits) throws InterruptedException {
        var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var count = new java.util.concurrent.atomic.AtomicInteger();
        var reader = new DocumentPartReader((generation, profile) -> {
            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "original provider identity");
            return provider.store();
        }, 2, 16_000_000, payload);
        var execution = new DocumentPublicationAssessmentExecution(tx, new DriveLedger(tx), reads,
                (captured, member, control) -> { count.incrementAndGet(); return reader.readAssessment(captured, member, control); },
                budget, limits, observation, Duration.ofMinutes(5), Duration.ofSeconds(5));
        try (reader; var restored = DocumentPublicationRestoration.restore(tx, budget, caller, owner, RepositoryReadControl.NONE)) {
            var start = new DocumentAssessmentStartJournal(tx, budget).load(caller, owner, command, RepositoryReadControl.NONE).orElseThrow();
            var original = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow().stage();
            for (var changed : java.util.List.of(new DocumentAssessmentStartJournal.Started(UUID.randomUUID(), start.retainUntil()),
                    new DocumentAssessmentStartJournal.Started(start.assessment(), start.retainUntil().plusSeconds(1)))) {
                try {
                    execution.resume(caller, owner, command, changed, RepositoryReadControl.NONE);
                    throw new AssertionError("Changed assessment coordinates were accepted");
                } catch (RepositoryException expected) {
                    require(expected.code() == RepositoryException.Code.FAILED_PRECONDITION
                            && expected.getMessage().equals("Discovered assessment differs from retained start"), "exact coordinate refusal");
                }
            }
            require(count.get() == 0, "mismatched coordinates never read provider content");
            var result = rejection(restored, caller, execution);
            var receipt = new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow();
            require(result.equals(receipt), "restored terminal signal carries durable receipt");
            require(receipt.getReasonValue() == 2
                    && receipt.getAssessment().getAssessmentId().equals(start.assessment().toString())
                    && receipt.getAssessment().getManifestSha256().equals(original.manifestSha256())
                    && receipt.getAssessment().getRetainUntilEpochMicros() == start.retainUntil().getEpochSecond() * 1_000_000
                            + start.retainUntil().getNano() / 1000, "restored rejection binds original assessment receipt");
            require(count.get() == command.intent().getMembersCount(), "original members read once for decision");
            int after = count.get();
            require(rejection(restored, caller, execution).equals(result), "restored exact terminal replay");
            require(count.get() == after, "terminal replay does not reread provider content");
        } finally {
            require(reader.awaitIdle(Duration.ofSeconds(5)), "restoration provider work drains");
            reads.releaseDrained(1);
        }
        require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0 && reads.outstandingReads() == 0,
                "restoration releases reservations and read pins");
        System.out.println("JOURNALED_ASSESSMENT_HANDLE_RESUME_OK");
    }

    private static ai.protomolt.proto.repo.v1.DocumentPublicationRejection rejection(DocumentPublicationRestoration restored,
            RepositoryCaller caller, DocumentPublicationAssessmentExecution execution) {
        try {
            restored.resume(caller, execution, RepositoryReadControl.NONE);
            throw new AssertionError("Invalid retained candidate produced success");
        } catch (DocumentPublicationReplay.Terminated rejected) { return rejected.receipt(); }
    }
}
