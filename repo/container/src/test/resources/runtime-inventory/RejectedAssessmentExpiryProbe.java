package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Real PostgreSQL row waits and database-clock expiry; no deadline rewriting. */
public final class RejectedAssessmentExpiryProbe {
    static void run(javax.sql.DataSource database, Tx tx, RepositoryCaller caller,
            DocumentPublicationCommand command, DocumentAssessmentCreation.Created stage) throws Exception {
        var existing = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var incoming = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var budget = new PayloadBudget(128_000_000);
        var receipt = new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow();
        try (var captured = existing.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE)) {
            try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                java.util.concurrent.Future<RepositoryException.Code> capture;
                java.util.concurrent.Future<RepositoryException.Code> delivery;
                try (var blocker = database.getConnection()) {
                    blocker.setAutoCommit(false);
                    int pid;
                    try (var statement = blocker.prepareStatement("SELECT pg_backend_pid() FROM document_assessment_owners WHERE assessment_id=? FOR UPDATE")) {
                        statement.setQueryTimeout(10); statement.setObject(1, stage.assessment());
                        try (var rows = statement.executeQuery()) { require(rows.next(), "retained assessment locked"); pid = rows.getInt(1); }
                    }
                    capture = workers.submit(() -> refused(() -> {
                        try (var unexpected = incoming.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE)) {
                            throw new AssertionError("Expired capture returned a handle");
                        }
                    }));
                    delivery = workers.submit(() -> refused(() -> {
                        try (var use = captured.use()) { captured.authorizeDelivery(use, RepositoryReadControl.NONE); }
                    }));
                    long stop = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                    while (tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))")
                            .setParameter("blocker", pid).getSingleResult()).intValue()) < 2) {
                        require(System.nanoTime() < stop, "both reads reached the real assessment row lock");
                        Thread.sleep(20);
                    }
                    stop = System.nanoTime() + Duration.ofSeconds(25).toNanos();
                    while (!tx.readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT retain_until<=clock_timestamp() FROM document_assessment_owners WHERE assessment_id=:id")
                            .setParameter("id", stage.assessment()).getSingleResult())) {
                        require(System.nanoTime() < stop, "retention expires within fixture bound");
                        Thread.sleep(50);
                    }
                    blocker.commit();
                }
                require(capture.get(5, TimeUnit.SECONDS) == RepositoryException.Code.FAILED_PRECONDITION, "capture refuses expiry after lock wait");
                require(delivery.get(5, TimeUnit.SECONDS) == RepositoryException.Code.FAILED_PRECONDITION, "delivery refuses expiry after lock wait");
            }
            require(incoming.outstandingReads() == 0, "expired capture consumes no capacity");
            try {
                release(tx, command, stage);
                throw new AssertionError("Expired assessment released while reader remained active");
            } catch (RuntimeException failure) {
                boolean guarded = false;
                for (Throwable cause = failure; cause != null; cause = cause.getCause())
                    if (cause instanceof java.sql.SQLException sql && "P0001".equals(sql.getSQLState())
                            && sql.getMessage().contains("all reader sessions to drain")) guarded = true;
                require(guarded, "active session protects expired assessment");
            }
        }
        require(existing.releaseDrained(1) == 1 && existing.outstandingReads() == 0, "expired delivery session drains");
        require(release(tx, command, stage), "expired evidence releases after drain");
        require(refused(() -> incoming.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE))
                == RepositoryException.Code.FAILED_PRECONDITION, "pruned evidence is unavailable");
        require(new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow().equals(receipt),
                "receipt replay survives evidence expiry and cleanup");
        require(budget.reservedBytes() == 0 && incoming.outstandingReads() == 0, "expiry leaves no read resources");
        System.out.println("REJECTED_ASSESSMENT_EXPIRY_WAITS_OK");
    }

    private static boolean release(Tx tx, DocumentPublicationCommand command, DocumentAssessmentCreation.Created stage) {
        return tx.inTransaction(em -> (Boolean) em.createNativeQuery(
                "SELECT release_expired_document_assessment(:account,'principal',:op,:id)")
                .setParameter("account", command.intent().getAccountId()).setParameter("op", command.operationId())
                .setParameter("id", stage.assessment()).getSingleResult());
    }
    private static RepositoryException.Code refused(Runnable action) {
        try { action.run(); throw new AssertionError("Expired evidence was delivered"); }
        catch (RepositoryException failure) { return failure.code(); }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
