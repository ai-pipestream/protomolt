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
        long started = System.nanoTime();
        var existing = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var incoming = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var budget = new PayloadBudget(128_000_000);
        var receipt = new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow();
        progress("REJECTED_EXPIRY_RECEIPT_LOADED", started);
        try (var captured = existing.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE)) {
            progress("REJECTED_EXPIRY_EXISTING_CAPTURED", started);
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
                    progress("REJECTED_EXPIRY_OWNER_ROW_LOCKED", started);
                    capture = workers.submit(() -> refused(() -> {
                        try (var unexpected = incoming.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE)) {
                            throw new AssertionError("Expired capture returned a handle");
                        }
                    }));
                    delivery = workers.submit(() -> refused(() -> {
                        try (var use = captured.use()) { captured.authorizeDelivery(use, RepositoryReadControl.NONE); }
                    }));
                    progress("REJECTED_EXPIRY_WAIT_READERS_BLOCKING_START", started);
                    long stop = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                    while (tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))")
                            .setParameter("blocker", pid).getSingleResult()).intValue()) < 2) {
                        require(System.nanoTime() < stop, "both reads reached the real assessment row lock");
                        Thread.sleep(20);
                    }
                    progress("REJECTED_EXPIRY_READERS_BLOCKED", started);
                    progress("REJECTED_EXPIRY_WAIT_RETENTION_START", started);
                    stop = System.nanoTime() + Duration.ofSeconds(25).toNanos();
                    while (!tx.readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT retain_until<=clock_timestamp() FROM document_assessment_owners WHERE assessment_id=:id")
                            .setParameter("id", stage.assessment()).getSingleResult())) {
                        require(System.nanoTime() < stop, "retention expires within fixture bound");
                        Thread.sleep(50);
                    }
                    progress("REJECTED_EXPIRY_RETENTION_REACHED", started);
                    blocker.commit();
                    progress("REJECTED_EXPIRY_BLOCKER_COMMITTED", started);
                }
                progress("REJECTED_EXPIRY_CAPTURE_RESULT_START", started);
                require(capture.get(5, TimeUnit.SECONDS) == RepositoryException.Code.FAILED_PRECONDITION, "capture refuses expiry after lock wait");
                progress("REJECTED_EXPIRY_CAPTURE_REFUSED", started);
                require(delivery.get(5, TimeUnit.SECONDS) == RepositoryException.Code.FAILED_PRECONDITION, "delivery refuses expiry after lock wait");
                progress("REJECTED_EXPIRY_DELIVERY_REFUSED", started);
            }
            require(incoming.outstandingReads() == 0, "expired capture consumes no capacity");
            progress("REJECTED_EXPIRY_ACTIVE_SESSION_GUARD_START", started);
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
            progress("REJECTED_EXPIRY_ACTIVE_SESSION_GUARD", started);
        }
        require(existing.releaseDrained(1) == 1 && existing.outstandingReads() == 0, "expired delivery session drains");
        progress("REJECTED_EXPIRY_SESSION_DRAINED", started);
        require(release(tx, command, stage), "expired evidence releases after drain");
        progress("REJECTED_EXPIRY_EVIDENCE_RELEASED", started);
        require(refused(() -> incoming.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE))
                == RepositoryException.Code.FAILED_PRECONDITION, "pruned evidence is unavailable");
        require(new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow().equals(receipt),
                "receipt replay survives evidence expiry and cleanup");
        require(budget.reservedBytes() == 0 && incoming.outstandingReads() == 0, "expiry leaves no read resources");
        System.out.println("REJECTED_ASSESSMENT_EXPIRY_WAITS_OK");
        progress("REJECTED_EXPIRY_COMPLETE", started);
    }

    private static void progress(String phase, long started) {
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        System.out.println(phase + "_" + elapsedMillis + "MS_OK");
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
