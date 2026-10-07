package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import java.sql.Connection;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.*;
import javax.sql.DataSource;

/** Holds the actual assessment owner row until PostgreSQL's clock crosses its retention deadline. */
final class HistoricalReconciliationExpiryProbe {
    @FunctionalInterface interface Reconcile { void run() throws Exception; }

    static void run(DataSource database, DocumentAssessmentStartJournal.Started started, Reconcile action) throws Exception {
        try (var workers = Executors.newSingleThreadExecutor(); var blocker = database.getConnection()) {
            blocker.setAutoCommit(false);
            Future<?> pending;
            try {
                int holder;
                try (var statement = blocker.prepareStatement("""
                        SELECT pg_backend_pid(),retain_until>clock_timestamp() FROM document_assessment_owners
                        WHERE assessment_id=? FOR UPDATE
                        """)) {
                    statement.setObject(1, started.assessment());
                    try (var rows = statement.executeQuery()) {
                        require(rows.next() && rows.getBoolean(2), "fixture assessment is live before contention");
                        holder = rows.getInt(1);
                        require(!rows.next(), "exactly one retained assessment");
                    }
                }
                pending = workers.submit(() -> { action.run(); return null; });
                try (var observer = database.getConnection()) {
                    long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (!blocked(observer, holder)) {
                        require(!pending.isDone() && System.nanoTime() < limit,
                                "reconciliation must wait on the held assessment row");
                        Thread.sleep(10);
                    }
                    require(!expired(observer, started), "row contention was observed before expiry");
                    limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
                    while (!expired(observer, started)) {
                        require(!pending.isDone() && System.nanoTime() < limit,
                                "reconciliation must remain blocked until actual expiry");
                        Thread.sleep(20);
                    }
                    require(!pending.isDone(), "worker is still held when assessment expires");
                }
            } finally { blocker.rollback(); }
            try {
                pending.get(10, TimeUnit.SECONDS);
                throw new AssertionError("Reconciliation adopted an assessment after waiting past expiry");
            } catch (ExecutionException failed) {
                var cause = failed.getCause();
                require(cause instanceof IllegalStateException && cause.getMessage().contains("Retained assessment")
                        || cause instanceof RepositoryException error && error.code() == RepositoryException.Code.FAILED_PRECONDITION
                                && error.getMessage().contains("assessment expired"),
                        "fresh expiry verification must refuse the retained stage, not a SQL timeout: " + cause);
            }
        }
    }
    private static boolean blocked(Connection connection, int holder) throws Exception {
        try (var statement = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE ?=ANY(pg_blocking_pids(pid))
                  AND query LIKE '%document_assessment_owners%')
                """)) {
            statement.setInt(1, holder);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static boolean expired(Connection connection, DocumentAssessmentStartJournal.Started started) throws Exception {
        try (var statement = connection.prepareStatement("SELECT clock_timestamp()>=?")) {
            statement.setObject(1, OffsetDateTime.ofInstant(started.retainUntil(), ZoneOffset.UTC));
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
