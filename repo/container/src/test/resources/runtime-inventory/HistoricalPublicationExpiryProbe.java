package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

/** Hold a real origin after promotion and prove that expiry rolls back publication. */
final class HistoricalPublicationExpiryProbe {
    @FunctionalInterface interface Publication { void run(Tx publication) throws Exception; }

    static void run(DataSource database, DocumentPublicationCommand command,
            DocumentAssessmentCreation.Created stage, Publication publication) throws Exception {
        var armed = new AtomicBoolean();
        var acquired = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        var publisherPid = new AtomicInteger();
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = invoke(database, method, args);
                    if (!method.getName().equals("getConnection") || !armed.compareAndSet(true, false)) return result;
                    var connection = (Connection) result;
                    try {
                        try (var query = connection.prepareStatement("SELECT pg_backend_pid()"); var rows = query.executeQuery()) {
                            if (!rows.next()) throw new AssertionError("Publication connection has no backend PID");
                            publisherPid.set(rows.getInt(1));
                        }
                        acquired.countDown();
                        if (!proceed.await(15, TimeUnit.SECONDS)) throw new AssertionError("Publication connection gate timed out");
                        return connection;
                    } catch (Throwable failure) {
                        try { connection.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                        throw failure;
                    }
                });
        try (var factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
            var tx = new Tx(factory).withTimeouts(new SqlTimeouts(Duration.ofSeconds(35), Duration.ofSeconds(45)));
            UUID object = command.intent().getMembersList().stream().flatMap(member -> member.getPartsList().stream())
                    .filter(part -> part.hasHistoricalReuse()).map(part -> UUID.fromString(part.getHistoricalReuse().getObject().getObjectId()))
                    .findFirst().orElseThrow();
            try (var workers = new BoundedWorker(proceed)) {
                armed.set(true);
                var pending = workers.submit(() -> { publication.run(tx); return null; });
                try {
                    if (!acquired.await(15, TimeUnit.SECONDS)) throw new AssertionError("Publication did not reach its commit connection");
                    try (var blocker = database.getConnection()) {
                        blocker.setAutoCommit(false);
                        try (AutoCloseable rollback = blocker::rollback) {
                            UUID origin;
                            try (var query = blocker.prepareStatement("SELECT source_id FROM repository_physical_locations WHERE object_id=? AND source_kind='DOCUMENT_PART'")) {
                                query.setObject(1, object);
                                try (var rows = query.executeQuery()) {
                                    if (!rows.next()) throw new AssertionError("Historical origin is missing");
                                    origin = rows.getObject(1, UUID.class);
                                }
                            }
                            int holderPid;
                            try (var query = blocker.prepareStatement("SELECT pg_backend_pid() FROM document_part_attempts WHERE attempt_id=? FOR UPDATE")) {
                                query.setObject(1, origin);
                                try (var rows = query.executeQuery()) {
                                    if (!rows.next()) throw new AssertionError("Origin attempt is missing");
                                    holderPid = rows.getInt(1);
                                }
                            }
                            try (var observer = database.getConnection()) {
                                if (expired(observer, stage)) throw new AssertionError("Fixture expired before origin contention");
                                proceed.countDown();
                                long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                                while (!blocked(observer, publisherPid.get(), holderPid)) {
                                    if (pending.isDone() || System.nanoTime() >= limit)
                                        throw new AssertionError("Publication did not wait on its historical origin");
                                    Thread.sleep(10);
                                }
                                limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                                while (!expired(observer, stage)) {
                                    if (pending.isDone() || System.nanoTime() >= limit)
                                        throw new AssertionError("Publication ended before its retained assessment expired");
                                    Thread.sleep(20);
                                }
                                if (pending.isDone() || !blocked(observer, publisherPid.get(), holderPid))
                                    throw new AssertionError("Publication must remain blocked across assessment expiry");
                            }
                        }
                    }
                    try {
                        pending.get(15, TimeUnit.SECONDS);
                        throw new AssertionError("Expired assessment was published");
                    } catch (ExecutionException failure) {
                        if (!(failure.getCause() instanceof RepositoryException refused)
                                || refused.code() != RepositoryException.Code.FAILED_PRECONDITION
                                || !"Publication assessment is no longer live".equals(refused.getMessage()))
                            throw new AssertionError("Expected final publication expiry fence", failure);
                    }
                } finally { proceed.countDown(); }
            }
        }
    }

    private static final class BoundedWorker implements AutoCloseable {
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final CountDownLatch proceed;
        private Future<?> pending;
        BoundedWorker(CountDownLatch proceed) { this.proceed = proceed; }
        <T> Future<T> submit(Callable<T> task) {
            var result = executor.submit(task); pending = result; return result;
        }
        @Override public void close() throws InterruptedException {
            proceed.countDown();
            if (pending != null && !pending.isDone()) pending.cancel(true);
            executor.shutdownNow();
            if (!executor.awaitTermination(15, TimeUnit.SECONDS))
                throw new AssertionError("Publication worker did not terminate after cancellation");
        }
    }

    private static boolean blocked(Connection connection, int publisher, int holder) throws Exception {
        try (var query = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=? AND ?=ANY(pg_blocking_pids(pid))
                AND wait_event_type='Lock' AND query LIKE '%document_part_attempts%ORDER BY attempt_id FOR UPDATE%')
                """)) {
            query.setInt(1, publisher); query.setInt(2, holder);
            try (var rows = query.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static boolean expired(Connection connection, DocumentAssessmentCreation.Created stage) throws Exception {
        try (var query = connection.prepareStatement("SELECT clock_timestamp()>=?")) {
            query.setObject(1, OffsetDateTime.ofInstant(stage.retainUntil(), ZoneOffset.UTC));
            try (var rows = query.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
}
