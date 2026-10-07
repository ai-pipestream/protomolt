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

/** A real origin lock delays finalization until the claim expires; takeover waits for rollback. */
final class HistoricalPublicationClaimExpiryProbe {
    @FunctionalInterface interface Publication { void run(Tx publication) throws Exception; }

    static void run(DataSource database, DocumentPublicationCommand command,
            DocumentAssessmentCreation.Created stage, Tx independent, RepositoryCaller coordinator, RepositoryOperationLedger.Owner owner, RepositorySuccessorInstall.Plan plan, Publication publication) throws Exception {
        var armed = new AtomicBoolean();
        var acquired = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        var publisherPid = new AtomicInteger();
        var claim = owner.executionClaim().orElseThrow();
        var predecessor = new RepositoryCoordinatorDrain.Identity(owner.key(), claim.commandSha256(), claim.epoch(),
                claim.token(), plan.reservation().successorIncarnation());
        var proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(predecessor, UUID.randomUUID(),
                UUID.randomUUID(), Duration.ofMinutes(2),
                new RepositoryCoordinatorReservation.OwnerIdentity(owner.generation(), owner.token()));
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
                Future<?> contender = null;
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
                                if (expired(observer, owner)) throw new AssertionError("Claim expired before origin contention");
                                proceed.countDown();
                                long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                                while (!blocked(observer, publisherPid.get(), holderPid)) {
                                    if (pending.isDone() || System.nanoTime() >= limit)
                                        throw new AssertionError("Publication did not wait on its historical origin");
                                    Thread.sleep(10);
                                }
                                limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                                while (!expired(observer, owner)) {
                                    if (pending.isDone() || System.nanoTime() >= limit)
                                        throw new AssertionError("Publication ended before its claim expired");
                                    Thread.sleep(20);
                                }
                                if (pending.isDone() || !blocked(observer, publisherPid.get(), holderPid))
                                    throw new AssertionError("Publication must remain blocked across claim expiry");
                                if (stageExpired(observer, stage)) throw new AssertionError("Stage expired before claim guard could be tested");
                                contender = workers.submit(() -> RepositoryCoordinatorExpiration.reserve(
                                        independent.withTimeouts(new SqlTimeouts(Duration.ofSeconds(15), Duration.ofSeconds(20))),
                                        coordinator, proposal, RepositoryReadControl.NONE));
                                limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                                while (!takeoverBlocked(observer, publisherPid.get())) {
                                    if (contender.isDone()) contender.get();
                                    if (System.nanoTime() >= limit) throw new AssertionError("V97 did not wait on publisher");
                                    Thread.sleep(10);
                                }
                                if (!blocked(observer, publisherPid.get(), holderPid))
                                    throw new AssertionError("Publisher lost origin lock wait before takeover observation");
                                if (stageExpired(observer, stage)) throw new AssertionError("Assessment deadline must remain live");
                                for (UUID attempt : plan.next().seeds().attempts().values()) {
                                    try (var query = observer.prepareStatement("SELECT lease_until>clock_timestamp() FROM document_part_attempts WHERE attempt_id=?")) {
                                        query.setObject(1, attempt);
                                        try (var rows = query.executeQuery()) {
                                            if (!rows.next() || !rows.getBoolean(1)) throw new AssertionError("Selected upload lease expired before claim check");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    try {
                        pending.get(15, TimeUnit.SECONDS);
                        throw new AssertionError("Expired claim published a result");
                    } catch (ExecutionException failure) {
                        if (!(failure.getCause() instanceof RepositoryExecutionClaimLedger.Fenced))
                            throw new AssertionError("Expected exact claim fence from capture revalidation", failure);
                    }
                    contender.get(10, TimeUnit.SECONDS);
                    if (RepositoryCoordinatorReservation.confirm(independent, coordinator, proposal, RepositoryReadControl.NONE).isEmpty())
                        throw new AssertionError("Takeover did not commit after publisher rollback");
                    long successes = independent.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM repository_operation_success WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    long revisions = independent.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    if (successes != 0 || revisions != 0) throw new AssertionError("Expired publication left durable results");
                    System.out.println("HISTORICAL_PRE_FINALIZATION_CLAIM_EXPIRY_OK");
                } finally { proceed.countDown(); }
            }
        }
    }

    private static final class BoundedWorker implements AutoCloseable {
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final CountDownLatch proceed;
        private final java.util.List<Future<?>> pending = new java.util.ArrayList<>();
        BoundedWorker(CountDownLatch proceed) { this.proceed = proceed; }
        <T> Future<T> submit(Callable<T> task) {
            var result = executor.submit(task); pending.add(result); return result;
        }
        @Override public void close() throws InterruptedException {
            proceed.countDown();
            for (var task : pending) if (!task.isDone()) task.cancel(true);
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
    private static boolean stageExpired(Connection connection, DocumentAssessmentCreation.Created stage) throws Exception {
        try (var query = connection.prepareStatement("SELECT clock_timestamp()>=?")) {
            query.setObject(1, OffsetDateTime.ofInstant(stage.retainUntil(), ZoneOffset.UTC));
            try (var rows = query.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static boolean expired(Connection connection, RepositoryOperationLedger.Owner owner) throws Exception {
        try (var query = connection.prepareStatement("""
                SELECT c.lease_until<=clock_timestamp() AND o.lease_until<=clock_timestamp()
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.account_id=? AND c.principal=? AND c.operation_id=?
                """)) {
            query.setString(1, owner.key().account()); query.setString(2, owner.key().principal()); query.setObject(3, owner.key().operationId());
            try (var rows = query.executeQuery()) { if (!rows.next()) throw new AssertionError("Owner missing"); return rows.getBoolean(1); }
        }
    }
    private static boolean takeoverBlocked(Connection connection, int publisher) throws Exception {
        try (var query = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND state='active'
                AND wait_event_type='Lock' AND ?=ANY(pg_blocking_pids(pid))
                AND query ILIKE '%INSERT INTO repository_coordinator_expirations%')
                """)) {
            query.setInt(1, publisher);
            try (var rows = query.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
}
