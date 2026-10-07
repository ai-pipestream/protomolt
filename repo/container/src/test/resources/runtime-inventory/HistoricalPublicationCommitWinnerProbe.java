package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import java.lang.reflect.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.sql.DataSource;

/** A real publication keeps its fencing locks after finalization, through JDBC commit. */
final class HistoricalPublicationCommitWinnerProbe {
    @FunctionalInterface interface Publication { DocumentPublicationResult run(Tx tx) throws Exception; }

    static DocumentPublicationResult run(DataSource database, Tx independent, RepositoryCaller coordinator,
            RepositoryOperationLedger.Owner owner, RepositorySuccessorInstall.Plan plan,
            Publication publication) throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var fired = new AtomicBoolean();
        var pid = new AtomicInteger();
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object value = invoke(database, method, args);
                    if (!method.getName().equals("getConnection")) return value;
                    var connection = (Connection) value;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                            (wrapper, action, arguments) -> {
                                if (action.getName().equals("commit") && !fired.get() && ownsPublication(connection, owner)) {
                                    require(fired.compareAndSet(false, true), "publication gate entered once");
                                    try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT pg_backend_pid()")) {
                                        rows.next(); pid.set(rows.getInt(1));
                                    }
                                    entered.countDown();
                                    if (!release.await(50, TimeUnit.SECONDS)) throw new AssertionError("publication commit gate timed out");
                                }
                                return invoke(connection, action, arguments);
                            });
                });
        try (var factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
            var workers = Executors.newVirtualThreadPerTaskExecutor();
            Future<DocumentPublicationResult> publisher = null;
            Future<?> contender = null;
            Throwable primary = null;
            try {
                publisher = workers.submit(() -> publication.run(new Tx(factory)));
                require(entered.await(10, TimeUnit.SECONDS), "real publication reached JDBC commit after finalization");
                var claim = owner.executionClaim().orElseThrow();
                var predecessor = new RepositoryCoordinatorDrain.Identity(owner.key(), claim.commandSha256(),
                        claim.epoch(), claim.token(), plan.reservation().successorIncarnation());
                var proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(predecessor, UUID.randomUUID(),
                        UUID.randomUUID(), Duration.ofMinutes(2),
                        new RepositoryCoordinatorReservation.OwnerIdentity(owner.generation(), owner.token()));
                try (var observer = database.getConnection()) {
                    require(!expired(observer, owner), "publication finalized before lease expiry");
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
                    while (!expired(observer, owner)) {
                        require(!publisher.isDone() && System.nanoTime() < until, "publication remains gated until database lease expiry");
                        Thread.sleep(10);
                    }
                    require(count(observer, "repository_operation_success", owner) == 0, "uncommitted success invisible to observer");
                    long reservations = count(observer, "repository_coordinator_expirations", owner);
                    contender = workers.submit(() -> RepositoryCoordinatorExpiration.reserve(
                            independent.withTimeouts(new SqlTimeouts(Duration.ofSeconds(15), Duration.ofSeconds(20))),
                            coordinator, proposal, RepositoryReadControl.NONE));
                    until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (!blocked(observer, pid.get())) {
                        if (contender.isDone()) contender.get();
                        require(System.nanoTime() < until, "V97 did not wait on exact publisher PID");
                        Thread.sleep(10);
                    }
                    require(!publisher.isDone(), "publisher owns locks while takeover waits");
                    release.countDown();
                    var result = publisher.get(10, TimeUnit.SECONDS);
                    try {
                        contender.get(10, TimeUnit.SECONDS);
                        throw new AssertionError("Takeover replaced a committed operation");
                    } catch (ExecutionException failure) {
                        require(hasMessage(failure, "Terminal operation cannot reserve expiration"),
                                "takeover reports the exact terminal SQL rejection: " + failure);
                    }
                    require(count(observer, "repository_operation_success", owner) == 1, "one committed publication result");
                    require(count(observer, "repository_coordinator_expirations", owner) == reservations, "no takeover reservation committed");
                    require(count(observer, "document_revision_commits", owner) == result.getMembersCount(), "only publisher revisions committed");
                    System.out.println("HISTORICAL_POST_FINALIZATION_PUBLICATION_WINS_OK");
                    return result;
                }
            } catch (Exception | Error failure) { primary = failure; throw failure; }
            finally {
                release.countDown();
                if (publisher != null && !publisher.isDone()) publisher.cancel(true);
                if (contender != null && !contender.isDone()) contender.cancel(true);
                workers.shutdownNow();
                try {
                    require(workers.awaitTermination(15, TimeUnit.SECONDS), "transaction workers stopped before factory close");
                } catch (Exception | Error cleanup) {
                    if (primary == null) throw cleanup;
                    if (primary != cleanup) primary.addSuppressed(cleanup);
                }
            }
        }
    }

    private static boolean ownsPublication(Connection connection, RepositoryOperationLedger.Owner owner) throws Exception {
        try (var query = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=? AND principal=?
                  AND operation_id=? AND owner_generation=? AND creation_xid=pg_current_xact_id_if_assigned())
                """)) {
            query.setString(1, owner.key().account()); query.setString(2, owner.key().principal());
            query.setObject(3, owner.key().operationId()); query.setLong(4, owner.generation());
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
            try (var rows = query.executeQuery()) { require(rows.next(), "claim and owner exist"); return rows.getBoolean(1); }
        }
    }
    private static boolean blocked(Connection connection, int publisher) throws Exception {
        try (var query = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND state='active'
                AND wait_event_type='Lock' AND ?=ANY(pg_blocking_pids(pid))
                AND query ILIKE '%INSERT INTO repository_coordinator_expirations%')
                """)) {
            query.setInt(1, publisher);
            try (var rows = query.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static long count(Connection connection, String table, RepositoryOperationLedger.Owner owner) throws Exception {
        try (var query = connection.prepareStatement("SELECT count(*) FROM " + table + " WHERE account_id=? AND principal=? AND operation_id=?")) {
            query.setString(1, owner.key().account()); query.setString(2, owner.key().principal()); query.setObject(3, owner.key().operationId());
            try (var rows = query.executeQuery()) { rows.next(); return rows.getLong(1); }
        }
    }
    private static boolean hasMessage(Throwable failure, String expected) {
        for (var cause = failure; cause != null; cause = cause.getCause())
            if (cause.getMessage() != null && cause.getMessage().contains(expected)) return true;
        return false;
    }
    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
