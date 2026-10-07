package ai.protomolt.proto.repo.container.ledger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

/** Holds a real commit before and after durability, without changing its result. */
final class HistoricalAuthorizationCommitGate implements AutoCloseable {
    enum Phase { SCHEMA_STAGE, ASSESSMENT_CREATE }
    private final Phase phase;
    private final AtomicBoolean fired = new AtomicBoolean();
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch allowCommit = new CountDownLatch(1);
    private final CountDownLatch committed = new CountDownLatch(1);
    private final CountDownLatch allowReturn = new CountDownLatch(1);
    private final jakarta.persistence.EntityManagerFactory factory;
    private final Tx tx;
    private volatile RepositoryOperationLedger.Owner owner;
    private volatile UUID assessment;
    private volatile int writerPid;

    HistoricalAuthorizationCommitGate(DataSource database, Phase phase) {
        this.phase = phase;
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = invoke(database, method, args);
                    if (!method.getName().equals("getConnection")) return result;
                    var connection = (Connection) result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                            (wrapper, action, arguments) -> {
                                boolean target = action.getName().equals("commit") && !fired.get() && matches(connection);
                                if (target) {
                                    if (!fired.compareAndSet(false, true)) throw new AssertionError("Concurrent targeted commit");
                                    try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                                        rows.next(); writerPid = rows.getInt(1);
                                    }
                                    entered.countDown();
                                    awaitJdbc(allowCommit, "allow real commit");
                                }
                                Object returned = invoke(connection, action, arguments);
                                if (target) {
                                    committed.countDown();
                                    awaitJdbc(allowReturn, "allow committed result return");
                                }
                                return returned;
                            });
                });
        factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
        tx = new Tx(factory);
    }
    Tx tx() { return tx; }
    void arm(RepositoryOperationLedger.Owner owner, UUID assessment) {
        if (this.owner != null) throw new IllegalStateException("Commit gate already armed");
        this.assessment = assessment;
        this.owner = owner;
    }
    int awaitWriter() throws InterruptedException { await(entered, "target commit"); return writerPid; }
    void commit() { allowCommit.countDown(); }
    void awaitCommitted() throws InterruptedException { await(committed, "durable commit"); }
    void release() { allowCommit.countDown(); allowReturn.countDown(); }
    private boolean matches(Connection connection) throws SQLException {
        var bound = owner;
        if (bound == null) return false;
        String sql = phase == Phase.ASSESSMENT_CREATE ? """
                SELECT EXISTS(SELECT 1 FROM document_assessment_owners
                WHERE assessment_id=? AND creation_xid=pg_current_xact_id_if_assigned())
                """ : """
                SELECT EXISTS(SELECT 1 FROM repository_schema_artifact_claims
                WHERE account_id=? AND principal=? AND operation_id=? AND owner_generation=?)
                """;
        try (var statement = connection.prepareStatement(sql)) {
            if (phase == Phase.ASSESSMENT_CREATE) statement.setObject(1, assessment);
            else {
                statement.setString(1, bound.key().account()); statement.setString(2, bound.key().principal());
                statement.setObject(3, bound.key().operationId()); statement.setLong(4, bound.generation());
            }
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static void await(CountDownLatch gate, String event) throws InterruptedException {
        if (!gate.await(20, TimeUnit.SECONDS)) throw new AssertionError("Timed out waiting to " + event);
    }
    private static void awaitJdbc(CountDownLatch gate, String event) throws SQLException {
        try { await(gate, event); }
        catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting to " + event, "HY008", failure);
        }
    }
    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
    @Override public void close() { release(); factory.close(); }
}
