package ai.protomolt.proto.repo.container.ledger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/** Fault only the transaction that really inserted the requested assessment owner. */
final class HistoricalCreateCommitFault implements AutoCloseable {
    private final AtomicReference<UUID> assessment = new AtomicReference<>();
    private final AtomicBoolean fired = new AtomicBoolean();
    private final jakarta.persistence.EntityManagerFactory factory;
    private final Tx tx;
    private final boolean lostAcknowledgement;

    HistoricalCreateCommitFault(DataSource database, boolean lostAcknowledgement) {
        this.lostAcknowledgement = lostAcknowledgement;
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = invoke(database, method, args);
                    if (!method.getName().equals("getConnection")) return result;
                    var connection = (Connection) result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                            (wrapper, action, arguments) -> {
                                boolean target = action.getName().equals("commit") && !fired.get()
                                        && ownsAssessment(connection, assessment.get());
                                if (target && !fired.compareAndSet(false, true))
                                    throw new AssertionError("Assessment commit fault was entered concurrently");
                                if (target && !lostAcknowledgement)
                                    throw new SQLException("Injected assessment commit rollback", "40001");
                                Object returned = invoke(connection, action, arguments);
                                if (target) throw new SQLException("Injected assessment commit acknowledgement loss", "08006");
                                return returned;
                            });
                });
        factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
        tx = new Tx(factory);
    }

    Tx tx() { return tx; }
    void arm(UUID id) {
        if (!assessment.compareAndSet(null, id)) throw new IllegalStateException("Commit fault already armed");
    }
    boolean lostAcknowledgement() { return lostAcknowledgement; }
    void requireFailure(RuntimeException failure) {
        String state = lostAcknowledgement ? "08006" : "40001";
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql && state.equals(sql.getSQLState()) && fired.get()) return;
        }
        throw new AssertionError("Expected the targeted assessment commit fault", failure);
    }
    private static boolean ownsAssessment(Connection connection, UUID id) throws SQLException {
        if (id == null) return false;
        try (var statement = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM document_assessment_owners
                    WHERE assessment_id=? AND creation_xid=pg_current_xact_id_if_assigned())
                """)) {
            statement.setObject(1, id);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
    @Override public void close() { factory.close(); }
}
