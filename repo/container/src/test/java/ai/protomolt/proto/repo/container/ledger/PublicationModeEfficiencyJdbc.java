package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/**
 * Counts real JDBC work (pool acquisitions, commits, rollbacks and every execute call
 * with its SQL text) through a proxied pool, and lets a test block or fail a statement
 * or a commit at an exact point. Every other call delegates unchanged.
 */
final class PublicationModeEfficiencyJdbc implements AutoCloseable {
    record Call(String method, String sql) {
        boolean touches(String table) { return sql != null && sql.contains(table); }
    }
    @FunctionalInterface interface StatementHook { void run(Call call) throws SQLException; }
    @FunctionalInterface interface CommitHook { void run(Connection connection) throws SQLException; }

    private final List<Call> calls = new ArrayList<>();
    private final AtomicInteger acquisitions = new AtomicInteger();
    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicInteger rollbacks = new AtomicInteger();
    private final AtomicReference<StatementHook> afterExecute = new AtomicReference<>(call -> {});
    private final AtomicReference<StatementHook> beforeExecute = new AtomicReference<>(call -> {});
    private final AtomicReference<CommitHook> beforeCommit = new AtomicReference<>(connection -> {});
    private final DataSource source;
    private final EntityManagerFactory factory;
    private final Tx tx;

    PublicationModeEfficiencyJdbc(DataSource delegate) {
        source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = invoke(delegate, method, args);
                    if (!method.getName().equals("getConnection")) return result;
                    acquisitions.incrementAndGet();
                    return connection((Connection) result);
                });
        factory = Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
        tx = new Tx(factory);
        reset(); // Schema validation at factory creation is not part of any measured call.
    }

    Tx tx() { return tx; }
    int acquisitions() { return acquisitions.get(); }
    int commits() { return commits.get(); }
    int rollbacks() { return rollbacks.get(); }
    synchronized List<Call> calls() { return List.copyOf(calls); }
    synchronized List<Call> executes() {
        return calls.stream().filter(call -> call.method().startsWith("execute")).toList();
    }
    void afterExecute(StatementHook hook) { afterExecute.set(hook); }
    void beforeExecute(StatementHook hook) { beforeExecute.set(hook); }
    void beforeCommit(CommitHook hook) { beforeCommit.set(hook); }

    synchronized void reset() {
        calls.clear(); acquisitions.set(0); commits.set(0); rollbacks.set(0);
        afterExecute.set(call -> {}); beforeExecute.set(call -> {}); beforeCommit.set(connection -> {});
    }

    String summary() {
        return "acquisitions=" + acquisitions() + " commits=" + commits() + " rollbacks=" + rollbacks()
                + " executes=" + executes().size() + " " + executes().stream()
                .map(call -> call.method() + ":" + firstWords(call.sql())).toList();
    }

    private static String firstWords(String sql) {
        if (sql == null) return "?";
        String flat = sql.strip().replaceAll("\\s+", " ");
        return flat.length() > 72 ? flat.substring(0, 72) : flat;
    }

    private Connection connection(Connection actual) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("unwrap") && args[0] == Connection.class) return proxy;
                    if (method.getName().equals("isWrapperFor") && args[0] == Connection.class) return true;
                    if (method.getName().equals("commit")) {
                        beforeCommit.get().run(actual);
                        Object result = invoke(actual, method, args);
                        commits.incrementAndGet();
                        return result;
                    }
                    if (method.getName().equals("rollback")) {
                        Object result = invoke(actual, method, args);
                        rollbacks.incrementAndGet();
                        return result;
                    }
                    Object result = invoke(actual, method, args);
                    String sql = method.getName().startsWith("prepare") && args != null && args.length > 0
                            && args[0] instanceof String text ? text : null;
                    if (result instanceof CallableStatement statement) return statement(statement, CallableStatement.class, sql);
                    if (result instanceof PreparedStatement statement) return statement(statement, PreparedStatement.class, sql);
                    if (result instanceof Statement statement) return statement(statement, Statement.class, sql);
                    return result;
                });
    }

    private Object statement(Statement actual, Class<?> type, String prepared) {
        return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
            if (method.getName().equals("unwrap") && ((Class<?>) args[0]).isInstance(proxy)) return proxy;
            if (method.getName().equals("isWrapperFor") && ((Class<?>) args[0]).isInstance(proxy)) return true;
            if (!method.getName().startsWith("execute")) return invoke(actual, method, args);
            String sql = args != null && args.length > 0 && args[0] instanceof String text ? text : prepared;
            var call = new Call(method.getName(), sql);
            synchronized (this) { calls.add(call); }
            beforeExecute.get().run(call);
            Object result = invoke(actual, method, args);
            afterExecute.get().run(call);
            return result;
        });
    }

    private static Object invoke(Object actual, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(actual, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }

    @Override public void close() { factory.close(); }
}
