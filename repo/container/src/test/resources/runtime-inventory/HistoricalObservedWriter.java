package ai.protomolt.proto.repo.container.ledger;

import java.lang.reflect.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.*;
import javax.sql.DataSource;

/** Records the actual JDBC connection used by one administrative transaction. */
final class HistoricalObservedWriter implements AutoCloseable {
    private final AtomicBoolean armed = new AtomicBoolean();
    final AtomicInteger pid = new AtomicInteger();
    private final jakarta.persistence.EntityManagerFactory factory;
    final Tx tx;

    HistoricalObservedWriter(DataSource database) {
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    Object result;
                    try { result = method.invoke(database, arguments); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                    if (method.getName().equals("getConnection") && armed.get()) {
                        var connection = (Connection) result;
                        try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                            if (!rows.next() || !pid.compareAndSet(0, rows.getInt(1)))
                                throw new AssertionError("Expected exactly one administrative connection");
                        } catch (Throwable failure) {
                            try { connection.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                            throw failure;
                        }
                    }
                    return result;
                });
        factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
        tx = new Tx(factory).withTimeouts(new SqlTimeouts(Duration.ofSeconds(12), Duration.ofSeconds(15)));
    }

    void run(Runnable action) {
        if (pid.get() != 0 || !armed.compareAndSet(false, true)) throw new IllegalStateException("Writer already used");
        try { action.run(); }
        finally { armed.set(false); }
    }
    @Override public void close() { factory.close(); }
}
