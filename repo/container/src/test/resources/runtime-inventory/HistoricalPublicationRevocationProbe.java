package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

/** Real credential revocation after promotion, before publication SQL obtains its locks. */
final class HistoricalPublicationRevocationProbe implements AutoCloseable {
    private final AtomicBoolean armed = new AtomicBoolean();
    private final AtomicInteger calls = new AtomicInteger();
    private final jakarta.persistence.EntityManagerFactory factory;
    private final Tx tx;

    HistoricalPublicationRevocationProbe(DataSource database, Tx independent, RepositoryCaller caller) {
        var credential = caller.credentialBinding().orElseThrow();
        if (caller.processAuthority()) throw new IllegalArgumentException("Fixture requires a scoped caller");
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = invoke(database, method, args);
                    if (!method.getName().equals("getConnection") || !armed.compareAndSet(true, false)) return result;
                    var connection = (Connection) result;
                    try {
                        calls.incrementAndGet();
                        new RepositoryCredentialAuthorities(independent.withTimeouts(
                                new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(10))))
                                .revoke(new RepositoryCaller("operator", true), credential, caller.principalName());
                        return connection;
                    } catch (Throwable failure) {
                        try { connection.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                        throw failure;
                    }
                });
        factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
        tx = new Tx(factory);
    }
    void arm() {
        if (calls.get() != 0 || !armed.compareAndSet(false, true)) throw new IllegalStateException("Revocation already armed");
    }
    Tx tx() { return tx; }
    void requireFired() {
        if (calls.get() != 1 || armed.get()) throw new AssertionError("Expected one publication-boundary revocation");
    }
    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
    @Override public void close() { factory.close(); }
}
