package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.*;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Test-only cumulative completed-call telemetry; every operation delegates to the real provider. */
final class ReplicaMetrics {
    private static final class Counter {
        private long count, nanos, failures;
        synchronized void record(long elapsed, boolean failed) { count++; nanos += elapsed; if (failed) failures++; }
        synchronized String row(String name) { return name + "," + count + "," + nanos + "," + failures + "\n"; }
    }
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final LongAdder timeouts = new LongAdder();
    private HikariDataSource pool;

    BlobStores providers() {
        var wrapped = new ArrayList<BlobStoreProvider>();
        for (var delegate : ServiceLoader.load(BlobStoreProvider.class)) wrapped.add(new BlobStoreProvider() {
            @Override public String id() { return delegate.id(); }
            @Override public BackendIdentity managedIdentity(Map<String, String> options) { return delegate.managedIdentity(options); }
            @Override public OpenedBlobStore open(Map<String, String> options) {
                var opened = delegate.open(options);
                try {
                    var store = opened.store();
                    var measured = (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class},
                            (proxy, method, args) -> {
                                if (method.getDeclaringClass() == Object.class) return method.invoke(store, args);
                                long start = System.nanoTime(); boolean failed = true;
                                try { Object result = method.invoke(store, args); failed = false; return result; }
                                catch (InvocationTargetException failure) { throw failure.getCause(); }
                                finally { record("provider_" + id() + "_" + method.getName(), System.nanoTime() - start, failed); }
                            });
                    // Preserve the opened handle's provisioning, reclamation and lifetime.
                    return new OpenedBlobStore(measured, opened, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
                } catch (RuntimeException | Error failure) {
                    try { opened.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                    throw failure;
                }
            }
        });
        return BlobStores.of(wrapped);
    }

    void attach(javax.sql.DataSource source) {
        if (!(source instanceof HikariDataSource actual)) throw new IllegalArgumentException("Benchmark requires the actual Hikari datasource");
        pool = actual;
        pool.setMetricsTrackerFactory((name, stats) -> new IMetricsTracker() {
            @Override public void recordConnectionAcquiredNanos(long nanos) { record("sql_acquire", nanos, false); }
            @Override public void recordConnectionUsageMillis(long millis) { record("sql_usage", Math.multiplyExact(millis, 1_000_000), false); }
            @Override public void recordConnectionTimeout() { timeouts.increment(); }
        });
    }

    private void record(String name, long nanos, boolean failed) {
        counters.computeIfAbsent(name, ignored -> new Counter()).record(nanos, failed);
    }

    /** Reply only after the parent asks; atomic replacement prevents partial snapshots. */
    void respond(Path ready) throws Exception {
        Path request = Path.of(ready + ".metrics-request");
        if (!Files.exists(request)) return;
        String id = Files.readString(request).trim();
        if (!id.matches("[0-9]+")) throw new IllegalArgumentException("Invalid metrics request identity");
        Files.delete(request);
        var result = new StringBuilder("request,").append(id).append("\nmetric,count,total_nanos,failures\n");
        new TreeMap<>(counters).forEach((name, counter) -> result.append(counter.row(name)));
        var view = pool.getHikariPoolMXBean();
        result.append("sql_timeouts,").append(timeouts.sum()).append(",0,0\n")
                .append("pool_total,").append(view.getTotalConnections()).append(",0,0\n")
                .append("pool_active,").append(view.getActiveConnections()).append(",0,0\n")
                .append("pool_waiting,").append(view.getThreadsAwaitingConnection()).append(",0,0\n");
        Path reply = Path.of(ready + ".metrics"), pending = Path.of(reply + ".pending");
        Files.writeString(pending, result);
        Files.move(pending, reply, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
