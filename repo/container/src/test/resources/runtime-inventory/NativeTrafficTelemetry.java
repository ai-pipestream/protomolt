package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Test-only completed-call counters; every invocation reaches the real provider/pool. */
final class NativeTrafficTelemetry {
    private static final class Counter {
        long count, nanos, failures;
        synchronized void add(long elapsed, boolean failed) { count++; nanos += elapsed; if (failed) failures++; }
        synchronized long[] snapshot() { return new long[]{count, nanos, failures}; }
    }
    private final Map<String,Counter> counters = new ConcurrentHashMap<>();
    private void record(String name, long elapsed, boolean failed) {
        counters.computeIfAbsent(name, ignored -> new Counter()).add(elapsed, failed);
    }
    void attach(javax.sql.DataSource source) {
        if (!(source instanceof HikariDataSource pool)) throw new IllegalArgumentException("Expected actual Hikari pool");
        pool.setMetricsTrackerFactory((name, stats) -> new IMetricsTracker() {
            @Override public void recordConnectionAcquiredNanos(long nanos) { record("sql_acquire", nanos, false); }
            @Override public void recordConnectionUsageMillis(long millis) { record("sql_usage", Math.multiplyExact(millis, 1_000_000), false); }
            @Override public void recordConnectionTimeout() { record("sql_timeout", 0, true); }
        });
    }
    BlobStore wrap(BlobStore actual) {
        return (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class}, (proxy, method, args) -> {
            long start = System.nanoTime(); boolean failed = true;
            try { Object result = method.invoke(actual, args); failed = false; return result; }
            catch (InvocationTargetException failure) { throw failure.getCause(); }
            finally { if (method.getDeclaringClass() != Object.class) record("provider_" + method.getName(), System.nanoTime() - start, failed); }
        });
    }
    Map<String,long[]> snapshot() {
        var result = new TreeMap<String,long[]>(); counters.forEach((key, counter) -> result.put(key, counter.snapshot())); return result;
    }
    void writeDelta(Path path, Map<String,long[]> before) throws java.io.IOException {
        var text = new StringBuilder("metric,count,nanos,failures\n");
        snapshot().forEach((key, values) -> {
            var old = before.getOrDefault(key, new long[3]);
            text.append(key);
            for (int i = 0; i < values.length; i++) {
                if (values[i] < old[i]) throw new IllegalStateException("Telemetry counter moved backwards");
                text.append(',').append(values[i] - old[i]);
            }
            text.append('\n');
        });
        Files.writeString(path, text, StandardOpenOption.CREATE_NEW);
    }
}
