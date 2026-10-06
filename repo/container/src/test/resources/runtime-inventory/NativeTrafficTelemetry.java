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
    record TraceKey(String operation, String metric, String completionSite) {}
    private final Map<TraceKey,Counter> trace = new ConcurrentHashMap<>();
    private final ThreadLocal<String> operation = new ThreadLocal<>();
    private final Scope inactiveScope = new Scope();
    private final boolean tracing;
    NativeTrafficTelemetry(boolean tracing) { this.tracing = tracing; }
    final class Scope implements AutoCloseable {
        private final Thread owner;
        private boolean closed;
        private Scope() { owner = null; }
        private Scope(String name) {
            owner = Thread.currentThread();
            if (operation.get() != null) throw new IllegalStateException("Nested traffic operation");
            operation.set(name);
        }
        @Override public void close() {
            if (owner == null) return; // Disabled tracing has no per-operation state or allocation.
            if (closed || Thread.currentThread() != owner) throw new IllegalStateException("Invalid traffic scope close");
            closed = true;
            operation.remove();
        }
    }
    Scope operation(String name) {
        if (!tracing) return inactiveScope;
        if (!Set.of("read", "publish", "reject", "replay").contains(name)) throw new IllegalArgumentException(name);
        return new Scope(name);
    }
    private void record(String name, long elapsed, boolean failed) {
        if (tracing) {
            String site = StackWalker.getInstance().walk(frames -> frames
                    .filter(f -> f.getClassName().startsWith("ai.protomolt.proto.repo.container.ledger."))
                    .filter(f -> !f.getClassName().startsWith("ai.protomolt.proto.repo.container.ledger.Native"))
                    .filter(f -> !f.getClassName().equals(Tx.class.getName()))
                    .findFirst().map(f -> f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1)
                            + "." + f.getMethodName()).orElse("outside_ledger"));
            String label = operation.get();
            synchronized (this) {
                counters.computeIfAbsent(name, ignored -> new Counter()).add(elapsed, failed);
                trace.computeIfAbsent(new TraceKey(label == null ? "unscoped" : label, name, site), ignored -> new Counter())
                        .add(elapsed, failed);
            }
        } else counters.computeIfAbsent(name, ignored -> new Counter()).add(elapsed, failed);
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
    record Snapshot(Map<String,long[]> metrics, Map<TraceKey,long[]> trace) {}
    synchronized Snapshot snapshot() {
        var metrics = new TreeMap<String,long[]>(); counters.forEach((key, counter) -> metrics.put(key, counter.snapshot()));
        var detail = new HashMap<TraceKey,long[]>(); trace.forEach((key, value) -> detail.put(key, value.snapshot()));
        if (tracing) {
            var totals = new TreeMap<String,long[]>();
            detail.forEach((key, values) -> {
                var sum = totals.computeIfAbsent(key.metric(), ignored -> new long[3]);
                for (int i = 0; i < 3; i++) sum[i] += values[i];
            });
            if (!metrics.keySet().equals(totals.keySet())) throw new AssertionError("Trace metrics differ");
            metrics.forEach((key, values) -> {
                if (!Arrays.equals(values, totals.get(key))) throw new AssertionError("Trace totals differ: " + key);
            });
        }
        return new Snapshot(metrics, detail);
    }
    private void writeTraceDelta(Path path, Map<TraceKey,long[]> before, Map<TraceKey,long[]> after) throws java.io.IOException {
        if (!tracing) throw new IllegalStateException("Tracing is disabled");
        var text = new StringBuilder("operation,metric,completion_site,count,nanos,failures\n");
        after.keySet().stream().sorted(Comparator.comparing(TraceKey::operation).thenComparing(TraceKey::metric)
                .thenComparing(TraceKey::completionSite)).forEach(key -> {
            var values = after.get(key); var old = before.getOrDefault(key, new long[3]);
            if (values[0] == old[0]) return;
            text.append(key.operation()).append(',').append(key.metric()).append(',').append(key.completionSite());
            for (int i = 0; i < values.length; i++) {
                if (values[i] < old[i]) throw new IllegalStateException("Trace counter moved backwards");
                text.append(',').append(values[i] - old[i]);
            }
            text.append('\n');
        });
        Files.writeString(path, text, StandardOpenOption.CREATE_NEW);
    }
    void writeDelta(Path root, String prefix, Snapshot before) throws java.io.IOException {
        var after = snapshot();
        var text = new StringBuilder("metric,count,nanos,failures\n");
        after.metrics().forEach((key, values) -> {
            var old = before.metrics().getOrDefault(key, new long[3]);
            text.append(key);
            for (int i = 0; i < values.length; i++) {
                if (values[i] < old[i]) throw new IllegalStateException("Telemetry counter moved backwards");
                text.append(',').append(values[i] - old[i]);
            }
            text.append('\n');
        });
        Files.writeString(root.resolve(prefix + "-metrics.csv"), text, StandardOpenOption.CREATE_NEW);
        if (tracing) writeTraceDelta(root.resolve(prefix + "-trace.csv"), before.trace(), after.trace());
    }
}
