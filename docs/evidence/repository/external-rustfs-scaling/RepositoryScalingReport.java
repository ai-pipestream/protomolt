import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.*;

/**
 * Dependency-free analyzer for raw nativeReplicaBenchmark directories produced with
 * the scaling sampler. Usage:
 *   java RepositoryScalingReport.java <output-dir> <label>=<raw-run-dir>...
 * A bare directory argument is labelled by its directory name, or by its parent's
 * name when the directory itself is called "raw" (the archive layout). Two inputs
 * with identical raw content are rejected. It writes:
 *   windows.csv   one row per measured window
 *   configs.csv   pooled across every window of the same topology, pool, client,
 *                 heap, sampling and journal settings, with the number of distinct runs
 *   workers.csv   per process (PID, measured operations, PUTs, GETs, acquisitions)
 *   warmup.csv    per configuration, mean latency of the first eight and last eight
 *                 measured iterations of every client, by operation kind
 *   trace-sites.csv  when a run was traced: per window, operation, metric and
 *                 completion site counts, nanoseconds and failures
 * Percentiles are nearest-rank over the measured primary operations; each write's
 * receipt replay is excluded from its timed value and is not a row of its own.
 * CPU rates use the window delta (begin to the moment every worker was done);
 * database counters use the settled delta, which spans the 1.2 s statistics settle.
 */
class RepositoryScalingReport {
    /** Every workload setting that distinguishes a configuration; pooled rows share all of them. */
    record Key(int replicas, int pool, int clients, int heap, String journaled, String sampleMillis,
            String payloadBytes, int iterations, String readSlots, String readHandles, String budgetBytes, String trace) {
        static final String HEADER = "journaled,trace,replicas,pool_per_replica,clients_total,heap_mib,sample_millis,payload_bytes,iterations_per_client,total_read_slots,total_read_handles,total_budget_bytes";
        String csv() {
            return journaled + "," + trace + "," + replicas + "," + pool + "," + clients + "," + heap + "," + sampleMillis + "," + payloadBytes + ","
                    + iterations + "," + readSlots + "," + readHandles + "," + budgetBytes;
        }
    }
    static final List<String> KINDS = List.of("read", "publish", "reject");
    static final class Window {
        String run, name; Key key; long operations, nanos;
        final Map<String, List<Long>> latency = new TreeMap<>();
        final Map<String, List<long[]>> byIteration = new TreeMap<>(); // kind -> (iteration, elapsed)
        final Map<String, long[]> metrics = new TreeMap<>();
        final Map<String, Double> window = new TreeMap<>(), settled = new TreeMap<>();
        double statementExecMs; long statementCalls;
        final List<long[]> workers = new ArrayList<>(); // index, pid, measured ops, puts, gets, acquisitions
        long childCpuWindowNanos;
        final List<String> trace = new ArrayList<>();
    }

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.ROOT);
        if (args.length < 2) throw new IllegalArgumentException("Usage: <output-dir> <label>=<raw-run-dir>...");
        Path out = Path.of(args[0]);
        Files.createDirectories(out);
        var windows = new ArrayList<Window>();
        var seen = new HashMap<String, String>();
        String sources = null, sourcesLabel = null;
        for (String argument : Arrays.copyOfRange(args, 1, args.length)) {
            String label; Path root;
            int eq = argument.indexOf('=');
            if (eq > 0) { label = argument.substring(0, eq); root = Path.of(argument.substring(eq + 1)); }
            else {
                root = Path.of(argument).toAbsolutePath().normalize();
                label = root.getFileName().toString().equals("raw") ? root.getParent().getFileName().toString() : root.getFileName().toString();
            }
            String identity = identity(root);
            if (seen.containsKey(identity)) throw new IllegalArgumentException("Duplicate raw input: " + label + " repeats " + seen.get(identity));
            if (seen.containsValue(label)) throw new IllegalArgumentException("Duplicate run label: " + label);
            seen.put(identity, label);
            // Pooled runs must come from the same artifacts, probe, sampler and images.
            String identityText = Files.readString(root.resolve("source-identity.txt"));
            if (sources == null) { sources = identityText; sourcesLabel = label; }
            else if (!sources.equals(identityText)) throw new IllegalArgumentException("source-identity.txt of " + label + " differs from " + sourcesLabel);
            windows.addAll(read(root, label));
        }
        // Tracing adds proxy overhead to every JDBC call; traced and untraced runs are never
        // summarised together, and nothing is written when they are mixed.
        if (windows.stream().map(w -> w.key.trace).distinct().count() > 1)
            throw new IllegalArgumentException("Traced and untraced runs cannot be pooled; analyse them separately");
        writeWindows(out, windows);
        writeConfigs(out, windows);
        writeWorkers(out, windows);
        writeWarmup(out, windows);
        writeTrace(out, windows);
        System.out.println("runs=" + seen.size() + " windows=" + windows.size() + " output=" + out);
    }

    /** Content hash of every file under the raw directory, so no retained file can change unnoticed. */
    static String identity(Path root) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var files = Files.walk(root)) {
            for (var file : files.filter(Files::isRegularFile).sorted().toList()) {
                digest.update(root.relativize(file).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                digest.update(Files.readAllBytes(file));
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static void writeWindows(Path out, List<Window> windows) throws Exception {
        var text = new StringBuilder("run,window," + Key.HEADER + ",operations,inclusive_s,ops_per_s,"
                + "read_p50_ms,read_p95_ms,read_p99_ms,publish_p50_ms,publish_p95_ms,publish_p99_ms,reject_p50_ms,reject_p95_ms,reject_p99_ms,"
                + "sql_connections_total,pool_busy_fraction,sql_acquire_mean_ms,sql_acquire_total_s,sql_acquisitions_per_iteration,"
                + "sql_usage_total_s,statement_exec_total_s,statement_calls,statements_per_iteration,"
                + "db_commits_settled,db_commits_per_iteration,wal_bytes_mib,wal_fsyncs_settled,wal_fsync_time_settled_s,wal_fsync_time_window_s,"
                + "wal_client_backend_fsync_time_settled_s,wal_write_time_settled_s,wal_fsync_share_of_settled_span,wal_fsync_share_of_window,relation_read_time_s,"
                + "provider_put_count,provider_put_mean_ms,provider_get_count,provider_get_mean_ms,"
                + "host_cpus,host_busy_cores,host_iowait_cores,postgres_cores,rustfs_cores,worker_cores,worker_cpu_s,settled_span_s,"
                + "loadavg_begin,loadavg_done\n");
        for (var w : windows) {
            double seconds = w.nanos / 1e9;
            double windowSpan = w.window.get("elapsed_nanos") / 1e9, settledSpan = w.settled.get("elapsed_nanos") / 1e9;
            int connections = w.key.replicas * w.key.pool;
            long[] acquire = w.metrics.get("sql_acquire"), usage = w.metrics.get("sql_usage");
            long[] put = w.metrics.get("provider_put"), get = w.metrics.get("provider_getBounded");
            double commits = w.settled.get("db_xact_commit") - w.settled.get("statements_sampler_calls");
            double jiffies = w.window.get("host_cpu_total_jiffies");
            int cpus = (int) Math.round(w.window.get("host_cpus"));
            text.append(w.run).append(',').append(w.name).append(',').append(w.key.csv()).append(',').append(w.operations)
                    .append(String.format(",%.3f,%.2f", seconds, w.operations / seconds));
            for (String kind : KINDS) text.append(percentiles(w.latency.get(kind), .5, .95, .99));
            text.append(',').append(connections)
                    .append(String.format(",%.3f,%.3f,%.3f,%.2f", usage[1] / 1e9 / seconds / connections, acquire[1] / 1e6 / Math.max(1, acquire[0]),
                            acquire[1] / 1e9, (double) acquire[0] / w.operations))
                    .append(String.format(",%.3f,%.3f,%d,%.2f", usage[1] / 1e9, w.statementExecMs / 1e3, w.statementCalls, (double) w.statementCalls / w.operations))
                    .append(String.format(",%.0f,%.2f,%.2f,%.0f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f", commits, commits / w.operations, w.settled.get("wal_wal_bytes") / 1048576,
                            w.settled.get("walio_fsyncs"), w.settled.get("walio_fsync_time") / 1e3, w.window.get("walio_fsync_time") / 1e3,
                            w.settled.getOrDefault("walio_client_backend_normal_fsync_time", 0.0) / 1e3, w.settled.get("walio_write_time") / 1e3,
                            w.settled.get("walio_fsync_time") / 1e3 / settledSpan, w.window.get("walio_fsync_time") / 1e3 / windowSpan, w.settled.get("relio_read_time") / 1e3))
                    .append(String.format(",%d,%.2f,%d,%.2f", put[0], put[1] / 1e6 / Math.max(1, put[0]), get[0], get[1] / 1e6 / Math.max(1, get[0])))
                    .append(String.format(",%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.3f", cpus, w.window.get("host_cpu_busy_jiffies") / jiffies * cpus,
                            w.window.get("host_cpu_iowait_jiffies") / jiffies * cpus,
                            w.window.get("container_postgres_cpu_usage_usec") / 1e6 / windowSpan,
                            w.window.get("container_rustfs_cpu_usage_usec") / 1e6 / windowSpan,
                            w.childCpuWindowNanos / 1e9 / windowSpan, w.childCpuWindowNanos / 1e9, settledSpan))
                    .append(',').append(w.window.get("loadavg_begin_1m")).append(',').append(w.window.get("loadavg_done_1m")).append('\n');
        }
        Files.writeString(out.resolve("windows.csv"), text);
    }

    static void writeConfigs(Path out, List<Window> windows) throws Exception {
        var configs = new StringBuilder(Key.HEADER + ",runs,windows,operations,"
                + "ops_per_s_min,ops_per_s_mean,ops_per_s_max,read_p50_ms,read_p95_ms,read_p99_ms,publish_p50_ms,publish_p95_ms,publish_p99_ms,"
                + "reject_p50_ms,reject_p95_ms,reject_p99_ms,pool_busy_fraction_mean,sql_acquire_mean_ms,db_commits_per_iteration_mean,"
                + "wal_fsync_share_of_settled_span_mean,wal_fsync_share_of_window_mean,provider_put_mean_ms,postgres_cores_mean,rustfs_cores_mean,worker_cores_mean,host_busy_cores_mean\n");
        var grouped = new TreeMap<String, List<Window>>();
        for (var w : windows) grouped.computeIfAbsent(w.key.csv(), k -> new ArrayList<>()).add(w);
        for (var entry : grouped.entrySet()) {
            var group = entry.getValue();
            var rates = group.stream().mapToDouble(w -> w.operations * 1e9 / w.nanos).sorted().toArray();
            configs.append(entry.getKey()).append(',').append(group.stream().map(w -> w.run).distinct().count()).append(',').append(group.size())
                    .append(',').append(group.stream().mapToLong(w -> w.operations).sum())
                    .append(String.format(",%.2f,%.2f,%.2f", rates[0], Arrays.stream(rates).average().orElseThrow(), rates[rates.length - 1]));
            for (String kind : KINDS) configs.append(percentiles(group.stream().flatMap(w -> w.latency.get(kind).stream()).collect(Collectors.toList()), .5, .95, .99));
            configs.append(String.format(",%.3f,%.3f,%.2f,%.3f,%.3f,%.2f,%.2f,%.2f,%.2f,%.2f",
                    mean(group, w -> w.metrics.get("sql_usage")[1] / 1e9 / (w.nanos / 1e9) / (w.key.replicas * w.key.pool)),
                    mean(group, w -> w.metrics.get("sql_acquire")[1] / 1e6 / Math.max(1, w.metrics.get("sql_acquire")[0])),
                    mean(group, w -> (w.settled.get("db_xact_commit") - w.settled.get("statements_sampler_calls")) / w.operations),
                    mean(group, w -> w.settled.get("walio_fsync_time") / 1e3 / (w.settled.get("elapsed_nanos") / 1e9)),
                    mean(group, w -> w.window.get("walio_fsync_time") / 1e3 / (w.window.get("elapsed_nanos") / 1e9)),
                    mean(group, w -> w.metrics.get("provider_put")[1] / 1e6 / Math.max(1, w.metrics.get("provider_put")[0])),
                    mean(group, w -> w.window.get("container_postgres_cpu_usage_usec") / 1e6 / (w.window.get("elapsed_nanos") / 1e9)),
                    mean(group, w -> w.window.get("container_rustfs_cpu_usage_usec") / 1e6 / (w.window.get("elapsed_nanos") / 1e9)),
                    mean(group, w -> w.childCpuWindowNanos / 1e9 / (w.window.get("elapsed_nanos") / 1e9)),
                    mean(group, w -> w.window.get("host_cpu_busy_jiffies") / w.window.get("host_cpu_total_jiffies") * w.window.get("host_cpus"))))
                    .append('\n');
        }
        Files.writeString(out.resolve("configs.csv"), configs);
    }

    static void writeWorkers(Path out, List<Window> windows) throws Exception {
        var workers = new StringBuilder("run,window,replicas,worker,pid,measured_operations,provider_puts,provider_gets,sql_acquisitions\n");
        for (var w : windows) for (long[] worker : w.workers)
            workers.append(w.run).append(',').append(w.name).append(',').append(w.key.replicas).append(',').append(worker[0]).append(',').append(worker[1])
                    .append(',').append(worker[2]).append(',').append(worker[3]).append(',').append(worker[4]).append(',').append(worker[5]).append('\n');
        Files.writeString(out.resolve("workers.csv"), workers);
    }

    /**
     * Within-window trend: cold-process effects show as a gap between the first and last
     * eight iterations of each client. Needs at least sixteen measured iterations per
     * client so the two buckets do not overlap; shorter runs get no rows.
     */
    static void writeWarmup(Path out, List<Window> windows) throws Exception {
        var text = new StringBuilder(Key.HEADER + ",kind,first8_mean_ms,last8_mean_ms,first8_over_last8\n");
        var grouped = new TreeMap<String, List<Window>>();
        for (var w : windows) if (w.key.iterations >= 16) grouped.computeIfAbsent(w.key.csv(), k -> new ArrayList<>()).add(w);
        for (var entry : grouped.entrySet()) {
            int iterations = entry.getValue().get(0).key.iterations;
            for (String kind : KINDS) {
                var early = new ArrayList<Long>(); var late = new ArrayList<Long>();
                for (var w : entry.getValue()) for (long[] sample : w.byIteration.get(kind)) {
                    if (sample[0] < 8) early.add(sample[1]);
                    else if (sample[0] >= iterations - 8) late.add(sample[1]);
                }
                if (early.isEmpty() || late.isEmpty()) continue;
                double first = early.stream().mapToLong(Long::longValue).average().orElseThrow() / 1e6;
                double last = late.stream().mapToLong(Long::longValue).average().orElseThrow() / 1e6;
                text.append(entry.getKey()).append(',').append(kind).append(String.format(",%.2f,%.2f,%.3f%n", first, last, first / last));
            }
        }
        Files.writeString(out.resolve("warmup.csv"), text);
    }

    static void writeTrace(Path out, List<Window> windows) throws Exception {
        if (windows.stream().noneMatch(w -> w.key.trace.equals("true"))) return;
        var totals = new TreeMap<String, long[]>();
        var text = new StringBuilder("run,window,replicas,pool_per_replica,operation,metric,completion_site,count,nanos,failures\n");
        for (var w : windows) {
            if (w.trace.isEmpty()) throw new IllegalStateException("Traced window without trace rows: " + w.run + " " + w.name);
            for (String line : w.trace) {
                String[] cells = line.split(",");
                var sum = totals.computeIfAbsent(w.run + "," + w.name + "," + w.key.replicas + "," + w.key.pool + "," + cells[0] + "," + cells[1] + "," + cells[2], k -> new long[3]);
                for (int i = 0; i < 3; i++) sum[i] += Long.parseLong(cells[3 + i]);
            }
        }
        totals.forEach((key, values) -> text.append(key).append(',').append(values[0]).append(',').append(values[1]).append(',').append(values[2]).append('\n'));
        Files.writeString(out.resolve("trace-sites.csv"), text);
    }

    static double mean(List<Window> group, java.util.function.ToDoubleFunction<Window> f) { return group.stream().mapToDouble(f).average().orElseThrow(); }

    static String percentiles(List<Long> values, double... ranks) {
        var sorted = new ArrayList<>(values); Collections.sort(sorted);
        var text = new StringBuilder();
        for (double rank : ranks) text.append(String.format(",%.2f", sorted.get((int) Math.ceil(sorted.size() * rank) - 1) / 1e6));
        return text.toString();
    }

    static List<Window> read(Path root, String label) throws Exception {
        var environment = new HashMap<String, String>();
        for (String line : Files.readAllLines(root.resolve("environment.txt"))) {
            int eq = line.indexOf('='); if (eq > 0) environment.put(line.substring(0, eq), line.substring(eq + 1));
        }
        int clients = Integer.parseInt(environment.get("clients"));
        String journaled = environment.get("journaled"), sampleMillis = environment.get("sample_millis");
        if (sampleMillis == null) throw new IllegalStateException("environment.txt has no sample_millis; raw directory predates the scaling sampler: " + root);
        int iterations = Integer.parseInt(environment.get("iterations_per_client"));
        boolean traced = environment.get("trace").equals("true");
        var rows = Files.readAllLines(root.resolve("windows.csv"));
        var plan = rows.subList(1, rows.size());
        if (plan.size() % 2 != 0) throw new IllegalStateException("Odd window count in " + root);
        var processes = new HashMap<String, Long>();
        for (String line : Files.readAllLines(root.resolve("processes.csv"))) {
            String[] cells = line.split(",");
            if (!cells[0].equals("window")) processes.put(cells[0] + "-" + cells[1], Long.parseLong(cells[2]));
        }
        var result = new ArrayList<Window>();
        for (String line : plan) {
            String[] row = line.split(",");
            var w = new Window();
            w.run = label; w.name = row[0];
            int replicas = Integer.parseInt(row[1]);
            w.key = new Key(replicas, Integer.parseInt(row[2]), clients, Integer.parseInt(row[6]), journaled, sampleMillis,
                    environment.get("payload_string_bytes"), iterations, environment.get("total_read_slots"),
                    environment.get("total_read_handles"), environment.get("total_payload_budget_bytes"), environment.get("trace"));
            w.operations = Long.parseLong(row[4]); w.nanos = Long.parseLong(row[5]);
            if (Integer.parseInt(row[3]) * replicas != clients) throw new IllegalStateException("Client split mismatch: " + w.name);
            for (String kind : KINDS) { w.latency.put(kind, new ArrayList<>()); w.byIteration.put(kind, new ArrayList<>()); }
            long count = 0;
            for (int worker = 0; worker < replicas; worker++) {
                String prefix = w.name + "-" + worker;
                long measured = 0;
                for (String operation : Files.readAllLines(root.resolve(prefix + "-operations.csv"))) {
                    String[] cells = operation.split(",");
                    if (!cells[0].equals("measure")) continue;
                    if (!KINDS.contains(cells[3])) throw new IllegalStateException("Unexpected operation kind " + cells[3] + " in " + prefix);
                    w.latency.get(cells[3]).add(Long.parseLong(cells[5]));
                    w.byIteration.get(cells[3]).add(new long[] {Long.parseLong(cells[2]), Long.parseLong(cells[5])});
                    count++; measured++;
                }
                var mine = new TreeMap<String, long[]>();
                for (String metric : Files.readAllLines(root.resolve(prefix + "-measure-metrics.csv"))) {
                    String[] cells = metric.split(",");
                    if (cells[0].equals("metric")) continue;
                    var values = new long[3];
                    for (int i = 0; i < 3; i++) values[i] = Long.parseLong(cells[i + 1]);
                    mine.put(cells[0], values);
                    var total = w.metrics.computeIfAbsent(cells[0], k -> new long[3]);
                    for (int i = 0; i < 3; i++) total[i] += values[i];
                    if (values[2] != 0) throw new IllegalStateException("Recorded failures in " + prefix + " " + cells[0]);
                }
                Long pid = processes.get(prefix);
                if (pid == null) throw new IllegalStateException("No process record for " + prefix);
                w.workers.add(new long[] {worker, pid, measured, mine.get("provider_put")[0], mine.get("provider_getBounded")[0], mine.get("sql_acquire")[0]});
                if (traced) {
                    var lines = Files.readAllLines(root.resolve(prefix + "-measure-trace.csv"));
                    w.trace.addAll(lines.subList(1, lines.size()));
                }
            }
            if (count != w.operations) throw new IllegalStateException("Operation count mismatch: " + w.name + " in " + root);
            for (String kind : KINDS) if (w.latency.get(kind).isEmpty()) throw new IllegalStateException("No " + kind + " samples in " + w.name);
            int children = 0;
            for (String line2 : Files.readAllLines(root.resolve(w.name + "-scaling.csv"))) {
                String[] cells = line2.split(",", -1);
                if (cells[0].equals("metric")) continue;
                if (cells.length != 6) throw new IllegalStateException("Scaling row needs six columns (begin, at_done, at_settled, two deltas): " + w.name);
                if (cells[0].startsWith("loadavg_")) { w.window.put(cells[0] + "_1m", Double.parseDouble(cells[1].split(" ")[0])); continue; }
                if (cells[0].startsWith("child_")) { w.childCpuWindowNanos += Long.parseLong(cells[4]); children++; continue; }
                if (cells[0].equals("host_cpus")) { w.window.put(cells[0], Double.parseDouble(cells[1])); continue; }
                w.window.put(cells[0], Double.parseDouble(cells[4]));
                w.settled.put(cells[0], Double.parseDouble(cells[5]));
            }
            if (children != replicas) throw new IllegalStateException("Expected " + replicas + " child CPU rows in " + w.name + ", found " + children);
            for (String line3 : Files.readAllLines(root.resolve(w.name + "-sql.csv"))) {
                String[] cells = line3.split(",");
                if (cells[0].equals("query_id")) continue;
                w.statementCalls += Long.parseLong(cells[1]); w.statementExecMs += Double.parseDouble(cells[2]);
            }
            var durable = Files.readAllLines(root.resolve(w.name + "-durable-delta.csv")).get(1).split(",");
            long publishes = w.latency.get("publish").size(), rejects = w.latency.get("reject").size();
            if (Long.parseLong(durable[0]) != publishes || Long.parseLong(durable[1]) != publishes || Long.parseLong(durable[2]) != rejects)
                throw new IllegalStateException("Durable delta differs from measured writes in " + w.name);
            result.add(w);
        }
        return result;
    }
}
