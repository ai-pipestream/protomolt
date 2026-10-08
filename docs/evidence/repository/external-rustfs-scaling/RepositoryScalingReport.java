import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/**
 * Dependency-free analyzer for raw nativeReplicaBenchmark directories produced on or
 * after the scaling-sampler change. Usage:
 *   java RepositoryScalingReport.java <output-dir> <raw-run-dir>...
 * It writes windows.csv (one row per measured window), configs.csv (pooled across
 * every window of the same topology, pool, client and heap settings), workers.csv
 * (per-process work, proving each JVM received traffic) and prints a short check log.
 * Percentiles are nearest-rank over measured primary operations; replay latency is the
 * receipt replay that follows each write is excluded from the timed value and is
 * not recorded as its own row.
 */
class RepositoryScalingReport {
    record Key(int replicas, int pool, int clients, int heap, String journaled) {}
    static final List<String> KINDS = List.of("read", "publish", "reject");
    static final class Window {
        String run, name; Key key; long operations, nanos;
        final Map<String, List<Long>> latency = new TreeMap<>();
        final Map<String, long[]> metrics = new TreeMap<>();
        final Map<String, Double> scaling = new TreeMap<>();
        double statementExecMs; long statementCalls;
        final List<long[]> workers = new ArrayList<>(); // index, measured ops, puts, gets, acquisitions
        long childCpuNanos;
    }

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.ROOT);
        if (args.length < 2) throw new IllegalArgumentException("Usage: <output-dir> <raw-run-dir>...");
        Path out = Path.of(args[0]);
        Files.createDirectories(out);
        var windows = new ArrayList<Window>();
        for (String argument : Arrays.copyOfRange(args, 1, args.length)) windows.addAll(read(Path.of(argument)));
        var text = new StringBuilder("run,window,journaled,replicas,pool_per_replica,clients_total,heap_mib,operations,inclusive_s,ops_per_s,"
                + "read_p50_ms,read_p95_ms,read_p99_ms,publish_p50_ms,publish_p95_ms,publish_p99_ms,reject_p50_ms,reject_p95_ms,reject_p99_ms,"
                + "sql_connections_total,pool_busy_fraction,sql_acquire_mean_ms,sql_acquire_total_s,sql_acquisitions_per_iteration,"
                + "sql_usage_total_s,statement_exec_total_s,statement_calls,statements_per_iteration,"
                + "db_commits,db_commits_per_iteration,wal_bytes_mib,wal_fsyncs,wal_fsync_time_s,wal_write_time_s,relation_read_time_s,"
                + "provider_put_count,provider_put_mean_ms,provider_get_count,provider_get_mean_ms,"
                + "host_cpus,host_busy_cores,host_iowait_cores,postgres_cores,rustfs_cores,worker_cores,worker_cpu_s,sampled_span_s,"
                + "loadavg_begin,loadavg_end\n");
        for (var w : windows) {
            double seconds = w.nanos / 1e9;
            // Resource deltas span the window plus the 1.2 s statistics settle; rate them over that span.
            double span = w.scaling.get("elapsed_nanos") / 1e9;
            int connections = w.key.replicas * w.key.pool;
            long[] acquire = w.metrics.getOrDefault("sql_acquire", new long[3]), usage = w.metrics.getOrDefault("sql_usage", new long[3]);
            long[] put = w.metrics.getOrDefault("provider_put", new long[3]), get = w.metrics.getOrDefault("provider_getBounded", new long[3]);
            double commits = w.scaling.get("db_xact_commit") - w.scaling.get("statements_sampler_calls");
            double jiffies = w.scaling.get("host_cpu_total_jiffies");
            int cpus = (int) Math.round(w.scaling.get("host_cpus"));
            text.append(w.run).append(',').append(w.name).append(',').append(w.key.journaled).append(',').append(w.key.replicas).append(',').append(w.key.pool)
                    .append(',').append(w.key.clients).append(',').append(w.key.heap).append(',').append(w.operations)
                    .append(String.format(",%.3f,%.2f", seconds, w.operations / seconds));
            for (String kind : KINDS) text.append(percentiles(w.latency.get(kind), .5, .95, .99));
            text.append(',').append(connections)
                    .append(String.format(",%.3f,%.3f,%.3f,%.2f", usage[1] / 1e9 / seconds / connections, acquire[1] / 1e6 / Math.max(1, acquire[0]),
                            acquire[1] / 1e9, (double) acquire[0] / w.operations))
                    .append(String.format(",%.3f,%.3f,%d,%.2f", usage[1] / 1e9, w.statementExecMs / 1e3, w.statementCalls, (double) w.statementCalls / w.operations))
                    .append(String.format(",%.0f,%.2f,%.2f,%.0f,%.3f,%.3f,%.3f", commits, commits / w.operations, w.scaling.get("wal_wal_bytes") / 1048576,
                            w.scaling.get("walio_fsyncs"), w.scaling.get("walio_fsync_time") / 1e3, w.scaling.get("walio_write_time") / 1e3,
                            w.scaling.get("relio_read_time") / 1e3))
                    .append(String.format(",%d,%.2f,%d,%.2f", put[0], put[1] / 1e6 / Math.max(1, put[0]), get[0], get[1] / 1e6 / Math.max(1, get[0])))
                    .append(String.format(",%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f", cpus, w.scaling.get("host_cpu_busy_jiffies") / jiffies * cpus,
                            w.scaling.get("host_cpu_iowait_jiffies") / jiffies * cpus,
                            w.scaling.get("container_postgres_cpu_usage_usec") / 1e6 / span,
                            w.scaling.get("container_rustfs_cpu_usage_usec") / 1e6 / span,
                            w.childCpuNanos / 1e9 / span, w.childCpuNanos / 1e9)).append(String.format(",%.3f", span))
                    .append(',').append(w.scaling.get("loadavg_begin_1m")).append(',').append(w.scaling.get("loadavg_end_1m")).append('\n');
        }
        Files.writeString(out.resolve("windows.csv"), text);

        var configs = new StringBuilder("journaled,replicas,pool_per_replica,clients_total,heap_mib,runs,windows,operations,"
                + "ops_per_s_min,ops_per_s_mean,ops_per_s_max,read_p50_ms,read_p95_ms,read_p99_ms,publish_p50_ms,publish_p95_ms,publish_p99_ms,"
                + "reject_p50_ms,reject_p95_ms,reject_p99_ms,pool_busy_fraction_mean,sql_acquire_mean_ms,db_commits_per_iteration_mean,"
                + "wal_fsync_s_per_window_mean,provider_put_mean_ms,postgres_cores_mean,rustfs_cores_mean,worker_cores_mean,host_busy_cores_mean\n");
        var grouped = new TreeMap<String, List<Window>>();
        for (var w : windows) grouped.computeIfAbsent(w.key.journaled + "," + w.key.replicas + "," + w.key.pool + "," + w.key.clients + "," + w.key.heap, k -> new ArrayList<>()).add(w);
        for (var entry : grouped.entrySet()) {
            var group = entry.getValue();
            var rates = group.stream().mapToDouble(w -> w.operations * 1e9 / w.nanos).sorted().toArray();
            configs.append(entry.getKey()).append(',').append(group.stream().map(w -> w.run).distinct().count()).append(',').append(group.size())
                    .append(',').append(group.stream().mapToLong(w -> w.operations).sum())
                    .append(String.format(",%.2f,%.2f,%.2f", rates[0], Arrays.stream(rates).average().orElseThrow(), rates[rates.length - 1]));
            for (String kind : KINDS) configs.append(percentiles(group.stream().flatMap(w -> w.latency.get(kind).stream()).collect(Collectors.toList()), .5, .95, .99));
            configs.append(String.format(",%.3f,%.3f,%.2f,%.3f,%.2f,%.2f,%.2f,%.2f,%.2f",
                    mean(group, w -> w.metrics.get("sql_usage")[1] / 1e9 / (w.nanos / 1e9) / (w.key.replicas * w.key.pool)),
                    mean(group, w -> w.metrics.get("sql_acquire")[1] / 1e6 / Math.max(1, w.metrics.get("sql_acquire")[0])),
                    mean(group, w -> (w.scaling.get("db_xact_commit") - w.scaling.get("statements_sampler_calls")) / w.operations),
                    mean(group, w -> w.scaling.get("walio_fsync_time") / 1e3),
                    mean(group, w -> w.metrics.get("provider_put")[1] / 1e6 / Math.max(1, w.metrics.get("provider_put")[0])),
                    mean(group, w -> w.scaling.get("container_postgres_cpu_usage_usec") / 1e6 / (w.scaling.get("elapsed_nanos") / 1e9)),
                    mean(group, w -> w.scaling.get("container_rustfs_cpu_usage_usec") / 1e6 / (w.scaling.get("elapsed_nanos") / 1e9)),
                    mean(group, w -> w.childCpuNanos / 1e9 / (w.scaling.get("elapsed_nanos") / 1e9)),
                    mean(group, w -> w.scaling.get("host_cpu_busy_jiffies") / w.scaling.get("host_cpu_total_jiffies") * w.scaling.get("host_cpus"))))
                    .append('\n');
        }
        Files.writeString(out.resolve("configs.csv"), configs);

        var workers = new StringBuilder("run,window,replicas,worker,measured_operations,provider_puts,provider_gets,sql_acquisitions\n");
        for (var w : windows) for (long[] worker : w.workers)
            workers.append(w.run).append(',').append(w.name).append(',').append(w.key.replicas).append(',').append(worker[0]).append(',')
                    .append(worker[1]).append(',').append(worker[2]).append(',').append(worker[3]).append(',').append(worker[4]).append('\n');
        Files.writeString(out.resolve("workers.csv"), workers);
        System.out.println("windows=" + windows.size() + " configs=" + grouped.size() + " output=" + out);
    }

    static double mean(List<Window> group, java.util.function.ToDoubleFunction<Window> f) { return group.stream().mapToDouble(f).average().orElseThrow(); }

    static String percentiles(List<Long> values, double... ranks) {
        var sorted = new ArrayList<>(values); Collections.sort(sorted);
        var text = new StringBuilder();
        for (double rank : ranks) text.append(String.format(",%.2f", sorted.get((int) Math.ceil(sorted.size() * rank) - 1) / 1e6));
        return text.toString();
    }

    static List<Window> read(Path root) throws Exception {
        var environment = new HashMap<String, String>();
        for (String line : Files.readAllLines(root.resolve("environment.txt"))) {
            int eq = line.indexOf('='); if (eq > 0) environment.put(line.substring(0, eq), line.substring(eq + 1));
        }
        int clients = Integer.parseInt(environment.get("clients"));
        String journaled = environment.get("journaled");
        var rows = Files.readAllLines(root.resolve("windows.csv"));
        var plan = rows.subList(1, rows.size());
        if (plan.size() % 2 != 0) throw new IllegalStateException("Odd window count in " + root);
        var result = new ArrayList<Window>();
        for (String line : plan) {
            String[] row = line.split(",");
            var w = new Window();
            w.run = root.getFileName().toString(); w.name = row[0];
            int replicas = Integer.parseInt(row[1]);
            w.key = new Key(replicas, Integer.parseInt(row[2]), clients, Integer.parseInt(row[6]), journaled);
            w.operations = Long.parseLong(row[4]); w.nanos = Long.parseLong(row[5]);
            if (Integer.parseInt(row[3]) * replicas != clients) throw new IllegalStateException("Client split mismatch: " + w.name);
            long count = 0;
            for (int worker = 0; worker < replicas; worker++) {
                String prefix = w.name + "-" + worker;
                long measured = 0;
                for (String operation : Files.readAllLines(root.resolve(prefix + "-operations.csv"))) {
                    String[] cells = operation.split(",");
                    if (!cells[0].equals("measure")) continue;
                    w.latency.computeIfAbsent(cells[3], k -> new ArrayList<>()).add(Long.parseLong(cells[5]));
                    if (!cells[3].equals("replay")) { count++; measured++; }
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
                w.workers.add(new long[] {worker, measured, mine.get("provider_put")[0], mine.get("provider_getBounded")[0], mine.get("sql_acquire")[0]});
            }
            if (count != w.operations) throw new IllegalStateException("Operation count mismatch: " + w.name + " in " + root);
            for (String kind : KINDS) if (w.latency.get(kind) == null) throw new IllegalStateException("No " + kind + " samples in " + w.name);
            for (String line2 : Files.readAllLines(root.resolve(w.name + "-scaling.csv"))) {
                String[] cells = line2.split(",", -1);
                if (cells[0].equals("metric")) continue;
                if (cells[0].startsWith("loadavg_")) { w.scaling.put(cells[0] + "_1m", Double.parseDouble(cells[1].split(" ")[0])); continue; }
                if (cells[0].startsWith("child_")) { w.childCpuNanos += Long.parseLong(cells[3]); continue; }
                if (cells[0].equals("host_cpus")) { w.scaling.put(cells[0], Double.parseDouble(cells[2])); continue; }
                w.scaling.put(cells[0], Double.parseDouble(cells[3]));
            }
            long children = Files.readAllLines(root.resolve(w.name + "-scaling.csv")).stream().filter(s -> s.startsWith("child_")).count();
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
