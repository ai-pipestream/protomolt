package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.testcontainers.DockerClientFactory;

/**
 * Window-bounded resource deltas for the replica scaling qualification: PostgreSQL
 * cumulative statistics, host CPU, container CPU/memory through cgroup v2 and exact
 * child-process CPU. Every value is a begin/end delta over one measured window; nothing
 * here samples duration inside the window or alters the database.
 */
final class RepositoryScalingSampler implements AutoCloseable {
    static final String MARKER = "/* native_benchmark_snapshot */";
    record Container(String role, String id, Path cgroup) {}
    record HostCpu(long busy, long idle, long iowait, long total) {
        /** First line of /proc/stat: user nice system idle iowait irq softirq steal. */
        static HostCpu parse(String line) {
            String[] fields = line.trim().split("\\s+");
            if (fields.length < 9 || !fields[0].equals("cpu")) throw new IllegalStateException("Unexpected /proc/stat line: " + line);
            long[] values = new long[8];
            for (int i = 0; i < 8; i++) values[i] = Long.parseLong(fields[i + 1]);
            long idle = values[3], iowait = values[4];
            long total = Arrays.stream(values).sum();
            return new HostCpu(total - idle - iowait, idle, iowait, total);
        }
    }
    record CgroupCpu(long usageMicros, long userMicros, long systemMicros) {
        /** cgroup v2 cpu.stat; the three usage counters are mandatory. */
        static CgroupCpu parse(List<String> lines) {
            var values = new HashMap<String, Long>();
            for (String line : lines) {
                String[] fields = line.trim().split("\\s+");
                if (fields.length == 2) values.put(fields[0], Long.parseLong(fields[1]));
            }
            for (String key : List.of("usage_usec", "user_usec", "system_usec"))
                if (!values.containsKey(key)) throw new IllegalStateException("cpu.stat has no " + key);
            return new CgroupCpu(values.get("usage_usec"), values.get("user_usec"), values.get("system_usec"));
        }
    }
    private record Snapshot(long nanos, String loadavg, HostCpu host, long hostAvailableKib,
            Map<String, CgroupCpu> containerCpu, Map<String, Long> containerMemory,
            Map<String, Double> database, Map<Long, Long> childCpuNanos) {}

    private final Connection connection;
    private final Path output;
    private final List<Container> containers;
    private final int hostCpus = Runtime.getRuntime().availableProcessors();
    private Snapshot begin;

    RepositoryScalingSampler(String url, String user, String password, Path output, Map<String, String> containerIds) throws Exception {
        this.output = output;
        var resolved = new ArrayList<Container>();
        for (var entry : containerIds.entrySet()) resolved.add(new Container(entry.getKey(), entry.getValue(), cgroup(entry.getValue())));
        containers = List.copyOf(resolved);
        connection = DriverManager.getConnection(url, user, password);
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            // Both are observation settings supplied to the benchmark container command;
            // the sampler refuses to measure without them rather than reporting zero timings.
            try (var rows = statement.executeQuery("SELECT name, setting FROM pg_settings WHERE name IN ('track_io_timing','track_wal_io_timing') " + MARKER)) {
                int seen = 0;
                while (rows.next()) { seen++; if (!rows.getString(2).equals("on")) throw new IllegalStateException(rows.getString(1) + " is not on"); }
                if (seen != 2) throw new IllegalStateException("Timing settings are missing");
            }
        } catch (Exception | Error failure) {
            try { connection.close(); } catch (Exception close) { failure.addSuppressed(close); }
            throw failure;
        }
    }

    /** Locate a Testcontainers-started container of this session by image substring; exactly one must exist. */
    static String containerIdByImage(String imageFragment) {
        var client = DockerClientFactory.instance().client();
        var matches = client.listContainersCmd()
                .withLabelFilter(Map.of("org.testcontainers.sessionId", DockerClientFactory.SESSION_ID)).exec().stream()
                .filter(container -> container.getImage() != null && container.getImage().contains(imageFragment)).toList();
        if (matches.size() != 1) throw new IllegalStateException("Expected one session container for " + imageFragment + ", found " + matches.size());
        return matches.get(0).getId();
    }

    /** Immutable image identity: registry digests when pulled, plus the local image ID. */
    static String describeImage(String image) {
        var inspected = DockerClientFactory.instance().client().inspectImageCmd(image).exec();
        var digests = inspected.getRepoDigests();
        return "image=" + image + " id=" + inspected.getId() + " repo_digests=" + (digests == null ? "" : String.join("|", digests));
    }

    private static Path cgroup(String containerId) throws Exception {
        long pid = DockerClientFactory.instance().client().inspectContainerCmd(containerId).exec().getState().getPidLong();
        if (pid <= 0) throw new IllegalStateException("Container " + containerId + " has no live init process");
        var lines = Files.readAllLines(Path.of("/proc", Long.toString(pid), "cgroup"));
        String unified = lines.stream().filter(line -> line.startsWith("0::")).findFirst()
                .orElseThrow(() -> new IllegalStateException("Container " + containerId + " is not on cgroup v2; CPU accounting unavailable"));
        var path = Path.of("/sys/fs/cgroup" + unified.substring(3));
        if (!Files.isReadable(path.resolve("cpu.stat"))) throw new IllegalStateException("Unreadable " + path.resolve("cpu.stat"));
        return path;
    }

    String describe() throws Exception {
        var text = new StringBuilder();
        text.append("host_cpus=").append(hostCpus).append('\n');
        for (String line : Files.readAllLines(Path.of("/proc/meminfo")))
            if (line.startsWith("MemTotal:")) text.append("host_mem_total_kib=").append(line.split("\\s+")[1]).append('\n');
        text.append("kernel=").append(Files.readString(Path.of("/proc/sys/kernel/osrelease")).trim()).append('\n');
        for (var container : containers) text.append("container_").append(container.role()).append("=").append(container.id())
                .append(" cgroup=").append(container.cgroup()).append('\n');
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            try (var rows = statement.executeQuery("SELECT name, setting FROM pg_settings WHERE name IN ('synchronous_commit','fsync',"
                    + "'wal_sync_method','shared_buffers','wal_buffers','max_connections','max_wal_size','checkpoint_timeout',"
                    + "'full_page_writes','wal_compression','commit_delay','track_io_timing','track_wal_io_timing','shared_preload_libraries') ORDER BY name " + MARKER)) {
                while (rows.next()) text.append("postgres.").append(rows.getString(1)).append('=').append(rows.getString(2)).append('\n');
            }
        }
        return text.toString();
    }

    void begin(String window, List<Process> children) throws Exception {
        if (begin != null) throw new IllegalStateException("Window " + window + " began before the previous one finished");
        begin = snapshot(children);
    }

    /** Writes metric,begin,end,delta rows; the child rows carry exact process CPU over the window. */
    void finish(String window, List<Process> children) throws Exception {
        if (begin == null) throw new IllegalStateException("Window " + window + " did not begin");
        var end = snapshot(children);
        var text = new StringBuilder("metric,begin,end,delta\n");
        row(text, "elapsed_nanos", begin.nanos(), end.nanos());
        text.append("loadavg_begin,").append(begin.loadavg()).append(",,\n");
        text.append("loadavg_end,").append(end.loadavg()).append(",,\n");
        row(text, "host_cpus", hostCpus, hostCpus);
        row(text, "host_cpu_busy_jiffies", begin.host().busy(), end.host().busy());
        row(text, "host_cpu_idle_jiffies", begin.host().idle(), end.host().idle());
        row(text, "host_cpu_iowait_jiffies", begin.host().iowait(), end.host().iowait());
        row(text, "host_cpu_total_jiffies", begin.host().total(), end.host().total());
        gauge(text, "host_mem_available_kib", begin.hostAvailableKib(), end.hostAvailableKib());
        for (var container : containers) {
            var before = begin.containerCpu().get(container.role()); var after = end.containerCpu().get(container.role());
            row(text, "container_" + container.role() + "_cpu_usage_usec", before.usageMicros(), after.usageMicros());
            row(text, "container_" + container.role() + "_cpu_user_usec", before.userMicros(), after.userMicros());
            row(text, "container_" + container.role() + "_cpu_system_usec", before.systemMicros(), after.systemMicros());
            gauge(text, "container_" + container.role() + "_memory_current_bytes", begin.containerMemory().get(container.role()), end.containerMemory().get(container.role()));
        }
        for (String key : begin.database().keySet()) {
            double before = begin.database().get(key), after = end.database().get(key);
            if (after < before) throw new IllegalStateException("Database counter moved backwards: " + key);
            text.append(key).append(',').append(before).append(',').append(after).append(',').append(after - before).append('\n');
        }
        for (var child : children) {
            Long before = begin.childCpuNanos().get(child.pid()), after = end.childCpuNanos().get(child.pid());
            if (before == null || after == null) throw new IllegalStateException("Child " + child.pid() + " CPU time unavailable at a window boundary");
            row(text, "child_" + child.pid() + "_cpu_nanos", before, after);
        }
        Files.writeString(output.resolve(window + "-scaling.csv"), text, StandardOpenOption.CREATE_NEW);
        begin = null;
    }

    /** Gauges may fall; the delta column is signed. */
    private static void gauge(StringBuilder text, String metric, long before, long after) {
        text.append(metric).append(',').append(before).append(',').append(after).append(',').append(after - before).append('\n');
    }

    private static void row(StringBuilder text, String metric, long before, long after) {
        if (after < before) throw new IllegalStateException("Counter moved backwards: " + metric);
        text.append(metric).append(',').append(before).append(',').append(after).append(',').append(after - before).append('\n');
    }

    private Snapshot snapshot(List<Process> children) throws Exception {
        long nanos = System.nanoTime();
        var host = HostCpu.parse(Files.readAllLines(Path.of("/proc/stat")).get(0));
        long available = Files.readAllLines(Path.of("/proc/meminfo")).stream().filter(line -> line.startsWith("MemAvailable:"))
                .map(line -> Long.parseLong(line.split("\\s+")[1])).findFirst()
                .orElseThrow(() -> new IllegalStateException("/proc/meminfo has no MemAvailable"));
        var cpu = new LinkedHashMap<String, CgroupCpu>();
        var memory = new LinkedHashMap<String, Long>();
        for (var container : containers) {
            cpu.put(container.role(), CgroupCpu.parse(Files.readAllLines(container.cgroup().resolve("cpu.stat"))));
            memory.put(container.role(), Long.parseLong(Files.readString(container.cgroup().resolve("memory.current")).trim()));
        }
        var childCpu = new LinkedHashMap<Long, Long>();
        for (var child : children) {
            // A child that already exited has no process CPU to attribute; the caller
            // treats that as a window failure rather than a zero measurement.
            if (child.isAlive()) child.info().totalCpuDuration().ifPresent(duration -> childCpu.put(child.pid(), duration.toNanos()));
        }
        return new Snapshot(nanos, Files.readString(Path.of("/proc/loadavg")).trim().replace(',', ' '), host, available,
                cpu, memory, database(), childCpu);
    }

    private Map<String, Double> database() throws SQLException {
        var values = new LinkedHashMap<String, Double>();
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            single(statement, values, "db_", """
                    SELECT xact_commit, xact_rollback, blks_read, blks_hit, blk_read_time, blk_write_time, deadlocks,
                    active_time, idle_in_transaction_time, temp_files, temp_bytes
                    FROM pg_stat_database WHERE datname = current_database()
                    """);
            single(statement, values, "wal_", "SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full FROM pg_stat_wal");
            single(statement, values, "walio_", """
                    SELECT coalesce(sum(writes),0), coalesce(sum(write_time),0), coalesce(sum(fsyncs),0), coalesce(sum(fsync_time),0)
                    FROM pg_stat_io WHERE object = 'wal'
                    """, "writes", "write_time", "fsyncs", "fsync_time");
            single(statement, values, "relio_", """
                    SELECT coalesce(sum(reads),0), coalesce(sum(read_time),0), coalesce(sum(writes),0), coalesce(sum(write_time),0),
                    coalesce(sum(fsyncs),0), coalesce(sum(fsync_time),0), coalesce(sum(hits),0), coalesce(sum(evictions),0),
                    coalesce(sum(extends),0), coalesce(sum(extend_time),0)
                    FROM pg_stat_io WHERE object = 'relation'
                    """, "reads", "read_time", "writes", "write_time", "fsyncs", "fsync_time", "hits", "evictions", "extends", "extend_time");
            single(statement, values, "checkpointer_", "SELECT num_timed, num_requested, write_time, sync_time, buffers_written FROM pg_stat_checkpointer");
            single(statement, values, "statements_", "SELECT coalesce(sum(calls),0), coalesce(sum(total_exec_time),0) FROM pg_stat_statements WHERE query LIKE '%native_benchmark_snapshot%'",
                    "sampler_calls", "sampler_exec_ms");
        }
        return values;
    }

    private static void single(Statement statement, Map<String, Double> values, String prefix, String sql, String... names) throws SQLException {
        try (var rows = statement.executeQuery(sql.strip() + " " + MARKER)) {
            if (!rows.next()) throw new IllegalStateException("No row for " + prefix);
            var metadata = rows.getMetaData();
            for (int column = 1; column <= metadata.getColumnCount(); column++) {
                String name = names.length == 0 ? metadata.getColumnLabel(column) : names[column - 1];
                values.put(prefix + name, rows.getDouble(column));
            }
            if (rows.next()) throw new IllegalStateException("Multiple rows for " + prefix);
        }
    }

    @Override public void close() throws SQLException { connection.close(); }
}
