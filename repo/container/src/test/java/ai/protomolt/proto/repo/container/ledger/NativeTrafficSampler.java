package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.*;
import java.sql.*;
import java.util.List;

/** Dedicated diagnostic connection outside service pools; lock samples are presence, not duration. */
final class NativeTrafficSampler implements AutoCloseable {
    private final Connection connection;
    private final Path output;
    private long start;
    private long[] baseline;
    NativeTrafficSampler(String url, String user, String password, Path output) throws Exception {
        this.output = output;
        connection = DriverManager.getConnection(url, user, password);
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            try (var rows = statement.executeQuery("SELECT version()")) {
                if (!rows.next()) throw new IllegalStateException("Missing database version");
                Files.writeString(output.resolve("database.txt"), rows.getString(1));
            }
        } catch (Exception | Error failure) {
            try { connection.close(); } catch (Exception close) { failure.addSuppressed(close); }
            throw failure;
        }
    }
    void begin(String name) throws Exception {
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            try (var row = statement.executeQuery("SELECT (SELECT count(*) FROM documents), (SELECT count(*) FROM document_revision_commits), (SELECT count(*) FROM repository_operation_rejection)")) {
                if (!row.next()) throw new IllegalStateException("Missing baseline counts");
                baseline = new long[]{row.getLong(1), row.getLong(2), row.getLong(3)};
                Files.writeString(output.resolve(name + "-baseline.csv"), "documents,revisions,rejections\n" + row.getLong(1) + "," + row.getLong(2) + "," + row.getLong(3) + "\n");
            }
            statement.execute("SELECT pg_stat_statements_reset() /* native_benchmark_snapshot */");
        }
        Files.writeString(output.resolve(name + "-locks.csv"), "elapsed_nanos,pid,wait_type,wait_event,blockers\n");
        Files.writeString(output.resolve(name + "-activity.csv"), "elapsed_nanos,pid,state,wait_type,wait_event,blockers\n");
        Files.writeString(output.resolve(name + "-rss.csv"), "elapsed_nanos,pid,rss_kib,state,cpu_nanos\n");
        start = System.nanoTime();
    }
    void sample(String name, List<Process> children) throws Exception {
        long elapsed = System.nanoTime() - start;
        var locks = new StringBuilder();
        var activity = new StringBuilder();
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            try (var rows = statement.executeQuery("""
                    SELECT pid,wait_event_type,wait_event,array_to_string(pg_blocking_pids(pid),'|'),state
                    FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
                    AND backend_type='client backend' ORDER BY pid /* native_benchmark_snapshot */
                    """)) {
                while (rows.next()) {
                    String waitType = rows.getString(2);
                    activity.append(elapsed).append(',').append(rows.getInt(1)).append(',')
                            .append(csv(rows.getString(5))).append(',').append(csv(waitType)).append(',')
                            .append(csv(rows.getString(3))).append(',').append(csv(rows.getString(4))).append('\n');
                    if ("Lock".equals(waitType)) locks.append(elapsed).append(',').append(rows.getInt(1)).append(',')
                            .append(waitType).append(',').append(csv(rows.getString(3))).append(',')
                            .append(csv(rows.getString(4))).append('\n');
                }
            }
        }
        if (activity.isEmpty()) activity.append(elapsed).append(",,,,,\n");
        Files.writeString(output.resolve(name + "-activity.csv"), activity, StandardOpenOption.APPEND);
        if (locks.isEmpty()) locks.append(elapsed).append(",,,,\n");
        Files.writeString(output.resolve(name + "-locks.csv"), locks, StandardOpenOption.APPEND);
        var rss = new StringBuilder();
        for (var child : children) {
            String bytes = "", state = "exited";
            if (child.isAlive()) {
                try {
                    var lines = Files.readAllLines(Path.of("/proc", Long.toString(child.pid()), "status"));
                    var memory = memoryStatus(lines);
                    bytes = memory.kib(); state = memory.state();
                } catch (NoSuchFileException gone) { if (child.isAlive()) throw gone; }
            }
            String cpu = child.info().totalCpuDuration().map(value -> Long.toString(value.toNanos())).orElse("");
            rss.append(elapsed).append(',').append(child.pid()).append(',').append(bytes).append(',').append(state)
                    .append(',').append(cpu).append('\n');
        }
        Files.writeString(output.resolve(name + "-rss.csv"), rss, StandardOpenOption.APPEND);
    }
    private static String csv(String value) { return value == null ? "" : value; }
    record MemoryStatus(String kib, String state) {}
    static MemoryStatus memoryStatus(List<String> lines) {
        String state = lines.stream().filter(line -> line.startsWith("State:")).findFirst()
                .orElseThrow(() -> new IllegalStateException("Process status has no State field")).split("\\s+")[1];
        // Linux may expose the zombie before Process.isAlive observes its exit.
        // Missing RSS is not a zero-byte measurement and must not hide the child log.
        if (state.equals("Z") || state.equals("X")) return new MemoryStatus("", "exited");
        String rss = lines.stream().filter(line -> line.startsWith("VmRSS:")).findFirst()
                .orElseThrow(() -> new IllegalStateException("Live process status has no VmRSS field")).split("\\s+")[1];
        if (Long.parseLong(rss) < 0) throw new IllegalStateException("Process RSS is negative");
        return new MemoryStatus(rss, "running");
    }
    void finish(String name, int clients, int iterations) throws Exception {
        var csv = new StringBuilder("query_id,calls,total_exec_ms,rows\n");
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            try (var rows = statement.executeQuery("""
                    SELECT queryid,calls,total_exec_time,rows,query FROM pg_stat_statements
                    WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
                    AND query NOT LIKE '%native_benchmark_snapshot%' AND toplevel
                    ORDER BY queryid /* native_benchmark_snapshot */
                    """)) {
                while (rows.next()) {
                    String id = Long.toString(rows.getLong(1));
                    csv.append(id).append(',').append(rows.getLong(2)).append(',').append(rows.getDouble(3)).append(',').append(rows.getLong(4)).append('\n');
                    Files.writeString(output.resolve(name + "-query-" + id + ".sql"), rows.getString(5), StandardOpenOption.CREATE_NEW);
                }
            }
        }
        Files.writeString(output.resolve(name + "-sql.csv"), csv);
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            try (var row = statement.executeQuery("SELECT (SELECT count(*) FROM documents), (SELECT count(*) FROM document_revision_commits), (SELECT count(*) FROM repository_operation_rejection)")) {
                if (!row.next()) throw new IllegalStateException("Missing final counts");
                long docs = row.getLong(1) - baseline[0], revisions = row.getLong(2) - baseline[1], rejections = row.getLong(3) - baseline[2];
                Files.writeString(output.resolve(name + "-durable-delta.csv"), "documents,revisions,rejections\n" + docs + "," + revisions + "," + rejections + "\n");
                long accepted = 3L * iterations / 8 * clients, rejected = (long) iterations / 8 * clients;
                if (docs != accepted || revisions != accepted || rejections != rejected)
                    throw new AssertionError("Measured durable counts differ: " + docs + "," + revisions + "," + rejections);
            }
        }
    }
    @Override public void close() throws SQLException { connection.close(); }
}
