package ai.protomolt.proto.repo.service;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Dedicated test-container connection, outside the service pools; top-level statements only. */
final class ReplicaSqlMetrics implements AutoCloseable {
    private final Connection connection;
    private final Path output;
    private Map<String, Long> previousCalls = Map.of();

    ReplicaSqlMetrics(String url, String user, String password, Path output) throws Exception {
        this.output = output;
        connection = DriverManager.getConnection(url, user, password);
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            try (var result = statement.executeQuery("SELECT current_setting('server_version'), current_setting('pg_stat_statements.track')")) {
                if (!result.next() || !result.getString(2).equals("top")) throw new IllegalStateException("Unexpected statement tracking configuration");
                Files.writeString(output.resolve("database-environment.txt"), "server_version=" + result.getString(1) + "\npg_stat_statements.track=" + result.getString(2) + "\n");
            }
            Files.createDirectories(output.resolve("queries"));
        } catch (Exception | Error failure) {
            try { connection.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Reset once after topology startup/seed, never inside a measured traffic window. */
    void reset() throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            statement.execute("SELECT pg_stat_statements_reset() /* benchmark_snapshot */");
            previousCalls = Map.of();
        }
    }

    void snapshot(String label) throws Exception {
        var rows = new StringBuilder("query_key,calls,total_exec_ms,rows,query_sha256\n");
        var calls = new HashMap<String, Long>();
        long readCalls = 0, writeCalls = 0;
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            try (var result = statement.executeQuery("""
                    SELECT userid::text || '_' || queryid::text || '_' || toplevel::text AS query_key,
                           calls, total_exec_time, rows, query
                    FROM pg_stat_statements
                    WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
                      AND query NOT LIKE '%benchmark_snapshot%'
                    ORDER BY query_key /* benchmark_snapshot */
                    """)) {
                while (result.next()) {
                    String key = result.getString(1);
                    if (key == null || !key.matches("[0-9]+_-?[0-9]+_(true|false)")) throw new IllegalStateException("Unexpected SQL query identity");
                    long count = result.getLong(2), delta = count - previousCalls.getOrDefault(key, 0L);
                    if (delta < 0 || calls.put(key, count) != null) throw new IllegalStateException("Invalid SQL counter progression");
                    String query = result.getString(5);
                    String lower = query.toLowerCase(Locale.ROOT);
                    if (lower.contains("acquire_archive_read_pin(")) readCalls += delta;
                    if (lower.startsWith("insert into archive_version_object_refs")) writeCalls += delta;
                    byte[] text = query.getBytes(StandardCharsets.UTF_8);
                    String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text));
                    Path definition = output.resolve("queries/" + digest + ".sql");
                    if (!Files.exists(definition)) Files.write(definition, text);
                    else if (!Arrays.equals(Files.readAllBytes(definition), text)) throw new IllegalStateException("SQL text digest collision");
                    rows.append(key).append(',').append(count).append(',').append(result.getDouble(3)).append(',').append(result.getLong(4))
                            .append(',').append(digest).append('\n');
                }
            }
        }
        if (!calls.keySet().containsAll(previousCalls.keySet())) throw new IllegalStateException("SQL statistics disappeared before topology reset");
        if (label.endsWith("-mixed") && (readCalls == 0 || writeCalls == 0)) throw new IllegalStateException("Request SQL counters did not advance for mixed traffic");
        Files.writeString(output.resolve(label + "-sql.csv"), rows);
        previousCalls = Map.copyOf(calls);
    }

    @Override public void close() throws SQLException { connection.close(); }
}
