package ai.protomolt.proto.repo.service;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test-only PostgreSQL wait observations. Samples are not a sum of lock-wait duration. */
final class ReplicaLockSamples implements AutoCloseable {
    private final Connection connection;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final FutureTask<Void> sampling;
    private final Thread thread;

    ReplicaLockSamples(String url, String user, String password, Path output) throws Exception {
        connection = DriverManager.getConnection(url, user, password);
        try {
            sampling = new FutureTask<>(() -> {
                try (BufferedWriter writer = Files.newBufferedWriter(output);
                        var statement = connection.createStatement()) {
                    statement.setQueryTimeout(5);
                    writer.write("sample,elapsed_nanos,pid,wait_event,query_family,blocker_pid,blocker_state,blocker_query_family\n");
                    long start = System.nanoTime(), sample = 0;
                    do {
                        long elapsed = System.nanoTime() - start;
                        boolean found = false;
                        // A database-wide observation: query text is classified, never stored with parameters.
                        // pg_blocking_pids also reports soft blockers ahead in a lock queue.
                        try (var result = statement.executeQuery("""
                                SELECT a.pid, a.wait_event,
                                    CASE WHEN a.query LIKE '%archive_stats%' THEN 'archive_stats'
                                         WHEN a.query LIKE '%archive_rendition_stats%' THEN 'archive_rendition_stats'
                                         ELSE 'other' END,
                                    b.pid, b.state,
                                    CASE WHEN b.query LIKE '%archive_stats%' THEN 'archive_stats'
                                         WHEN b.query LIKE '%archive_rendition_stats%' THEN 'archive_rendition_stats'
                                         ELSE 'other' END
                                FROM pg_stat_activity a
                                LEFT JOIN LATERAL unnest(pg_blocking_pids(a.pid)) blockers(pid) ON true
                                LEFT JOIN pg_stat_activity b ON b.pid=blockers.pid
                                WHERE a.datname=current_database() AND a.pid<>pg_backend_pid()
                                  AND a.wait_event_type='Lock'
                                ORDER BY a.pid, b.pid /* benchmark_snapshot */
                                """)) {
                            while (result.next()) {
                                found = true;
                                writer.write(sample + "," + elapsed);
                                for (int column = 1; column <= 6; column++) {
                                    String value = result.getString(column);
                                    if (value != null && (value.contains(",") || value.contains("\n") || value.contains("\r")))
                                        throw new IllegalStateException("Unexpected PostgreSQL lock observation value");
                                    writer.write("," + (value == null ? "" : value));
                                }
                                writer.newLine();
                            }
                        }
                        // Preserve empty polls so observation coverage is auditable.
                        if (!found) { writer.write(sample + "," + elapsed + ",,,,,,\n"); }
                        sample++;
                        if (!stop.get()) Thread.sleep(10);
                    } while (!stop.get());
                }
                return null;
            });
            thread = Thread.ofVirtual().name("replica-lock-samples").start(sampling);
        } catch (Exception | Error failure) {
            try { connection.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    @Override public void close() throws Exception {
        stop.set(true);
        try {
            sampling.get(7, TimeUnit.SECONDS);
        } catch (Exception | Error failure) {
            thread.interrupt();
            try { connection.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
        connection.close();
    }
}
