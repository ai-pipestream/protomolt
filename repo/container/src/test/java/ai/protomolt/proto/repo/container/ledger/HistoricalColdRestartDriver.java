package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.*;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;

/** The recovery JVM starts only after the exact writer process and its SQL sessions exit. */
final class HistoricalColdRestartDriver {
    static void run(StorageRuntimeProbeCompiler.Compiled compiled, Map<String, String> environment, Path directory, String phase) throws Exception {
        assertThat(phase).isIn("initial", "reserved", "installed");
        Files.createDirectories(directory);
        var shared = new HashMap<>(environment);
        shared.put("PROTOMOLT_TEST_COLD_REQUEST", directory.resolve("request.properties").toString());
        shared.put("PROTOMOLT_TEST_COLD_CREDENTIAL", UUID.randomUUID().toString());
        shared.put("PROTOMOLT_TEST_COLD_PHASE", phase);
        String application = "cold-writer-" + UUID.randomUUID();
        var writerEnvironment = new HashMap<>(shared);
        writerEnvironment.put("PROTOMOLT_TEST_COLD_WRITER_APP", application);
        String jdbc = Objects.requireNonNull(shared.get("PROTOMOLT_TEST_JDBC"));
        writerEnvironment.put("PROTOMOLT_TEST_JDBC", jdbc + (jdbc.contains("?") ? "&" : "?") + "ApplicationName=" + application);
        var writerLog = directory.resolve("writer.log");
        var writer = start(compiled, writerEnvironment, writerLog, "HistoricalOwnerReconciliationHost", "cold-restart-writer");
        try {
            assertThat(writer.waitFor(90, TimeUnit.SECONDS)).as("Writer exited; log: %s", writerLog).isTrue();
            assertThat(Files.size(writerLog)).isLessThan(1_048_576);
            String output = Files.readString(writerLog);
            assertThat(writer.exitValue()).as(output).isEqualTo(23);
            assertThat(output).contains("HISTORICAL_COLD_WRITER_CHECKPOINT_OK", "HISTORICAL_COLD_WRITER_PHASE_" + phase);
        } finally { stop(writer); }
        try (var connection = DriverManager.getConnection(jdbc, shared.get("PROTOMOLT_TEST_USER"), shared.get("PROTOMOLT_TEST_PASSWORD"));
             var query = connection.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE application_name=?")) {
            query.setString(1, application);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (true) {
                try (var rows = query.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    if (rows.getLong(1) == 0) break;
                }
                assertThat(System.nanoTime()).as("Writer SQL sessions exited").isLessThan(deadline);
                Thread.sleep(25);
            }
        }
        System.out.println("HISTORICAL_COLD_WRITER_EXIT_23_SQL_SESSIONS_GONE " + phase);
        var recoveryLog = directory.resolve("recovery.log");
        var recovery = start(compiled, shared, recoveryLog, "HistoricalColdRestartProbe");
        try {
            assertThat(recovery.waitFor(90, TimeUnit.SECONDS)).as("Recovery exited; log: %s", recoveryLog).isTrue();
            assertThat(Files.size(recoveryLog)).isLessThan(1_048_576);
            String output = Files.readString(recoveryLog);
            assertThat(recovery.exitValue()).as(output).isZero();
            assertThat(output).contains("HISTORICAL_COLD_PROCESS_RESTART_OK", "SCOPED_HISTORICAL_COLD_OWNER_INSTALLED_OK",
                    "SCOPED_HISTORICAL_COLD_OWNER_PUBLICATION_OK", "SCOPED_INSTALLED_HISTORICAL_TERMINAL_RETIRED_OK");
            System.out.println("HISTORICAL_COLD_PROCESS_RESTART_OK " + phase);
        } finally { stop(recovery); }
    }

    private static Process start(StorageRuntimeProbeCompiler.Compiled compiled, Map<String, String> environment,
            Path log, String main, String... args) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                compiled.classpath() + java.io.File.pathSeparator + compiled.probe(),
                "ai.protomolt.proto.repo.container.ledger." + main, compiled.bundle().toString()));
        command.addAll(List.of(args));
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().putAll(environment);
        return builder.start();
    }
    private static void stop(Process process) throws Exception {
        if (process.isAlive()) {
            process.destroyForcibly();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).as("Test process stopped").isTrue();
        }
    }
}
