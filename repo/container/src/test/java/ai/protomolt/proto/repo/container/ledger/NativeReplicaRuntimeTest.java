package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real observation and database host in a fresh standard JVM, without ambient test classes. */
class NativeReplicaRuntimeTest {
    @TempDir Path directory;

    @Test void nativeWorkersPublishAndReadAcrossProcesses() throws Exception {
        String bundleProperty = System.getProperty("protomolt.test.admissionRuntimeBundle");
        String hostProperty = System.getProperty("protomolt.test.storageHostClasspath");
        assertThat(bundleProperty).as("Run :protomolt-repo-container:nativeReplicaTest").isNotBlank();
        assertThat(hostProperty).isNotBlank();
        var bundle = Path.of(bundleProperty);
        var inventory = DocumentRuntimeInventory.read(bundle, () -> {});
        var jars = new LinkedHashMap<String, Path>();
        inventory.identities().forEach(artifact -> jars.put(artifact.getArtifactSha256(),
                bundle.resolve("artifacts/" + artifact.getArtifactSha256() + ".jar")));
        // The production host shares admission dependencies. Include each exact
        // artifact once, by bytes; conflicting class providers still fail observation.
        for (String entry : hostProperty.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            var path = Path.of(entry);
            var identity = DocumentRuntimeArtifact.observe("storage-host", path,
                    new DocumentRuntimeArtifact.Limits(Files.size(path), 65536, 64), () -> {});
            jars.putIfAbsent(identity.getArtifactSha256(), path);
        }
        String classpath = String.join(java.io.File.pathSeparator, jars.values().stream().map(Path::toString).toList());
        var classes = Files.createDirectory(directory.resolve("classes"));
        var sources = new ArrayList<String>();
        for (String name : List.of("ObservedAssessmentProbe", "NativeReplicaProbe", "NativeMixedTrafficProbe", "NativeTrafficTelemetry", "NativeTrafficMaintenance")) {
            var source = directory.resolve(name + ".java");
            try (var input = getClass().getResourceAsStream("/runtime-inventory/" + name + ".java")) {
                assertThat(input).isNotNull(); Files.copy(input, source);
            }
            sources.add(source.toString());
        }
        var arguments = new ArrayList<>(List.of("-proc:none", "-classpath", classpath, "-d", classes.toString()));
        arguments.addAll(sources);
        assertThat(javax.tools.ToolProvider.getSystemJavaCompiler().run(null, null, null, arguments.toArray(String[]::new))).isZero();
        var probe = directory.resolve("storage-probe.jar");
        try (var output = new java.util.jar.JarOutputStream(Files.newOutputStream(probe)); var paths = Files.walk(classes)) {
            for (var file : paths.filter(Files::isRegularFile).sorted().toList()) {
                output.putNextEntry(new java.util.jar.JarEntry(classes.relativize(file).toString().replace(java.io.File.separatorChar, '/')));
                Files.copy(file, output); output.closeEntry();
            }
        }
        try (var postgres = new PostgreSQLContainer("postgres:18-alpine");
                var storage = new AssessmentStorageBackend("rustfs")) {
            if (Boolean.getBoolean("protomolt.test.nativeBenchmark")) postgres.withCommand("postgres", "-c",
                    "shared_preload_libraries=pg_stat_statements", "-c", "pg_stat_statements.track=top");
            postgres.start(); storage.start();
            var builder = new ProcessBuilder();
            builder.environment().put("PROTOMOLT_TEST_JDBC", postgres.getJdbcUrl());
            builder.environment().put("PROTOMOLT_TEST_USER", postgres.getUsername());
            builder.environment().put("PROTOMOLT_TEST_PASSWORD", postgres.getPassword());
            builder.environment().put("PROTOMOLT_TEST_RUNTIME_BUNDLE", bundle.toString());
            builder.environment().put("PROTOMOLT_TEST_S3_ENDPOINT", storage.getEndpoint().toString());
            builder.environment().put("PROTOMOLT_TEST_S3_ACCESS", storage.getAccessKey());
            builder.environment().put("PROTOMOLT_TEST_S3_SECRET", storage.getSecretKey());
            String childClasspath = classpath + java.io.File.pathSeparator + probe;
            run(builder, childClasspath, "seed", "seed");
            for (String topology : new String[] {"r1", "r2", "r4", "race"}) {
                int replicas = topology.equals("race") ? 2 : Integer.parseInt(topology.substring(1));
                var children = new ArrayList<Process>();
                long deadline = System.nanoTime() + java.time.Duration.ofSeconds(45).toNanos();
                Throwable primary = null;
                try {
                    for (int index = 0; index < replicas; index++) {
                        String worker = topology + "-" + index;
                        command(builder, childClasspath, "write", worker);
                        children.add(builder.redirectErrorStream(true).redirectOutput(directory.resolve(worker + ".log").toFile()).start());
                    }
                    for (int index = 0; index < children.size(); index++) {
                        String worker = topology + "-" + index;
                        while (!Files.exists(directory.resolve(worker + ".ready"))) {
                            if (!children.get(index).isAlive()) finished(children.get(index), worker, "WRITE");
                            assertThat(System.nanoTime() < deadline).as("writer readiness barrier").isTrue();
                            Thread.sleep(10);
                        }
                    }
                    Files.writeString(directory.resolve(topology + ".go"), "go", java.nio.file.StandardOpenOption.CREATE_NEW);
                    for (int index = 0; index < children.size(); index++) {
                        String worker = topology + "-" + index;
                        finished(children.get(index), worker, "WRITE");
                    }
                } catch (Exception | Error failure) {
                    primary = failure; throw failure;
                } finally {
                    stopChildren(children, primary);
                }
                run(builder, childClasspath, "read", "read-" + topology);
            }
            try (var files = Files.list(directory)) {
                assertThat(files.filter(p -> p.getFileName().toString().startsWith("race-") && p.toString().endsWith(".result")).count()).isEqualTo(1);
            }
            try (var files = Files.list(directory)) {
                assertThat(files.filter(p -> p.getFileName().toString().startsWith("race-") && p.toString().endsWith(".rejection")).count()).isEqualTo(1);
            }
            try (var files = Files.list(directory)) {
                assertThat(files.filter(p -> p.toString().endsWith(".result")).count()).isEqualTo(15);
            }
            try (var files = Files.list(directory)) {
                assertThat(files.filter(p -> p.toString().endsWith(".rejection")).count()).isEqualTo(8);
            }
            if (Boolean.getBoolean("protomolt.test.nativeBenchmark")) benchmark(builder, childClasspath, postgres);
        }
    }

    private void benchmark(ProcessBuilder builder, String classpath, PostgreSQLContainer postgres) throws Exception {
        int totalClients = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkClients", "4"));
        if (totalClients != 4 && totalClients != 8 && totalClients != 16)
            throw new IllegalArgumentException("Benchmark client count must be 4, 8 or 16");
        int totalReadSlots = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkReadSlots", "0"));
        if (totalReadSlots < 0 || totalReadSlots > 64 || totalReadSlots % 4 != 0)
            throw new IllegalArgumentException("Total reader slots must be zero or a multiple of four up to 64");
        int totalReadHandles = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkReadHandles", "0"));
        if (totalReadHandles < 0 || totalReadHandles > 256 || totalReadHandles % 4 != 0)
            throw new IllegalArgumentException("Total read handles must be zero or a multiple of four up to 256");
        Path output = Path.of(System.getProperty("protomolt.test.nativeBenchmarkOutput")).resolve(java.util.UUID.randomUUID().toString());
        Files.createDirectories(output);
        Files.writeString(output.resolve("environment.txt"), "java=" + System.getProperty("java.version")
                + "\nos=" + System.getProperty("os.name") + " " + System.getProperty("os.arch")
                + "\nloadavg=" + Files.readString(Path.of("/proc/loadavg")).trim()
                + "\nclients=" + totalClients + "\ntotal_read_slots=" + totalReadSlots
                + "\ntotal_read_handles=" + totalReadHandles + "\nzero_read_handles_means=32 per worker"
                + "\nzero_read_slots_means=8 per worker\nworker_heap_limit=512MiB\npayload=small typed StringValue\nNo host isolation or container CPU/memory limits; trusted internal Java path.\n");
        try (var sampler = new NativeTrafficSampler(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), output)) {
            var windows = new StringBuilder("window,replicas,pool_per_replica,clients_per_replica,operations,inclusive_nanos\n");
            int window = 0;
            for (String config : new String[] {"f1", "a4", "f2", "a1", "f4", "a2", "a2", "f4", "a1", "f2", "a4", "f1"}) {
                String name = String.format(java.util.Locale.ROOT, "t%02d", window++);
                int replicas = Integer.parseInt(config.substring(1)), clients = totalClients / replicas;
                int pool = config.startsWith("f") ? 8 / replicas : 8;
                builder.environment().put("PROTOMOLT_NATIVE_POOL", Integer.toString(pool));
                builder.environment().put("PROTOMOLT_NATIVE_CLIENTS", Integer.toString(clients));
                builder.environment().put("PROTOMOLT_NATIVE_READ_SLOTS", Integer.toString(totalReadSlots == 0 ? 8 : totalReadSlots / replicas));
                builder.environment().put("PROTOMOLT_NATIVE_READ_HANDLES", Integer.toString(totalReadHandles == 0 ? 32 : totalReadHandles / replicas));
                var children = new ArrayList<Process>();
                Throwable primary = null;
                try {
                    long readyDeadline = System.nanoTime() + java.time.Duration.ofSeconds(90).toNanos();
                    for (int index = 0; index < replicas; index++) {
                        String worker = name + "-" + index;
                        command(builder, classpath, "traffic", worker);
                        children.add(builder.redirectErrorStream(true).redirectOutput(output.resolve(worker + ".log").toFile()).start());
                    }
                    for (int index = 0; index < replicas; index++) {
                        while (!Files.exists(directory.resolve(name + "-" + index + ".ready"))) {
                            if (!children.get(index).isAlive()) throw new AssertionError(Files.readString(output.resolve(name + "-" + index + ".log")));
                            assertThat(System.nanoTime() < readyDeadline).as("traffic warmup readiness").isTrue();
                            Thread.sleep(10);
                        }
                    }
                    sampler.begin(name);
                    long start = System.nanoTime();
                    Files.writeString(directory.resolve(name + ".go"), "go", java.nio.file.StandardOpenOption.CREATE_NEW);
                    long finishDeadline = start + java.time.Duration.ofSeconds(90).toNanos();
                    boolean done;
                    do {
                        done = true;
                        for (int index = 0; index < replicas; index++) {
                            done &= Files.exists(directory.resolve(name + "-" + index + ".done"));
                            if (!children.get(index).isAlive() && children.get(index).exitValue() != 0)
                                throw new AssertionError(Files.readString(output.resolve(name + "-" + index + ".log")));
                        }
                        sampler.sample(name, children);
                        assertThat(System.nanoTime() < finishDeadline).as("traffic window deadline").isTrue();
                        if (!done) Thread.sleep(25);
                    } while (!done);
                    long elapsed = System.nanoTime() - start;
                    sampler.finish(name, totalClients);
                    Files.writeString(directory.resolve(name + ".release"), "release", java.nio.file.StandardOpenOption.CREATE_NEW);
                    for (int index = 0; index < replicas; index++) {
                        var process = children.get(index);
                        assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
                        var log = output.resolve(name + "-" + index + ".log");
                        assertThat(process.exitValue()).as(Files.readString(log)).isZero();
                        assertThat(Files.readString(log)).contains("NATIVE_REPLICA_TRAFFIC_OK");
                        String worker = name + "-" + index;
                        for (String suffix : List.of("-operations.csv", "-warmup-metrics.csv", "-measure-metrics.csv", "-config.txt"))
                            Files.copy(directory.resolve(worker + suffix), output.resolve(worker + suffix));
                        var metrics = Files.readAllLines(output.resolve(worker + "-measure-metrics.csv"));
                        for (String metric : List.of("provider_put", "provider_getBounded", "sql_acquire", "sql_usage")) {
                            var fields = metrics.stream().filter(line -> line.startsWith(metric + ",")).findFirst().orElseThrow().split(",");
                            assertThat(Long.parseLong(fields[1])).as(metric).isPositive();
                            assertThat(Long.parseLong(fields[2])).as(metric + " duration").isNotNegative();
                            assertThat(Long.parseLong(fields[3])).as(metric + " failures").isZero();
                        }
                        assertThat(metrics.stream().filter(line -> line.startsWith("sql_timeout,")).map(line -> Long.parseLong(line.split(",")[1])))
                                .allMatch(count -> count == 0);
                        var rows = Files.readAllLines(output.resolve(worker + "-operations.csv"));
                        assertThat(rows.stream().filter(line -> line.startsWith("measure,")).count()).isEqualTo(32L * clients);
                        for (String kind : List.of("read", "publish", "reject")) {
                            long expected = (kind.equals("read") ? 16 : kind.equals("publish") ? 12 : 4) * clients;
                            assertThat(rows.stream().filter(line -> line.startsWith("measure,") && line.split(",")[3].equals(kind)).count()).isEqualTo(expected);
                        }
                    }
                    windows.append(name).append(',').append(replicas).append(',').append(pool).append(',').append(clients)
                            .append(',').append(32L * totalClients).append(',').append(elapsed).append('\n');
                    Files.writeString(output.resolve("windows.csv"), windows);
                } catch (Exception | Error failure) { primary = failure; throw failure; }
                finally { stopChildren(children, primary); }
            }
        }
        System.out.println("NATIVE_BENCHMARK_OUTPUT=" + output);
    }

    private static void stopChildren(List<Process> children, Throwable primary) {
        var failures = new ArrayList<Throwable>();
        boolean interrupted = false;
        for (var child : children) {
            try { if (child.isAlive()) child.destroyForcibly(); }
            catch (RuntimeException failure) { failures.add(failure); }
        }
        for (var child : children) {
            try {
                if (!child.waitFor(10, TimeUnit.SECONDS)) failures.add(new AssertionError("Child did not terminate"));
            } catch (InterruptedException failure) {
                interrupted = true; failures.add(failure);
                // The interrupt was consumed by waitFor. Attempt a bounded reap
                // before moving on, and restore the interrupt after all children.
                try {
                    if (!child.waitFor(10, TimeUnit.SECONDS)) failures.add(new AssertionError("Interrupted child did not terminate"));
                } catch (InterruptedException again) { failures.add(again); }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
        if (!failures.isEmpty()) {
            var failure = new AssertionError("Native child cleanup failed");
            failures.forEach(failure::addSuppressed);
            if (primary != null) primary.addSuppressed(failure);
            else throw failure;
        }
    }

    private void run(ProcessBuilder builder, String classpath, String mode, String name) throws Exception {
        command(builder, classpath, mode, name);
        var process = builder.redirectErrorStream(true).redirectOutput(directory.resolve(name + ".log").toFile()).start();
        Throwable primary = null;
        try { finished(process, name, mode.toUpperCase(java.util.Locale.ROOT)); }
        catch (Exception | Error failure) { primary = failure; throw failure; }
        finally { stopChildren(List.of(process), primary); }
    }
    private void command(ProcessBuilder builder, String classpath, String mode, String name) {
        builder.command(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx512m", "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp", classpath,
                "ai.protomolt.proto.repo.container.ledger.NativeReplicaProbe", mode, directory.toString(), name);
    }
    private void finished(Process process, String name, String marker) throws Exception {
        assertThat(process.waitFor(90, TimeUnit.SECONDS)).as("native child %s exits", name).isTrue();
        var log = directory.resolve(name + ".log");
        assertThat(Files.size(log)).isLessThan(1_048_576);
        String result = Files.readString(log);
        assertThat(process.exitValue()).as(result).isZero();
        assertThat(result).contains("NATIVE_REPLICA_" + marker + "_OK");
    }
}
