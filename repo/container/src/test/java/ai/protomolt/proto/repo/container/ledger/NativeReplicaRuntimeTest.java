package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
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
                var entry = new java.util.jar.JarEntry(classes.relativize(file).toString().replace(java.io.File.separatorChar, '/'));
                entry.setTime(315532800000L); // fixed timestamp: identical classes give an identical probe JAR hash
                output.putNextEntry(entry);
                Files.copy(file, output); output.closeEntry();
            }
        }
        try (var postgres = new PostgreSQLContainer("postgres:18-alpine");
                var storage = new AssessmentStorageBackend("rustfs")) {
            if (Boolean.getBoolean("protomolt.test.nativeBenchmark")) postgres.withCommand("postgres", "-c",
                    "shared_preload_libraries=pg_stat_statements", "-c", "pg_stat_statements.track=top",
                    "-c", "track_io_timing=on", "-c", "track_wal_io_timing=on"); // benchmark observation only
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
            if (Boolean.getBoolean("protomolt.test.nativeBenchmark")) benchmark(builder, childClasspath, postgres, jars, probe);
        }
    }


    /** Window plans: every topology in a plan appears an even number of times, mirrored around the midpoint. */
    static String[] plan(String name) {
        return switch (name) {
            case "mirrored" -> new String[] {"f1", "a4", "f2", "a1", "f4", "a2", "a2", "f4", "a1", "f2", "a4", "f1"};
            case "scaleout" -> new String[] {"a2", "a4", "f2", "f4", "f4", "f2", "a4", "a2"};
            default -> throw new IllegalArgumentException("Benchmark plan must be mirrored or scaleout");
        };
    }

    /** Heap per replica from a fixed aggregate; zero keeps the original 512 MiB per worker. */
    static int heapPerReplica(int totalHeapMiB, int replicas) {
        if (totalHeapMiB == 0) return 512;
        if (totalHeapMiB < 512 || totalHeapMiB > 8192 || totalHeapMiB % 4 != 0)
            throw new IllegalArgumentException("Total heap must be zero or a multiple of four MiB from 512 to 8192");
        int heap = totalHeapMiB / replicas;
        if (heap < 128) throw new IllegalArgumentException("Per-replica heap below 128 MiB");
        return heap;
    }

    private void benchmark(ProcessBuilder builder, String classpath, PostgreSQLContainer postgres, Map<String, Path> artifacts, Path probe) throws Exception {
        String journaled = System.getProperty("protomolt.test.nativeBenchmarkJournaled", "false");
        if (!List.of("true", "false").contains(journaled)) throw new IllegalArgumentException("Journaled mode must be true or false");
        builder.environment().put("PROTOMOLT_NATIVE_JOURNALED", journaled);
        String trace = System.getProperty("protomolt.test.nativeBenchmarkTrace", "false");
        if (!List.of("true", "false").contains(trace)) throw new IllegalArgumentException("Trace mode must be true or false");
        builder.environment().put("PROTOMOLT_NATIVE_TRACE", trace);
        String planName = System.getProperty("protomolt.test.nativeBenchmarkPlan", "mirrored");
        String[] plan = plan(planName);
        int totalClients = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkClients", "4"));
        if (totalClients != 4 && totalClients != 8 && totalClients != 16 && totalClients != 32)
            throw new IllegalArgumentException("Benchmark client count must be 4, 8, 16 or 32");
        // The child workload admits at most sixteen clients and 384,000,000 budget bytes per
        // process, so thirty-two total clients need a plan without a one-replica window.
        if (totalClients == 32 && java.util.Arrays.stream(plan).anyMatch(config -> config.endsWith("1")))
            throw new IllegalArgumentException("32 clients require the scaleout plan");
        int totalReadSlots = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkReadSlots", "0"));
        if (totalReadSlots < 0 || totalReadSlots > 64 || totalReadSlots % 4 != 0)
            throw new IllegalArgumentException("Total reader slots must be zero or a multiple of four up to 64");
        int totalReadHandles = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkReadHandles", "0"));
        if (totalReadHandles < 0 || totalReadHandles > 256 || totalReadHandles % 4 != 0)
            throw new IllegalArgumentException("Total read handles must be zero or a multiple of four up to 256");
        long totalBudget = Long.parseLong(System.getProperty("protomolt.test.nativeBenchmarkBudgetBytes", "0"));
        if (totalBudget < 0 || totalBudget > 768_000_000 || totalBudget % 4 != 0)
            throw new IllegalArgumentException("Total payload budget must be zero or a multiple of four up to 768000000");
        int totalHeap = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkHeapMiB", "0"));
        // Backend-state sampling is active SQL work whose cost grows with connection count; the
        // interval is recorded so a control run can show its effect. Zero disables it.
        int sampleMillis = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkSampleMillis", "25"));
        if (sampleMillis < 0 || sampleMillis > 5000) throw new IllegalArgumentException("Sample interval must be 0 (off) to 5000 ms");
        for (String config : plan) heapPerReplica(totalHeap, Integer.parseInt(config.substring(1)));
        Path output = Path.of(System.getProperty("protomolt.test.nativeBenchmarkOutput")).resolve(java.util.UUID.randomUUID().toString());
        int payloadBytes = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkPayloadBytes", "0"));
        int iterations = Integer.parseInt(System.getProperty("protomolt.test.nativeBenchmarkIterations", "32"));
        if (payloadBytes != 0 && (payloadBytes < 256 || payloadBytes > 786_432))
            throw new IllegalArgumentException("Payload must be zero (original small workload) or 256 to 786432 bytes");
        if (iterations < 8 || iterations > 256 || iterations % 8 != 0)
            throw new IllegalArgumentException("Measured iterations must be a multiple of eight from 8 to 256");
        builder.environment().put("PROTOMOLT_NATIVE_PAYLOAD_BYTES", Integer.toString(payloadBytes));
        builder.environment().put("PROTOMOLT_NATIVE_ITERATIONS", Integer.toString(iterations));
        Files.createDirectories(output);
        Files.writeString(output.resolve("environment.txt"), "java=" + System.getProperty("java.version")
                + "\nos=" + System.getProperty("os.name") + " " + System.getProperty("os.arch")
                + "\nloadavg=" + Files.readString(Path.of("/proc/loadavg")).trim()
                + "\nclients=" + totalClients + "\ntotal_read_slots=" + totalReadSlots
                + "\ntotal_read_handles=" + totalReadHandles + "\nzero_read_handles_means=32 per worker"
                + "\nzero_read_slots_means=8 per worker\nworker_heap_limit=" + (totalHeap == 0 ? "512MiB" : totalHeap + "MiB total divided by replicas")
                + "\ntotal_heap_mib=" + totalHeap + "\nzero_heap_means=512 per worker\npayload_string_bytes=" + payloadBytes
                + "\nzero_payload_means=original small workload\niterations_per_client=" + iterations
                + "\ntotal_payload_budget_bytes=" + totalBudget + "\nzero_budget_means=128000000 per worker"
                + "\njournaled=" + journaled
                + "\ntrace=" + trace
                + "\nplan=" + planName
                + "\nsample_millis=" + sampleMillis + "\nzero_sample_means=no backend-state sampling"
                + "\nNo host isolation or container CPU/memory limits; trusted internal Java path.\n");
        // Immutable identity of what ran: every production artifact by content hash, the compiled probe, and both images.
        var identity = new StringBuilder();
        artifacts.forEach((sha, path) -> identity.append(sha).append("  ").append(path.getFileName()).append('\n'));
        identity.append(sha256(probe)).append("  ").append(probe.getFileName()).append(" (compiled test probe, fixed entry timestamps)\n");
        try (var sources = Files.list(directory)) {
            for (var source : sources.filter(path -> path.toString().endsWith(".java")).sorted().toList())
                identity.append(sha256(source)).append("  ").append(source.getFileName()).append(" (probe source)\n");
        }
        identity.append(sha256(Path.of(RepositoryScalingSampler.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                .resolve("ai/protomolt/proto/repo/container/ledger/RepositoryScalingSampler.class"))).append("  RepositoryScalingSampler.class (parent sampler)\n");
        identity.append(RepositoryScalingSampler.describeImage(postgres.getDockerImageName())).append('\n');
        String rustfsId = RepositoryScalingSampler.containerIdByImage("rustfs");
        identity.append(RepositoryScalingSampler.describeImage(DockerClientFactory.instance().client()
                .inspectContainerCmd(rustfsId).exec().getConfig().getImage())).append('\n');
        Files.writeString(output.resolve("source-identity.txt"), identity);
        try (var sampler = new NativeTrafficSampler(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), output);
                var scaling = new RepositoryScalingSampler(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), output,
                        new LinkedHashMap<>(Map.of("postgres", postgres.getContainerId(), "rustfs", rustfsId)))) {
            Files.writeString(output.resolve("environment.txt"), scaling.describe(), java.nio.file.StandardOpenOption.APPEND);
            var windows = new StringBuilder("window,replicas,pool_per_replica,clients_per_replica,operations,inclusive_nanos,heap_mib\n");
            var processes = new StringBuilder("window,index,pid,heap_mib,exit\n");
            int window = 0;
            for (String config : plan) {
                String name = String.format(java.util.Locale.ROOT, "t%02d", window++);
                int replicas = Integer.parseInt(config.substring(1)), clients = totalClients / replicas;
                int pool = config.startsWith("f") ? 8 / replicas : 8;
                int heap = heapPerReplica(totalHeap, replicas);
                builder.environment().put("PROTOMOLT_NATIVE_POOL", Integer.toString(pool));
                builder.environment().put("PROTOMOLT_NATIVE_CLIENTS", Integer.toString(clients));
                builder.environment().put("PROTOMOLT_NATIVE_BUDGET_BYTES", Long.toString(totalBudget == 0 ? 128_000_000 : totalBudget / replicas));
                builder.environment().put("PROTOMOLT_NATIVE_READ_SLOTS", Integer.toString(totalReadSlots == 0 ? 8 : totalReadSlots / replicas));
                builder.environment().put("PROTOMOLT_NATIVE_READ_HANDLES", Integer.toString(totalReadHandles == 0 ? 32 : totalReadHandles / replicas));
                var children = new ArrayList<Process>();
                Throwable primary = null;
                try {
                    long readyDeadline = System.nanoTime() + java.time.Duration.ofSeconds(90).toNanos();
                    for (int index = 0; index < replicas; index++) {
                        String worker = name + "-" + index;
                        command(builder, classpath, "traffic", worker, heap);
                        children.add(builder.redirectErrorStream(true).redirectOutput(output.resolve(worker + ".log").toFile()).start());
                    }
                    for (int index = 0; index < replicas; index++) {
                        while (!Files.exists(directory.resolve(name + "-" + index + ".ready"))) {
                            if (!children.get(index).isAlive()) throw new AssertionError(Files.readString(output.resolve(name + "-" + index + ".log")));
                            assertThat(System.nanoTime() < readyDeadline).as("traffic warmup readiness").isTrue();
                            Thread.sleep(10);
                        }
                        assertThat(Files.readString(directory.resolve(name + "-" + index + "-config.txt")))
                                .contains("\njournaled=" + journaled + "\n", "\ntrace=" + trace + "\n",
                                        "\npayload_budget_bytes=" + (totalBudget == 0 ? 128_000_000 : totalBudget / replicas) + "\n");
                    }
                    sampler.begin(name);
                    scaling.begin(name, children);
                    long start = System.nanoTime();
                    Files.writeString(directory.resolve(name + ".go"), "go", java.nio.file.StandardOpenOption.CREATE_NEW);
                    long finishDeadline = start + java.time.Duration.ofSeconds(90).toNanos();
                    boolean done;
                    long lastSample = start - sampleMillis * 1_000_000L; // first poll samples at once
                    do {
                        done = true;
                        for (int index = 0; index < replicas; index++) {
                            done &= Files.exists(directory.resolve(name + "-" + index + ".done"));
                            if (!children.get(index).isAlive() && children.get(index).exitValue() != 0)
                                throw new AssertionError(Files.readString(output.resolve(name + "-" + index + ".log")));
                        }
                        if (sampleMillis > 0 && System.nanoTime() - lastSample >= sampleMillis * 1_000_000L) { sampler.sample(name, children); lastSample = System.nanoTime(); }
                        assertThat(System.nanoTime() < finishDeadline).as("traffic window deadline").isTrue();
                        if (!done) Thread.sleep(25);
                    } while (!done);
                    long elapsed = System.nanoTime() - start;
                    scaling.markDone(name, children);
                    sampler.finish(name, totalClients, iterations);
                    // Cumulative PostgreSQL statistics flush at most once per second per backend.
                    // The settle is outside the inclusive window and before the children are released.
                    Thread.sleep(1200);
                    scaling.finish(name, children);
                    Files.writeString(directory.resolve(name + ".release"), "release", java.nio.file.StandardOpenOption.CREATE_NEW);
                    for (int index = 0; index < replicas; index++) {
                        var process = children.get(index);
                        assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
                        var log = output.resolve(name + "-" + index + ".log");
                        assertThat(process.exitValue()).as(Files.readString(log)).isZero();
                        assertThat(Files.readString(log)).contains("NATIVE_REPLICA_TRAFFIC_OK");
                        processes.append(name).append(',').append(index).append(',').append(process.pid()).append(',').append(heap)
                                .append(',').append(process.exitValue()).append('\n');
                        String worker = name + "-" + index;
                        for (String suffix : List.of("-operations.csv", "-warmup-metrics.csv", "-measure-metrics.csv", "-config.txt"))
                            Files.copy(directory.resolve(worker + suffix), output.resolve(worker + suffix));
                        if (trace.equals("true")) {
                            for (String phase : List.of("warmup", "measure"))
                                Files.copy(directory.resolve(worker + "-" + phase + "-trace.csv"), output.resolve(worker + "-" + phase + "-trace.csv"));
                            var traced = Files.readAllLines(output.resolve(worker + "-measure-trace.csv"));
                            for (String operation : List.of("read", "publish", "reject", "replay"))
                                assertThat(traced.stream().filter(line -> line.startsWith(operation + ",sql_acquire,"))
                                        .mapToLong(line -> Long.parseLong(line.split(",")[3])).sum())
                                        .as(operation + " traced acquisitions").isPositive();
                            for (String operation : List.of("publish", "reject", "replay")) {
                                assertThat(traced.stream().filter(line -> line.startsWith(operation + ",jdbc_commit,"))
                                        .mapToLong(line -> Long.parseLong(line.split(",")[3])).sum())
                                        .as(operation + " actual JDBC commits").isPositive();
                                assertThat(traced.stream().filter(line -> line.startsWith(operation + ",jdbc_execute"))
                                        .mapToLong(line -> Long.parseLong(line.split(",")[3])).sum())
                                        .as(operation + " actual JDBC executions").isPositive();
                            }
                            for (String line : traced.subList(1, traced.size()))
                                assertThat(Long.parseLong(line.split(",")[5])).as("trace failures").isZero();
                        }
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
                        assertThat(rows.stream().filter(line -> line.startsWith("measure,")).count()).isEqualTo((long) iterations * clients);
                        for (String kind : List.of("read", "publish", "reject")) {
                            long expected = (kind.equals("read") ? iterations / 2 : kind.equals("publish") ? iterations * 3 / 8 : iterations / 8) * clients;
                            assertThat(rows.stream().filter(line -> line.startsWith("measure,") && line.split(",")[3].equals(kind)).count()).isEqualTo(expected);
                        }
                    }
                    windows.append(name).append(',').append(replicas).append(',').append(pool).append(',').append(clients)
                            .append(',').append((long) iterations * totalClients).append(',').append(elapsed).append(',').append(heap).append('\n');
                    Files.writeString(output.resolve("windows.csv"), windows);
                    Files.writeString(output.resolve("processes.csv"), processes);
                } catch (Exception | Error failure) { primary = failure; throw failure; }
                finally { stopChildren(children, primary); }
            }
        }
        System.out.println("NATIVE_BENCHMARK_OUTPUT=" + output);
    }

    private static String sha256(Path file) throws Exception {
        var digest = java.security.MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(file)) {
            var buffer = new byte[65536];
            for (int read; (read = input.read(buffer)) > 0; ) digest.update(buffer, 0, read);
        }
        return java.util.HexFormat.of().formatHex(digest.digest());
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
    private void command(ProcessBuilder builder, String classpath, String mode, String name) { command(builder, classpath, mode, name, 512); }
    private void command(ProcessBuilder builder, String classpath, String mode, String name, int heapMiB) {
        builder.command(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx" + heapMiB + "m", "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp", classpath,
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
