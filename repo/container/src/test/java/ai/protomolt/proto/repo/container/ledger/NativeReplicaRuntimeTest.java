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
        for (String name : List.of("ObservedAssessmentProbe", "NativeReplicaProbe")) {
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
            for (int replicas : new int[] {1, 2, 4}) {
                var children = new ArrayList<Process>();
                long deadline = System.nanoTime() + java.time.Duration.ofSeconds(45).toNanos();
                Throwable primary = null;
                try {
                    for (int index = 0; index < replicas; index++) {
                        String worker = "r" + replicas + "-" + index;
                        command(builder, childClasspath, "write", worker);
                        children.add(builder.redirectErrorStream(true).redirectOutput(directory.resolve(worker + ".log").toFile()).start());
                    }
                    for (int index = 0; index < children.size(); index++) {
                        String worker = "r" + replicas + "-" + index;
                        while (!Files.exists(directory.resolve(worker + ".ready"))) {
                            if (!children.get(index).isAlive()) finished(children.get(index), worker, "WRITE");
                            assertThat(System.nanoTime() < deadline).as("writer readiness barrier").isTrue();
                            Thread.sleep(10);
                        }
                    }
                    Files.writeString(directory.resolve("r" + replicas + ".go"), "go", java.nio.file.StandardOpenOption.CREATE_NEW);
                    for (int index = 0; index < children.size(); index++) {
                        String worker = "r" + replicas + "-" + index;
                        finished(children.get(index), worker, "WRITE");
                    }
                } catch (Exception | Error failure) {
                    primary = failure; throw failure;
                } finally {
                    stopChildren(children, primary);
                }
                run(builder, childClasspath, "read", "read-" + replicas);
            }
            try (var files = Files.list(directory)) {
                assertThat(files.filter(p -> p.toString().endsWith(".result")).count()).isEqualTo(14);
            }
            try (var files = Files.list(directory)) {
                assertThat(files.filter(p -> p.toString().endsWith(".rejection")).count()).isEqualTo(7);
            }
        }
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
