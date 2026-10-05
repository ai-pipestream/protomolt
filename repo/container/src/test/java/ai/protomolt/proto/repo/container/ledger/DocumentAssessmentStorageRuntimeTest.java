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
class DocumentAssessmentStorageRuntimeTest {
    @TempDir Path directory;

    @Test void observedAssessmentAndSqlRunTogetherOnProductionJars() throws Exception {
        String bundleProperty = System.getProperty("protomolt.test.admissionRuntimeBundle");
        String hostProperty = System.getProperty("protomolt.test.storageHostClasspath");
        assertThat(bundleProperty).as("Run :protomolt-repo-container:admissionStorageTest").isNotBlank();
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
        for (String name : List.of("ObservedAssessmentProbe", "AssessmentCreationProbe", "AssessmentCaptureFaultProbe", "AssessmentProviderProbe", "AssessmentMixedReuseProbe", "AssessmentReplayInputsProbe", "AssessmentOperationReplayProbe", "AssessmentStorageProbe", "AssessmentRestartProbe")) {
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
                var storage = new org.testcontainers.localstack.LocalStackContainer("localstack/localstack:3.8")
                        .withServices("s3")) {
            postgres.start();
            storage.start();
            var log = directory.resolve("host.log");
            var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    classpath + java.io.File.pathSeparator + probe,
                    "ai.protomolt.proto.repo.container.ledger.AssessmentStorageProbe", bundle.toString());
            builder.environment().put("PROTOMOLT_TEST_JDBC", postgres.getJdbcUrl());
            builder.environment().put("PROTOMOLT_TEST_USER", postgres.getUsername());
            builder.environment().put("PROTOMOLT_TEST_PASSWORD", postgres.getPassword());
            builder.environment().put("PROTOMOLT_TEST_S3_ENDPOINT", storage.getEndpoint().toString());
            builder.environment().put("PROTOMOLT_TEST_S3_REGION", storage.getRegion());
            builder.environment().put("PROTOMOLT_TEST_S3_ACCESS", storage.getAccessKey());
            builder.environment().put("PROTOMOLT_TEST_S3_SECRET", storage.getSecretKey());
            var request = directory.resolve("restart-request.properties");
            builder.environment().put("PROTOMOLT_TEST_RESTART_REQUEST", request.toString());
            var process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                assertThat(process.waitFor(60, TimeUnit.SECONDS)).as("Observed SQL host completed").isTrue();
                assertThat(Files.size(log)).isLessThan(1_048_576);
                String result = Files.readString(log);
                assertThat(process.exitValue()).as(result).isZero();
                assertThat(result).contains("OBSERVED_SQL_HOST_OK");
                assertThat(result).contains("OBSERVED_ASSESSMENT_CREATION_OK", "CLOSED_SCOPE_ASSESSMENT_ACK_OK");
                assertThat(result).contains("ASSESSMENT_CAPTURE_FAULTS_OK");
                assertThat(result).contains("ASSESSMENT_PROVIDER_READS_OK");
                assertThat(result).contains("ASSESSMENT_MIXED_REUSE_OK");
                assertThat(result).contains("ASSESSMENT_SOURCE_ADVANCED_OK");
                assertThat(result).contains("ASSESSMENT_REPLAY_INPUTS_OK");
                assertThat(result).contains("ASSESSMENT_OPERATION_REPLAY_OK");
                assertThat(result).contains("ASSESSMENT_MIXED_REPLAY_OK");
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
            assertThat(Files.isRegularFile(request)).as("Writer persisted restart identities").isTrue();
            var restartLog = directory.resolve("restart.log");
            builder.command(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    classpath + java.io.File.pathSeparator + probe,
                    "ai.protomolt.proto.repo.container.ledger.AssessmentRestartProbe", request.toString());
            var restarted = builder.redirectOutput(restartLog.toFile()).start();
            try {
                assertThat(restarted.waitFor(75, TimeUnit.SECONDS)).as("Restart acknowledgement completed").isTrue();
                assertThat(Files.size(restartLog)).isLessThan(1_048_576);
                String result = Files.readString(restartLog);
                assertThat(restarted.exitValue()).as(result).isZero();
                assertThat(result).contains("RESTARTED_ASSESSMENT_ACK_OK", "RESTARTED_ASSESSMENT_REVOCATION_OK",
                        "RESTARTED_ASSESSMENT_CAPTURE_REVOCATION_OK",
                        "RESTARTED_ASSESSMENT_OWNER_FENCE_OK");
            } finally {
                if (restarted.isAlive()) {
                    restarted.destroyForcibly();
                    assertThat(restarted.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
        }
    }
}
