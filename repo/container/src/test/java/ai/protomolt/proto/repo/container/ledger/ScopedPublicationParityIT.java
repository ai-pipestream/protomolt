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
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the scoped publication authorization parity probe on the production
 * JAR host: real PostgreSQL and versioned LocalStack S3, the journaled runtime
 * and facade, and an authenticated in-process gRPC channel, all in a fresh
 * standard JVM whose classpath is exactly the observed admission/transport
 * runtime plus the compiled probe. The scenario matrix lives in
 * {@code runtime-inventory/ScopedPublicationProbe.java}; this harness only
 * assembles the runtime, compiles and launches it, and checks its markers.
 */
class ScopedPublicationParityIT {
    @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path directory;

    @Test void scopedPublicationAuthorizationIsEquivalentAcrossLibraryAndAuthenticatedGrpc() throws Exception {
        String bundleProperty = System.getProperty("protomolt.test.admissionRuntimeBundle");
        String hostProperty = System.getProperty("protomolt.test.storageHostClasspath");
        assertThat(bundleProperty).as("Run :protomolt-repo-container:scopedPublicationTest").isNotBlank();
        assertThat(hostProperty).isNotBlank();
        var bundle = Path.of(bundleProperty);
        var inventory = DocumentRuntimeInventory.read(bundle, () -> {});
        var jars = new LinkedHashMap<String, Path>();
        inventory.identities().forEach(artifact -> jars.put(artifact.getArtifactSha256(),
                bundle.resolve("artifacts/" + artifact.getArtifactSha256() + ".jar")));
        // The production host shares admission dependencies. Include each exact artifact once, by bytes.
        for (String entry : hostProperty.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            var path = Path.of(entry);
            var identity = DocumentRuntimeArtifact.observe("storage-host", path,
                    new DocumentRuntimeArtifact.Limits(Files.size(path), 65536, 64), () -> {});
            jars.putIfAbsent(identity.getArtifactSha256(), path);
        }
        String classpath = String.join(java.io.File.pathSeparator, jars.values().stream().map(Path::toString).toList());
        var classes = Files.createDirectory(directory.resolve("classes"));
        var source = directory.resolve("ScopedPublicationProbe.java");
        try (var input = getClass().getResourceAsStream("/runtime-inventory/ScopedPublicationProbe.java")) {
            assertThat(input).isNotNull();
            Files.copy(input, source);
        }
        var compilerErrors = new java.io.ByteArrayOutputStream();
        int compiled = javax.tools.ToolProvider.getSystemJavaCompiler().run(null, null, compilerErrors,
                "-proc:none", "-classpath", classpath, "-d", classes.toString(), source.toString());
        assertThat(compiled).as("probe compilation: %s", compilerErrors.toString()).isZero();
        var probe = directory.resolve("scoped-publication-probe.jar");
        try (var output = new java.util.jar.JarOutputStream(Files.newOutputStream(probe)); var paths = Files.walk(classes)) {
            for (var file : paths.filter(Files::isRegularFile).sorted().toList()) {
                output.putNextEntry(new java.util.jar.JarEntry(classes.relativize(file).toString().replace(java.io.File.separatorChar, '/')));
                Files.copy(file, output);
                output.closeEntry();
            }
        }
        try (var postgres = new PostgreSQLContainer("postgres:18-alpine");
                var storage = new AssessmentStorageBackend("localstack")) {
            postgres.start();
            storage.start();
            var log = directory.resolve("scoped-publication.log");
            var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    classpath + java.io.File.pathSeparator + probe,
                    "ai.protomolt.proto.repo.container.ledger.ScopedPublicationProbe", bundle.toString());
            builder.environment().put("PROTOMOLT_TEST_JDBC", postgres.getJdbcUrl());
            builder.environment().put("PROTOMOLT_TEST_USER", postgres.getUsername());
            builder.environment().put("PROTOMOLT_TEST_PASSWORD", postgres.getPassword());
            builder.environment().put("PROTOMOLT_TEST_S3_ENDPOINT", storage.getEndpoint().toString());
            builder.environment().put("PROTOMOLT_TEST_S3_REGION", storage.getRegion());
            builder.environment().put("PROTOMOLT_TEST_S3_ACCESS", storage.getAccessKey());
            builder.environment().put("PROTOMOLT_TEST_S3_SECRET", storage.getSecretKey());
            var process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                assertThat(process.waitFor(600, TimeUnit.SECONDS))
                        .as("Scoped publication parity probe completed; log: %s", log).isTrue();
                assertThat(Files.size(log)).isLessThan(1_048_576);
                String result = Files.readString(log);
                assertThat(process.exitValue()).as(result).isZero();
                var markers = new ArrayList<String>();
                for (String via : List.of("LIBRARY", "GRPC")) {
                    for (String scenario : List.of("TYPED", "OPAQUE", "KEY_SEPARATION", "ROTATION", "UNBOUND",
                            "ACCOUNT", "COMMAND", "MIXED_ATOMICITY", "GRANT_REVOCATION", "GRANT_EXPIRY",
                            "REPLAY_ROTATION", "READ_REVOCATION", "UPLOAD_HELD", "COMMIT_WINS",
                            "SHARED_KEY_CONCURRENCY", "RETRY", "CONTRACT_VS_AUTHZ")) {
                        markers.add("SCOPED_PUBLICATION_" + scenario + "_" + via + "_OK");
                    }
                    for (String ordering : List.of("REVOKED", "HELD_LIVE", "EXPIRED")) {
                        markers.add("SCOPED_PUBLICATION_FINAL_CHECK_" + ordering + "_" + via + "_OK");
                    }
                }
                markers.add("SCOPED_PUBLICATION_TRANSPORT_FAIL_CLOSED_OK");
                markers.add("SCOPED_PUBLICATION_PARITY_OK");
                for (String marker : markers) {
                    assertThat(result).as("probe log contains %s", marker).contains(marker);
                }
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
        }
    }
}
