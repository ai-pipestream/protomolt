package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Streamed put, openStream and openSnapshot over rediss:// on the selected server.
 * The certificate names localhost only, so a 127.0.0.1 URI must fail hostname verification.
 */
@Testcontainers
class RedisTlsStreamingIT {
    private static final int TLS_PORT = 6380;
    private static final char[] PASSWORD = "changeit".toCharArray();
    private static final Path TLS = createMaterial();
    @Container static final GenericContainer<?> SERVER = server();

    @Test void streamedPutOpenStreamAndSnapshotUseOneContextAndRejectTheWrongHost() throws Exception {
        String match = "rediss://localhost:" + SERVER.getMappedPort(TLS_PORT);
        String wrongHost = "rediss://127.0.0.1:" + SERVER.getMappedPort(TLS_PORT);
        withTrustStore(() -> {
            int before = RedisTls.contextsCreated();
            byte[] body = {1, 2, 3, 4, 5};
            var spec = new BlobStore.PutSpec("tls", "object", "application/octet-stream", Map.of("k", "v"), null);
            try (var store = new RedisBlobStore(new RedisBlobStoreConfig(match, 0, 0, UUID.randomUUID().toString()))) {
                var written = store.put(spec, new ByteArrayInputStream(body), body.length);
                try (var stream = store.openStream("tls", "object", 1, 3)) {
                    assertThat(stream.readAllBytes()).containsExactly(2, 3, 4);
                }
                try (var snapshot = store.openSnapshot("tls", "object")) {
                    assertThat(snapshot.metadata().eTag()).isEqualTo(written.eTag());
                    assertThat(snapshot.metadata().length()).isEqualTo(body.length);
                    assertThat(snapshot.metadata().attributes()).containsEntry("k", "v");
                    assertThat(snapshot.stream().readAllBytes()).containsExactly(body);
                }
                assertThat(RedisTls.contextsCreated()).isEqualTo(before + 1);
            }
            int wrongBefore = RedisTls.contextsCreated();
            try (var wrong = new RedisBlobStore(new RedisBlobStoreConfig(wrongHost, 0, 0, UUID.randomUUID().toString()))) {
                Throwable failure = catchThrowable(() -> wrong.openStream("tls", "object", 0, -1));
                assertThat(failure).isNotNull();
                assertThat(wrongHostCertificate(failure)).as(chain(failure)).isTrue();
            }
            assertThat(RedisTls.contextsCreated()).isEqualTo(wrongBefore + 1);
        });
    }

    private static boolean wrongHostCertificate(Throwable failure) {
        boolean host = false;
        boolean nameMismatch = false;
        Throwable current = failure;
        for (int depth = 0; depth < 8 && current != null; depth++) {
            String message = current.getMessage();
            if (message != null && message.contains("127.0.0.1")) host = true;
            if (message != null && message.contains("subject alternative")) nameMismatch = true;
            current = current.getCause();
        }
        return host && nameMismatch;
    }

    private static String chain(Throwable failure) {
        var text = new StringBuilder();
        Throwable current = failure;
        for (int depth = 0; depth < 8 && current != null; depth++) {
            if (depth > 0) text.append(" | ");
            text.append(current.getClass().getName()).append(": ").append(current.getMessage());
            current = current.getCause();
        }
        return text.toString();
    }

    private static void withTrustStore(Work work) throws Exception {
        String storeProp = "javax.net.ssl.trustStore";
        String passwordProp = "javax.net.ssl.trustStorePassword";
        String typeProp = "javax.net.ssl.trustStoreType";
        String previousStore = System.getProperty(storeProp);
        String previousPassword = System.getProperty(passwordProp);
        String previousType = System.getProperty(typeProp);
        System.setProperty(storeProp, TLS.resolve("trust.p12").toString());
        System.setProperty(passwordProp, new String(PASSWORD));
        System.setProperty(typeProp, "PKCS12");
        try { work.run(); }
        finally {
            restore(storeProp, previousStore);
            restore(passwordProp, previousPassword);
            restore(typeProp, previousType);
        }
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }

    private static GenericContainer<?> server() {
        return RedisTestServers.container()
                .withExposedPorts(RedisTestServers.PORT, TLS_PORT)
                .withCopyFileToContainer(MountableFile.forHostPath(TLS.resolve("redis.crt"), 0644), "/tls/redis.crt")
                .withCopyFileToContainer(MountableFile.forHostPath(TLS.resolve("redis.key"), 0644), "/tls/redis.key")
                .withCommand(RedisTestServers.server(),
                        "--bind", "0.0.0.0",
                        "--protected-mode", "no",
                        "--port", Integer.toString(RedisTestServers.PORT),
                        "--tls-port", Integer.toString(TLS_PORT),
                        "--tls-cert-file", "/tls/redis.crt",
                        "--tls-key-file", "/tls/redis.key",
                        "--tls-auth-clients", "no");
    }

    private static Path createMaterial() {
        try {
            Path dir = Files.createTempDirectory("redis-tls-");
            Path store = dir.resolve("server.p12");
            String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
            run(keytool, "-genkeypair", "-alias", "redis", "-keyalg", "RSA", "-keysize", "2048",
                    "-validity", "2", "-storetype", "PKCS12", "-keystore", store.toString(),
                    "-storepass", new String(PASSWORD), "-keypass", new String(PASSWORD),
                    "-dname", "CN=localhost", "-ext", "san=dns:localhost");
            KeyStore keys = KeyStore.getInstance("PKCS12");
            try (InputStream input = Files.newInputStream(store)) { keys.load(input, PASSWORD); }
            writePem(dir.resolve("redis.crt"), "CERTIFICATE", keys.getCertificate("redis").getEncoded());
            writePem(dir.resolve("redis.key"), "PRIVATE KEY",
                    ((PrivateKey) keys.getKey("redis", PASSWORD)).getEncoded());
            Path trust = dir.resolve("trust.p12");
            run(keytool, "-importcert", "-alias", "redis", "-file", dir.resolve("redis.crt").toString(),
                    "-keystore", trust.toString(), "-storetype", "PKCS12",
                    "-storepass", new String(PASSWORD), "-noprompt");
            return dir;
        } catch (Exception failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private static void writePem(Path path, String kind, byte[] der) throws IOException {
        String body = Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(der);
        Files.writeString(path, "-----BEGIN " + kind + "-----\n" + body + "\n-----END " + kind + "-----\n");
    }

    private static void run(String... command) throws IOException, InterruptedException {
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = process.getInputStream().readAllBytes();
        int status = process.waitFor();
        if (status != 0) {
            throw new IOException(command[0] + " exited " + status + ": "
                    + new String(output, java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @FunctionalInterface
    private interface Work { void run() throws Exception; }
}
