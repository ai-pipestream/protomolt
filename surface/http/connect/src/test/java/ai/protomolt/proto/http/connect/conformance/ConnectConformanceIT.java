package ai.protomolt.proto.http.connect.conformance;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Connect conformance suite against the gateway: the pinned {@code connectconformance}
 * runner, in server mode, drives {@link ConformanceServer} with every test case its
 * {@code server-config.yaml} features select (Connect over HTTP/1.1, binary protobuf, identity
 * compression, unary and server streaming) and fails on any case the gateway gets wrong.
 *
 * <p>The runner is the project's Linux x86-64 release, fetched once into the module's build
 * directory and checked against its published SHA-256 before it runs.
 */
class ConnectConformanceIT {

    private static final String VERSION = "1.0.6";
    private static final String ARCHIVE = "connectconformance-v" + VERSION + "-Linux-x86_64.tar.gz";
    private static final String SHA256 = "803a9ed1778583f9b98f44b4b77ab06897d1027183ef2064de32a0fb7d688ab0";
    private static final URI DOWNLOAD = URI.create(
            "https://github.com/connectrpc/conformance/releases/download/v" + VERSION + "/" + ARCHIVE);

    @Test
    void theGatewayPassesTheConnectConformanceSuite() throws Exception {
        // The pinned runner is the Linux x86-64 release; any other platform
        // skips the suite instead of failing the build. CI runs Linux amd64,
        // so the suite always runs where it gates merges.
        org.junit.jupiter.api.Assumptions.assumeTrue(
                System.getProperty("os.name").startsWith("Linux")
                        && List.of("amd64", "x86_64").contains(System.getProperty("os.arch")),
                "connectconformance v" + VERSION + " is pinned to Linux x86-64; skipping on "
                        + System.getProperty("os.name") + "/" + System.getProperty("os.arch"));
        Path runner = runner();
        Path config = Files.createTempFile("connect-conformance", ".yaml");
        try (InputStream in = ConnectConformanceIT.class.getResourceAsStream("server-config.yaml")) {
            assertThat(in).as("server-config.yaml on the test classpath").isNotNull();
            Files.copy(in, config, StandardCopyOption.REPLACE_EXISTING);
        }
        String classpath = System.getProperty("protomolt.test.runtimeClasspath");
        assertThat(classpath).as("protomolt.test.runtimeClasspath, set by the build").isNotBlank();
        String java = ProcessHandle.current().info().command().orElseThrow();
        Path log = Files.createTempFile("connect-conformance", ".log");
        Process process = new ProcessBuilder(List.of(runner.toString(), "--mode", "server",
                "--conf", config.toString(), "-v", "--",
                java, "-cp", classpath, ConformanceServer.class.getName()))
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        boolean finished = process.waitFor(15, TimeUnit.MINUTES);
        if (!finished) {
            process.destroyForcibly();
        }
        String output = Files.readString(log, StandardCharsets.UTF_8);
        assertThat(finished).as("the runner finished within 15 minutes:\n%s", output).isTrue();
        assertThat(process.exitValue()).as("connectconformance exit status:\n%s", output).isZero();
        assertThat(output).contains(" passed, 0 failed").doesNotContain("\n0 passed");
    }

    /** The runner binary, fetched and verified on first use. */
    private static Path runner() throws Exception {
        Path directory = Path.of("build", "connect-conformance", VERSION).toAbsolutePath();
        Path binary = directory.resolve("connectconformance");
        if (Files.isExecutable(binary)) {
            return binary;
        }
        HttpResponse<byte[]> download = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build()
                .send(HttpRequest.newBuilder(DOWNLOAD).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(download.statusCode()).as("download of %s", DOWNLOAD).isEqualTo(200);
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(download.body()));
        assertThat(digest).as("SHA-256 of %s", ARCHIVE).isEqualTo(SHA256);
        Files.createDirectories(directory);
        Path partial = directory.resolve("connectconformance.partial");
        Files.write(partial, tarEntry(download.body(), "connectconformance"));
        Files.setPosixFilePermissions(partial, PosixFilePermissions.fromString("rwxr-xr-x"));
        Files.move(partial, binary, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return binary;
    }

    /** One regular file's bytes from a gzipped ustar archive. */
    private static byte[] tarEntry(byte[] archive, String name) throws IOException {
        try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(archive))) {
            byte[] header = new byte[512];
            while (in.readNBytes(header, 0, 512) == 512) {
                String entry = field(header, 0, 100);
                if (entry.isEmpty()) {
                    break;
                }
                long size = Long.parseLong(field(header, 124, 12).trim(), 8);
                byte[] body = in.readNBytes((int) size);
                in.skipNBytes((512 - size % 512) % 512);
                if (entry.equals(name) || entry.endsWith("/" + name)) {
                    return body;
                }
            }
        }
        throw new IOException(name + " is not in the conformance runner archive");
    }

    private static String field(byte[] header, int offset, int length) {
        int end = offset;
        while (end < offset + length && header[end] != 0) {
            end++;
        }
        return new String(header, offset, end - offset, StandardCharsets.US_ASCII);
    }
}
