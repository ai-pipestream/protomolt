package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.ExactVersionOutcome;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static org.assertj.core.api.Assertions.*;

/**
 * Scripted HTTP endpoint exercises the real SDK client against every
 * classification of the four-request sequence; no container and no real
 * storage. Request lines the stub received are asserted, not mocked away.
 */
class S3ExactVersionReclaimerTest {
    private static final String BUCKET = "reclaim-bucket";
    private static final String KEY = "ledger/blob.bin";
    private static final String VERSION = "v123abc";

    private static S3Client client(int port) {
        return S3Client.builder().endpointOverride(URI.create("http://127.0.0.1:" + port))
                .region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder().connectionTimeout(Duration.ofSeconds(1))
                        .socketTimeout(Duration.ofMillis(500)))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(3)).retryStrategy(r -> r.maxAttempts(1)))
                .build();
    }

    private static final class Stub {
        final List<String> lines = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger requests = new AtomicInteger();

        void record(HttpExchange exchange) {
            requests.incrementAndGet();
            lines.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
        }
    }

    private static byte[] versioningXml(String status) {
        String body = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + (status == null ? "" : "<Status>" + status + "</Status>") + "</VersioningConfiguration>";
        return body.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] errorXml(String code) {
        return ("<Error><Code>" + code + "</Code><Message>injected failure</Message></Error>")
                .getBytes(StandardCharsets.UTF_8);
    }

    private static void respond(HttpExchange exchange, int status, byte[] body, String contentType) throws IOException {
        if (contentType != null) exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body == null ? -1 : body.length);
        if (body != null) {
            try (var output = exchange.getResponseBody()) { output.write(body); }
        }
        exchange.close();
    }

    static Stream<Arguments> invalidTargets() {
        return Stream.of(
                Arguments.of(null, "key", VERSION),
                Arguments.of("", "key", VERSION),
                Arguments.of("   ", "key", VERSION),
                Arguments.of(BUCKET, null, VERSION),
                Arguments.of(BUCKET, "", VERSION),
                Arguments.of(BUCKET, KEY, null),
                Arguments.of(BUCKET, KEY, ""),
                Arguments.of(BUCKET, KEY, "   "),
                Arguments.of(BUCKET, KEY, "null"));
    }

    @ParameterizedTest
    @MethodSource("invalidTargets")
    void invalidTargetsThrowBeforeAnyProviderCall(String bucket, String key, String versionId) throws Exception {
        var stub = new Stub();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            respond(exchange, 500, errorXml("InternalError"), "application/xml");
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            var reclaimer = new S3ExactVersionReclaimer(sdk);
            assertThatThrownBy(() -> reclaimer.reclaim(bucket, key, versionId))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("required");
            assertThat(stub.requests).hasValue(0);
        } finally { server.stop(0); }
    }

    @Test void suspendedVersioningIsFailedPrecondition() throws Exception {
        var stub = new Stub();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            respond(exchange, 200, versioningXml("Suspended"), "application/xml");
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isInstanceOfSatisfying(BlobStoreException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(BlobStoreException.Code.FAILED_PRECONDITION);
                        assertThat(failure.getMessage()).contains(BUCKET);
                    });
            assertThat(stub.lines).noneMatch(line -> line.startsWith("DELETE"));
        } finally { server.stop(0); }
    }

    @Test void neverVersionedIsFailedPrecondition() throws Exception {
        var stub = new Stub();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            respond(exchange, 200, versioningXml(null), "application/xml");
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.FAILED_PRECONDITION));
            assertThat(stub.lines).noneMatch(line -> line.startsWith("DELETE"));
        } finally { server.stop(0); }
    }

    @Test void missingBucketIsNotFound() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> respond(exchange, 404, errorXml("NoSuchBucket"), "application/xml"));
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.NOT_FOUND));
        } finally { server.stop(0); }
    }

    @Test void deniedVersioningReadIsPermissionDenied() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> respond(exchange, 403, errorXml("AccessDenied"), "application/xml"));
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.PERMISSION_DENIED));
        } finally { server.stop(0); }
    }

    @Test void absentVersionIsAbsentAndDeletesNothing() throws Exception {
        var stub = new Stub();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, versioningXml("Enabled"), "application/xml");
            } else {
                respond(exchange, 404, null, null);
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isEqualTo(ExactVersionOutcome.ABSENT);
            assertThat(stub.lines).noneMatch(line -> line.startsWith("DELETE"));
        } finally { server.stop(0); }
    }

    @Test void enabledHeadDeleteVerifiedHeadYieldsDeletedWithExactVersionAddressing() throws Exception {
        var stub = new Stub();
        var headCalls = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> {
                    int call = headCalls.incrementAndGet();
                    respond(exchange, call == 1 ? 200 : 404, null, null);
                }
                case "DELETE" -> respond(exchange, 204, null, null);
                default -> respond(exchange, 400, errorXml("BadRequest"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isEqualTo(ExactVersionOutcome.DELETED);
            assertThat(stub.lines).containsExactly(
                    "GET /" + BUCKET + "?versioning",
                    "HEAD /" + BUCKET + "/" + KEY + "?versionId=" + VERSION,
                    "DELETE /" + BUCKET + "/" + KEY + "?versionId=" + VERSION,
                    "HEAD /" + BUCKET + "/" + KEY + "?versionId=" + VERSION);
        } finally { server.stop(0); }
    }

    @Test void deleteServerErrorIsUncertain() throws Exception {
        var stub = new Stub();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> respond(exchange, 200, null, null);
                default -> respond(exchange, 500, errorXml("InternalError"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .satisfies(outcome -> {
                        assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.UNCERTAIN);
                        assertThat(outcome.phase()).isEqualTo(ExactVersionOutcome.Phase.DELETE);
                        assertThat(outcome.failure().code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                        assertThat(outcome.failure().getCause()).isNotNull();
                    });
            assertThat(stub.lines).anyMatch(line -> line.startsWith("DELETE"));
        } finally { server.stop(0); }
    }

    @Test void deleteStallIsUncertain() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, versioningXml("Enabled"), "application/xml");
            } else if ("HEAD".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, null, null);
            } else {
                entered.countDown();
                try { release.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                finally { exchange.close(); }
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .satisfies(outcome -> {
                        assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.UNCERTAIN);
                        assertThat(outcome.phase()).isEqualTo(ExactVersionOutcome.Phase.DELETE);
                        assertThat(outcome.failure()).isNotNull();
                        assertThat(outcome.failure().getCause()).isNotNull();
                    });
            assertThat(entered.getCount()).isZero();
        } finally { release.countDown(); server.stop(0); }
    }

    @Test void deniedDeleteIsPermissionDenied() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> respond(exchange, 200, null, null);
                default -> respond(exchange, 403, errorXml("AccessDenied"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.PERMISSION_DENIED));
        } finally { server.stop(0); }
    }

    @Test void deleteMarkerVersionIsStillAnExactTarget() throws Exception {
        var stub = new Stub();
        var headCalls = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> {
                    int call = headCalls.incrementAndGet();
                    respond(exchange, call == 1 ? 405 : 404, null, null);
                }
                case "DELETE" -> respond(exchange, 204, null, null);
                default -> respond(exchange, 400, errorXml("BadRequest"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isEqualTo(ExactVersionOutcome.DELETED);
            assertThat(stub.lines).anyMatch(line -> line.startsWith("DELETE"));
        } finally { server.stop(0); }
    }

    @Test void acknowledgedDeleteThatStillResolvesIsUncertain() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> respond(exchange, 200, null, null);
                case "DELETE" -> respond(exchange, 204, null, null);
                default -> respond(exchange, 400, errorXml("BadRequest"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .satisfies(outcome -> {
                        assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.UNCERTAIN);
                        assertThat(outcome.phase()).isEqualTo(ExactVersionOutcome.Phase.VERIFICATION);
                        assertThat(outcome.failure().code()).isEqualTo(BlobStoreException.Code.ABORTED);
                        assertThat(outcome.failure().getCause()).isNotNull();
                    });
        } finally { server.stop(0); }
    }

    @Test void qualifyServerErrorIsUncertainAndDeletesNothing() throws Exception {
        var stub = new Stub();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            respond(exchange, 500, errorXml("InternalError"), "application/xml");
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .satisfies(outcome -> {
                        assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.UNCERTAIN);
                        assertThat(outcome.phase()).isEqualTo(ExactVersionOutcome.Phase.QUALIFICATION);
                        assertThat(outcome.failure().code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                        assertThat(outcome.failure().getCause()).isNotNull();
                    });
            assertThat(stub.lines).noneMatch(line -> line.startsWith("DELETE"));
        } finally { server.stop(0); }
    }

    @Test void unimplementedVersioningReadThrowsUnimplemented() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> respond(exchange, 501, errorXml("NotImplemented"), "application/xml"));
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.UNIMPLEMENTED));
        } finally { server.stop(0); }
    }

    @ParameterizedTest
    @CsvSource({"400,INVALID_ARGUMENT", "401,UNAUTHENTICATED", "403,PERMISSION_DENIED"})
    void existenceHeadRefusalsThrow(int status, BlobStoreException.Code expected) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, versioningXml("Enabled"), "application/xml");
            } else {
                respond(exchange, status, null, null);
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(expected));
        } finally { server.stop(0); }
    }

    @Test void existenceHeadServerErrorIsUncertain() throws Exception {
        var stub = new Stub();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, versioningXml("Enabled"), "application/xml");
            } else {
                respond(exchange, 500, null, null);
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .satisfies(outcome -> {
                        assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.UNCERTAIN);
                        assertThat(outcome.phase()).isEqualTo(ExactVersionOutcome.Phase.EXISTENCE);
                        assertThat(outcome.failure().code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                        assertThat(outcome.failure().getCause()).isNotNull();
                    });
            assertThat(stub.lines).noneMatch(line -> line.startsWith("DELETE"));
        } finally { server.stop(0); }
    }

    @Test void verificationHeadRefusalIsUncertainNotDenialAsAbsence() throws Exception {
        var headCalls = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> {
                    int call = headCalls.incrementAndGet();
                    respond(exchange, call == 1 ? 200 : 403, null, null);
                }
                case "DELETE" -> respond(exchange, 204, null, null);
                default -> respond(exchange, 400, errorXml("BadRequest"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .satisfies(outcome -> {
                        assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.UNCERTAIN);
                        assertThat(outcome.phase()).isEqualTo(ExactVersionOutcome.Phase.VERIFICATION);
                        assertThat(outcome.failure().code()).isEqualTo(BlobStoreException.Code.PERMISSION_DENIED);
                        assertThat(outcome.failure().getCause()).isNotNull();
                    });
        } finally { server.stop(0); }
    }

    @Test void deleteNoSuchVersionContinuesToVerificationAsAbsent() throws Exception {
        var stub = new Stub();
        var headCalls = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            stub.record(exchange);
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> {
                    int call = headCalls.incrementAndGet();
                    respond(exchange, call == 1 ? 200 : 404, null, null);
                }
                case "DELETE" -> respond(exchange, 404, errorXml("NoSuchVersion"), "application/xml");
                default -> respond(exchange, 400, errorXml("BadRequest"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isEqualTo(ExactVersionOutcome.ABSENT);
            assertThat(stub.lines).containsExactly(
                    "GET /" + BUCKET + "?versioning",
                    "HEAD /" + BUCKET + "/" + KEY + "?versionId=" + VERSION,
                    "DELETE /" + BUCKET + "/" + KEY + "?versionId=" + VERSION,
                    "HEAD /" + BUCKET + "/" + KEY + "?versionId=" + VERSION);
        } finally { server.stop(0); }
    }

    @Test void deleteNoSuchVersionWithInconclusiveVerificationIsUncertain() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> respond(exchange, 200, null, null);
                case "DELETE" -> respond(exchange, 404, errorXml("NoSuchVersion"), "application/xml");
                default -> respond(exchange, 400, errorXml("BadRequest"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThat(new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .satisfies(outcome -> {
                        assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.UNCERTAIN);
                        assertThat(outcome.phase()).isEqualTo(ExactVersionOutcome.Phase.VERIFICATION);
                        assertThat(outcome.failure().code()).isEqualTo(BlobStoreException.Code.ABORTED);
                        assertThat(outcome.failure().getCause()).isNotNull();
                    });
        } finally { server.stop(0); }
    }

    @Test void deleteNoSuchBucketIsNotFound() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            switch (exchange.getRequestMethod()) {
                case "GET" -> respond(exchange, 200, versioningXml("Enabled"), "application/xml");
                case "HEAD" -> respond(exchange, 200, null, null);
                default -> respond(exchange, 404, errorXml("NoSuchBucket"), "application/xml");
            }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(sdk).reclaim(BUCKET, KEY, VERSION))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.NOT_FOUND));
        } finally { server.stop(0); }
    }

    @Test void interruptionDuringDeleteIsUncertainAndRestoresInterruptFlag() {
        S3Client interrupted = (S3Client) java.lang.reflect.Proxy.newProxyInstance(
                S3ExactVersionReclaimerTest.class.getClassLoader(), new Class<?>[] {S3Client.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getBucketVersioning" -> software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse
                            .builder().status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED).build();
                    case "headObject" -> software.amazon.awssdk.services.s3.model.HeadObjectResponse.builder().build();
                    case "deleteObject" -> throw software.amazon.awssdk.core.exception.SdkClientException.builder()
                            .message("interrupted during delete")
                            .cause(new InterruptedException("injected interruption")).build();
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        try {
            assertThat(new S3ExactVersionReclaimer(interrupted).reclaim(BUCKET, KEY, VERSION))
                    .satisfies(outcome -> {
                        assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.UNCERTAIN);
                        assertThat(outcome.phase()).isEqualTo(ExactVersionOutcome.Phase.DELETE);
                        assertThat(outcome.failure().code()).isEqualTo(BlobStoreException.Code.CANCELLED);
                        assertThat(outcome.failure().getCause()).isNotNull();
                    });
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted(); // clear the restored flag so the test JVM thread stays clean
        }
    }
}
