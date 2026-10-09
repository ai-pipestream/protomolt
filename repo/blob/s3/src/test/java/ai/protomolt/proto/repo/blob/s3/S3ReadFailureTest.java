package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import static org.assertj.core.api.Assertions.*;

/** Fault-only HTTP endpoint exercises the real SDK error parser and adapter; no successful storage is simulated. */
class S3ReadFailureTest {
    private static S3Client client(int port, Duration timeout) {
        return S3Client.builder().endpointOverride(URI.create("http://127.0.0.1:" + port))
                .region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder().connectionTimeout(Duration.ofSeconds(1))
                        .socketTimeout(Duration.ofSeconds(2)))
                .overrideConfiguration(c -> c.apiCallTimeout(timeout).retryStrategy(r -> r.maxAttempts(1)))
                .build();
    }

    @ParameterizedTest
    @CsvSource({"503,SlowDown,UNAVAILABLE", "500,InternalError,UNAVAILABLE",
            "403,AccessDenied,PERMISSION_DENIED", "401,InvalidToken,UNAUTHENTICATED",
            "429,TooManyRequests,RESOURCE_EXHAUSTED", "404,NoSuchBucket,FAILED_PRECONDITION",
            "400,InvalidArgument,INVALID_ARGUMENT", "400,RequestTimeout,DEADLINE_EXCEEDED"})
    void serviceErrorsHaveProviderNeutralCodes(int status, String error, BlobStoreException.Code expected) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = ("<Error><Code>" + error + "</Code><Message>injected failure</Message></Error>")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/xml");
            exchange.sendResponseHeaders(status, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
            exchange.close();
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort(), Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).get("bucket", "key", "original-version"))
                    .isInstanceOfSatisfying(BlobStoreException.class, failure -> assertThat(failure.code()).isEqualTo(expected))
                    .hasCauseInstanceOf(S3Exception.class);
        } finally { server.stop(0); }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(longs = {500, 5000})
    void deadlineKeepsItsMeaning(long apiTimeoutMillis) throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            entered.countDown();
            try { release.await(5, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort(), Duration.ofMillis(apiTimeoutMillis))) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).get("bucket", "key", "original-version"))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.DEADLINE_EXCEEDED));
            assertThat(entered.getCount()).isZero();
        } finally { release.countDown(); server.stop(0); }
    }

    @Test void refusedConnectionIsUnavailableWithItsCause() throws Exception {
        int port;
        try (var socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            port = socket.getLocalPort();
        }
        try (var sdk = client(port, Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).get("bucket", "key", "original-version"))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE))
                    .hasCauseInstanceOf(software.amazon.awssdk.core.exception.SdkClientException.class);
        }
    }

    @ParameterizedTest @CsvSource({"NoSuchKey", "NoSuchVersion"})
    void confirmedMissingObjectStaysDistinct(String code) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = ("<Error><Code>" + code + "</Code></Error>").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/xml");
            exchange.sendResponseHeaders(404, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
            exchange.close();
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort(), Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).get("bucket", "key", "original-version"))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class).hasCauseInstanceOf(S3Exception.class);
        } finally { server.stop(0); }
    }
}
