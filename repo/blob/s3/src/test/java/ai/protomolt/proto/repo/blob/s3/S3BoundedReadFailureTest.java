package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static org.assertj.core.api.Assertions.*;

/** Fault-only endpoint; successful storage behavior is covered by S3BoundedReadIT. */
class S3BoundedReadFailureTest {
    private static S3Client client(int port) {
        return S3Client.builder().endpointOverride(URI.create("http://127.0.0.1:" + port))
                .region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder().connectionTimeout(Duration.ofSeconds(1))
                        .socketTimeout(Duration.ofMillis(500)))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(3)).retryStrategy(r -> r.maxAttempts(1)))
                .build();
    }

    @Test void chunkedOversizedBodyIsRejectedRatherThanReturningPrefix() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 0); // chunked: no declared size to trust
            try (var output = exchange.getResponseBody()) { output.write(new byte[1025]); }
            finally { exchange.close(); }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).getBounded("bucket", "key", null, 1024))
                    .isInstanceOf(BlobStore.BlobReadLimitException.class);
        } finally { server.stop(0); }
    }

    @Test void oversizedHeaderRejectsWithoutWaitingForBody() throws Exception {
        var release = new CountDownLatch(1);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 1025);
            exchange.getResponseBody().flush();
            try { release.await(5, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).getBounded("bucket", "key", null, 1024))
                    .isInstanceOf(BlobStore.BlobReadLimitException.class);
        } finally { release.countDown(); server.stop(0); }
    }

    @Test void stalledBodyKeepsTimeoutMeaningAfterHeaders() throws Exception {
        var release = new CountDownLatch(1);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 10);
            exchange.getResponseBody().write(1);
            exchange.getResponseBody().flush();
            try { release.await(5, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).getBounded("bucket", "key", null, 10))
                    .isInstanceOfSatisfying(BlobStoreException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.DEADLINE_EXCEEDED));
        } finally { release.countDown(); server.stop(0); }
    }

    /** The provider answers 200 with a declared length and ends the body early: the same wire picture a dropped connection leaves. */
    @Test void bodyEndingBeforeDeclaredLengthIsUnavailableAndNamesTheBytes() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 10);
            exchange.getResponseBody().write(new byte[] {1, 2, 3, 4});
            exchange.getResponseBody().flush();
            exchange.close();
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).getBounded("bucket", "key", "version", 10))
                    .isInstanceOfSatisfying(BlobStoreException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                        assertThat(failure.getMessage()).contains("received 4 of 10 declared bytes");
                        assertThat(failure.getMessage()).doesNotContain("bucket", "key", "version", "127.0.0.1");
                        assertThat(causes(failure)).anyMatch(cause -> cause instanceof IOException);
                    });
        } finally { server.stop(0); }
    }

    /** Zero body bytes after full headers is what a provider that verifies a block before sending it leaves behind. */
    @Test void emptyBodyAfterDeclaredLengthIsUnavailableAndNamesTheBytes() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 1_048_576);
            exchange.getResponseBody().flush();
            exchange.close();
        });
        server.start();
        try (var sdk = client(server.getAddress().getPort())) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).getBounded("bucket", "key", null, 1_048_576))
                    .isInstanceOfSatisfying(BlobStoreException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                        assertThat(failure.getMessage()).contains("received 0 of 1048576 declared bytes");
                    });
        } finally { server.stop(0); }
    }

    /** A closed port is the other UNAVAILABLE: no response at all, so no byte counts in the message. */
    @Test void refusedConnectionOnBoundedReadIsUnavailableWithoutByteCounts() throws Exception {
        int port;
        try (var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) { port = socket.getLocalPort(); }
        try (var sdk = client(port)) {
            assertThatThrownBy(() -> new S3BlobStore(sdk).getBounded("bucket", "key", null, 10))
                    .isInstanceOfSatisfying(BlobStoreException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                        assertThat(failure.getMessage()).doesNotContain("received");
                        assertThat(failure).hasCauseInstanceOf(SdkClientException.class);
                    });
        }
    }

    private static List<Throwable> causes(Throwable failure) {
        var chain = new ArrayList<Throwable>();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) chain.add(cause);
        return chain;
    }
}
