package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
}
