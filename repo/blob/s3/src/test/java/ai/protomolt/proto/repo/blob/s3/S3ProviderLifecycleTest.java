package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStoreProvider;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Selection rejection and owned-resource lifecycle of the real S3 factory. */
class S3ProviderLifecycleTest {
    private static Map<String, String> options() {
        return new java.util.HashMap<>(Map.ofEntries(
                Map.entry("endpoint", "http://127.0.0.1:1"),
                Map.entry("region", "us-east-1"),
                Map.entry("access-key", "test"),
                Map.entry("secret-key", "test-secret"),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")));
    }

    @Test
    void unsupportedCapabilityRequirementReleasesTheSelectedHandle() {
        var actual = new S3BlobStoreProvider().open(options());
        var selected = new BlobStoreProvider() {
            public String id() { return "s3"; }
            public OpenedBlobStore open(Map<String, String> ignored) { return actual; }
        };
        assertThatThrownBy(() -> BlobStores.of(List.of(selected)).open("s3", Map.of(),
                java.util.Set.of(BlobCapability.OBJECT_EXPIRY)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(actual::store).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeReleasesTheOwnedClientAndIsIdempotent() throws Exception {
        var handle = new S3BlobStoreProvider().open(options());
        var store = handle.store();
        handle.close();
        handle.close();
        assertThatThrownBy(handle::store).isInstanceOf(IllegalStateException.class);
        // Observable closure of the underlying client: a store reference kept
        // past close fails explicitly, never hangs or silently succeeds. The
        // closed SDK client's terminated timeout scheduler rejects the call;
        // the adapter does not translate it.
        assertThatThrownBy(() -> store.get("bucket", "key"))
                .isExactlyInstanceOf(java.util.concurrent.RejectedExecutionException.class)
                .hasMessageContaining("Terminated");
    }

    @Test
    void startupFailureSurfacesAtTheFirstOperationAndStillCloses() throws Exception {
        // Every option is validated before acquisition and client construction
        // performs no I/O, so an unreachable endpoint is a first-operation
        // failure, not an open-time one.
        var handle = new S3BlobStoreProvider().open(options());
        assertThatThrownBy(() -> handle.store().get("bucket", "key"))
                .isInstanceOfSatisfying(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(
                                ai.protomolt.proto.repo.blob.spi.BlobStoreException.Code.UNAVAILABLE));
        handle.close();
    }

    @Test
    void cleanupFailureRetainsPrimaryAndSuppressedFailures() {
        // Narrowly instrumented REAL client: every method delegates to a live
        // S3Client except close(), which fails. The composed cleanup path is the
        // production one; only the close failure is injected. (S3Client.close
        // declares no checked exception, so the injected failure is unchecked.)
        var clientCloseFailure = new RuntimeException("client close failed");
        var credentialsCloseFailure = new Exception("credentials close failed");
        var closes = new AtomicInteger();
        var realClient = S3Client.builder()
                .endpointOverride(java.net.URI.create("http://127.0.0.1:1"))
                .credentialsProvider(() -> AwsBasicCredentials.create("test", "test-secret"))
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true)
                .build();
        S3Client instrumented = (S3Client) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{S3Client.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        closes.incrementAndGet();
                        throw clientCloseFailure;
                    }
                    try {
                        return method.invoke(realClient, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
        var credentials = new CloseableCredentials(credentialsCloseFailure);
        assertThatThrownBy(() -> S3BlobStoreProvider.close(instrumented, credentials))
                .isSameAs(clientCloseFailure)
                .satisfies(failure -> assertThat(failure.getSuppressed()).containsExactly(credentialsCloseFailure));
        assertThat(closes.get()).isEqualTo(1);
        realClient.close();

        // When only the credentials cleanup fails, that failure surfaces.
        var realSecond = S3Client.builder()
                .endpointOverride(java.net.URI.create("http://127.0.0.1:1"))
                .credentialsProvider(() -> AwsBasicCredentials.create("test", "test-secret"))
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true)
                .build();
        assertThatThrownBy(() -> S3BlobStoreProvider.close(realSecond, new CloseableCredentials(credentialsCloseFailure)))
                .isSameAs(credentialsCloseFailure);
    }

    /** Closeable static credentials used to exercise the composed cleanup path. */
    private static final class CloseableCredentials implements AwsCredentialsProvider, AutoCloseable {
        private final Exception failure;

        private CloseableCredentials(Exception failure) {
            this.failure = failure;
        }

        @Override public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("test", "test-secret");
        }

        @Override public void close() throws Exception {
            throw failure;
        }
    }
}
