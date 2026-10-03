package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStores;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ProviderTest {
    private static Map<String, String> options() {
        return new HashMap<>(Map.of("endpoint", "http://127.0.0.1:1", "region", "us-east-1",
                "access-key", "test", "secret-key", "test-secret", "path-style", "true",
                "conditional-writes", "false"));
    }

    @Test void discoveryAndOpeningDoNotContactTheEndpoint() throws Exception {
        var providers = BlobStores.discover();
        assertThat(providers.providerIds()).containsExactly("s3");
        try (var opened = providers.open("s3", options())) {
            assertThat(opened.store()).isInstanceOf(S3BlobStore.class);
            assertThat(opened.capabilities()).contains(
                    ai.protomolt.proto.repo.blob.spi.BlobCapability.NON_EXPIRING_WRITES);
        }
    }

    @Test void refusesUnknownAndInvalidConfiguration() {
        var providers = BlobStores.discover();
        var unknown = options();
        unknown.put("unknown", "value");
        assertThatThrownBy(() -> providers.open("s3", unknown)).isInstanceOf(IllegalArgumentException.class);
        var invalid = options();
        invalid.put("conditional-writes", "yes");
        assertThatThrownBy(() -> providers.open("s3", invalid)).isInstanceOf(IllegalArgumentException.class);
        var credentialUrl = options();
        credentialUrl.put("endpoint", "http://user:secret@localhost");
        assertThatThrownBy(() -> providers.open("s3", credentialUrl))
                .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("secret");
    }

    @Test void explicitDefaultChainDoesNotResolveCredentialsDuringAcquisition() throws Exception {
        var options = options();
        options.remove("access-key");
        options.remove("secret-key");
        options.put("credentials-mode", "default-chain");
        options.put("endpoint", "");
        options.put("path-style", "false");
        try (var opened = BlobStores.discover().open("s3", options)) {
            assertThat(opened.store()).isInstanceOf(S3BlobStore.class);
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"0", "-1", "forever", "2147483648", "9223372036854775808"})
    void rejectsInvalidTimeoutsBeforeAcquisition(String value) {
        var options = options();
        options.put("api-call-timeout-ms", value);
        assertThatThrownBy(() -> BlobStores.discover().open("s3", options))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positive milliseconds");
    }

    @Test void rejectsInconsistentTimeoutBudget() {
        var options = options();
        options.put("api-call-timeout-ms", "1000");
        assertThatThrownBy(() -> BlobStores.discover().open("s3", options))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API attempt <= API call");
    }

    @Test void providerAppliesFiniteTimeoutToStalledHttpRead() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            entered.countDown();
            try { release.await(10, java.util.concurrent.TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        server.start();
        var options = options();
        options.put("endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        options.put("api-call-timeout-ms", "1500");
        options.put("api-attempt-timeout-ms", "1000");
        options.put("connection-timeout-ms", "500");
        options.put("socket-timeout-ms", "1000");
        try (var opened = BlobStores.discover().open("s3", options)) {
            assertThatThrownBy(() -> opened.store().get("bucket", "key"))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class,
                            e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.blob.spi.BlobStoreException.Code.DEADLINE_EXCEEDED));
            assertThat(entered.getCount()).isZero();
        } finally { release.countDown(); server.stop(0); }
    }
}
