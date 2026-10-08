package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStores;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisProviderTest {
    private static Map<String, String> options(String uri) {
        return Map.of("uri", uri, "ttl-seconds", "0", "max-object-bytes", "1024", "key-prefix", "", "write-policy", "replace");
    }

    @Test void discoveryDoesNotOpenAnEndpointAndRequiresExplicitSelection() {
        var providers = BlobStores.discover();
        assertThat(providers.providerIds()).containsExactly("redis");
        assertThatThrownBy(() -> providers.open("absent", Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not installed");
        assertThatThrownBy(() -> BlobStores.of(List.of(new RedisBlobStoreProvider(), new RedisBlobStoreProvider())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
    }

    @Test void invalidConfigurationIsRejectedBeforeOpening() {
        var providers = BlobStores.discover();
        assertThatThrownBy(() -> providers.open("redis", Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> providers.open("redis", options("https://localhost")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> providers.open("redis", options("redis://user:secret value@localhost")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("secret");
    }

    @Test void writePolicyIsRequiredWithNoDefault() {
        var options = new java.util.HashMap<>(options("redis://127.0.0.1:1"));
        options.remove("write-policy");
        assertThatThrownBy(() -> BlobStores.discover().open("redis", options))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("write-policy");
        assertThatThrownBy(() -> BlobStores.discover().managedIdentity("redis", options))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("write-policy");
    }

    @Test void handleOwnsThePoolAndRefusesAccessAfterClose() throws Exception {
        var handle = BlobStores.discover().open("redis", options("redis://127.0.0.1:1"));
        assertThat(handle.store()).isInstanceOf(RedisBlobStore.class);
        assertThat(handle.capabilities()).contains(ai.protomolt.proto.repo.blob.spi.BlobCapability.ATOMIC_CONDITIONAL_WRITE)
                .doesNotContain(ai.protomolt.proto.repo.blob.spi.BlobCapability.STREAMING_WRITE);
        handle.close();
        handle.close();
        assertThatThrownBy(handle::store).isInstanceOf(IllegalStateException.class);
    }

    @Test void incompatibleCapabilityReleasesTheSelectedHandle() {
        var actual = new RedisBlobStoreProvider().open(options("redis://127.0.0.1:1"));
        var selected = new ai.protomolt.proto.repo.blob.spi.BlobStoreProvider() {
            public String id() { return "redis"; }
            public ai.protomolt.proto.repo.blob.spi.OpenedBlobStore open(Map<String, String> ignored) {
                return actual;
            }
        };
        assertThatThrownBy(() -> BlobStores.of(List.of(selected)).open("redis", Map.of(),
                java.util.Set.of(ai.protomolt.proto.repo.blob.spi.BlobCapability.STREAMING_WRITE)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(actual::store).isInstanceOf(IllegalStateException.class);
    }

    @Test void unreachableEndpointFailsTheFirstOperationAndStillClosesCleanly() throws Exception {
        // The pool connects lazily: open acquired nothing, so startup failure
        // surfaces at the first real operation, never as a silent success.
        var handle = BlobStores.discover().open("redis", options("redis://127.0.0.1:1"));
        var store = handle.store();
        assertThatThrownBy(() -> store.get("namespace", "key"))
                .isInstanceOf(redis.clients.jedis.exceptions.JedisConnectionException.class);
        handle.close();
        handle.close();
        // After close, the owned pool refuses work observably instead of hanging.
        assertThatThrownBy(() -> store.get("namespace", "key"))
                .isInstanceOf(redis.clients.jedis.exceptions.JedisException.class)
                .hasStackTraceContaining("Pool not open");
    }

    @Test void nonExpiringCapabilityDependsOnConfiguredTtl() throws Exception {
        var capability = ai.protomolt.proto.repo.blob.spi.BlobCapability.NON_EXPIRING_WRITES;
        var options = new java.util.HashMap<>(options("redis://127.0.0.1:1"));
        try (var persistent = BlobStores.discover().open("redis", options)) {
            assertThat(persistent.capabilities()).contains(capability);
        }
        options.put("ttl-seconds", "300");
        try (var expiring = BlobStores.discover().open("redis", options)) {
            assertThat(expiring.capabilities()).doesNotContain(capability);
        }
        assertThatThrownBy(() -> BlobStores.discover().open("redis", options, java.util.Set.of(capability)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
