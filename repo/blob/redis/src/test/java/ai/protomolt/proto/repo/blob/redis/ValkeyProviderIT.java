package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Both names acquire the real server and retain the same physical objects and identities. */
@Testcontainers
class ValkeyProviderIT {
    @Container static final GenericContainer<?> SERVER = new GenericContainer<>(
            System.getenv().getOrDefault("PROTOMOLT_REDIS_TEST_IMAGE", "valkey/valkey:9.1.3-alpine"))
            .withExposedPorts(6379);

    @ParameterizedTest
    @ValueSource(strings = {"replace", "create-only"})
    void aliasesSharePersistedIdentityAndBytesWithoutAddingCapabilities(String policy) throws Exception {
        var providers = BlobStores.discover();
        var options = Map.of("uri", "redis://" + SERVER.getHost() + ":" + SERVER.getMappedPort(6379),
                "ttl-seconds", "0", "max-object-bytes", "1024", "key-prefix", UUID.randomUUID().toString(),
                "write-policy", policy);
        assertThat(providers.managedIdentity("valkey", options)).isEqualTo(providers.managedIdentity("redis", options));
        assertThat(providers.managedIdentity("valkey", options).provider()).isEqualTo("redis");
        byte[] original = {8, 1, 18, 3, 0, (byte) 255, 42};
        var spec = new BlobStore.PutSpec("objects", "original", "application/x-protobuf", Map.of("origin", "legacy"), null);
        try (var legacy = providers.open("redis", options); var preferred = providers.open("valkey", options)) {
            assertThat(preferred.capabilities()).isEqualTo(legacy.capabilities())
                    .doesNotContain(BlobCapability.STREAMING_WRITE);
            var result = legacy.store().put(spec, original);
            assertThat(preferred.store().get("objects", "original").data()).isEqualTo(original);
            assertThat(preferred.store().get("objects", "original").eTag()).isEqualTo(result.eTag());
            preferred.store().copy("objects", "original", "objects", "copy");
            assertThat(legacy.store().get("objects", "copy").data()).isEqualTo(original);
            if (policy.equals("create-only")) {
                assertThatThrownBy(() -> preferred.store().put(spec, original))
                        .isInstanceOf(BlobStore.BlobConflictException.class);
            }
            assertThat(preferred.reclaimer().reclaim("objects", "original")).isTrue();
            assertThatThrownBy(() -> legacy.store().get("objects", "original"))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
    }
}
