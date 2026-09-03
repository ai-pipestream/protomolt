package ai.protomolt.proto.repo.container.blob;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RedisBlobStoreConfig} validation: the record's compact-constructor
 * guards (URI required, non-negative TTL and size ceiling, null prefix
 * normalized to none) and the LOCAL development default.
 */
class RedisBlobStoreConfigTest {

    @Test
    void localMatchesTheServiceWiringDefaults() {
        assertThat(RedisBlobStoreConfig.LOCAL.uri()).isEqualTo("redis://localhost:6379");
        assertThat(RedisBlobStoreConfig.LOCAL.ttlSeconds()).isZero(); // no expiry
        assertThat(RedisBlobStoreConfig.LOCAL.maxObjectBytes()).isZero(); // unbounded
        assertThat(RedisBlobStoreConfig.LOCAL.keyPrefix()).isEmpty();
    }

    @Test
    void aMissingUriIsRejected() {
        assertThatThrownBy(() -> new RedisBlobStoreConfig(null, 0, 0, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uri is required");
        assertThatThrownBy(() -> new RedisBlobStoreConfig("  ", 0, 0, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uri is required");
    }

    @Test
    void negativeTtlAndCeilingAreRejected() {
        assertThatThrownBy(() -> new RedisBlobStoreConfig("redis://h:6379", -1, 0, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttlSeconds");
        assertThatThrownBy(() -> new RedisBlobStoreConfig("redis://h:6379", 0, -1L, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxObjectBytes");
    }

    @Test
    void aNullKeyPrefixNormalizesToNoPrefix() {
        assertThat(new RedisBlobStoreConfig("redis://h:6379", 0, 0, null).keyPrefix()).isEmpty();
    }

    @Test
    void boundaryValuesAreAccepted() {
        // 0 ttl = no expiry, 0 ceiling = unbounded — both legal, not errors.
        RedisBlobStoreConfig config =
                new RedisBlobStoreConfig("redis://:pw@h:6379/2", 0, 0L, "repo:");
        assertThat(config.uri()).isEqualTo("redis://:pw@h:6379/2");
        assertThat(config.ttlSeconds()).isZero();
        assertThat(config.maxObjectBytes()).isZero();
        assertThat(config.keyPrefix()).isEqualTo("repo:");

        RedisBlobStoreConfig bounded =
                new RedisBlobStoreConfig("redis://h:6379", 3600, 64L * 1024 * 1024, "p");
        assertThat(bounded.ttlSeconds()).isEqualTo(3600);
        assertThat(bounded.maxObjectBytes()).isEqualTo(64L * 1024 * 1024);
    }
}
