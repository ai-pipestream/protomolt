package ai.protomolt.proto.repo.blob.redis;

import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RedisBackendIdentityTest {
    @Test void credentialsAreExcludedAndEquivalentPhysicalEndpointsAreCanonical() {
        var identity = RedisBackendIdentity.of("redis://user:secret@LOCALHOST", "prefix");
        assertThat(identity).isEqualTo(RedisBackendIdentity.of("redis://rotated:other@localhost:6379/0", "prefix"));
        assertThat(identity.provider()).isEqualTo("redis");
        assertThat(identity.toString()).doesNotContain("secret", "user", "rotated", "other");
        assertThat(RedisBackendIdentity.of("redis://localhost/01", "prefix"))
                .isEqualTo(RedisBackendIdentity.of("redis://localhost/1", "prefix"));
        for (String endpoint : new String[]{"rediss://localhost", "redis://localhost:6380", "redis://localhost/1", "redis://otherhost"})
            assertThat(RedisBackendIdentity.of(endpoint, "prefix")).isNotEqualTo(identity);
        assertThat(RedisBackendIdentity.of("redis://localhost", "other-prefix")).isNotEqualTo(identity);
    }
    @Test void managedIdentityRequiresValidExplicitOptionsWithoutAcquiringConnections() {
        var provider = new RedisBlobStoreProvider();
        var options = Map.of("uri", "redis://127.0.0.1:1/2", "ttl-seconds", "0", "max-object-bytes", "0", "key-prefix", "x", "write-policy", "replace");
        assertThat(provider.managedIdentity(options)).isEqualTo(RedisBackendIdentity.of("redis://127.0.0.1:1/2", "x"));
        assertThatThrownBy(() -> provider.managedIdentity(Map.of())).isInstanceOf(IllegalArgumentException.class);
        for (String invalid : new String[]{"redis://u:password@localhost:0", "redis://u:password@localhost/abc", "redis://u:password@localhost/999999999999"})
            assertThatThrownBy(() -> RedisBackendIdentity.of(invalid, "x")).isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("password");
        assertThatThrownBy(() -> RedisBackendIdentity.of("redis://localhost", "\uD800")).isInstanceOf(IllegalArgumentException.class);
    }
}
