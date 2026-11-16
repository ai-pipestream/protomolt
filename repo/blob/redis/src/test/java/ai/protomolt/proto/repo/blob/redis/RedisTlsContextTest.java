package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SSLContext construction is per store. Jedis must not be asked to build another one per socket. */
class RedisTlsContextTest {
    @Test void plaintextStoreDoesNotBuildAContext() {
        int before = RedisTls.contextsCreated();
        try (var store = new RedisBlobStore(new RedisBlobStoreConfig("redis://127.0.0.1:1", 0, 0, "plain"))) {
            assertThat(store).isNotNull();
        }
        assertThat(RedisTls.contextsCreated()).isEqualTo(before);
    }

    @Test
    @SuppressWarnings("deprecation")
    void eachRedissStoreBuildsOneContextAndReusesItsSocketFactory() throws Exception {
        int before = RedisTls.contextsCreated();
        var tls = RedisTls.open();
        assertThat(RedisTls.contextsCreated()).isEqualTo(before + 1);
        var first = RedisTransfer.clientConfig(URI.create("rediss://localhost:6380"), tls);
        var second = RedisTransfer.clientConfig(URI.create("rediss://localhost:6380"), tls);
        assertThat(first.getSslOptions()).isNull();
        assertThat(second.getSslOptions()).isNull();
        assertThat(first.isSsl()).isTrue();
        assertThat(first.getSslSocketFactory()).isSameAs(tls.socketFactory()).isSameAs(second.getSslSocketFactory());
        assertThat(first.getSslParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        var plain = RedisTransfer.clientConfig(URI.create("redis://localhost:6379"), null);
        assertThat(plain.isSsl()).isFalse();
        assertThat(plain.getSslOptions()).isNull();
        assertThat(plain.getSslSocketFactory()).isNull();
        assertThatThrownBy(() -> RedisTransfer.clientConfig(URI.create("rediss://localhost:6380"), null))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("TLS context");

        var secure = new RedisBlobStoreConfig("rediss://127.0.0.1:1", 0, 0, "secure", 2, RedisWritePolicy.REPLACE);
        try (var store = new RedisBlobStore(secure)) {
            var spec = new BlobStore.PutSpec("n", "c", "application/octet-stream", Map.of(), null);
            assertThatThrownBy(() -> store.openStream("n", "a", 0, -1)).isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> store.openSnapshot("n", "b")).isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> store.put(spec, new ByteArrayInputStream(new byte[]{1}), 1))
                    .isInstanceOf(RuntimeException.class);
        }
        assertThat(RedisTls.contextsCreated()).isEqualTo(before + 2);
        try (var other = new RedisBlobStore(secure)) {
            assertThatThrownBy(() -> other.openStream("n", "a", 0, -1)).isInstanceOf(RuntimeException.class);
        }
        assertThat(RedisTls.contextsCreated()).isEqualTo(before + 3);
    }
}
