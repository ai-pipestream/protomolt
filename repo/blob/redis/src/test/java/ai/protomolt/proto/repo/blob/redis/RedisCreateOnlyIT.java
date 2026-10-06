package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class RedisCreateOnlyIT {
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    private static String uri() { return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379); }
    private static RedisBlobStore store(String prefix, RedisWritePolicy policy) {
        return new RedisBlobStore(new RedisBlobStoreConfig(uri(), 0, 0, prefix, policy));
    }
    private static BlobStore.PutSpec spec(String key) {
        return new BlobStore.PutSpec("namespace", key, "application/octet-stream", Map.of(), null);
    }

    @Test void byteStreamConditionalAndCopyCannotReplaceAnExistingKey() {
        try (var store = store(UUID.randomUUID().toString(), RedisWritePolicy.CREATE_ONLY)) {
            var original = store.put(spec("target"), new byte[]{1});
            store.put(spec("source"), new byte[]{2});
            assertThatThrownBy(() -> store.put(spec("target"), new byte[]{3})).isInstanceOf(BlobStore.BlobConflictException.class);
            assertThatThrownBy(() -> store.put(spec("target"), new ByteArrayInputStream(new byte[]{3}), 1))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            assertThatThrownBy(() -> store.conditionalPut(spec("target"), new byte[]{3}, BlobStore.WriteCondition.matching(original.eTag())))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> store.copy("namespace", "source", "namespace", "target"))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            assertThatThrownBy(() -> store.copy("namespace", "target", "namespace", "target"))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            store.copy("namespace", "source", "namespace", "new");
            store.put(spec("stream"), new ByteArrayInputStream(new byte[]{4}), 1);
            assertThat(store.get("namespace", "stream").data()).containsExactly(4);
            assertThatThrownBy(() -> store.put(spec("stream"), new ByteArrayInputStream(new byte[]{5}), 1))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            assertThat(store.get("namespace", "new").data()).containsExactly(2);
            assertThat(store.get("namespace", "target").data()).containsExactly(1);
        }
    }

    @Test void replacementClientsCannotAddressCreateOnlyObjectsWithSameLogicalCoordinates() {
        String prefix = UUID.randomUUID().toString();
        try (var immutable = store(prefix, RedisWritePolicy.CREATE_ONLY); var ordinary = store(prefix, RedisWritePolicy.REPLACE)) {
            immutable.put(spec("key"), new byte[]{1});
            assertThatThrownBy(() -> ordinary.get("namespace", "key")).isInstanceOf(BlobStore.BlobNotFoundException.class);
            ordinary.put(spec("key"), new byte[]{2});
            ordinary.delete("namespace", "key");
            assertThat(immutable.get("namespace", "key").data()).containsExactly(1);
        }
    }

    @Test void createOnlyRejectsExpiryAndOversizedBuffersWithoutMutation() {
        assertThatThrownBy(() -> new RedisBlobStoreConfig(uri(), 1, 0, "x", RedisWritePolicy.CREATE_ONLY))
                .isInstanceOf(IllegalArgumentException.class);
        try (var store = store(UUID.randomUUID().toString(), RedisWritePolicy.CREATE_ONLY)) {
            assertThatThrownBy(() -> store.put(spec("ttl"), new byte[]{1}, 1)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.put(spec("large"), new byte[BlobStore.MAX_CONDITIONAL_BYTES + 1]))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(store.list("namespace", "")).isEmpty();
        }
    }

    @Test void providerSelectionBindsDisjointIdentityAndHonestCapabilities() throws Exception {
        var options = Map.of("uri", uri(), "ttl-seconds", "0", "max-object-bytes", "0",
                "key-prefix", UUID.randomUUID().toString(), "write-policy", "create-only");
        var provider = new RedisBlobStoreProvider();
        assertThat(provider.managedIdentity(options)).isEqualTo(RedisBackendIdentity.of(uri(), options.get("key-prefix"), RedisWritePolicy.CREATE_ONLY))
                .isNotEqualTo(RedisBackendIdentity.of(uri(), options.get("key-prefix")));
        try (var opened = ai.protomolt.proto.repo.blob.spi.BlobStores.discover().open("redis", options)) {
            assertThat(opened.capabilities()).contains(ai.protomolt.proto.repo.blob.spi.BlobCapability.AUTHORITATIVE_CONDITIONAL_READ)
                    .doesNotContain(ai.protomolt.proto.repo.blob.spi.BlobCapability.ATOMIC_CONDITIONAL_WRITE,
                            ai.protomolt.proto.repo.blob.spi.BlobCapability.STREAMING_WRITE);
            opened.store().put(spec("key"), new byte[]{1});
            assertThatThrownBy(() -> opened.store().put(spec("key"), new byte[]{2})).isInstanceOf(BlobStore.BlobConflictException.class);
        }
        var invalid = new java.util.HashMap<>(options);
        invalid.put("ttl-seconds", "1");
        assertThatThrownBy(() -> provider.managedIdentity(invalid)).isInstanceOf(IllegalArgumentException.class);
        invalid.put("ttl-seconds", "0"); invalid.put("write-policy", "typo");
        assertThatThrownBy(() -> provider.open(invalid)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void concurrentCopyAndPutHaveExactlyOneWinner() throws Exception {
        String prefix = UUID.randomUUID().toString();
        try (var a = store(prefix, RedisWritePolicy.CREATE_ONLY); var b = store(prefix, RedisWritePolicy.CREATE_ONLY);
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            a.put(spec("source"), new byte[]{1});
            var ready = new java.util.concurrent.CountDownLatch(2);
            var start = new java.util.concurrent.CountDownLatch(1);
            var copy = executor.submit(() -> race(ready, start, () -> a.copy("namespace", "source", "namespace", "target")));
            var put = executor.submit(() -> race(ready, start, () -> b.put(spec("target"), new byte[]{2})));
            assertThat(ready.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue(); start.countDown();
            boolean copyWon = copy.get(10, java.util.concurrent.TimeUnit.SECONDS);
            boolean putWon = put.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(copyWon ^ putWon).isTrue();
            assertThat(a.get("namespace", "target").data()).containsExactly(copyWon ? 1 : 2);
        }
    }

    private static boolean race(java.util.concurrent.CountDownLatch ready, java.util.concurrent.CountDownLatch start,
            Runnable write) throws Exception {
        ready.countDown();
        if (!start.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("Race start timed out");
        try { write.run(); return true; } catch (BlobStore.BlobConflictException expected) { return false; }
    }

    @Test void lostCallerAckRequiresReconciliationAndDeletionStillAllowsLateRecreation() {
        String prefix = UUID.randomUUID().toString();
        try (var a = store(prefix, RedisWritePolicy.CREATE_ONLY)) {
            assertThatThrownBy(() -> {
                a.put(spec("key"), new byte[]{1});
                throw new java.io.IOException("lost caller acknowledgment after actual Redis write");
            }).isInstanceOf(java.io.IOException.class);
        }
        try (var b = store(prefix, RedisWritePolicy.CREATE_ONLY)) {
            assertThatThrownBy(() -> b.put(spec("key"), new byte[]{1})).isInstanceOf(BlobStore.BlobConflictException.class);
            assertThat(b.getForUpdate("namespace", "key").data()).containsExactly(1);
            assertThat(b.reclaim("namespace", "key")).isTrue();
            b.put(spec("key"), new byte[]{2}); // No tombstone: lifecycle must fence/drain late writers.
            assertThat(b.get("namespace", "key").data()).containsExactly(2);
        }
    }

    @Test void copyConflictPreservesMetadataAndMalformedTargetFailsExplicitly() {
        String prefix = UUID.randomUUID().toString();
        String physical = new RedisObjectKeys(prefix, RedisWritePolicy.CREATE_ONLY).object("namespace", "target");
        try (var store = store(prefix, RedisWritePolicy.CREATE_ONLY);
                var admin = new redis.clients.jedis.Jedis(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            store.put(spec("source"), new byte[]{1}); store.put(spec("target"), new byte[]{2});
            var before = admin.hgetAll(physical);
            assertThat(admin.pttl(physical)).isEqualTo(-1);
            assertThatThrownBy(() -> store.copy("namespace", "source", "namespace", "target"))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            assertThat(admin.hgetAll(physical)).isEqualTo(before);
            assertThat(admin.pttl(physical)).isEqualTo(-1);
            admin.hdel(physical, "data");
            assertThatThrownBy(() -> store.copy("namespace", "source", "namespace", "target"))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class,
                            e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.blob.spi.BlobStoreException.Code.DATA_LOSS));
            assertThat(admin.hexists(physical, "data")).isFalse();
            assertThatThrownBy(() -> store.copy("namespace", "missing", "namespace", "new"))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
    }

    @Test void oversizedExternallyModifiedSourceCannotBeCopied() {
        String prefix = UUID.randomUUID().toString();
        String physical = new RedisObjectKeys(prefix, RedisWritePolicy.CREATE_ONLY).object("namespace", "source");
        try (var store = store(prefix, RedisWritePolicy.CREATE_ONLY);
                var admin = new redis.clients.jedis.Jedis(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            store.put(spec("source"), new byte[]{1});
            // Direct administrator corruption, beyond the adapter's supported write limit.
            admin.hset(physical.getBytes(java.nio.charset.StandardCharsets.UTF_8), new byte[]{'d','a','t','a'},
                    new byte[BlobStore.MAX_CONDITIONAL_BYTES + 1]);
            assertThatThrownBy(() -> store.copy("namespace", "source", "namespace", "target"))
                    .isInstanceOf(BlobStore.BlobReadLimitException.class);
            assertThatThrownBy(() -> store.get("namespace", "target")).isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
    }
}
