package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.util.Map;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class RedisAtomicStorageIT {
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    private RedisBlobStore store;
    private static String uri() { return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379); }
    @BeforeEach void open() { store = new RedisBlobStore(new RedisBlobStoreConfig(uri(), 0, 1024, "atomic")); }
    @AfterEach void close() { store.close(); }
    private static BlobStore.PutSpec spec(String namespace, String key) { return new BlobStore.PutSpec(namespace, key, "text/plain", Map.of(), null); }

    @Test void namespacesAndPrefixIdentitiesCannotAlias() {
        store.put(spec("a/b", "c"), new byte[]{1});
        store.put(spec("a", "b/c"), new byte[]{2});
        assertThat(store.get("a/b", "c").data()).containsExactly(1);
        assertThat(store.get("a", "b/c").data()).containsExactly(2);
        try (var one = new RedisBlobStore(new RedisBlobStoreConfig(uri(), 0, 1024, "x"));
                var two = new RedisBlobStore(new RedisBlobStoreConfig(uri(), 0, 1024, "xa"))) {
            one.put(spec("ab", "key"), new byte[]{3}); two.put(spec("b", "key"), new byte[]{4});
            assertThat(one.get("ab", "key").data()).containsExactly(3);
            assertThat(two.get("b", "key").data()).containsExactly(4);
        }
    }
    @Test void metadataSuffixIsAnOrdinaryObjectName() {
        store.put(spec("suffix", "a"), new byte[]{1});
        store.put(spec("suffix", "a$meta"), new byte[]{2});
        assertThat(store.get("suffix", "a").data()).containsExactly(1);
        assertThat(store.get("suffix", "a$meta").data()).containsExactly(2);
        assertThat(store.list("suffix", "")).extracting(BlobStore.ListedObject::key).containsExactlyInAnyOrder("a", "a$meta");
        store.delete("suffix", "a");
        assertThat(store.get("suffix", "a$meta").data()).containsExactly(2);
    }
    @Test void listingPrefixIsLiteralNotRedisGlobSyntax() {
        store.put(spec("listing", "a*literal"), new byte[]{1});
        store.put(spec("listing", "another"), new byte[]{2});
        assertThat(store.list("listing", "a*")).extracting(BlobStore.ListedObject::key).containsExactly("a*literal");
    }
    @Test void versionsAreRejectedRatherThanReturningCurrentBytes() {
        store.put(spec("versions", "key"), new byte[]{1});
        assertThatThrownBy(() -> store.get("versions", "key", "requested-version")).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void boundedReadReturnsExactBytesOrExplicitFailure() {
        store.put(spec("bounded", "key"), new byte[]{1, 2, 3});
        assertThat(store.getBounded("bounded", "key", null, 3).data()).containsExactly(1, 2, 3);
        assertThatThrownBy(() -> store.getBounded("bounded", "key", null, 2)).isInstanceOf(BlobStore.BlobReadLimitException.class);
        store.put(spec("bounded", "empty"), new byte[0]);
        assertThat(store.getBounded("bounded", "empty", null, 0).data()).isEmpty();
    }
    @Test void streamingWriteRequiresExactLengthBeforeAnyWrite() {
        store.put(spec("stream", "key"), new byte[]{7});
        for (var bytes : new byte[][]{{1}, {1, 2, 3}}) {
            assertThatThrownBy(() -> store.put(spec("stream", "key"), new java.io.ByteArrayInputStream(bytes), 2))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(store.get("stream", "key").data()).containsExactly(7);
        }
    }

    @Test void oversizedReadRejectsOnServerBeforeFetchingBody() {
        store.put(spec("bounded-proof", "key"), new byte[100]);
        try (var admin = new redis.clients.jedis.Jedis(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            long before = hgetCalls(admin);
            assertThatThrownBy(() -> store.getBounded("bounded-proof", "key", null, 99)).isInstanceOf(BlobStore.BlobReadLimitException.class);
            assertThat(hgetCalls(admin)).isEqualTo(before);
            assertThat(store.getBounded("bounded-proof", "key", null, 100).data()).hasSize(100);
            assertThat(hgetCalls(admin)).isGreaterThan(before);
        }
    }

    @Test @Timeout(30) void concurrentReadsNeverMixBytesContentTypeOrDigest() throws Exception {
        byte[] a = new byte[100]; byte[] b = new byte[100]; java.util.Arrays.fill(b, (byte) 1);
        var typeA = new BlobStore.PutSpec("concurrent", "key", "type/a", Map.of(), null);
        var typeB = new BlobStore.PutSpec("concurrent", "key", "type/b", Map.of(), null);
        store.put(typeA, a);
        try (var tasks = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var one = tasks.submit(() -> { for (int i = 0; i < 300; i++) store.put(typeA, a); });
            var two = tasks.submit(() -> { for (int i = 0; i < 300; i++) store.put(typeB, b); });
            for (int i = 0; i < 600; i++) {
                var got = store.getBounded("concurrent", "key", null, 100);
                assertThat(got.contentType()).isEqualTo(got.data()[0] == 0 ? "type/a" : "type/b");
                assertThat(got.eTag()).isEqualTo("\"" + ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(got.data()) + "\"");
            }
            one.get(); two.get();
        }
    }

    @Test void legacyBytesAreNotSilentlyReadAndReclamationCatchesLaterWrites() throws Exception {
        try (var admin = new redis.clients.jedis.Jedis(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            admin.set("atomiclegacy/key", "old bytes");
        }
        assertThatThrownBy(() -> store.get("legacy", "key")).isInstanceOf(BlobStore.BlobNotFoundException.class);
        try (var handle = new RedisBlobStoreProvider().open(Map.of("uri", uri(), "ttl-seconds", "0", "max-object-bytes", "1024", "key-prefix", "atomic"))) {
            assertThat(handle.reclaimer().reclaim("recovery", "key")).isTrue();
            handle.store().put(spec("recovery", "key"), new byte[]{1});
            assertThat(handle.reclaimer().reclaim("recovery", "key")).isTrue();
            assertThatThrownBy(() -> handle.store().get("recovery", "key")).isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
    }

    @Test void streamLimitsAreCheckedBeforeReadingAndSelfCopyPreservesBytes() {
        var touched = new java.util.concurrent.atomic.AtomicBoolean();
        var input = new java.io.InputStream() { public int read() { touched.set(true); return -1; } };
        assertThatThrownBy(() -> store.put(spec("stream", "too-large"), input, 1025)).isInstanceOf(IllegalArgumentException.class);
        assertThat(touched).isFalse();
        store.put(spec("copy", "same"), new byte[]{1, 2});
        store.copy("copy", "same", "copy", "same");
        assertThat(store.get("copy", "same").data()).containsExactly(1, 2);
    }

    @Test void incompleteMetadataFailsClosedAndCannotOverwriteCopyTarget() {
        store.put(spec("corrupt", "source"), new byte[]{1});
        store.put(spec("corrupt", "target"), new byte[]{2});
        try (var admin = new redis.clients.jedis.Jedis(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            admin.hdel(new RedisObjectKeys("atomic").object("corrupt", "source"), "etag");
        }
        assertThatThrownBy(() -> store.get("corrupt", "source"))
                .isInstanceOf(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class);
        assertThatThrownBy(() -> store.copy("corrupt", "source", "corrupt", "target"))
                .isInstanceOf(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class);
        assertThat(store.get("corrupt", "target").data()).containsExactly(2);
    }

    @Test void overwriteAndCopyApplyDestinationExpiryInBothDirections() {
        try (var expiring = new RedisBlobStore(new RedisBlobStoreConfig(uri(), 60, 1024, "atomic"));
                var admin = new redis.clients.jedis.Jedis(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            var address = new RedisObjectKeys("atomic");
            store.put(spec("ttl", "source"), new byte[]{1});
            store.put(spec("ttl", "target"), new byte[]{2});
            expiring.copy("ttl", "source", "ttl", "target");
            assertThat(admin.ttl(address.object("ttl", "target"))).isBetween(1L, 60L);
            store.copy("ttl", "source", "ttl", "target");
            assertThat(admin.ttl(address.object("ttl", "target"))).isEqualTo(-1L);
            expiring.put(spec("ttl", "target"), new byte[]{3});
            assertThat(admin.ttl(address.object("ttl", "target"))).isBetween(1L, 60L);
            store.put(spec("ttl", "target"), new byte[]{4});
            assertThat(admin.ttl(address.object("ttl", "target"))).isEqualTo(-1L);
        }
    }

    @Test void oversizedOrMalformedMetadataCannotReplaceExistingObject() {
        store.put(spec("metadata", "key"), new byte[]{7});
        var tooMany = new java.util.HashMap<String, String>();
        for (int i = 0; i < 257; i++) tooMany.put("key" + i, "value");
        for (var metadata : java.util.List.of(tooMany, Map.of("key", "x".repeat(65536)),
                Map.of("key", "é".repeat(32768)), Map.of("key", "\uD800"))) {
            assertThatThrownBy(() -> store.put(new BlobStore.PutSpec("metadata", "key", "text/plain", metadata, null), new byte[]{1}))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(store.get("metadata", "key").data()).containsExactly(7);
        }
        assertThatThrownBy(() -> store.put(new BlobStore.PutSpec("metadata", "key", "x".repeat(65537), Map.of(), null), new byte[]{1}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.get("metadata", "key").data()).containsExactly(7);
        store.put(new BlobStore.PutSpec("metadata", "boundary", "", Map.of("k", "x".repeat(65535)), null), new byte[]{8});
        assertThat(store.get("metadata", "boundary").data()).containsExactly(8);
    }

    private static long hgetCalls(redis.clients.jedis.Jedis admin) {
        var match = java.util.regex.Pattern.compile("cmdstat_hget:calls=(\\d+)").matcher(admin.info("commandstats"));
        return match.find() ? Long.parseLong(match.group(1)) : 0;
    }
}
