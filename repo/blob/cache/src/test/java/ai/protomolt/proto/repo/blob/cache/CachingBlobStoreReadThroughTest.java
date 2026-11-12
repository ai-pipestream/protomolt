package ai.protomolt.proto.repo.blob.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.ExpiringBlobStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CachingBlobStore} read-through streaming, verified-stream
 * corruption recovery, physical backing namespace isolation, and cache outage
 * fallback.
 */
class CachingBlobStoreReadThroughTest {

    private static final String BUCKET = "shared-bucket";

    @Test
    void isolatesCacheEntriesByBackingNamespaceAcrossStores() throws Exception {
        var backingA = new InMemoryBlobStore();
        var backingB = new InMemoryBlobStore();
        var sharedCache = new InMemoryBlobStore();

        var storeA = new CachingBlobStore(backingA, sharedCache, 60, 1024, "s3:endpoint-a:us-east-1:path");
        var storeB = new CachingBlobStore(backingB, sharedCache, 60, 1024, "s3:endpoint-b:us-east-1:path");

        byte[] bytesA = "alpha-payload".getBytes(StandardCharsets.UTF_8);
        byte[] bytesB = "bravo-payload-different".getBytes(StandardCharsets.UTF_8);
        String key = "documents/stacks/same-key";

        backingA.put(spec(BUCKET, key), bytesA);
        backingB.put(spec(BUCKET, key), bytesB);

        // Populate both caches via openVerifiedStream using the same (bucket, key)
        try (InputStream inA = storeA.openVerifiedStream(BUCKET, key, bytesA.length, sha256Hex(bytesA))) {
            assertThat(inA.readAllBytes()).isEqualTo(bytesA);
        }
        try (InputStream inB = storeB.openVerifiedStream(BUCKET, key, bytesB.length, sha256Hex(bytesB))) {
            assertThat(inB.readAllBytes()).isEqualTo(bytesB);
        }
        assertThat(backingA.streamReads).isEqualTo(1);
        assertThat(backingB.streamReads).isEqualTo(1);

        // Subsequent reads hit their respective isolated cache entries without touching backing
        try (InputStream inA = storeA.openVerifiedStream(BUCKET, key, bytesA.length, sha256Hex(bytesA))) {
            assertThat(inA.readAllBytes()).isEqualTo(bytesA);
        }
        try (InputStream inB = storeB.openVerifiedStream(BUCKET, key, bytesB.length, sha256Hex(bytesB))) {
            assertThat(inB.readAllBytes()).isEqualTo(bytesB);
        }
        assertThat(backingA.streamReads).isEqualTo(1);
        assertThat(backingB.streamReads).isEqualTo(1);
        assertThat(storeA.cacheBucket(BUCKET)).isNotEqualTo(storeB.cacheBucket(BUCKET));

        // Reclaiming on storeA evicts only storeA's cache entry, leaving storeB's intact
        var reclaimerA = storeA.reclaimer(backingA::delete);
        assertThat(reclaimerA.reclaim(BUCKET, key)).isTrue();
        assertThatThrownBy(() -> sharedCache.get(storeA.cacheBucket(BUCKET), key))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThat(sharedCache.get(storeB.cacheBucket(BUCKET), key).data()).isEqualTo(bytesB);
    }

    @Test
    void openStreamPopulatesCacheWhenWithinCeilingAndSkipsOversizedOrEarlyClosedStreams() throws Exception {
        var backing = new InMemoryBlobStore();
        var cache = new InMemoryBlobStore();
        var store = new CachingBlobStore(backing, cache, 120, 16, "s3:primary");

        byte[] small = "0123456789abcdef".getBytes(StandardCharsets.UTF_8); // 16 bytes (fits)
        byte[] large = "0123456789abcdefX".getBytes(StandardCharsets.UTF_8); // 17 bytes (exceeds 16)
        backing.put(spec(BUCKET, "small"), small);
        backing.put(spec(BUCKET, "large"), large);

        // Early close before EOF does not populate the cache
        try (InputStream partial = store.openStream(BUCKET, "small", 0, -1)) {
            assertThat(partial.readNBytes(4)).isEqualTo("0123".getBytes(StandardCharsets.UTF_8));
        }
        assertThatThrownBy(() -> cache.get(store.cacheBucket(BUCKET), "small"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);

        // Full read populates the cache with configured TTL
        try (InputStream full = store.openStream(BUCKET, "small", 0, -1)) {
            assertThat(full.readAllBytes()).isEqualTo(small);
        }
        assertThat(cache.get(store.cacheBucket(BUCKET), "small").data()).isEqualTo(small);
        assertThat(cache.lastTtlSeconds).isEqualTo(120);
        int readsBeforeHit = backing.streamReads;
        try (InputStream cachedHit = store.openStream(BUCKET, "small", 0, -1)) {
            assertThat(cachedHit.readAllBytes()).isEqualTo(small);
        }
        assertThat(backing.streamReads).isEqualTo(readsBeforeHit);

        // Range read bypasses the cache
        try (InputStream range = store.openStream(BUCKET, "small", 2, 4)) {
            assertThat(range.readAllBytes()).isEqualTo("2345".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(backing.streamReads).isEqualTo(readsBeforeHit + 1);

        // Oversized stream read streams through cleanly without populating the cache
        try (InputStream oversized = store.openStream(BUCKET, "large", 0, -1)) {
            assertThat(oversized.readAllBytes()).isEqualTo(large);
        }
        assertThatThrownBy(() -> cache.get(store.cacheBucket(BUCKET), "large"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test
    void openVerifiedStreamEvictsCorruptOrTruncatedCacheEntryAndRecoversFromBacking() throws Exception {
        var backing = new InMemoryBlobStore();
        var cache = new InMemoryBlobStore();
        var store = new CachingBlobStore(backing, cache, 60, 1024, "s3:verified");

        byte[] authoritative = "verified-authoritative-bytes".getBytes(StandardCharsets.UTF_8);
        String sha = sha256Hex(authoritative);
        store.put(spec(BUCKET, "obj"), authoritative);

        // Poison the cache with wrong-digest bytes of the same length
        byte[] corruptedSameLength = authoritative.clone();
        corruptedSameLength[0] ^= 0x01;
        cache.data.put(InMemoryBlobStore.coord(store.cacheBucket(BUCKET), "obj"), corruptedSameLength);

        try (InputStream stream = store.openVerifiedStream(BUCKET, "obj", authoritative.length, sha)) {
            assertThat(stream.readAllBytes()).isEqualTo(authoritative);
        }
        // The corrupt entry was evicted and repopulated with the verified authoritative bytes
        assertThat(cache.get(store.cacheBucket(BUCKET), "obj").data()).isEqualTo(authoritative);

        // Poison the cache with truncated bytes
        cache.data.put(InMemoryBlobStore.coord(store.cacheBucket(BUCKET), "obj"),
                Arrays.copyOf(authoritative, authoritative.length - 3));
        try (InputStream stream = store.openVerifiedStream(BUCKET, "obj", authoritative.length, sha)) {
            assertThat(stream.readAllBytes()).isEqualTo(authoritative);
        }
        assertThat(cache.get(store.cacheBucket(BUCKET), "obj").data()).isEqualTo(authoritative);
    }

    @Test
    void cacheOutageFallsThroughToAuthoritativeBackingForReadsWritesAndDeletes() throws Exception {
        var backing = new InMemoryBlobStore();
        var brokenCache = new InMemoryBlobStore();
        brokenCache.failAll = true;
        var store = new CachingBlobStore(backing, brokenCache, 60, 1024, "s3:outage");

        byte[] payload = "resilient-payload".getBytes(StandardCharsets.UTF_8);
        store.put(spec(BUCKET, "k1"), payload);
        assertThat(store.get(BUCKET, "k1").data()).isEqualTo(payload);

        try (InputStream s1 = store.openStream(BUCKET, "k1", 0, -1)) {
            assertThat(s1.readAllBytes()).isEqualTo(payload);
        }
        try (InputStream s2 = store.openVerifiedStream(BUCKET, "k1", payload.length, sha256Hex(payload))) {
            assertThat(s2.readAllBytes()).isEqualTo(payload);
        }
        assertThat(store.objectLength(BUCKET, "k1")).isEqualTo(payload.length);
        assertThat(store.delete(BUCKET, "k1")).isTrue();
    }

    @Test
    void aCacheHitForAnObjectMissingFromBackingFailsNotFoundOnEveryReadPathAndEvicts() throws Exception {
        var backing = new InMemoryBlobStore();
        var cache = new InMemoryBlobStore();
        var store = new CachingBlobStore(backing, cache, 60, 1024, "s3:truth");
        byte[] payload = "cached-but-gone".getBytes(StandardCharsets.UTF_8);
        String sha = sha256Hex(payload);

        List<Runnable> reads = List.of(
                () -> store.get(BUCKET, "gone"),
                () -> store.getBounded(BUCKET, "gone", null, 1024),
                () -> readAll(store.openStream(BUCKET, "gone", 0, -1)),
                () -> readAll(store.openVerifiedStream(BUCKET, "gone", payload.length, sha)));
        for (Runnable read : reads) {
            cache.put(spec(store.cacheBucket(BUCKET), "gone"), payload);
            assertThatThrownBy(read::run).isInstanceOf(BlobStore.BlobNotFoundException.class);
            assertThatThrownBy(() -> cache.get(store.cacheBucket(BUCKET), "gone"))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
        assertThat(backing.streamReads).isZero();
        assertThat(backing.boundedReads).isZero();

        // Existence is answered by backing alone, never by a cache entry.
        cache.put(spec(store.cacheBucket(BUCKET), "gone"), payload);
        assertThatThrownBy(() -> store.headObject(BUCKET, "gone"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);

        // A present object is served from the cache after the existence check.
        backing.put(spec(BUCKET, "here"), payload);
        cache.put(spec(store.cacheBucket(BUCKET), "here"), payload);
        assertThat(store.get(BUCKET, "here").data()).isEqualTo(payload);
        try (InputStream in = store.openVerifiedStream(BUCKET, "here", payload.length, sha)) {
            assertThat(in.readAllBytes()).isEqualTo(payload);
        }
        assertThat(backing.streamReads).isZero();
    }

    @Test
    void getBoundedServesHitsWithinTheLimitAndLetsBackingEnforceMissesOversizeAndOutage() throws Exception {
        var backing = new InMemoryBlobStore();
        var cache = new InMemoryBlobStore();
        var store = new CachingBlobStore(backing, cache, 60, 16, "s3:bounded");
        byte[] small = "0123456789".getBytes(StandardCharsets.UTF_8);
        byte[] large = "0123456789abcdefXYZ".getBytes(StandardCharsets.UTF_8);
        backing.put(spec(BUCKET, "small"), small);
        backing.put(spec(BUCKET, "large"), large);

        // Miss reads backing within the limit and populates the cache.
        assertThat(store.getBounded(BUCKET, "small", null, 10).data()).isEqualTo(small);
        assertThat(backing.boundedReads).isEqualTo(1);
        assertThat(cache.get(store.cacheBucket(BUCKET), "small").data()).isEqualTo(small);

        // Hit within the limit: no backing body read.
        assertThat(store.getBounded(BUCKET, "small", null, 10).data()).isEqualTo(small);
        assertThat(backing.boundedReads).isEqualTo(1);

        // A cached object over the caller's limit is still refused by backing, never truncated.
        assertThatThrownBy(() -> store.getBounded(BUCKET, "small", null, 9))
                .isInstanceOf(BlobStore.BlobReadLimitException.class);
        assertThat(backing.boundedReads).isEqualTo(2);

        // Oversized for the cache ceiling: backing enforces the limit and nothing is cached.
        assertThat(store.getBounded(BUCKET, "large", null, 64).data()).isEqualTo(large);
        assertThatThrownBy(() -> store.getBounded(BUCKET, "large", null, 18))
                .isInstanceOf(BlobStore.BlobReadLimitException.class);
        assertThatThrownBy(() -> cache.get(store.cacheBucket(BUCKET), "large"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThatThrownBy(() -> store.getBounded(BUCKET, "small", null, -1))
                .isInstanceOf(IllegalArgumentException.class);

        // Cache outage: bounded reads come from backing.
        var brokenCache = new InMemoryBlobStore();
        brokenCache.failAll = true;
        var outage = new CachingBlobStore(backing, brokenCache, 60, 16, "s3:bounded");
        assertThat(outage.getBounded(BUCKET, "small", null, 10).data()).isEqualTo(small);
        assertThatThrownBy(() -> outage.getBounded(BUCKET, "large", null, 18))
                .isInstanceOf(BlobStore.BlobReadLimitException.class);
    }

    @Test
    void unavailableAuthorityIsNeverHiddenByAValidCachedBody() {
        var backing = new InMemoryBlobStore();
        var cache = new InMemoryBlobStore();
        var store = new CachingBlobStore(backing, cache, 60, 1024);
        store.put(spec(BUCKET, "present"), new byte[]{1, 2, 3});
        backing.failAll = true;
        assertThatThrownBy(() -> store.get(BUCKET, "present"))
                .isInstanceOf(IllegalStateException.class).hasMessage("Simulated cache outage");
        assertThatThrownBy(() -> store.openVerifiedStream(BUCKET, "present", 3, null))
                .isInstanceOf(IllegalStateException.class).hasMessage("Simulated cache outage");
    }

    private static void readAll(InputStream in) {
        try (in) {
            in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void interruptionDuringCacheFillDoesNotBecomeSuccessfulEof() throws Exception {
        var backing = new InMemoryBlobStore();
        var cache = new InMemoryBlobStore();
        var store = new CachingBlobStore(backing, cache, 60, 1024);
        byte[] payload = {1, 2, 3};
        backing.put(spec(BUCKET, "cancelled"), payload);
        cache.interruptStreamPut = true;
        try (InputStream input = store.openVerifiedStream(BUCKET, "cancelled", payload.length, sha256Hex(payload))) {
            byte[] received = new byte[payload.length];
            assertThat(input.read(received)).isEqualTo(payload.length);
            assertThat(received).isEqualTo(payload);
            assertThatThrownBy(input::read).isInstanceOf(IOException.class)
                    .hasMessage("Cache read cancelled")
                    .hasRootCauseMessage("Interrupted cache transfer");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThatThrownBy(() -> cache.get(BUCKET, "cancelled"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    private static BlobStore.PutSpec spec(String bucket, String key) {
        return new BlobStore.PutSpec(bucket, key, "application/octet-stream", Map.of(), null);
    }

    private static String sha256Hex(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static final class InMemoryBlobStore implements ExpiringBlobStore {
        private final Map<String, byte[]> data = new HashMap<>();
        private final Map<String, String> etags = new HashMap<>();
        private final Map<String, Map<String, String>> attributes = new HashMap<>();
        boolean failAll;
        boolean interruptStreamPut;
        int streamReads;
        int boundedReads;
        int lastTtlSeconds;

        private static String coord(String bucket, String key) {
            return bucket + "/" + key;
        }

        private void checkAvailable() {
            if (failAll) {
                throw new IllegalStateException("Simulated cache outage");
            }
        }

        @Override
        public PutResult put(PutSpec spec, byte[] body) {
            return put(spec, body, 0);
        }

        @Override
        public PutResult put(PutSpec spec, byte[] body, int ttlSeconds) {
            checkAvailable();
            lastTtlSeconds = ttlSeconds;
            byte[] copy = body.clone();
            String k = coord(spec.bucket(), spec.key());
            data.put(k, copy);
            attributes.put(k, spec.metadata() == null ? Map.of() : Map.copyOf(spec.metadata()));
            try {
                String etag = "\"" + sha256Hex(copy) + "\"";
                etags.put(k, etag);
                return new PutResult(etag, null);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public PutResult put(PutSpec spec, InputStream body, long contentLength) {
            checkAvailable();
            try {
                return put(spec, body.readAllBytes(), 0);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public PutResult put(PutSpec spec, InputStream body, long length, int ttl) {
            if (interruptStreamPut) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted cache transfer");
            }
            try { return put(spec, body.readAllBytes(), ttl); }
            catch (IOException failure) { throw new UncheckedIOException(failure); }
        }

        @Override
        public ObjectMetadata statObject(String bucket, String key) {
            headObject(bucket, key);
            String k = coord(bucket, key);
            return new ObjectMetadata(data.get(k).length, "application/octet-stream", etags.get(k), null,
                    attributes.getOrDefault(k, Map.of()));
        }

        @Override
        public ObjectRead openSnapshot(String bucket, String key) {
            return new ObjectRead(statObject(bucket, key), openStream(bucket, key, 0, -1));
        }

        @Override
        public GetResult get(String bucket, String key, String versionId) {
            checkAvailable();
            String k = coord(bucket, key);
            byte[] found = data.get(k);
            if (found == null) {
                throw new BlobNotFoundException("Not found: " + k);
            }
            return new GetResult(found.clone(), "application/octet-stream", etags.get(k), null);
        }

        @Override
        public GetResult getBounded(String bucket, String key, String versionId, int maxBytes) {
            checkAvailable();
            boundedReads++;
            GetResult got = get(bucket, key, versionId);
            if (got.data().length > maxBytes) {
                throw new BlobReadLimitException(maxBytes);
            }
            return got;
        }

        @Override
        public InputStream openStream(String bucket, String key, long offset, long length) {
            checkAvailable();
            streamReads++;
            String k = coord(bucket, key);
            byte[] found = data.get(k);
            if (found == null) {
                throw new BlobNotFoundException("Not found: " + k);
            }
            int start = (int) offset;
            int count = length < 0 ? found.length - start : (int) Math.min(length, found.length - start);
            return new ByteArrayInputStream(found, start, count);
        }

        @Override
        public long objectLength(String bucket, String key) {
            checkAvailable();
            String k = coord(bucket, key);
            byte[] found = data.get(k);
            if (found == null) {
                throw new BlobNotFoundException("Not found: " + k);
            }
            return found.length;
        }

        @Override
        public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
            checkAvailable();
            GetResult got = get(srcBucket, srcKey, null);
            put(spec(dstBucket, dstKey), got.data());
        }

        @Override
        public boolean delete(String bucket, String key) {
            checkAvailable();
            String k = coord(bucket, key);
            etags.remove(k);
            return data.remove(k) != null;
        }

        @Override
        public BatchDeleteResult deleteAll(String bucket, List<String> keys) {
            checkAvailable();
            for (String key : keys) {
                delete(bucket, key);
            }
            return new BatchDeleteResult(Map.of());
        }

        @Override
        public List<ListedObject> list(String bucket, String prefix) {
            checkAvailable();
            return List.of();
        }

        @Override
        public void headBucket(String bucket) {
            checkAvailable();
        }

        @Override
        public void headObject(String bucket, String key) {
            checkAvailable();
            if (!data.containsKey(coord(bucket, key))) {
                throw new BlobNotFoundException("Not found: " + bucket + "/" + key);
            }
        }
    }
}
