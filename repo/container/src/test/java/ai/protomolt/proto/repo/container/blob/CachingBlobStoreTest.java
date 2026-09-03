package ai.protomolt.proto.repo.container.blob;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link CachingBlobStore} semantics over two {@link InMemoryBlobStore}s —
 * the cache is expendable, the backing store is truth. The tests hold both
 * store references so they can observe the cache directly (read-through
 * population, write-through landing, eviction).
 */
class CachingBlobStoreTest {

    private static final String BUCKET = "docs";

    private static BlobStore.PutSpec spec(String key, byte[] body) {
        return new BlobStore.PutSpec(BUCKET, key, "text/plain", null, null);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void readThroughPopulatesTheCache() {
        InMemoryBlobStore backing = new InMemoryBlobStore();
        InMemoryBlobStore cache = new InMemoryBlobStore();
        CachingBlobStore store = new CachingBlobStore(backing, cache, 0, 1024);
        backing.put(spec("k", bytes("truth")), bytes("truth"));

        assertThat(store.get(BUCKET, "k").data()).isEqualTo(bytes("truth"));
        // The miss populated the cache...
        assertThat(cache.list(BUCKET, "")).extracting(BlobStore.ListedObject::key)
                .containsExactly("k");
        // ...and the second read is served from it: corrupt the backing copy
        // and the cache still answers (truth restored afterwards).
        backing.put(spec("k", bytes("corrupted")), bytes("corrupted"));
        assertThat(store.get(BUCKET, "k").data()).isEqualTo(bytes("truth"));
    }

    @Test
    void writeThroughLandsInBothStores() {
        InMemoryBlobStore backing = new InMemoryBlobStore();
        InMemoryBlobStore cache = new InMemoryBlobStore();
        CachingBlobStore store = new CachingBlobStore(backing, cache, 0, 1024);

        store.put(spec("k", bytes("v")), bytes("v"));

        assertThat(backing.get(BUCKET, "k").data()).isEqualTo(bytes("v"));
        assertThat(cache.get(BUCKET, "k").data()).isEqualTo(bytes("v"));
    }

    @Test
    void oversizedObjectBypassesTheCache() {
        InMemoryBlobStore backing = new InMemoryBlobStore();
        InMemoryBlobStore cache = new InMemoryBlobStore();
        CachingBlobStore store = new CachingBlobStore(backing, cache, 0, 8);
        byte[] big = bytes("way-over-the-ceiling");

        store.put(spec("big", big), big);
        assertThat(backing.get(BUCKET, "big").data()).isEqualTo(big);
        assertThat(cache.list(BUCKET, "")).isEmpty();

        // Reads of oversized objects never populate the cache either.
        assertThat(store.get(BUCKET, "big").data()).isEqualTo(big);
        assertThat(cache.list(BUCKET, "")).isEmpty();
    }

    @Test
    void deleteEvictsTheCache() {
        InMemoryBlobStore backing = new InMemoryBlobStore();
        InMemoryBlobStore cache = new InMemoryBlobStore();
        CachingBlobStore store = new CachingBlobStore(backing, cache, 0, 1024);
        store.put(spec("k", bytes("v1")), bytes("v1"));

        assertThat(store.delete(BUCKET, "k")).isTrue();
        assertThat(cache.list(BUCKET, "")).isEmpty();

        // A fresh backing write is not shadowed by a stale cache entry.
        backing.put(spec("k", bytes("v2")), bytes("v2"));
        assertThat(store.get(BUCKET, "k").data()).isEqualTo(bytes("v2"));
    }

    @Test
    void copyEvictsTheCachedDestination() {
        InMemoryBlobStore backing = new InMemoryBlobStore();
        InMemoryBlobStore cache = new InMemoryBlobStore();
        CachingBlobStore store = new CachingBlobStore(backing, cache, 0, 1024);
        store.put(spec("src", bytes("new")), bytes("new"));
        store.put(spec("dst", bytes("stale")), bytes("stale")); // cached

        store.copy(BUCKET, "src", BUCKET, "dst");

        // The destination read reflects the copy, not the stale cache entry.
        assertThat(store.get(BUCKET, "dst").data()).isEqualTo(bytes("new"));
    }

    @Test
    void listAndHeadBucketNeverTouchTheCache() {
        InMemoryBlobStore backing = new InMemoryBlobStore();
        InMemoryBlobStore cache = new InMemoryBlobStore();
        CachingBlobStore store = new CachingBlobStore(backing, cache, 0, 1024);
        store.put(spec("k", bytes("v")), bytes("v"));

        // list comes from backing: drop the backing entry directly and the
        // listing reflects it even though the cache still holds the key.
        backing.delete(BUCKET, "k");
        assertThat(store.list(BUCKET, "")).isEmpty();
        assertThat(cache.list(BUCKET, "")).isNotEmpty();

        // headBucket delegates to backing only: this bucket exists ONLY in
        // backing, and the cache would answer "not reachable" for a bucket it
        // never heard of (InMemoryBlobStore's headBucket contract).
        backing.put(new BlobStore.PutSpec("backing-only", "k", "text/plain", null, null),
                bytes("v"));
        store.headBucket("backing-only");
    }

    @Test
    void headObjectAnswersFromTheCacheButFallsThroughOnMiss() {
        InMemoryBlobStore backing = new InMemoryBlobStore();
        InMemoryBlobStore cache = new InMemoryBlobStore();
        CachingBlobStore store = new CachingBlobStore(backing, cache, 0, 1024);
        store.put(spec("k", bytes("v")), bytes("v"));

        // Backing entry secretly removed: the cache still answers existence
        // (existence caching — documented).
        backing.delete(BUCKET, "k");
        store.headObject(BUCKET, "k");

        // Cache miss falls through to backing; absence everywhere maps to
        // BlobNotFoundException.
        cache.delete(BUCKET, "k");
        assertThatThrownBy(() -> store.headObject(BUCKET, "k"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }
}
