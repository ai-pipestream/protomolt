package ai.protomolt.proto.repo.container.blob;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConditionalBlobStoreTest {
    private static final String BUCKET = "conditional";
    private static final String KEY = "transcript";

    @Test
    void rejectsWeakWildcardAndListEtagsBeforeAnyWrite() {
        for (String invalid : new String[]{"*", "W/\"tag\"", "tag", "\"a\",\"b\"",
                "\"line\nfeed\"", "\"\""}) {
            assertThatThrownBy(() -> BlobStore.WriteCondition.matching(invalid))
                    .as(invalid).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(BlobStore.WriteCondition.matching("\"opaque-tag\"").expectedEtag())
                .isEqualTo("\"opaque-tag\"");
        assertThat(BlobStore.WriteCondition.matching("\"opaque,tag\"").expectedEtag())
                .isEqualTo("\"opaque,tag\"");
        assertThatThrownBy(() -> new BlobStore.WriteCondition(false, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BlobStore.WriteCondition(true, "\"tag\""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unsupportedStoreRefusesBothConditionalOperations() {
        BlobStore store = new InMemoryBlobStore();
        assertThatThrownBy(() -> store.getForUpdate(BUCKET, KEY))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.conditionalPut(spec(), bytes("value"),
                BlobStore.WriteCondition.absent()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.get(BUCKET, KEY))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test
    void cacheNeverSuppliesAnAuthoritativeTagAndEvictsOnSuccessfulReplacement() {
        AtomicConditionalStore backing = new AtomicConditionalStore();
        InMemoryBlobStore cache = new InMemoryBlobStore();
        CachingBlobStore store = new CachingBlobStore(backing, cache, 0, 1024);
        var first = store.conditionalPut(spec(), bytes("first"), BlobStore.WriteCondition.absent());
        store.get(BUCKET, KEY); // populate expendable cache
        assertThat(cache.get(BUCKET, KEY).data()).isEqualTo(bytes("first"));

        // A different writer changes the truth while the old cache entry remains.
        backing.conditionalPut(spec(), bytes("second"), BlobStore.WriteCondition.matching(first.eTag()));
        assertThat(store.get(BUCKET, KEY).data()).isEqualTo(bytes("first"));
        assertThat(store.getForUpdate(BUCKET, KEY).data()).isEqualTo(bytes("second"));
        assertThat(store.getForUpdate(BUCKET, KEY).eTag()).isEqualTo(backing.getForUpdate(BUCKET, KEY).eTag());
        assertThatThrownBy(() -> store.conditionalPut(spec(), bytes("stale"),
                BlobStore.WriteCondition.matching(first.eTag())))
                .isInstanceOf(BlobStore.BlobConflictException.class);
        assertThat(cache.get(BUCKET, KEY).data()).isEqualTo(bytes("first"));

        var current = store.getForUpdate(BUCKET, KEY);
        store.conditionalPut(spec(), bytes("third"), BlobStore.WriteCondition.matching(current.eTag()));
        assertThatThrownBy(() -> cache.get(BUCKET, KEY))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThat(store.get(BUCKET, KEY).data()).isEqualTo(bytes("third"));
    }

    private static BlobStore.PutSpec spec() {
        return new BlobStore.PutSpec(BUCKET, KEY, "application/octet-stream", null, null);
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    /** Atomic test double only; production Redis remains unsupported. */
    private static final class AtomicConditionalStore extends InMemoryBlobStore {
        private int generation;

        @Override public synchronized GetResult getForUpdate(String bucket, String key) {
            var data = super.get(bucket, key);
            return new GetResult(data.data(), data.contentType(), tag(), null);
        }

        @Override public synchronized PutResult conditionalPut(PutSpec spec, byte[] body,
                WriteCondition condition) {
            GetResult current = null;
            try { current = getForUpdate(spec.bucket(), spec.key()); }
            catch (BlobNotFoundException absent) { /* create precondition handles absence */ }
            if (condition.ifAbsent() ? current != null : current == null
                    || !condition.expectedEtag().equals(current.eTag())) {
                throw new BlobConflictException("condition failed");
            }
            super.put(spec, body);
            generation++;
            return new PutResult(tag(), null);
        }

        private String tag() { return "\"generation-" + generation + "\""; }
    }
}
