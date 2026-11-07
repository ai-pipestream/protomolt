package ai.protomolt.proto.repo.admission;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;

class DocumentSchemaTypeCacheTest {
    private static final DocumentSchemaTypeCache.Key KEY = new DocumentSchemaTypeCache.Key(
            new ClosedDescriptorSet.Limits(1_000_000, 16, 16, 16),
            PublicationSchemaCondition.getDefaultInstance());

    private static DocumentSchemaTypeCache cache() {
        return new DocumentSchemaTypeCache(new DocumentSchemaTypeCache.Limits(4, 64L * 1024 * 1024));
    }

    @Test
    void aSliceOfALargerBufferIsStoredAsExactlyThatSlice() {
        ByteString larger = ByteString.copyFromUtf8("before-the-artifact-after");
        ByteString slice = larger.substring(7, 19);
        var cache = cache();

        cache.put(KEY, StringValue.getDescriptor(), slice, "sha");

        var stored = cache.find(KEY, slice);
        assertThat(stored).isNotNull();
        assertThat(stored.artifact().toStringUtf8()).isEqualTo("the-artifact");
        assertThat(stored.artifact()).isNotSameAs(slice);
        assertThat(cache.artifactBytes()).isEqualTo(slice.size());
    }

    @Test
    void aRopeArtifactIsStoredWhole() {
        ByteString rope = ByteString.copyFromUtf8("first-").concat(ByteString.copyFromUtf8("second"));
        var cache = cache();

        cache.put(KEY, StringValue.getDescriptor(), rope, "sha");

        assertThat(cache.find(KEY, rope).artifact().toStringUtf8()).isEqualTo("first-second");
    }

    @Test
    void admittingAnArtifactCopiesItOnce() {
        int size = 16 * 1024 * 1024;
        ByteString slice = ByteString.copyFrom(new byte[size + 1]).substring(1);
        var cache = cache();
        var threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long thread = Thread.currentThread().threadId();
        long before = threads.getThreadAllocatedBytes(thread);

        cache.put(KEY, StringValue.getDescriptor(), slice, "sha");

        // One copy is 16 MiB. Two would be 32 MiB.
        assertThat(threads.getThreadAllocatedBytes(thread) - before).isLessThan(size + size / 2);
    }
}
