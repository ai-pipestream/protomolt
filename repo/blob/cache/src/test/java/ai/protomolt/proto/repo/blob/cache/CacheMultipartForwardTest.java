package ai.protomolt.proto.repo.blob.cache;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The cache wrapper forwards multipart to its backing store (the s3-redis-cache
 * mode's large streamed writes): the backing decides whether multipart is
 * supported, and the cache stays read-through on the next read.
 */
class CacheMultipartForwardTest {

    @Test
    void putMultipartForwardsToTheBackingStore() {
        List<String> backingCalls = new ArrayList<>();
        BlobStore backing = recording(backingCalls);
        List<String> cacheCalls = new ArrayList<>();
        BlobStore cache = recording(cacheCalls);

        var store = new CachingBlobStore(backing, cache, 0, 1024);
        store.putMultipart(new BlobStore.PutSpec("bucket", "key", "application/octet-stream",
                java.util.Map.of(), null), new ByteArrayInputStream(new byte[16]), 8);

        assertThat(backingCalls).containsExactly("putMultipart");
        assertThat(cacheCalls).as("the cache fills read-through, not on write").isEmpty();
    }

    private static BlobStore recording(List<String> calls) {
        return (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    calls.add(method.getName());
                    if (method.getName().equals("putMultipart")) {
                        return new BlobStore.PutResult("\"etag\"", null);
                    }
                    if (method.getReturnType() == boolean.class) return false;
                    return null;
                });
    }
}
