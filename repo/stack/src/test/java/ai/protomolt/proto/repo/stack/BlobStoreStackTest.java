package ai.protomolt.proto.repo.stack;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Item 5 of the review of 1a240b18f: a provider retry cannot rewind a stream under the digest. */
class BlobStoreStackTest {
    @Test void streamedPutsAreNotRewindableAndArrayPutsSurviveARetry() throws Exception {
        var store = new RetryingStore();
        var stack = new BlobStoreStack("s1", store, "bucket", "prefix");
        var body = "twelve bytes".getBytes();

        // The array put lets the provider retry from the array; the digest matches the stored bytes.
        var ref = stack.put(body);
        assertThat(store.attempts).isEqualTo(2);
        assertThat(store.objects.get(ref.getKey())).isEqualTo(body);
        assertThat(ref.getLength()).isEqualTo(body.length);
        try (var read = stack.open(ref)) {
            assertThat(read.readAllBytes()).isEqualTo(body);
        }

        // A streamed put refuses the rewind instead of letting a short or empty body through.
        store.attempts = 0;
        assertThatThrownBy(() -> stack.put(new ByteArrayInputStream(body), body.length))
                .isInstanceOf(IOException.class).hasMessageContaining("Stack write failed")
                .rootCause().hasMessageContaining("cannot be rewound");
        assertThat(store.objects).hasSize(1);
    }

    /** Fails the first attempt after consuming the body, then retries like the SDK: mark/reset. */
    private static final class RetryingStore implements BlobStore {
        final Map<String, byte[]> objects = new HashMap<>();
        int attempts;

        /** The provider's own retry: the first attempt fails after the body was sent, the second succeeds. */
        @Override public PutResult put(PutSpec spec, byte[] body) {
            attempts++; // 503 SlowDown after the body went out
            attempts++;
            objects.put(spec.key(), body.clone());
            return new PutResult("\"etag\"", null);
        }

        /** The same retry over a stream: the provider rewinds the body before resending. */
        @Override public PutResult put(PutSpec spec, InputStream body, long contentLength) {
            try {
                if (body.markSupported()) body.mark(Integer.MAX_VALUE);
                body.readAllBytes();
                attempts++;
                body.reset(); // the retry
                attempts++;
                objects.put(spec.key(), body.readAllBytes());
                return new PutResult("\"etag\"", null);
            } catch (IOException failure) {
                throw new IllegalStateException(failure);
            }
        }

        @Override public InputStream openStream(String bucket, String key, long offset, long length) {
            var bytes = objects.get(key);
            if (bytes == null) throw new BlobNotFoundException("missing " + key);
            return new ByteArrayInputStream(bytes);
        }

        @Override public GetResult get(String bucket, String key, String versionId) { throw new UnsupportedOperationException(); }
        @Override public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) { throw new UnsupportedOperationException(); }
        @Override public boolean delete(String bucket, String key) { return objects.remove(key) != null; }
        @Override public BatchDeleteResult deleteAll(String bucket, List<String> keys) { return new BatchDeleteResult(Map.of()); }
        @Override public List<ListedObject> list(String bucket, String prefix) { return List.of(); }
        @Override public void headBucket(String bucket) {}
        @Override public void headObject(String bucket, String key) {}
    }
}
