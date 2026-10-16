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

/**
 * Item 5 of the review of 1a240b18f: a provider retry cannot rewind a stream under
 * the digest. Streamed puts now send from stack-owned buffers, so a provider retry
 * resends the buffer and the caller's stream is never asked to mark or reset.
 */
class BlobStoreStackTest {
    @Test void streamedPutsAreNotRewindableAndArrayPutsSurviveARetry() throws Exception {
        var store = new RetryingStore();
        var stack = new BlobStoreStack("s1", store, "bucket", "prefix", 8, () -> {});
        var body = "twelve bytes".getBytes();

        // The array put lets the provider retry from the array; the digest matches the stored bytes.
        var ref = stack.put(body);
        assertThat(store.attempts).isEqualTo(2);
        assertThat(store.objects.get(ref.getKey())).isEqualTo(body);
        assertThat(ref.getLength()).isEqualTo(body.length);
        try (var read = stack.open(ref)) {
            assertThat(read.readAllBytes()).isEqualTo(body);
        }

        // A streamed put of one part is buffered and sent as an array: the retry resends the array.
        var small = "seven b".getBytes();
        store.attempts = 0;
        var one = stack.put(new Unrewindable(small), small.length);
        assertThat(store.attempts).isEqualTo(2);
        assertThat(store.objects.get(one.getKey())).isEqualTo(small);
        assertThat(one.getSha256()).isEqualTo(VerifyingInputStream.sha256Hex(small));

        // A larger streamed put goes in parts; each part's retry resends that part's buffer.
        store.attempts = 0;
        var many = stack.put(new Unrewindable(body), body.length);
        assertThat(store.attempts).isEqualTo(4);
        assertThat(store.objects.get(many.getKey())).isEqualTo(body);
        assertThat(many.getSha256()).isEqualTo(VerifyingInputStream.sha256Hex(body));
        assertThat(store.streamedPuts).isZero();
    }

    /** The caller's stream: any rewind attempt fails the test. */
    private static final class Unrewindable extends ByteArrayInputStream {
        Unrewindable(byte[] bytes) { super(bytes); }
        @Override public boolean markSupported() { return false; }
        @Override public synchronized void mark(int limit) { throw new AssertionError("mark"); }
        @Override public synchronized void reset() { throw new AssertionError("reset"); }
    }

    /** Fails the first attempt after consuming the body, then retries like the SDK: mark/reset. */
    private static final class RetryingStore implements BlobStore {
        final Map<String, byte[]> objects = new HashMap<>();
        int attempts;
        int streamedPuts;

        /** The provider's own retry: the first attempt fails after the body was sent, the second succeeds. */
        @Override public PutResult put(PutSpec spec, byte[] body) {
            attempts++; // 503 SlowDown after the body went out
            attempts++;
            objects.put(spec.key(), body.clone());
            return new PutResult("\"etag\"", null);
        }

        /** The same retry over a stream: the provider rewinds the body before resending. */
        @Override public PutResult put(PutSpec spec, InputStream body, long contentLength) {
            streamedPuts++;
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

        /** Each part fails once after it was sent and is resent from the stack's part buffer. */
        @Override public PutResult putMultipart(PutSpec spec, InputStream body, int partSize) {
            try {
                var stored = new java.io.ByteArrayOutputStream();
                byte[] part;
                do {
                    part = body.readNBytes(partSize);
                    attempts++; // the first send of this part fails
                    attempts++;
                    stored.write(part);
                } while (part.length == partSize);
                objects.put(spec.key(), stored.toByteArray());
                return new PutResult("\"etag\"", null);
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
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
