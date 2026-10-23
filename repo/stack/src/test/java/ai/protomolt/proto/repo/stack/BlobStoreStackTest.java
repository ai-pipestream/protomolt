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

        // A single-part streamed put streams directly to the provider and refuses a
        // provider rewind, even when the caller's stream supports mark/reset.
        var small = "seven b".getBytes();
        store.attempts = 0;
        assertThatThrownBy(() -> stack.put(new ByteArrayInputStream(small), small.length))
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("Stack write streams cannot be rewound");
        assertThat(store.streamedPuts).isEqualTo(1);
        assertThat(store.objects).hasSize(1);

        // A larger streamed put goes in parts; each part's retry resends that part's buffer.
        store.attempts = 0;
        store.streamedPuts = 0;
        var many = stack.put(new Unrewindable(body), body.length);
        assertThat(store.attempts).isEqualTo(4);
        assertThat(store.objects.get(many.getKey())).isEqualTo(body);
        assertThat(many.getSha256()).isEqualTo(VerifyingInputStream.sha256Hex(body));
        assertThat(store.streamedPuts).isZero();
    }

    @Test void singlePartStreamedPutsStreamWithBoundedBuffersAndEnforceExactLength() throws Exception {
        var store = new ChunkedStreamStore(64);
        var stack = new BlobStoreStack("s1", store, "bucket", "prefix");
        var body = new byte[100_000];
        for (int i = 0; i < body.length; i++) body[i] = (byte) (i * 31 + 7);

        var tracked = new MaxReadBufferStream(body);
        var ref = stack.put(tracked, body.length);
        assertThat(store.arrayPuts).isZero();
        assertThat(store.multipartPuts).isZero();
        assertThat(store.streamedPuts).isEqualTo(1);
        assertThat(tracked.maxRequestedRead).isLessThanOrEqualTo(64);
        assertThat(store.objects.get(ref.getKey())).isEqualTo(body);
        assertThat(ref.getLength()).isEqualTo(body.length);
        assertThat(ref.getSha256()).isEqualTo(VerifyingInputStream.sha256Hex(body));
        try (var read = stack.open(ref)) {
            assertThat(read.readAllBytes()).isEqualTo(body);
        }

        store.objects.clear();
        assertThatThrownBy(() -> stack.put(new Unrewindable(body), body.length + 1L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("ended at " + body.length + " of " + (body.length + 1));
        assertThat(store.objects).isEmpty();

        assertThatThrownBy(() -> stack.put(new Unrewindable(body), body.length - 1L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("longer than its declared length " + (body.length - 1));
        assertThat(store.objects).isEmpty();

        assertThatThrownBy(() -> stack.put(new Unrewindable(new byte[] {1}), 0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("longer than its declared length 0");
        assertThat(store.objects).isEmpty();
    }

    @Test void rejectsBlobStoreWithoutStreamedReadCapabilityAndPropagatesUnsupportedOperations() throws Exception {
        var withoutStreamedReads = new BlobStore() {
            @Override public PutResult put(PutSpec spec, byte[] body) { throw new UnsupportedOperationException(); }
            @Override public PutResult put(PutSpec spec, InputStream body, long contentLength) { throw new UnsupportedOperationException(); }
            @Override public GetResult get(String bucket, String key, String versionId) { throw new UnsupportedOperationException(); }
            @Override public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) { throw new UnsupportedOperationException(); }
            @Override public boolean delete(String bucket, String key) { return false; }
            @Override public BatchDeleteResult deleteAll(String bucket, List<String> keys) { return new BatchDeleteResult(Map.of()); }
            @Override public List<ListedObject> list(String bucket, String prefix) { return List.of(); }
            @Override public void headBucket(String bucket) {}
            @Override public void headObject(String bucket, String key) {}
        };

        assertThatThrownBy(() -> new BlobStoreStack("s1", withoutStreamedReads, "bucket", "prefix"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("BlobStore does not support streamed object reads");

        var store = new RetryingStore();
        var stack = new BlobStoreStack("s1", store, "bucket", "prefix", 8, () -> {});
        var ref = stack.put("abc".getBytes());

        // RetryingStore leaves objectLength unimplemented (default UnsupportedOperationException on BlobStore);
        // head(ref) must propagate UnsupportedOperationException rather than disguising it as IOException.
        assertThatThrownBy(() -> stack.head(ref))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("object length lookup is unsupported");
    }

    @Test void fallsBackToTheKnownLengthStreamingPutWhenMultipartIsUnsupported() throws Exception {
        var store = new StreamOnlyStore();
        var stack = new BlobStoreStack("s1", store, "bucket", "prefix", 8, () -> {});
        var body = "well past the eight byte part size".getBytes();

        var ref = stack.put(new Unrewindable(body), body.length);
        assertThat(store.objects.get(ref.getKey())).isEqualTo(body);
        assertThat(ref.getSha256()).isEqualTo(VerifyingInputStream.sha256Hex(body));
        assertThat(ref.getLength()).isEqualTo(body.length);
        try (var read = stack.open(ref)) {
            assertThat(read.readAllBytes()).isEqualTo(body);
        }

        // An unknown-length body has no known-length route on such a provider.
        assertThatThrownBy(() -> stack.put(new Unrewindable(body), Stack.UNKNOWN_LENGTH))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("multipart streamed write is unsupported");
    }

    /** A provider with the required puts only: no multipart override (Redis-shaped). */
    private static final class StreamOnlyStore implements BlobStore {
        final Map<String, byte[]> objects = new HashMap<>();

        @Override public PutResult put(PutSpec spec, byte[] body) {
            objects.put(spec.key(), body.clone());
            return new PutResult("\"etag\"", null);
        }

        @Override public PutResult put(PutSpec spec, InputStream body, long contentLength) {
            try {
                byte[] bytes = body.readNBytes((int) contentLength);
                if (bytes.length != contentLength || body.read() != -1)
                    throw new IllegalArgumentException("Stream differs from declared contentLength");
                objects.put(spec.key(), bytes);
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

    /** The caller's stream: any rewind attempt fails the test. */
    private static final class Unrewindable extends ByteArrayInputStream {
        Unrewindable(byte[] bytes) { super(bytes); }
        @Override public boolean markSupported() { return false; }
        @Override public synchronized void mark(int limit) { throw new AssertionError("mark"); }
        @Override public synchronized void reset() { throw new AssertionError("reset"); }
    }

    /** Records the largest buffer length requested by a caller reading this stream. */
    private static final class MaxReadBufferStream extends InputStream {
        private final ByteArrayInputStream delegate;
        int maxRequestedRead;

        MaxReadBufferStream(byte[] bytes) { this.delegate = new ByteArrayInputStream(bytes); }

        @Override public int read() {
            maxRequestedRead = Math.max(maxRequestedRead, 1);
            return delegate.read();
        }

        @Override public int read(byte[] buffer, int offset, int length) {
            maxRequestedRead = Math.max(maxRequestedRead, length);
            return delegate.read(buffer, offset, length);
        }
    }

    /** Streams known-length puts in fixed chunks and rejects array or multipart puts. */
    private static final class ChunkedStreamStore implements BlobStore {
        final Map<String, byte[]> objects = new HashMap<>();
        private final int chunkSize;
        int arrayPuts;
        int streamedPuts;
        int multipartPuts;

        ChunkedStreamStore(int chunkSize) { this.chunkSize = chunkSize; }

        @Override public PutResult put(PutSpec spec, byte[] body) {
            arrayPuts++;
            throw new AssertionError("Unexpected byte[] put");
        }

        @Override public PutResult put(PutSpec spec, InputStream body, long contentLength) {
            streamedPuts++;
            try {
                var out = new java.io.ByteArrayOutputStream();
                byte[] buffer = new byte[chunkSize];
                long remaining = contentLength;
                while (remaining > 0) {
                    int r = body.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (r < 0) throw new IOException("Premature EOF before contentLength");
                    out.write(buffer, 0, r);
                    remaining -= r;
                }
                objects.put(spec.key(), out.toByteArray());
                return new PutResult("\"etag\"", null);
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
        }

        @Override public PutResult putMultipart(PutSpec spec, InputStream body, int partSize) {
            multipartPuts++;
            throw new AssertionError("Unexpected multipart put");
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
