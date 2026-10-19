package ai.protomolt.proto.repo.stack;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.repo.v2.StackRef;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * The stack contract (#314 scope 2) against an S3-compatible store: streamed puts
 * with exact and unknown length, length mismatches and cancellation, verified whole
 * reads, length-checked ranges, head, delete, CachingStack, and provider discovery.
 * Concrete lanes supply the store; the contract holds on every one of them.
 */
abstract class AbstractStackContractIT {
    static final int PART = 5 * 1024 * 1024;

    abstract S3Client client();
    abstract String bucket();
    abstract String endpoint();
    abstract String region();
    abstract String accessKey();
    abstract String secretKey();

    private BlobStoreStack stack(String prefix) {
        return new BlobStoreStack("stack-" + prefix, new S3BlobStore(client()), bucket(), prefix, PART, () -> {});
    }

    // ------------------------------------------------------------------ puts

    @Test void exactAndUnknownLengthPutsReturnComputedReferences() throws Exception {
        var stack = stack("puts-" + UUID.randomUUID());
        var small = bytes(1000, 1);
        var exact = stack.put(new NonMarkable(small), small.length);
        assertThat(exact.getLength()).isEqualTo(1000);
        assertThat(exact.getSha256()).isEqualTo(sha256(small));
        assertThat(read(stack.open(exact))).isEqualTo(small);

        // More than two parts with an exact length, and the same bytes with an unknown length.
        var large = bytes(2 * PART + 12345, 2);
        var multipart = stack.put(new NonMarkable(large), large.length);
        var unknown = stack.put(new NonMarkable(large), Stack.UNKNOWN_LENGTH);
        for (var ref : new StackRef[] {multipart, unknown}) {
            assertThat(ref.getLength()).isEqualTo(large.length);
            assertThat(ref.getSha256()).isEqualTo(sha256(large));
            assertThat(read(stack.open(ref))).isEqualTo(large);
        }
        assertThat(multipart.getKey()).isNotEqualTo(unknown.getKey());

        // An unknown-length empty body and an exact empty body are both empty objects.
        for (long length : new long[] {0, Stack.UNKNOWN_LENGTH}) {
            var empty = stack.put(new NonMarkable(new byte[0]), length);
            assertThat(empty.getLength()).isZero();
            assertThat(empty.getSha256()).isEqualTo(sha256(new byte[0]));
            assertThat(read(stack.open(empty))).isEmpty();
            assertThat(read(stack.openRange(empty, 0, 0))).isEmpty();
        }
        assertThatThrownBy(() -> stack.put(new NonMarkable(small), -2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void aBodyThatDiffersFromItsExactLengthReturnsNoReferenceAndLeavesNoObject() throws Exception {
        var prefix = "mismatch-" + UUID.randomUUID();
        var stack = stack(prefix);
        var small = bytes(100, 3);
        assertThatThrownBy(() -> stack.put(new NonMarkable(small), 101)).isInstanceOf(IOException.class)
                .hasMessageContaining("ended at 100 of 101");
        assertThatThrownBy(() -> stack.put(new NonMarkable(small), 99)).isInstanceOf(IOException.class)
                .hasMessageContaining("longer than its declared length 99");
        var large = bytes(PART + 10, 4);
        assertThatThrownBy(() -> stack.put(new NonMarkable(large), large.length + 1L)).isInstanceOf(IOException.class)
                .hasMessageContaining("ended at");
        assertThatThrownBy(() -> stack.put(new NonMarkable(large), large.length - 1L)).isInstanceOf(IOException.class)
                .hasMessageContaining("longer than its declared length");
        assertNoObjectsOrUploads(prefix);
    }

    @Test void closingTheBodyMidPutAbortsTheUploadAndReturnsNoReference() throws Exception {
        var prefix = "cancel-" + UUID.randomUUID();
        var stack = stack(prefix);
        var body = new CancellingStream(bytes(3 * PART, 5), PART + PART / 2);
        assertThatThrownBy(() -> stack.put(body, Stack.UNKNOWN_LENGTH)).isInstanceOf(IOException.class)
                .hasMessageContaining("closed by the caller");
        assertThat(body.closedByStack).isFalse();
        assertNoObjectsOrUploads(prefix);
    }

    @Test void theStackNeverClosesTheCallersBody() throws Exception {
        var stack = stack("owner-" + UUID.randomUUID());
        var small = new CancellingStream(bytes(10, 6), Integer.MAX_VALUE);
        stack.put(small, 10);
        var large = new CancellingStream(bytes(PART + 1, 7), Integer.MAX_VALUE);
        stack.put(large, Stack.UNKNOWN_LENGTH);
        assertThat(small.closedByStack).isFalse();
        assertThat(large.closedByStack).isFalse();
    }

    // ------------------------------------------------------------------ reads

    @Test void aTruncatedOrChangedObjectFailsAtTheEndAndAMissingObjectFailsAtOpen() throws Exception {
        var stack = stack("damage-" + UUID.randomUUID());
        var original = bytes(4096, 8);
        var truncated = stack.put(original);
        overwrite(truncated, Arrays.copyOf(original, 4000));
        assertThatThrownBy(() -> read(stack.open(truncated))).isInstanceOf(StackDataLoss.class).hasMessageContaining("truncated");

        var changed = stack.put(original);
        var flipped = original.clone();
        flipped[2000] ^= 1;
        overwrite(changed, flipped);
        assertThatThrownBy(() -> read(stack.open(changed))).isInstanceOf(StackDataLoss.class).hasMessageContaining("SHA-256");
        // A whole-object range is the verified full read; a partial range of the same object
        // returns its bytes with a length check only, and claims nothing about the digest.
        assertThatThrownBy(() -> read(stack.openRange(changed, 0, 4096))).isInstanceOf(StackDataLoss.class);
        assertThat(read(stack.openRange(changed, 1990, 20))).isEqualTo(Arrays.copyOfRange(flipped, 1990, 2010));

        var longer = stack.put(original);
        overwrite(longer, Arrays.copyOf(original, 5000));
        assertThatThrownBy(() -> read(stack.open(longer))).isInstanceOf(StackDataLoss.class).hasMessageContaining("longer");
        // The range runs past the end of what the reference covers: short read.
        assertThatThrownBy(() -> read(stack.openRange(truncated, 3990, 100))).isInstanceOf(StackDataLoss.class)
                .hasMessageContaining("ended at 10 of 100");

        var missing = truncated.toBuilder().setKey(truncated.getKey() + "-missing").build();
        assertThatThrownBy(() -> stack.open(missing)).isInstanceOf(StackDataLoss.class);
        assertThatThrownBy(() -> stack.openRange(missing, 1, 2)).isInstanceOf(StackDataLoss.class);
        assertThatThrownBy(() -> stack.openRange(missing, 5, 0)).isInstanceOf(StackDataLoss.class);
    }

    @Test void rangesAreExactAndOutOfBoundsRangesFailBeforeAnyRead() throws Exception {
        var stack = stack("ranges-" + UUID.randomUUID());
        var bytes = bytes(10_000, 9);
        var ref = stack.put(bytes);
        assertThat(read(stack.openRange(ref, 0, 100))).isEqualTo(Arrays.copyOfRange(bytes, 0, 100));
        assertThat(read(stack.openRange(ref, 4000, 1234))).isEqualTo(Arrays.copyOfRange(bytes, 4000, 5234));
        assertThat(read(stack.openRange(ref, 9000, 1000))).isEqualTo(Arrays.copyOfRange(bytes, 9000, 10_000));
        assertThat(read(stack.openRange(ref, 0, 10_000))).isEqualTo(bytes);
        assertThat(read(stack.openRange(ref, 0, 0))).isEmpty();
        assertThat(read(stack.openRange(ref, 10_000, 0))).isEmpty();
        // The transport's "to the end" at a nonzero offset arrives here as ref.length - offset.
        assertThat(read(stack.openRange(ref, 7777, ref.getLength() - 7777))).isEqualTo(Arrays.copyOfRange(bytes, 7777, 10_000));

        var foreign = ref.toBuilder().setStackId("another").build();
        var unread = stack(UUID.randomUUID().toString());
        for (long[] range : new long[][] {{-1, 1}, {0, -1}, {10_001, 0}, {9_999, 2}, {1, Long.MAX_VALUE}, {Long.MAX_VALUE, 1}}) {
            assertThatThrownBy(() -> stack.openRange(ref, range[0], range[1])).isInstanceOf(IllegalArgumentException.class);
            // No I/O: the same bounds fail on a stack that holds no such object.
            assertThatThrownBy(() -> unread.openRange(ref.toBuilder().setStackId(unread.stackId()).build(), range[0], range[1]))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> stack.openRange(foreign, 0, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void closingARangeOrReadEarlyAbandonsTheTransfer() throws Exception {
        var stack = stack("early-" + UUID.randomUUID());
        var large = stack.put(new NonMarkable(bytes(2 * PART, 10)), 2 * PART);
        long started = System.nanoTime();
        for (int attempt = 0; attempt < 20; attempt++) {
            try (var whole = stack.open(large)) { assertThat(whole.read()).isGreaterThanOrEqualTo(0); }
            try (var range = stack.openRange(large, 1, 2L * PART - 2)) { assertThat(range.read()).isGreaterThanOrEqualTo(0); }
        }
        // Forty abandoned 10 MiB transfers that drained would move 400 MiB; abandoning moves next to nothing.
        assertThat(System.nanoTime() - started).isLessThan(java.util.concurrent.TimeUnit.SECONDS.toNanos(30));
    }

    @Test void headReportsTheStoredLengthAndDeleteIsIdempotent() throws Exception {
        var stack = stack("head-" + UUID.randomUUID());
        var ref = stack.put(bytes(321, 11));
        assertThat(stack.head(ref)).isEqualTo(321);
        overwrite(ref, bytes(320, 11));
        assertThatThrownBy(() -> stack.head(ref)).isInstanceOf(StackDataLoss.class).hasMessageContaining("320");
        stack.delete(ref);
        assertThatThrownBy(() -> stack.head(ref)).isInstanceOf(StackDataLoss.class).hasMessageContaining("missing");
        stack.delete(ref);
        assertThatThrownBy(() -> stack.open(ref)).isInstanceOf(StackDataLoss.class);
    }

    // ------------------------------------------------------------------ caching

    @Test void cachingStackKeepsOnlyVerifiedSmallWholeObjects() throws Exception {
        var counting = new CountingStack(stack("cache-" + UUID.randomUUID()));
        var cache = new CachingStack(counting, 1024, 4096);
        var small = cache.put(bytes(1000, 12));
        var large = cache.put(bytes(2000, 13));

        // An early close caches nothing; a complete verified read does.
        try (var partial = cache.open(small)) { partial.readNBytes(10); }
        assertThat(cache.cached(small)).isFalse();
        assertThat(read(cache.open(small))).isEqualTo(bytes(1000, 12));
        assertThat(cache.cached(small)).isTrue();
        int opens = counting.opens;
        assertThat(read(cache.open(small))).isEqualTo(bytes(1000, 12));
        assertThat(read(cache.openRange(small, 0, 1000))).isEqualTo(bytes(1000, 12));
        assertThat(counting.opens).isEqualTo(opens);

        // Larger objects and partial ranges go to the stack every time.
        read(cache.open(large));
        read(cache.open(large));
        assertThat(cache.cached(large)).isFalse();
        read(cache.openRange(small, 10, 10));
        assertThat(counting.opens).isEqualTo(opens + 2);
        assertThat(counting.ranges).isEqualTo(1);

        // A read that fails verification caches nothing.
        var damaged = cache.put(bytes(500, 14));
        overwrite(damaged, bytes(500, 15));
        assertThatThrownBy(() -> read(cache.open(damaged))).isInstanceOf(StackDataLoss.class);
        assertThat(cache.cached(damaged)).isFalse();

        // Eviction keeps the total bounded; delete drops the entry and the bytes.
        var refs = new StackRef[6];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = cache.put(bytes(1000, 20 + i));
            read(cache.open(refs[i]));
        }
        assertThat(cache.cached(refs[0])).isFalse();
        assertThat(cache.cached(refs[5])).isTrue();
        cache.delete(refs[5]);
        assertThat(cache.cached(refs[5])).isFalse();
        assertThatThrownBy(() -> cache.open(refs[5])).isInstanceOf(StackDataLoss.class);
    }

    // ------------------------------------------------------------------ discovery

    @Test void providersAreDiscoveredAndSelectedByTheRecordsProvider() throws Exception {
        var providers = StackProviders.discover();
        assertThat(providers.ids()).contains("s3");
        var record = StackRecord.newBuilder().setStackId("discovered").setProvider("s3")
                .setEndpoint(endpoint()).setNamespace(bucket() + "/discovered/" + UUID.randomUUID()).build();
        var options = Map.ofEntries(Map.entry("endpoint", endpoint()), Map.entry("region", region()),
                Map.entry("path-style", "true"), Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"), Map.entry("access-key", accessKey()),
                Map.entry("secret-key", secretKey()), Map.entry("api-call-timeout-ms", "60000"),
                Map.entry("api-attempt-timeout-ms", "30000"), Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "30000"));
        try (var stack = providers.open(record, options)) {
            assertThat(stack.stackId()).isEqualTo("discovered");
            var ref = stack.put(bytes(77, 30));
            assertThat(ref.getKey()).startsWith(record.getNamespace().substring(bucket().length() + 1) + "/");
            assertThat(read(stack.open(ref))).isEqualTo(bytes(77, 30));
        }
        assertThatThrownBy(() -> providers.open(record.toBuilder().setProvider("azure").build(), options))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not installed");
        assertThatThrownBy(() -> providers.open(record.toBuilder().setNamespace("bucket-only").build(), options))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("<bucket>/<prefix>");
    }

    // ------------------------------------------------------------------ helpers

    private void assertNoObjectsOrUploads(String prefix) {
        assertThat(client().listObjectsV2(ListObjectsV2Request.builder().bucket(bucket()).prefix(prefix).build()).contents())
                .isEmpty();
        assertThat(client().listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(bucket()).prefix(prefix).build())
                .uploads()).isEmpty();
    }

    private void overwrite(StackRef ref, byte[] bytes) {
        client().putObject(PutObjectRequest.builder().bucket(bucket()).key(ref.getKey()).build(), RequestBody.fromBytes(bytes));
    }

    static byte[] bytes(int length, long seed) {
        var bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    static byte[] read(InputStream stream) throws IOException {
        try (stream) { return stream.readAllBytes(); }
    }

    static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    /** A caller's stream: no mark support, and it records a close it did not ask for. */
    static class NonMarkable extends ByteArrayInputStream {
        NonMarkable(byte[] bytes) { super(bytes); }
        @Override public boolean markSupported() { return false; }
        @Override public synchronized void mark(int limit) { throw new AssertionError("the stack asked to mark"); }
        @Override public synchronized void reset() { throw new AssertionError("the stack asked to reset"); }
    }

    /** Fails reads after {@code cancelAt} bytes as a stream closed by its caller does. */
    static final class CancellingStream extends InputStream {
        private final byte[] bytes;
        private final int cancelAt;
        private int pos;
        boolean closedByStack;

        CancellingStream(byte[] bytes, int cancelAt) { this.bytes = bytes; this.cancelAt = cancelAt; }

        @Override public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override public int read(byte[] buffer, int offset, int length) throws IOException {
            if (pos >= cancelAt) throw new IOException("Stream closed by the caller");
            if (pos == bytes.length) return -1;
            int n = Math.min(length, Math.min(cancelAt, bytes.length) - pos);
            System.arraycopy(bytes, pos, buffer, offset, n);
            pos += n;
            return n;
        }

        @Override public void close() { closedByStack = true; }
    }

    /** Counts whole and range opens reaching the stack behind a cache. */
    static final class CountingStack implements Stack {
        private final Stack delegate;
        int opens;
        int ranges;

        CountingStack(Stack delegate) { this.delegate = delegate; }
        @Override public String stackId() { return delegate.stackId(); }
        @Override public StackRef put(InputStream body, long expectedLength) throws IOException { return delegate.put(body, expectedLength); }
        @Override public StackRef put(byte[] body) throws IOException { return delegate.put(body); }
        @Override public InputStream open(StackRef ref) throws IOException { opens++; return delegate.open(ref); }
        @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
            ranges++;
            return delegate.openRange(ref, offset, length);
        }
        @Override public long head(StackRef ref) throws IOException { return delegate.head(ref); }
        @Override public void delete(StackRef ref) throws IOException { delegate.delete(ref); }
    }
}
