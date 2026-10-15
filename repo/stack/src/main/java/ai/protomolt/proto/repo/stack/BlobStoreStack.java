package ai.protomolt.proto.repo.stack;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.v2.StackRef;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A stack over one bucket and key prefix of a {@link BlobStore} provider. Keys
 * are {@code <prefix>/<random UUID>}, never derived from content or address, so
 * no write targets an existing key. The provider's streamed put and streamed
 * read are used as they are; nothing here depends on versioning or ETags.
 */
public final class BlobStoreStack implements Stack {
    private static final String CONTENT_TYPE = "application/octet-stream";
    private final String stackId;
    private final BlobStore store;
    private final String bucket;
    private final String prefix;

    public BlobStoreStack(String stackId, BlobStore store, String bucket, String prefix) {
        this.stackId = requireText(stackId, "stackId");
        this.store = Objects.requireNonNull(store, "store");
        this.bucket = requireText(bucket, "bucket");
        this.prefix = requireText(prefix, "prefix");
        if (prefix.startsWith("/") || prefix.endsWith("/")) throw new IllegalArgumentException("Prefix has no edge slashes");
    }

    @Override public String stackId() { return stackId; }

    @Override public StackRef put(InputStream body, long length) throws IOException {
        Objects.requireNonNull(body, "body");
        if (length < 0) throw new IllegalArgumentException("Length must not be negative");
        var key = prefix + "/" + UUID.randomUUID();
        var digest = new DigestInputStream(body, VerifyingInputStream.sha256());
        var counted = new CountingInputStream(digest, length);
        try {
            store.put(new BlobStore.PutSpec(bucket, key, CONTENT_TYPE, Map.of(), null), counted, length);
        } catch (RuntimeException failure) {
            throw new IOException("Stack write failed", failure);
        }
        if (counted.count != length || body.read() >= 0)
            throw new IOException("Stack write body differs from its declared length " + length);
        return StackRef.newBuilder().setStackId(stackId).setKey(key)
                .setSha256(HexFormat.of().formatHex(digest.getMessageDigest().digest())).setLength(length).build();
    }

    @Override public InputStream open(StackRef ref) throws IOException {
        Objects.requireNonNull(ref, "ref");
        if (!ref.getStackId().equals(stackId)) throw new IllegalArgumentException("Reference names another stack");
        final InputStream raw;
        try {
            raw = store.openStream(bucket, ref.getKey(), 0, -1);
        } catch (BlobStore.BlobNotFoundException missing) {
            throw new StackDataLoss("Referenced stack object is missing", missing);
        } catch (RuntimeException failure) {
            throw new IOException("Stack read failed", failure);
        }
        return new VerifyingInputStream(raw, ref.getLength(), ref.getSha256());
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
        return value;
    }

    /** Stops at the declared length so a longer body is detected rather than truncated silently. */
    private static final class CountingInputStream extends FilterInputStream {
        private final long limit;
        private long count;
        CountingInputStream(InputStream in, long limit) { super(in); this.limit = limit; }
        @Override public int read() throws IOException {
            if (count >= limit) return -1;
            int value = super.read();
            if (value >= 0) count++;
            return value;
        }
        @Override public int read(byte[] buffer, int offset, int length) throws IOException {
            if (count >= limit) return -1;
            int read = super.read(buffer, offset, (int) Math.min(length, limit - count));
            if (read > 0) count += read;
            return read;
        }
        @Override public void close() {}
    }
}
