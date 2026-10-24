package ai.protomolt.proto.repo.stack;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.v2.StackRef;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A stack over one bucket and key prefix of a {@link BlobStore} provider. Keys
 * are {@code <prefix>/<random UUID>}, never derived from content or address, so
 * no write targets an existing key. Nothing here depends on versioning or ETags.
 * <p>
 * A known-length streamed put of at most one part streams through the provider's
 * single-part streaming put: the counting stream checks for excess bytes before
 * the final byte is delivered and, when the provider marks the stream at the
 * start, retains the read bytes in a replay buffer bounded by the part size so a
 * provider retry can reset the stream and recompute the digest without rewinding
 * the caller's stream. A larger or unknown-length put is a multipart upload sent
 * from one part buffer at a time: memory stays bounded by the part size and a
 * retry resends a part, never rewinding the caller's stream. A provider without
 * multipart takes a larger known-length body through its required streaming put
 * instead, digest still computed while streaming, so a Redis-backed stack
 * accepts any object its own limits allow.
 */
public final class BlobStoreStack implements Stack {
    /** Part buffer for streamed puts: above S3's 5 MiB minimum part size. */
    public static final int DEFAULT_PART_SIZE = 8 * 1024 * 1024;
    private static final String CONTENT_TYPE = "application/octet-stream";
    private final String stackId;
    private final BlobStore store;
    private final String bucket;
    private final String prefix;
    private final int partSize;
    private final AutoCloseable lifetime;

    /**
     * Creates a stack with {@link #DEFAULT_PART_SIZE} and a no-op close hook.
     *
     * @param stackId registered stack identifier
     * @param store backing byte store (must implement {@link BlobStore#openStream})
     * @param bucket object storage bucket
     * @param prefix key prefix without leading or trailing slashes
     */
    public BlobStoreStack(String stackId, BlobStore store, String bucket, String prefix) {
        this(stackId, store, bucket, prefix, DEFAULT_PART_SIZE, () -> {});
    }

    /**
     * Creates a stack with an explicit multipart buffer size and lifetime hook.
     *
     * @param stackId registered stack identifier
     * @param store backing byte store (must implement {@link BlobStore#openStream})
     * @param bucket object storage bucket
     * @param prefix key prefix without leading or trailing slashes
     * @param partSize the streamed put buffer; the provider enforces its own minimum
     * @param lifetime released by {@link #close()}: the provider resources this stack owns
     */
    public BlobStoreStack(String stackId, BlobStore store, String bucket, String prefix, int partSize,
            AutoCloseable lifetime) {
        this.stackId = requireText(stackId, "stackId");
        this.store = requireStreamedReads(Objects.requireNonNull(store, "store"));
        this.bucket = requireText(bucket, "bucket");
        this.prefix = requireText(prefix, "prefix");
        if (prefix.startsWith("/") || prefix.endsWith("/")) throw new IllegalArgumentException("Prefix has no edge slashes");
        if (partSize < 1) throw new IllegalArgumentException("Part size must be positive");
        this.partSize = partSize;
        this.lifetime = Objects.requireNonNull(lifetime, "lifetime");
    }

    @Override public String stackId() { return stackId; }

    @Override public StackRef put(byte[] body) throws IOException {
        Objects.requireNonNull(body, "body");
        var key = newKey();
        try {
            store.put(new BlobStore.PutSpec(bucket, key, CONTENT_TYPE, Map.of(), null), body);
        } catch (UnsupportedOperationException unsupported) {
            throw unsupported;
        } catch (RuntimeException failure) {
            throw new IOException("Stack write failed", failure);
        }
        return ref(key, VerifyingInputStream.sha256().digest(body), body.length);
    }

    @Override public StackRef put(InputStream body, long expectedLength) throws IOException {
        Objects.requireNonNull(body, "body");
        if (expectedLength < UNKNOWN_LENGTH) throw new IllegalArgumentException("Expected length must be exact or unknown");
        boolean singlePart = expectedLength >= 0 && expectedLength <= partSize;
        var counted = new CountingInputStream(body, expectedLength, singlePart);
        if (expectedLength == 0) {
            counted.requireEnd();
        }
        var key = newKey();
        var spec = new BlobStore.PutSpec(bucket, key, CONTENT_TYPE, Map.of(), null);
        if (singlePart) {
            putStreamed(spec, counted, expectedLength);
        } else {
            try {
                store.putMultipart(spec, counted, partSize);
            } catch (UncheckedIOException failure) {
                throw failure.getCause();
            } catch (UnsupportedOperationException multipartUnsupported) {
                // A provider without multipart (Redis; the caching wrapper over such a
                // backing) still has the required known-length streaming put, with no
                // extra size ceiling of this stack's own. The refusal is thrown before
                // the body is touched; a partially consumed body cannot be retried, and
                // an unknown-length body has no known-length route, so both rethrow.
                if (counted.count > 0 || expectedLength < 0) {
                    throw multipartUnsupported;
                }
                putStreamed(spec, counted, expectedLength);
            } catch (RuntimeException failure) {
                if (counted.bodyFailure != null) {
                    counted.bodyFailure.addSuppressed(failure);
                    throw counted.bodyFailure;
                }
                throw new IOException("Stack write failed", failure);
            }
        }
        // The counting stream fails a short or long body before the upload completes.
        if (expectedLength >= 0 && counted.count != expectedLength)
            throw new IllegalStateException("Provider stopped reading before the body ended");
        counted.requireEnd();
        return ref(key, counted.digest.digest(), counted.count);
    }

    private void putStreamed(BlobStore.PutSpec spec, CountingInputStream counted, long expectedLength)
            throws IOException {
        try {
            store.put(spec, counted, expectedLength);
        } catch (UncheckedIOException failure) {
            throw failure.getCause();
        } catch (UnsupportedOperationException unsupported) {
            throw unsupported;
        } catch (RuntimeException failure) {
            if (counted.bodyFailure != null) {
                counted.bodyFailure.addSuppressed(failure);
                throw counted.bodyFailure;
            }
            throw new IOException("Stack write failed", failure);
        }
    }

    @Override public InputStream open(StackRef ref) throws IOException {
        requireOwn(ref);
        return new VerifyingInputStream(raw(ref, 0, -1), ref.getLength(), ref.getSha256());
    }

    @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
        requireOwn(ref);
        Stack.requireRange(ref, offset, length);
        if (offset == 0 && length == ref.getLength()) return open(ref);
        if (length == 0) {
            head(ref);
            return InputStream.nullInputStream();
        }
        return new ExactLengthInputStream(raw(ref, offset, length), length);
    }

    @Override public long head(StackRef ref) throws IOException {
        requireOwn(ref);
        final long stored;
        try {
            stored = store.objectLength(bucket, ref.getKey());
        } catch (BlobStore.BlobNotFoundException missing) {
            throw new StackDataLoss("Referenced stack object is missing", missing);
        } catch (UnsupportedOperationException unsupported) {
            throw unsupported;
        } catch (RuntimeException failure) {
            throw new IOException("Stack head failed", failure);
        }
        if (stored != ref.getLength())
            throw new StackDataLoss("Stored object is " + stored + " bytes; its reference says " + ref.getLength());
        return stored;
    }

    @Override public void delete(StackRef ref) throws IOException {
        requireOwn(ref);
        try {
            store.delete(bucket, ref.getKey());
        } catch (UnsupportedOperationException unsupported) {
            throw unsupported;
        } catch (RuntimeException failure) {
            throw new IOException("Stack delete failed", failure);
        }
    }

    @Override public void close() throws IOException {
        try {
            lifetime.close();
        } catch (IOException | RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IOException("Stack resources did not close", failure);
        }
    }

    private InputStream raw(StackRef ref, long offset, long length) throws IOException {
        try {
            return store.openStream(bucket, ref.getKey(), offset, length);
        } catch (BlobStore.BlobNotFoundException missing) {
            throw new StackDataLoss("Referenced stack object is missing", missing);
        } catch (UnsupportedOperationException unsupported) {
            throw unsupported;
        } catch (RuntimeException failure) {
            throw new IOException("Stack read failed", failure);
        }
    }

    private static BlobStore requireStreamedReads(BlobStore store) {
        try {
            var method = store.getClass().getMethod(
                    "openStream", String.class, String.class, long.class, long.class);
            if (method.getDeclaringClass() == BlobStore.class) {
                throw new UnsupportedOperationException("BlobStore does not support streamed object reads");
            }
            return store;
        } catch (NoSuchMethodException impossible) {
            throw new IllegalStateException("BlobStore.openStream lookup failed", impossible);
        }
    }

    private void requireOwn(StackRef ref) {
        Objects.requireNonNull(ref, "ref");
        if (!ref.getStackId().equals(stackId)) throw new IllegalArgumentException("Reference names another stack");
    }

    private String newKey() { return prefix + "/" + UUID.randomUUID(); }

    private StackRef ref(String key, byte[] sha256, long length) {
        return StackRef.newBuilder().setStackId(stackId).setKey(key).setSha256(HexFormat.of().formatHex(sha256))
                .setLength(length).build();
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
        return value;
    }

    /**
     * Hashes and counts the caller's body as the provider reads it. It never
     * closes, marks or resets the caller's stream. For a single-part known-length
     * write, a provider mark at the start of the stream retains the read bytes in
     * a bounded replay buffer so a retry can reset this stream to zero and
     * recompute the digest; larger or unmarked writes refuse reset. With an exact
     * length it reports the end at that length after checking the body has no more
     * bytes, and fails a body that ends early; either failure aborts the upload
     * before it completes.
     */
    private static final class CountingInputStream extends FilterInputStream {
        final MessageDigest digest = VerifyingInputStream.sha256();
        private final long expected;
        private final boolean replayable;
        long count;
        private boolean marked;
        private byte[] replay;
        private int buffered;
        private boolean endVerified;
        IOException bodyFailure;

        CountingInputStream(InputStream in, long expected, boolean replayable) {
            super(in);
            this.expected = expected;
            this.replayable = replayable;
        }

        @Override public boolean markSupported() { return replayable; }

        @Override public void mark(int limit) {
            if (replayable && count == 0 && limit >= 0) {
                marked = true;
            } else {
                marked = false;
                replay = null;
                buffered = 0;
            }
        }

        @Override public void reset() throws IOException {
            if (bodyFailure != null) throw bodyFailure;
            if (!marked) throw fail("Stack write streams cannot be rewound");
            count = 0;
            digest.reset();
        }

        @Override public long skip(long n) throws IOException { throw fail("Stack write streams cannot skip"); }
        @Override public void close() {}

        @Override public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override public int read(byte[] buffer, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, buffer.length);
            if (length == 0) return 0;
            if (expected >= 0 && count == expected) { requireEnd(); return -1; }
            if (count < buffered) {
                int replayed = (int) Math.min(length, buffered - count);
                System.arraycopy(replay, (int) count, buffer, offset, replayed);
                digest.update(buffer, offset, replayed);
                count += replayed;
                if (expected >= 0 && count == expected) requireEnd();
                return replayed;
            }
            int want = expected >= 0 ? (int) Math.min(length, expected - count) : length;
            final int read;
            try {
                read = in.read(buffer, offset, want);
            } catch (IOException failure) {
                bodyFailure = failure;
                throw failure;
            }
            if (read < 0) {
                if (expected >= 0) throw fail("Stack write body ended at " + count + " of " + expected + " bytes");
                return -1;
            }
            if (marked) recordReplay(buffer, offset, read);
            digest.update(buffer, offset, read);
            count += read;
            if (expected >= 0 && count == expected) requireEnd();
            return read;
        }

        private void recordReplay(byte[] buffer, int offset, int length) {
            int target = buffered + length;
            if (replay == null) {
                replay = new byte[(int) Math.min(expected, Math.max(length, 8192L))];
            } else if (target > replay.length) {
                int next = (int) Math.min(expected, Math.max((long) replay.length * 2, target));
                replay = Arrays.copyOf(replay, next);
            }
            System.arraycopy(buffer, offset, replay, buffered, length);
            buffered = target;
        }

        void requireEnd() throws IOException {
            if (expected >= 0 && count != expected)
                throw fail("Stack write body ended at " + count + " of " + expected + " bytes");
            if (expected >= 0 && !endVerified) {
                final int next;
                try {
                    next = in.read();
                } catch (IOException failure) {
                    bodyFailure = failure;
                    throw failure;
                }
                if (next >= 0)
                    throw fail("Stack write body is longer than its declared length " + expected);
                endVerified = true;
            }
        }

        private IOException fail(String message) {
            var failure = new IOException(message);
            bodyFailure = failure;
            return failure;
        }
    }
}
