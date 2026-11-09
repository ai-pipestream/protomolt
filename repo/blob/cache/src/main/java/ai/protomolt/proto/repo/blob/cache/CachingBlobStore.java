package ai.protomolt.proto.repo.blob.cache;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.ExpiringBlobStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-through/write-through {@link BlobStore} decorator: a fast expendable
 * cache in front of the backing store of truth.
 *
 * <p><b>The cache is always expendable; the backing store is truth.</b> Every
 * write lands on the backing store FIRST and is only then mirrored into the
 * cache best-effort — a cache failure never fails an operation that the
 * backing store completed, and a restart with an empty cache loses nothing.
 *
 * <p>Semantics per operation:
 * <ul>
 *   <li>{@code get} — cache hit returns immediately. Miss → {@code
 *       backing.get}, populate the cache when the object fits the ceiling
 *       ({@code maxCacheableBytes}; larger objects are never cached), return.
 *       Versioned gets ({@code versionId != null}) bypass the cache: the cache
 *       holds only current versions.</li>
 *   <li>{@code openStream(bucket, key, 0, -1)} and {@code openVerifiedStream}
 *       — whole-object current-version reads check the cache first; on a miss
 *       they stream from {@code backing} and populate the cache at end of
 *       stream when the object fits {@code maxCacheableBytes}. Corrupt or
 *       mismatched cache entries are evicted and fall back to {@code backing}.
 *       Partial ranges and versioned stream reads go directly to
 *       {@code backing}.</li>
 *   <li>{@code put} — {@code backing.put} honoring the verified-write spec
 *       (the SHA-256 rides to the backing store untouched), then best-effort
 *       {@code cache.put} with the configured TTL when the object fits.</li>
 *   <li>{@code copy} — {@code backing.copy}, then evict the DESTINATION from
 *       the cache (a stale cached destination would otherwise shadow the
 *       copied bytes).</li>
 *   <li>{@code delete}/{@code deleteAll} — backing first, then evict.</li>
 *   <li>{@code list}/{@code headBucket}/{@code objectLength} — backing only;
 *       lists are never cached (enumerations go stale silently and are cheap
 *       to get wrong).</li>
 *   <li>{@code headObject} — a cache hit answers (existence caching: a cached
 *       object exists in backing, because every cached byte came from or
 *       through it); a miss falls through to backing. A cache miss says
 *       nothing about backing.</li>
 * </ul>
 */
public final class CachingBlobStore implements BlobStore, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CachingBlobStore.class);
    private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private static final int MAX_BUFFER_BYTES = Integer.MAX_VALUE - 8;

    private final BlobStore backing;
    private final BlobStore cache;
    private final int ttlSeconds;
    private final long maxCacheableBytes;
    private final String cacheNamespace;

    /**
     * @param backing the store of truth (every write lands here first)
     * @param cache the expendable front cache (e.g. an expiring store)
     * @param ttlSeconds cache-entry TTL; {@code 0} = no expiry. Applied when
     *        the cache is TTL-aware (today: an expiring store); other
     *        caches ignore it
     * @param maxCacheableBytes largest object admitted to the cache; objects
     *        larger than this bypass the cache entirely. {@code 0} or
     *        negative = cache nothing
     */
    public CachingBlobStore(BlobStore backing, BlobStore cache, int ttlSeconds, long maxCacheableBytes) {
        this(backing, cache, ttlSeconds, maxCacheableBytes, "");
    }

    /**
     * Constructs a caching decorator that isolates cache entries under a
     * physical backing identity namespace so equal {@code (bucket, key)}
     * coordinates on distinct authoritative backing stores never collide in a
     * shared cache.
     *
     * @param backing the store of truth (every write lands here first)
     * @param cache the expendable front cache (e.g. an expiring store)
     * @param ttlSeconds cache-entry TTL; {@code 0} = no expiry
     * @param maxCacheableBytes largest object admitted to the cache
     * @param cacheNamespace physical backing identity prefix for cache keys;
     *        blank or {@code null} leaves bucket names unprefixed
     */
    public CachingBlobStore(
            BlobStore backing,
            BlobStore cache,
            int ttlSeconds,
            long maxCacheableBytes,
            String cacheNamespace) {
        this.backing = Objects.requireNonNull(backing, "backing");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.ttlSeconds = Math.max(ttlSeconds, 0);
        this.maxCacheableBytes = maxCacheableBytes;
        this.cacheNamespace = cacheNamespace == null ? "" : cacheNamespace.trim();
    }

    /** Returns the physical backing namespace isolating this decorator's cache keys. */
    public String cacheNamespace() {
        return cacheNamespace;
    }

    /**
     * Returns the cache bucket/namespace coordinate for an authoritative bucket.
     *
     * @param bucket the authoritative bucket name
     * @return the isolated cache bucket name
     */
    public String cacheBucket(String bucket) {
        Objects.requireNonNull(bucket, "bucket");
        if (cacheNamespace.isEmpty()) {
            return bucket;
        }
        return cacheNamespace.length() + ":" + cacheNamespace + ":" + bucket;
    }

    @Override
    public PutResult put(PutSpec spec, byte[] body) {
        PutResult result = backing.put(spec, body);
        if (fits(body.length)) {
            try {
                cachePut(cacheSpec(spec), body);
            } catch (RuntimeException cacheFailure) {
                LOG.warn("cache put failed for {}/{} (backing write landed) — continuing",
                        spec.bucket(), spec.key(), cacheFailure);
            }
        }
        return result;
    }

    @Override
    public PutResult put(PutSpec spec, InputStream body, long contentLength) {
        PutResult result = backing.put(spec, body, contentLength);
        // Streamed bodies are not replayable; the next read populates the
        // cache read-through instead of buffering the stream here.
        return result;
    }

    @Override
    public PutResult putMultipart(PutSpec spec, InputStream body, int partSize) {
        // The backing store owns multipart (or refuses it); streamed bodies are
        // not replayable, so the next read populates the cache read-through.
        return backing.putMultipart(spec, body, partSize);
    }

    @Override
    public GetResult getForUpdate(String bucket, String key) {
        return backing.getForUpdate(bucket, key);
    }

    @Override
    public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
        PutResult result = backing.conditionalPut(spec, body, condition);
        // A successful replacement makes any current-version cache entry stale.
        // The conditional API itself always reads the authoritative backing store.
        evict(spec.bucket(), spec.key());
        return result;
    }

    @Override
    public GetResult get(String bucket, String key, String versionId) {
        String cBucket = cacheBucket(bucket);
        if (versionId == null || versionId.isEmpty()) {
            try {
                GetResult cached = cache.get(cBucket, key);
                if (etagMatchesIfSha256(cached)) {
                    return cached;
                }
                LOG.warn("cached object {}/{} failed ETag digest check — evicting and reading from backing",
                        bucket, key);
                evict(bucket, key);
            } catch (BlobNotFoundException miss) {
                // fall through to backing
            } catch (RuntimeException cacheFailure) {
                LOG.warn("cache get failed for {}/{} — falling through to backing",
                        bucket, key, cacheFailure);
            }
        }
        GetResult got = backing.get(bucket, key, versionId);
        if ((versionId == null || versionId.isEmpty()) && fits(got.data().length)) {
            try {
                cachePut(new PutSpec(cBucket, key, got.contentType(), null, null), got.data());
            } catch (RuntimeException cacheFailure) {
                LOG.warn("cache populate failed for {}/{} — continuing", bucket, key, cacheFailure);
            }
        }
        return got;
    }

    @Override
    public InputStream openVerifiedStream(
            String bucket, String key, long expectedLength, String expectedSha256Hex) {
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(key, "key");
        if (!fits(expectedLength)) {
            return backing.openStream(bucket, key, 0, -1);
        }
        String cBucket = cacheBucket(bucket);
        try {
            GetResult cached = cache.get(cBucket, key);
            byte[] data = cached.data();
            if (data != null
                    && data.length == expectedLength
                    && (expectedSha256Hex == null
                            || expectedSha256Hex.isEmpty()
                            || sha256Hex(data).equalsIgnoreCase(expectedSha256Hex))) {
                return new ByteArrayInputStream(data);
            }
            LOG.warn(
                    "cached object {}/{} failed verification (expected {} bytes sha256={}, got {} bytes) — evicting and reading from backing",
                    bucket, key, expectedLength, expectedSha256Hex, data == null ? -1 : data.length);
            evict(bucket, key);
        } catch (BlobNotFoundException miss) {
            // fall through to backing
        } catch (RuntimeException cacheFailure) {
            LOG.warn("cache openVerifiedStream failed for {}/{} — falling through to backing",
                    bucket, key, cacheFailure);
        }
        InputStream stream = backing.openStream(bucket, key, 0, -1);
        return new VerifiedFillStream(stream, bucket, key, (int) expectedLength, expectedSha256Hex);
    }

    /** Whole-object reads use the read-through cache when within {@code maxCacheableBytes}; ranges go to backing. */
    @Override
    public InputStream openStream(String bucket, String key, long offset, long length) {
        return openStream(bucket, key, null, offset, length);
    }

    /** Streamed version and range reads go to backing; unversioned whole-object reads use the cache. */
    @Override
    public InputStream openStream(String bucket, String key, String versionId, long offset, long length) {
        if ((versionId != null && !versionId.isEmpty()) || offset != 0 || length >= 0 || maxCacheableBytes <= 0) {
            return versionId == null
                    ? backing.openStream(bucket, key, offset, length)
                    : backing.openStream(bucket, key, versionId, offset, length);
        }
        String cBucket = cacheBucket(bucket);
        try {
            GetResult cached = cache.get(cBucket, key);
            if (etagMatchesIfSha256(cached)) {
                return new ByteArrayInputStream(cached.data());
            }
            LOG.warn("cached object {}/{} failed ETag digest check — evicting and reading from backing",
                    bucket, key);
            evict(bucket, key);
        } catch (BlobNotFoundException miss) {
            // fall through to backing
        } catch (RuntimeException cacheFailure) {
            LOG.warn("cache openStream failed for {}/{} — falling through to backing",
                    bucket, key, cacheFailure);
        }
        InputStream stream = versionId == null
                ? backing.openStream(bucket, key, 0, -1)
                : backing.openStream(bucket, key, versionId, 0, -1);
        int limit = (int) Math.min(maxCacheableBytes, MAX_BUFFER_BYTES);
        return new BoundedFillStream(stream, bucket, key, limit);
    }

    @Override
    public long objectLength(String bucket, String key) {
        return backing.objectLength(bucket, key);
    }

    @Override
    public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
        backing.copy(srcBucket, srcKey, dstBucket, dstKey);
        evict(dstBucket, dstKey);
    }

    @Override
    public boolean delete(String bucket, String key) {
        boolean deleted = backing.delete(bucket, key);
        evict(bucket, key);
        return deleted;
    }

    /**
     * Compose lifecycle reclamation with strict cache eviction. The supplied port
     * must belong to this instance's authoritative backing store. Unlike ordinary
     * cache operations, eviction failures propagate so durable cleanup is retried.
     * Late in-flight cache fills require subsequent tombstone reconciliation.
     */
    public ai.protomolt.proto.repo.blob.spi.ObjectReclaimer reclaimer(
            ai.protomolt.proto.repo.blob.spi.ObjectReclaimer backingReclaimer) {
        java.util.Objects.requireNonNull(backingReclaimer, "backingReclaimer");
        return (bucket, key) -> {
            String cBucket = cacheBucket(bucket);
            boolean absent = backingReclaimer.reclaim(bucket, key);
            cache.delete(cBucket, key);
            try {
                cache.headObject(cBucket, key);
                return false;
            } catch (BlobStore.BlobNotFoundException missing) {
                return absent;
            }
        };
    }

    @Override
    public BatchDeleteResult deleteAll(String bucket, List<String> keys) {
        BatchDeleteResult result = backing.deleteAll(bucket, keys);
        for (String key : keys) {
            if (key != null && !key.isBlank()) {
                evict(bucket, key);
            }
        }
        return result;
    }

    @Override
    public List<ListedObject> list(String bucket, String prefix) {
        return backing.list(bucket, prefix);
    }

    @Override
    public void headBucket(String bucket) {
        backing.headBucket(bucket);
    }

    @Override
    public void headObject(String bucket, String key) {
        try {
            cache.headObject(cacheBucket(bucket), key);
            return; // existence caching: cached bytes came from backing
        } catch (BlobNotFoundException miss) {
            // A cache miss says nothing about backing — fall through.
        } catch (RuntimeException cacheFailure) {
            LOG.warn("cache headObject failed for {}/{} — falling through to backing",
                    bucket, key, cacheFailure);
        }
        backing.headObject(bucket, key);
    }

    /** Closes the backing store and the cache when they are closeable. */
    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            if (backing instanceof AutoCloseable closeable) closeable.close();
        } catch (Exception e) {
            failure = e;
        }
        try {
            if (cache != backing && cache instanceof AutoCloseable closeable) closeable.close();
        } catch (Exception e) {
            if (failure == null) failure = e;
            else if (failure != e) failure.addSuppressed(e);
        }
        if (failure != null) throw failure;
    }

    private boolean fits(long sizeBytes) {
        return maxCacheableBytes > 0
                && sizeBytes >= 0
                && sizeBytes <= maxCacheableBytes
                && sizeBytes <= MAX_BUFFER_BYTES;
    }

    private PutSpec cacheSpec(PutSpec spec) {
        if (cacheNamespace.isEmpty()) {
            return spec;
        }
        return new PutSpec(
                cacheBucket(spec.bucket()),
                spec.key(),
                spec.contentType(),
                spec.metadata(),
                spec.sha256Hex());
    }

    /** A cache put honoring the TTL when the cache is TTL-aware. */
    private void cachePut(PutSpec spec, byte[] body) {
        if (cache instanceof ExpiringBlobStore redis) {
            redis.put(spec, body, ttlSeconds);
        } else {
            cache.put(spec, body);
        }
    }

    private void evict(String bucket, String key) {
        try {
            cache.delete(cacheBucket(bucket), key);
        } catch (RuntimeException cacheFailure) {
            LOG.warn("cache evict failed for {}/{} — continuing", bucket, key, cacheFailure);
        }
    }

    private static boolean etagMatchesIfSha256(GetResult cached) {
        if (cached == null || cached.data() == null) {
            return false;
        }
        String etag = cached.eTag();
        if (etag == null || etag.isEmpty()) {
            return true;
        }
        String unquoted = etag.length() >= 2 && etag.charAt(0) == '"' && etag.charAt(etag.length() - 1) == '"'
                ? etag.substring(1, etag.length() - 1)
                : etag;
        if (!isHexSha256(unquoted)) {
            return true;
        }
        return sha256Hex(cached.data()).equalsIgnoreCase(unquoted);
    }

    private static boolean isHexSha256(String value) {
        if (value.length() != 64) {
            return false;
        }
        for (int i = 0; i < 64; i++) {
            char c = value.charAt(i);
            boolean hex = (c >= '0' && c <= '9')
                    || (c >= 'a' && c <= 'f')
                    || (c >= 'A' && c <= 'F');
            if (!hex) {
                return false;
            }
        }
        return true;
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private final class VerifiedFillStream extends FilterInputStream {
        private final String bucket;
        private final String key;
        private final int expectedLength;
        private final String expectedSha256Hex;
        private final byte[] buffer;
        private int count;
        private boolean overflowed;
        private boolean finished;

        VerifiedFillStream(
                InputStream in,
                String bucket,
                String key,
                int expectedLength,
                String expectedSha256Hex) {
            super(in);
            this.bucket = bucket;
            this.key = key;
            this.expectedLength = expectedLength;
            this.expectedSha256Hex = expectedSha256Hex;
            this.buffer = new byte[expectedLength];
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value < 0) {
                finishOnEof();
                return -1;
            }
            if (!overflowed) {
                if (count < expectedLength) {
                    buffer[count++] = (byte) value;
                } else {
                    overflowed = true;
                }
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n < 0) {
                finishOnEof();
                return -1;
            }
            if (!overflowed && n > 0) {
                if (count + n <= expectedLength) {
                    System.arraycopy(b, off, buffer, count, n);
                    count += n;
                } else {
                    overflowed = true;
                }
            }
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            byte[] scratch = new byte[(int) Math.min(8192L, Math.max(n, 1L))];
            long skipped = 0;
            while (skipped < n) {
                int r = read(scratch, 0, (int) Math.min(scratch.length, n - skipped));
                if (r < 0) {
                    break;
                }
                skipped += r;
            }
            return skipped;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        private void finishOnEof() {
            if (finished) {
                return;
            }
            finished = true;
            if (overflowed || count != expectedLength) {
                return;
            }
            if (expectedSha256Hex != null
                    && !expectedSha256Hex.isEmpty()
                    && !sha256Hex(buffer).equalsIgnoreCase(expectedSha256Hex)) {
                return;
            }
            try {
                cachePut(
                        new PutSpec(cacheBucket(bucket), key, DEFAULT_CONTENT_TYPE, null, null),
                        buffer);
            } catch (RuntimeException cacheFailure) {
                LOG.warn("cache populate failed for {}/{} — continuing", bucket, key, cacheFailure);
            }
        }
    }

    private final class BoundedFillStream extends FilterInputStream {
        private final String bucket;
        private final String key;
        private final int maxBytes;
        private ByteArrayOutputStream buffer;
        private int count;
        private boolean overflowed;
        private boolean finished;

        BoundedFillStream(InputStream in, String bucket, String key, int maxBytes) {
            super(in);
            this.bucket = bucket;
            this.key = key;
            this.maxBytes = maxBytes;
            this.buffer = new ByteArrayOutputStream(Math.min(maxBytes, 8192));
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value < 0) {
                finishOnEof();
                return -1;
            }
            if (!overflowed) {
                if (count < maxBytes) {
                    buffer.write(value);
                    count++;
                } else {
                    overflowed = true;
                    buffer = null;
                }
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n < 0) {
                finishOnEof();
                return -1;
            }
            if (!overflowed && n > 0) {
                if (count + n <= maxBytes) {
                    buffer.write(b, off, n);
                    count += n;
                } else {
                    overflowed = true;
                    buffer = null;
                }
            }
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            byte[] scratch = new byte[(int) Math.min(8192L, Math.max(n, 1L))];
            long skipped = 0;
            while (skipped < n) {
                int r = read(scratch, 0, (int) Math.min(scratch.length, n - skipped));
                if (r < 0) {
                    break;
                }
                skipped += r;
            }
            return skipped;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        private void finishOnEof() {
            if (finished) {
                return;
            }
            finished = true;
            if (overflowed || buffer == null) {
                return;
            }
            byte[] bytes = buffer.toByteArray();
            buffer = null;
            try {
                cachePut(
                        new PutSpec(cacheBucket(bucket), key, DEFAULT_CONTENT_TYPE, null, null),
                        bytes);
            } catch (RuntimeException cacheFailure) {
                LOG.warn("cache populate failed for {}/{} — continuing", bucket, key, cacheFailure);
            }
        }
    }
}
