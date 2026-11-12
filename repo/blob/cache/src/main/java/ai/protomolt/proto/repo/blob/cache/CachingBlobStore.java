package ai.protomolt.proto.repo.blob.cache;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.ExpiringBlobStore;
import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expendable read-through cache. Authoritative metadata identifies every hit;
 * a late fill from an older read cannot masquerade as the current object.
 * Verified cache candidates are staged in owner-only temporary files and checked
 * before exposure. Fills use bounded disk staging and streaming writes, never a
 * second whole-object heap buffer. Closing early abandons a fill.
 * Providers without metadata/snapshot or streaming cache capabilities bypass the
 * unsupported cache operation; authoritative errors still propagate.
 */
public final class CachingBlobStore implements BlobStore, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CachingBlobStore.class);
    private static final String SOURCE_ETAG = "protomolt-cache-source-etag";
    private static final String SOURCE_VERSION = "protomolt-cache-source-version";
    private static final String SOURCE_LENGTH = "protomolt-cache-source-length";
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

    @Override public PutResult put(PutSpec spec, byte[] body) {
        PutResult result = backing.put(spec, body);
        evict(spec.bucket(), spec.key());
        populate(spec.bucket(), spec.key(), new ObjectMetadata(body.length, spec.contentType(),
                result.eTag(), result.versionId(), Map.of()), new ByteArrayInputStream(body));
        return result;
    }

    @Override public PutResult put(PutSpec spec, InputStream body, long contentLength) {
        PutResult result = backing.put(spec, body, contentLength);
        evict(spec.bucket(), spec.key());
        return result;
    }

    @Override public PutResult putMultipart(PutSpec spec, InputStream body, int partSize) {
        PutResult result = backing.putMultipart(spec, body, partSize);
        evict(spec.bucket(), spec.key());
        return result;
    }

    @Override public GetResult getForUpdate(String bucket, String key) { return backing.getForUpdate(bucket, key); }
    @Override public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
        PutResult result = backing.conditionalPut(spec, body, condition);
        evict(spec.bucket(), spec.key());
        return result;
    }

    @Override public GetResult get(String bucket, String key, String versionId) {
        return getCurrent(bucket, key, versionId, -1);
    }

    @Override public GetResult getBounded(String bucket, String key, String versionId, int maxBytes) {
        if (maxBytes < 0) throw new IllegalArgumentException("Read limit must not be negative");
        return getCurrent(bucket, key, versionId, maxBytes);
    }

    private GetResult getCurrent(String bucket, String key, String version, int limit) {
        boolean current = version == null || version.isEmpty();
        ObjectMetadata metadata = current ? currentMetadata(bucket, key) : null;
        if (metadata != null && (limit < 0 || metadata.length() <= limit) && metadata.length() <= Integer.MAX_VALUE) {
            InputStream cached = cachedStream(bucket, key, metadata, -1, null);
            if (cached != null) {
                try (cached) {
                    byte[] bytes = new byte[(int) metadata.length()];
                    if (cached.readNBytes(bytes, 0, bytes.length) != bytes.length || cached.read() != -1)
                        throw new IOException("Verified cache spool changed length");
                    return new GetResult(bytes, metadata.contentType(), metadata.eTag(), metadata.versionId());
                } catch (IOException failure) { throw new UncheckedIOException(failure); }
            }
        }
        GetResult got = limit < 0 ? backing.get(bucket, key, version) : backing.getBounded(bucket, key, version, limit);
        if (current) populate(bucket, key, new ObjectMetadata(got.data().length, got.contentType(), got.eTag(),
                got.versionId(), Map.of()), new ByteArrayInputStream(got.data()));
        return got;
    }

    @Override public InputStream openVerifiedStream(String bucket, String key, long expectedLength, String expectedSha256Hex) {
        if (expectedLength >= 0 && !fits(expectedLength))
            return backing.openVerifiedStream(bucket, key, expectedLength, expectedSha256Hex);
        ObjectMetadata metadata = currentMetadata(bucket, key);
        if (metadata != null) {
            InputStream hit = cachedStream(bucket, key, metadata, expectedLength, expectedSha256Hex);
            if (hit != null) return hit;
        }
        return fillStream(bucket, key, expectedLength, expectedSha256Hex);
    }

    @Override public InputStream openStream(String bucket, String key, long offset, long length) {
        return openStream(bucket, key, null, offset, length);
    }

    @Override public InputStream openStream(String bucket, String key, String version, long offset, long length) {
        if ((version != null && !version.isEmpty()) || offset != 0 || length >= 0 || maxCacheableBytes <= 0)
            return backing.openStream(bucket, key, version, offset, length);
        ObjectMetadata metadata = currentMetadata(bucket, key);
        if (metadata != null) {
            InputStream hit = cachedStream(bucket, key, metadata, -1, null);
            if (hit != null) return hit;
        }
        return fillStream(bucket, key, -1, null);
    }

    @Override public ObjectMetadata statObject(String bucket, String key) { return backing.statObject(bucket, key); }
    @Override public ObjectRead openSnapshot(String bucket, String key) { return backing.openSnapshot(bucket, key); }

    private ObjectMetadata currentMetadata(String bucket, String key) {
        if (maxCacheableBytes <= 0) return null;
        try { return backing.statObject(bucket, key); }
        catch (UnsupportedOperationException unsupported) { return null; }
        catch (BlobNotFoundException missing) { evict(bucket, key); throw missing; }
    }

    private static boolean sameIdentity(ObjectMetadata a, ObjectMetadata b) {
        return a.length() == b.length() && Objects.equals(a.eTag(), b.eTag())
                && Objects.equals(a.versionId(), b.versionId());
    }

    private boolean cacheable(ObjectMetadata metadata) {
        if (metadata == null || !fits(metadata.length())) return false;
        try { BlobStore.requireStrongEtag(metadata.eTag()); return true; }
        catch (IllegalArgumentException weakOrMissing) { return false; }
    }

    private InputStream cachedStream(String bucket, String key, ObjectMetadata source, long expectedLength, String expectedSha) {
        if (!cacheable(source)) return null;
        CacheSpool spool = null;
        boolean verified = false;
        try {
            ObjectMetadata entry = cache.statObject(cacheBucket(bucket), key);
            if (entry.length() != source.length()
                    || !Objects.equals(entry.attributes().get(SOURCE_ETAG), source.eTag())
                    || !Objects.equals(entry.attributes().get(SOURCE_VERSION), Objects.toString(source.versionId(), ""))
                    || !Objects.equals(entry.attributes().get(SOURCE_LENGTH), Long.toString(source.length()))) {
                evict(bucket, key);
                return null;
            }
            String digest = shaEtag(entry.eTag());
            if (digest == null) return null; // no trustworthy digest for a concurrent cache replacement
            if (expectedLength >= 0 && source.length() != expectedLength) return null;
            spool = new CacheSpool(source.length());
            try (InputStream input = cache.openStream(cacheBucket(bucket), key, 0, -1)) {
                byte[] window = new byte[8192];
                for (int count; (count = input.read(window)) != -1;) if (count > 0) spool.write(window, 0, count);
            }
            spool.finish();
            if (spool.size() != source.length() || !spool.sha().equalsIgnoreCase(digest)
                    || (expectedSha != null && !expectedSha.isEmpty() && !spool.sha().equalsIgnoreCase(expectedSha))) {
                evict(bucket, key);
                return null;
            }
            verified = true;
            // Cache failures may fall back; authoritative failures must not be swallowed.
        } catch (BlobNotFoundException miss) { return null; }
        catch (IOException | RuntimeException failure) {
            if (Thread.currentThread().isInterrupted())
                throw new BlobStoreException(BlobStoreException.Code.CANCELLED, "Cache read cancelled", failure);
            LOG.warn("Cache candidate unavailable for {}/{}; reading backing", bucket, key, failure);
            evict(bucket, key);
            return null;
        } finally {
            // Ownership is handed off below only after the authoritative recheck.
            if (spool != null && !verified) discard(spool);
        }
        try {
            ObjectMetadata now = currentMetadata(bucket, key);
            if (now == null || !sameIdentity(source, now)) { evict(bucket, key); return null; }
            InputStream result = spool.ownedStream();
            spool = null;
            return result;
        } catch (IOException failure) {
            if (Thread.currentThread().isInterrupted())
                throw new BlobStoreException(BlobStoreException.Code.CANCELLED, "Cache read cancelled", failure);
            LOG.warn("Cache spool unavailable for {}/{}; reading backing", bucket, key, failure);
            return null;
        } finally { if (spool != null) discard(spool); }
    }

    private InputStream fillStream(String bucket, String key, long expectedLength, String expectedSha) {
        ObjectRead read;
        try { read = backing.openSnapshot(bucket, key); }
        catch (UnsupportedOperationException unsupported) { return backing.openStream(bucket, key, 0, -1); }
        if (!cacheable(read.metadata())
                || (expectedLength >= 0 && read.metadata().length() != expectedLength)) return read.stream();
        CacheSpool spool;
        try { spool = new CacheSpool(read.metadata().length()); }
        catch (IOException | RuntimeException failure) {
            if (Thread.currentThread().isInterrupted()) {
                try { read.close(); } catch (IOException | RuntimeException close) { failure.addSuppressed(close); }
                throw new BlobStoreException(BlobStoreException.Code.CANCELLED, "Cache read cancelled", failure);
            }
            LOG.warn("Cache staging unavailable for {}/{}; streaming backing", bucket, key, failure);
            return read.stream();
        }
        return new FillStream(read, bucket, key, spool, expectedLength, expectedSha);
    }

    private void populate(String bucket, String key, ObjectMetadata source, InputStream body) {
        if (!cacheable(source)) return;
        var attributes = Map.of(SOURCE_ETAG, source.eTag(), SOURCE_VERSION, Objects.toString(source.versionId(), ""),
                SOURCE_LENGTH, Long.toString(source.length()));
        var spec = new PutSpec(cacheBucket(bucket), key, source.contentType(), attributes, null);
        try {
            if (cache instanceof ExpiringBlobStore expiring) expiring.put(spec, body, source.length(), ttlSeconds);
            else cache.put(spec, body, source.length());
        } catch (RuntimeException failure) {
            if (Thread.currentThread().isInterrupted())
                throw new BlobStoreException(BlobStoreException.Code.CANCELLED, "Cache fill cancelled", failure);
            LOG.warn("Cache fill failed for {}/{}; backing is authoritative", bucket, key, failure);
        }
    }

    private boolean fits(long size) { return maxCacheableBytes > 0 && size >= 0 && size <= maxCacheableBytes; }

    private void evict(String bucket, String key) {
        try { cache.delete(cacheBucket(bucket), key); }
        catch (RuntimeException failure) { LOG.warn("Cache eviction failed for {}/{}", bucket, key, failure); }
    }

    private static String shaEtag(String etag) {
        if (etag == null) return null;
        String value = etag.length() == 66 && etag.charAt(0) == '"' && etag.charAt(65) == '"'
                ? etag.substring(1, 65) : etag;
        if (value.length() != 64) return null;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F')) return null;
        }
        return value;
    }

    private static void discard(CacheSpool spool) {
        try { spool.close(); }
        catch (IOException failure) { LOG.warn("Failed to remove expendable cache staging file", failure); }
    }

    private final class FillStream extends FilterInputStream {
        private final ObjectMetadata source;
        private final String bucket;
        private final String key;
        private final long expectedLength;
        private final String expectedSha;
        private CacheSpool spool;
        private boolean ended;
        private final byte[] single = new byte[1];

        FillStream(ObjectRead read, String bucket, String key, CacheSpool spool, long expectedLength, String expectedSha) {
            super(read.stream());
            this.source = read.metadata(); this.bucket = bucket; this.key = key;
            this.spool = spool; this.expectedLength = expectedLength; this.expectedSha = expectedSha;
        }

        @Override public int read() throws IOException {
            int count = read(single, 0, 1);
            return count < 0 ? -1 : single[0] & 0xff;
        }

        @Override public int read(byte[] bytes, int offset, int count) throws IOException {
            int n;
            try { n = in.read(bytes, offset, count); }
            catch (IOException | RuntimeException failure) {
                abandon();
                try { in.close(); } catch (IOException | RuntimeException close) { failure.addSuppressed(close); }
                throw failure;
            }
            if (spool != null) {
                try {
                    if (n > 0) spool.write(bytes, offset, n);
                    else if (n < 0 && !ended) {
                        ended = true;
                        spool.finish();
                        if (spool.size() == source.length() && (expectedLength < 0 || spool.size() == expectedLength)
                                && (expectedSha == null || expectedSha.isEmpty() || spool.sha().equalsIgnoreCase(expectedSha))) {
                            try (var input = spool.open()) { populate(bucket, key, source, input); }
                        }
                        abandon();
                    }
                } catch (IOException | RuntimeException failure) {
                    if (Thread.currentThread().isInterrupted()) {
                        abandon();
                        try { in.close(); } catch (IOException | RuntimeException close) { failure.addSuppressed(close); }
                        throw new IOException("Cache read cancelled", failure);
                    }
                    LOG.warn("Cache staging failed for {}/{}; backing read continues", bucket, key, failure);
                    abandon();
                }
            }
            return n;
        }

        @Override public long skip(long requested) throws IOException {
            byte[] buffer = new byte[8192];
            long skipped = 0;
            while (skipped < requested) {
                int count = read(buffer, 0, (int) Math.min(buffer.length, requested - skipped));
                if (count < 0) break;
                skipped += count;
            }
            return skipped;
        }
        @Override public boolean markSupported() { return false; }
        @Override public synchronized void reset() throws IOException { throw new IOException("mark/reset is unsupported"); }
        private void abandon() { if (spool != null) { discard(spool); spool = null; } }
        @Override public void close() throws IOException { try { in.close(); } finally { abandon(); } }
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
    public ObjectReclaimer reclaimer(ObjectReclaimer backingReclaimer) {
        Objects.requireNonNull(backingReclaimer, "backingReclaimer");
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

}
