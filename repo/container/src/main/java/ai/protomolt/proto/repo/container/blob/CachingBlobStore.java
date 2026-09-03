package ai.protomolt.proto.repo.container.blob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;

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
 *   <li>{@code put} — {@code backing.put} honoring the verified-write spec
 *       (the SHA-256 rides to the backing store untouched), then best-effort
 *       {@code cache.put} with the configured TTL when the object fits.</li>
 *   <li>{@code copy} — {@code backing.copy}, then evict the DESTINATION from
 *       the cache (a stale cached destination would otherwise shadow the
 *       copied bytes).</li>
 *   <li>{@code delete}/{@code deleteAll} — backing first, then evict.</li>
 *   <li>{@code list}/{@code headBucket} — backing only; lists are never
 *       cached (enumerations go stale silently and are cheap to get wrong).</li>
 *   <li>{@code headObject} — a cache hit answers (existence caching: a cached
 *       object exists in backing, because every cached byte came from or
 *       through it); a miss falls through to backing. A cache miss says
 *       nothing about backing.</li>
 * </ul>
 */
public final class CachingBlobStore implements BlobStore, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CachingBlobStore.class);

    private final BlobStore backing;
    private final BlobStore cache;
    private final int ttlSeconds;
    private final long maxCacheableBytes;

    /**
     * @param backing the store of truth (every write lands here first)
     * @param cache the expendable front cache (e.g. a {@link RedisBlobStore})
     * @param ttlSeconds cache-entry TTL; {@code 0} = no expiry. Applied when
     *        the cache is TTL-aware (today: {@link RedisBlobStore}); other
     *        caches ignore it
     * @param maxCacheableBytes largest object admitted to the cache; objects
     *        larger than this bypass the cache entirely. {@code 0} or
     *        negative = cache nothing
     */
    public CachingBlobStore(BlobStore backing, BlobStore cache, int ttlSeconds, long maxCacheableBytes) {
        this.backing = backing;
        this.cache = cache;
        this.ttlSeconds = Math.max(ttlSeconds, 0);
        this.maxCacheableBytes = maxCacheableBytes;
    }

    @Override
    public PutResult put(PutSpec spec, byte[] body) {
        PutResult result = backing.put(spec, body);
        if (fits(body.length)) {
            try {
                cachePut(spec, body);
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
    public GetResult get(String bucket, String key, String versionId) {
        if (versionId == null || versionId.isEmpty()) {
            try {
                return cache.get(bucket, key);
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
                cachePut(new PutSpec(bucket, key, got.contentType(), null, null), got.data());
            } catch (RuntimeException cacheFailure) {
                LOG.warn("cache populate failed for {}/{} — continuing", bucket, key, cacheFailure);
            }
        }
        return got;
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
            cache.headObject(bucket, key);
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
        if (backing instanceof AutoCloseable closeable) {
            closeable.close();
        }
        if (cache instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    private boolean fits(long sizeBytes) {
        return maxCacheableBytes > 0 && sizeBytes <= maxCacheableBytes;
    }

    /** A cache put honoring the TTL when the cache is TTL-aware. */
    private void cachePut(PutSpec spec, byte[] body) {
        if (cache instanceof RedisBlobStore redis) {
            redis.put(spec, body, ttlSeconds);
        } else {
            cache.put(spec, body);
        }
    }

    private void evict(String bucket, String key) {
        try {
            cache.delete(bucket, key);
        } catch (RuntimeException cacheFailure) {
            LOG.warn("cache evict failed for {}/{} — continuing", bucket, key, cacheFailure);
        }
    }
}
