package ai.protomolt.proto.repo.blob.spi;

/** Optional per-object expiry for storage used as a cache. */
public interface ExpiringBlobStore extends BlobStore {
    /** Writes bytes with the supplied expiry in seconds; zero means no expiry. */
    PutResult put(PutSpec spec, byte[] body, int ttlSeconds);
}
