package ai.protomolt.proto.repo.blob.spi;

/** Optional per-object expiry for storage used as a cache. */
public interface ExpiringBlobStore extends BlobStore {
    /** Writes bytes with the supplied expiry in seconds; zero means no expiry. */
    PutResult put(PutSpec spec, byte[] body, int ttlSeconds);
    /**
     * Writes a caller-owned stream with the supplied expiry and exact length.
     * Unsupported providers refuse; there is no implicit whole-object buffering.
     * @param spec object identity and verified-write metadata
     * @param body caller-owned body
     * @param contentLength exact body length
     * @param ttlSeconds expiry seconds, zero for no expiry
     * @return the committed provider identity
     */
    default PutResult put(PutSpec spec, java.io.InputStream body, long contentLength, int ttlSeconds) {
        throw new UnsupportedOperationException("Streamed expiring write is unsupported");
    }
}
