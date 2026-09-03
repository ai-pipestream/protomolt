package ai.protomolt.proto.repo.container.blob;

/**
 * Configuration for {@link RedisBlobStore}: where Redis is and how objects
 * behave there.
 *
 * @param uri the Redis connection URI ({@code redis://[:password@]host:port[/db]})
 * @param ttlSeconds per-object expiry in seconds; {@code 0} = no expiry
 * @param maxObjectBytes largest object the store accepts; {@code 0} = unbounded.
 *        Redis values live in memory — the ceiling is the guard rail that keeps
 *        a stray multi-gigabyte put from OOMing the server
 * @param keyPrefix prefix prepended to every physical key (e.g.
 *        {@code "repo:"}), so one Redis database can host several stores
 *        without key collisions; empty = no prefix
 */
public record RedisBlobStoreConfig(String uri, int ttlSeconds, long maxObjectBytes, String keyPrefix) {

    /** Defaults matching the service wiring: localhost, no expiry, unbounded, no prefix. */
    public static final RedisBlobStoreConfig LOCAL =
            new RedisBlobStoreConfig("redis://localhost:6379", 0, 0L, "");

    public RedisBlobStoreConfig {
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("uri is required (redis://[:password@]host:port[/db])");
        }
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("ttlSeconds must be >= 0 (0 = no expiry)");
        }
        if (maxObjectBytes < 0) {
            throw new IllegalArgumentException("maxObjectBytes must be >= 0 (0 = unbounded)");
        }
        keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }
}
