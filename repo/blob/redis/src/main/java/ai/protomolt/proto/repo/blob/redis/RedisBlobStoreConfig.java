package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.redis.RedisBlobStore;

/**
 * Configuration for {@link RedisBlobStore}: where Redis is and how objects
 * behave there.
 *
 * @param uri the Redis connection URI ({@code redis://[:password@]host:port[/db]})
 * @param ttlSeconds per-object expiry in seconds; {@code 0} = no expiry
 * @param maxObjectBytes largest object the store accepts; {@code 0} = adapter ceiling
 *        (512 MiB for replacement, 9 MiB for create-only).
 *        Redis values live in memory — the ceiling is the guard rail that keeps
 *        a stray multi-gigabyte put from OOMing the server
 * @param keyPrefix identity component encoded into every physical key (e.g.
 *        {@code "repo:"}), so one Redis database can host several stores
 *        without key collisions; empty is a distinct configured identity
 * @param maxConcurrentTransfers maximum dedicated streaming sockets. The documented
 *        bound is {@link #MAX_CONCURRENT_TRANSFERS}. A value below 1 is rejected.
 *        There is no unlimited default
 * @param writePolicy replacement in v2, or bounded create-only writes in an isolated v3 layout
 */
public record RedisBlobStoreConfig(String uri, int ttlSeconds, long maxObjectBytes, String keyPrefix,
        int maxConcurrentTransfers, RedisWritePolicy writePolicy) {

    /**
     * Documented cap on dedicated streaming sockets for one store. These sockets
     * bypass the Jedis pool. Convenience constructors and the Redis provider apply
     * this constant. Construction rejects a configured value below 1.
     */
    public static final int MAX_CONCURRENT_TRANSFERS = 32;

    static {
        if (MAX_CONCURRENT_TRANSFERS < 1) {
            throw new ExceptionInInitializerError("MAX_CONCURRENT_TRANSFERS must be >= 1");
        }
    }

    /** Applies {@link #MAX_CONCURRENT_TRANSFERS} and {@link RedisWritePolicy#REPLACE}. */
    public RedisBlobStoreConfig(String uri, int ttlSeconds, long maxObjectBytes, String keyPrefix) {
        this(uri, ttlSeconds, maxObjectBytes, keyPrefix, MAX_CONCURRENT_TRANSFERS, RedisWritePolicy.REPLACE);
    }

    /** Applies {@link #MAX_CONCURRENT_TRANSFERS}. */
    public RedisBlobStoreConfig(String uri, int ttlSeconds, long maxObjectBytes, String keyPrefix,
            RedisWritePolicy writePolicy) {
        this(uri, ttlSeconds, maxObjectBytes, keyPrefix, MAX_CONCURRENT_TRANSFERS, writePolicy);
    }

    /** Local defaults: localhost, no expiry, adapter size ceiling, empty prefix, documented transfer cap. */
    public static final RedisBlobStoreConfig LOCAL =
            new RedisBlobStoreConfig("redis://localhost:6379", 0, 0L, "");

    public RedisBlobStoreConfig {
        java.util.Objects.requireNonNull(writePolicy);
        if (writePolicy == RedisWritePolicy.CREATE_ONLY && ttlSeconds != 0)
            throw new IllegalArgumentException("Create-only storage requires zero TTL");
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("uri is required (redis://[:password@]host:port[/db])");
        }
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("ttlSeconds must be >= 0 (0 = no expiry)");
        }
        if (maxObjectBytes < 0) {
            throw new IllegalArgumentException("maxObjectBytes must be >= 0 (0 = adapter ceiling)");
        }
        if (maxConcurrentTransfers < 1) {
            throw new IllegalArgumentException("maxConcurrentTransfers must be >= 1");
        }
        keyPrefix = keyPrefix == null ? "" : keyPrefix;
        RedisBackendIdentity.of(uri, keyPrefix);
    }
}
