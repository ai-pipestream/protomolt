package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStoreProvider;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/** Redis factory; configuration is explicit, including zero limits and empty prefix. */
public final class RedisBlobStoreProvider implements BlobStoreProvider {
    private static final Set<String> OPTIONS = Set.of("uri", "ttl-seconds", "max-object-bytes", "key-prefix");

    @Override public String id() { return "redis"; }

    @Override public OpenedBlobStore open(Map<String, String> options) {
        if (!options.keySet().equals(OPTIONS)) {
            throw new IllegalArgumentException("Redis requires uri, ttl-seconds, max-object-bytes and key-prefix only");
        }
        URI uri;
        try { uri = URI.create(options.get("uri")); }
        catch (IllegalArgumentException e) {
            // URI exception messages may contain credentials. Do not echo them.
            throw new IllegalArgumentException("Invalid Redis URI");
        }
        if (!("redis".equals(uri.getScheme()) || "rediss".equals(uri.getScheme()))
                || uri.getHost() == null || uri.getFragment() != null || uri.getQuery() != null) {
            throw new IllegalArgumentException("Redis URI requires redis/rediss and a host, without query or fragment");
        }
        int ttl;
        long max;
        try {
            ttl = Integer.parseInt(options.get("ttl-seconds"));
            max = Long.parseLong(options.get("max-object-bytes"));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Redis ttl-seconds and max-object-bytes must be integers");
        }
        var config = new RedisBlobStoreConfig(uri.toString(), ttl, max, options.get("key-prefix"));
        RedisBlobStore store = new RedisBlobStore(config);
        var capabilities = java.util.EnumSet.of(
                ai.protomolt.proto.repo.blob.spi.BlobCapability.LIST,
                ai.protomolt.proto.repo.blob.spi.BlobCapability.OBJECT_EXPIRY);
        if (ttl == 0) capabilities.add(ai.protomolt.proto.repo.blob.spi.BlobCapability.NON_EXPIRING_WRITES);
        return new OpenedBlobStore(store, store, capabilities, store::headBucket);
    }
}
