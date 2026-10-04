package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import java.net.URI;
import java.util.Locale;
import java.util.Map;

/** Physical v2 identity without credentials, TTL policy or client resource acquisition. */
public final class RedisBackendIdentity {
    private RedisBackendIdentity() {}
    public static BackendIdentity of(String endpoint, String keyPrefix) {
        URI uri;
        try { uri = URI.create(endpoint); }
        catch (RuntimeException invalid) { throw new IllegalArgumentException("Invalid Redis identity URI"); }
        if (!("redis".equals(uri.getScheme()) || "rediss".equals(uri.getScheme())) || uri.getHost() == null
                || uri.getQuery() != null || uri.getFragment() != null || uri.getPort() == 0 || uri.getPort() > 65535)
            throw new IllegalArgumentException("Redis identity requires a host and valid redis/rediss origin without query or fragment");
        String path = uri.getRawPath();
        int database = 0;
        if (path != null && !path.isEmpty() && !path.equals("/")) {
            if (!path.matches("/[0-9]+")) throw new IllegalArgumentException("Invalid Redis database identity");
            try { database = Integer.parseInt(path.substring(1)); }
            catch (NumberFormatException invalid) { throw new IllegalArgumentException("Invalid Redis database identity"); }
        }
        RedisObjectKeys.encode(keyPrefix); // Reject strings whose UTF-8 encoding would collapse distinct identities.
        return new BackendIdentity("redis", "redis/v2", Map.of("scheme", uri.getScheme(),
                "host", uri.getHost().toLowerCase(Locale.ROOT), "port", Integer.toString(uri.getPort() < 0 ? 6379 : uri.getPort()),
                "database", Integer.toString(database), "key-prefix", keyPrefix));
    }
}
