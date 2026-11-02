package ai.protomolt.proto.repo.blob.redis;

/**
 * The one pinned Redis-compatible server for container tests. CI reruns the suites on
 * Redis by setting both {@code PROTOMOLT_REDIS_TEST_IMAGE} and
 * {@code PROTOMOLT_REDIS_TEST_SERVER}; setting only one is refused, since an image
 * and server command from different products cannot start.
 */
public final class RedisTestServers {
    /** Pinned default; matches {@code deploy/document-platform/compose.valkey.yml}. */
    public static final String DEFAULT_IMAGE = "valkey/valkey:9.1.3-alpine";
    /** Server command inside {@link #DEFAULT_IMAGE}. */
    public static final String DEFAULT_SERVER = "valkey-server";
    static final String ENV_IMAGE = "PROTOMOLT_REDIS_TEST_IMAGE";
    static final String ENV_SERVER = "PROTOMOLT_REDIS_TEST_SERVER";

    private RedisTestServers() {}

    /** @return the selected container image */
    public static String image() {
        requirePaired();
        return System.getenv().getOrDefault(ENV_IMAGE, DEFAULT_IMAGE);
    }

    /** @return the server executable inside {@link #image()} */
    public static String server() {
        requirePaired();
        return System.getenv().getOrDefault(ENV_SERVER, DEFAULT_SERVER);
    }

    private static void requirePaired() {
        if ((System.getenv(ENV_IMAGE) == null) != (System.getenv(ENV_SERVER) == null))
            throw new IllegalStateException(ENV_IMAGE + " and " + ENV_SERVER + " must be set together");
    }
}
