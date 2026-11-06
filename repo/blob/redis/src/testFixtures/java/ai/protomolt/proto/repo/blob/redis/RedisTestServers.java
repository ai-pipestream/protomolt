package ai.protomolt.proto.repo.blob.redis;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

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
    /** The RESP port every suite maps from the container. */
    public static final int PORT = 6379;
    static final String ENV_IMAGE = "PROTOMOLT_REDIS_TEST_IMAGE";
    static final String ENV_SERVER = "PROTOMOLT_REDIS_TEST_SERVER";
    private static final Duration PROBE_INTERVAL = Duration.ofMillis(25);
    private static final int PROBE_TIMEOUT_MS = 500;

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

    /**
     * The shared test container. Readiness is a real RESP {@code PING} answered with
     * {@code PONG}, not a listening port: the Docker port proxy accepts connections
     * before the server is serving, so a port-only wait reports started while Jedis
     * still hits an unexpected end of stream.
     */
    public static GenericContainer<?> container() {
        return new GenericContainer<>(image()).withExposedPorts(PORT).waitingFor(new RespPingWait());
    }

    /**
     * One readiness probe. Success is exactly a {@code PONG} answer. A connection-level failure or the
     * explicit transient {@code LOADING} reply means the server is not serving yet;
     * any other reply is a refusal and must surface instead of polling into a pass.
     */
    static void probe(String host, int port) {
        try (var jedis = new Jedis(host, port, PROBE_TIMEOUT_MS)) {
            if (!"PONG".equals(jedis.ping()))
                throw new IllegalStateException("Unexpected PING reply (expected PONG)");
        } catch (JedisConnectionException notServing) {
            throw new NotServingException(notServing);
        } catch (JedisDataException reply) {
            // The server accepts connections while it replays its dataset and answers
            // PING with the explicit transient LOADING error during that window.
            if (reply.getMessage() != null && reply.getMessage().startsWith("LOADING "))
                throw new NotServingException(reply);
            throw reply;
        }
    }

    static final class NotServingException extends RuntimeException {
        NotServingException(RuntimeException cause) {
            super(cause);
        }
    }

    static final class RespPingWait extends AbstractWaitStrategy {
        @Override protected void waitUntilReady() {
            String host = waitStrategyTarget.getHost();
            int port = waitStrategyTarget.getMappedPort(PORT);
            long deadline = System.nanoTime() + startupTimeout.toNanos();
            NotServingException last = null;
            while (true) {
                try {
                    probe(host, port);
                    return;
                } catch (NotServingException notServing) {
                    last = notServing;
                }
                if (System.nanoTime() >= deadline)
                    throw new IllegalStateException("Redis-compatible server at " + host + ':' + port
                            + " did not answer PING with PONG within " + startupTimeout, last);
                try {
                    Thread.sleep(PROBE_INTERVAL.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted waiting for " + host + ':' + port, interrupted);
                }
            }
        }
    }

    private static void requirePaired() {
        if ((System.getenv(ENV_IMAGE) == null) != (System.getenv(ENV_SERVER) == null))
            throw new IllegalStateException(ENV_IMAGE + " and " + ENV_SERVER + " must be set together");
    }
}
