package ai.protomolt.proto.repo.blob.redis;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shared readiness probe against fake RESP sockets: a container is ready only when a
 * real PING answers PONG. Accepting a connection without serving — what Docker's port
 * proxy does before the server starts — must not count as ready (ticket #792's
 * "Unexpected end of stream").
 */
class RedisTestServersIT {

    @Test void pongMeansServing() throws Exception {
        try (var server = new FakeServer(reply("+PONG\r\n"))) {
            assertThatCode(() -> RedisTestServers.probe("127.0.0.1", server.port()))
                    .doesNotThrowAnyException();
        }
    }

    @Test void acceptedConnectionClosedWithoutReplyMeansNotServing() throws Exception {
        try (var server = new FakeServer((client, nth) -> { /* accept and close: the port-proxy race */ })) {
            assertThatThrownBy(() -> RedisTestServers.probe("127.0.0.1", server.port()))
                    .isInstanceOfSatisfying(RedisTestServers.NotServingException.class,
                            notServing -> assertThat(notServing.getCause())
                                    .isInstanceOf(JedisConnectionException.class)
                                    .hasMessageContaining("Unexpected end of stream"));
        }
    }

    @Test void silentConnectionMeansNotServing() throws Exception {
        try (var server = new FakeServer((client, nth) -> Thread.sleep(5_000))) {
            assertThatThrownBy(() -> RedisTestServers.probe("127.0.0.1", server.port()))
                    .isInstanceOf(RedisTestServers.NotServingException.class)
                    .satisfies(notServing -> assertThat(notServing.getCause())
                            .isInstanceOf(JedisConnectionException.class));
        }
    }

    @Test void loadingReplyMeansNotServing() throws Exception {
        try (var server = new FakeServer(reply("-LOADING dataset is loading\r\n"))) {
            assertThatThrownBy(() -> RedisTestServers.probe("127.0.0.1", server.port()))
                    .isInstanceOfSatisfying(RedisTestServers.NotServingException.class,
                            notServing -> assertThat(notServing.getCause())
                                    .isInstanceOf(JedisDataException.class)
                                    .hasMessageStartingWith("LOADING "));
        }
    }

    @Test void refusalReplySurfacesInsteadOfPolling() throws Exception {
        try (var server = new FakeServer(reply("-NOAUTH Authentication required.\r\n"))) {
            assertThatThrownBy(() -> RedisTestServers.probe("127.0.0.1", server.port()))
                    .isInstanceOf(JedisDataException.class)
                    .isNotInstanceOf(RedisTestServers.NotServingException.class);
        }
    }

    @Test void waitKeepsProbingThroughTheRaceWindowUntilPong() throws Exception {
        try (var server = new FakeServer((client, nth) -> {
            if (nth > 3) reply("+PONG\r\n").handle(client, nth); // first connections: the accept-without-serve race
        })) {
            assertThatCode(() -> new RedisTestServers.RespPingWait().waitUntilReady(target(server.port())))
                    .doesNotThrowAnyException();
        }
    }

    @Test void waitTimesOutWithCauseWhileNothingServes() throws Exception {
        try (var server = new FakeServer((client, nth) -> { /* never serves */ })) {
            assertThatThrownBy(() -> new RedisTestServers.RespPingWait()
                    .withStartupTimeout(Duration.ofMillis(400))
                    .waitUntilReady(target(server.port())))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("did not answer PING with PONG")
                    .satisfies(timeout -> assertThat(timeout.getCause())
                            .isInstanceOf(RedisTestServers.NotServingException.class)
                            .satisfies(notServing -> assertThat(notServing.getCause())
                                    .isInstanceOf(JedisConnectionException.class)));
        }
    }

    private static WaitStrategyTarget target(int port) {
        return (WaitStrategyTarget) Proxy.newProxyInstance(
                WaitStrategyTarget.class.getClassLoader(),
                new Class<?>[]{WaitStrategyTarget.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getHost" -> "127.0.0.1";
                    case "getMappedPort" -> port;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static SocketHandler reply(String response) {
        // One canned reply per incoming command (a '*' RESP array header per command).
        return (client, nth) -> {
            var in = client.getInputStream();
            var out = client.getOutputStream();
            var buffer = new byte[4096];
            client.setSoTimeout(2_000);
            while (true) {
                int read;
                try {
                    read = in.read(buffer);
                } catch (java.net.SocketTimeoutException nothingMore) {
                    return;
                }
                if (read < 0) return;
                int commands = 0;
                for (int at = 0; at < read; at++) {
                    if ((at == 0 || buffer[at - 1] == '\n') && buffer[at] == '*') commands++;
                }
                for (int i = 0; i < Math.max(commands, 1); i++)
                    out.write(response.getBytes(StandardCharsets.UTF_8));
            }
        };
    }

    @FunctionalInterface
    private interface SocketHandler {
        void handle(Socket client, int connection) throws Exception;
    }

    private static final class FakeServer implements AutoCloseable {
        private final ServerSocket socket;
        private final Thread acceptor;

        FakeServer(SocketHandler handler) throws IOException {
            socket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            var connections = new AtomicInteger();
            acceptor = new Thread(() -> {
                while (!socket.isClosed()) {
                    try {
                        var client = socket.accept();
                        int nth = connections.incrementAndGet();
                        Thread.ofPlatform().daemon().start(() -> {
                            try (client) {
                                handler.handle(client, nth);
                            } catch (Exception ignored) {
                                // The probe is allowed to give up first.
                            }
                        });
                    } catch (IOException closed) {
                        return;
                    }
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return socket.getLocalPort();
        }

        @Override public void close() throws IOException {
            socket.close();
        }
    }
}
