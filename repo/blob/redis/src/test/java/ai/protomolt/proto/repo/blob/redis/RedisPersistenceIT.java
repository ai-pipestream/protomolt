package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Process-crash evidence only: the Docker host and its storage remain running. */
@Testcontainers
class RedisPersistenceIT {
    private static final String PREFIX = "restart:";
    private static final String NAMESPACE = "documents";

    @Test
    void acknowledgedWritesCopiesAndReclamationSurviveProcessKillWithAlwaysSyncedAof() throws Exception {
        verifyRestart(true);
    }

    @Test
    void nonExpiringCapabilityDoesNotPromisePersistence() throws Exception {
        verifyRestart(false);
    }

    private static void verifyRestart(boolean persistent) throws Exception {
        try (var redis = new GenericContainer<>("redis:7-alpine")
                .withExposedPorts(6379)
                .withCommand("redis-server", "--save", "", "--appendonly", persistent ? "yes" : "no",
                        "--appendfsync", "always", "--maxmemory-policy", "noeviction")) {
            redis.start();
            int firstPort = port(redis);
            byte[] body = "retained protobuf bytes\u0000\u00ff".getBytes(StandardCharsets.UTF_8);
            String sourceKey = new RedisObjectKeys(PREFIX).object(NAMESPACE, "source");
            byte[] attributes;
            String etag;
            try (var client = new Jedis(redis.getHost(), firstPort);
                    var handle = open(redis.getHost(), firstPort)) {
                assertThat(client.configGet("appendonly")).containsEntry("appendonly", persistent ? "yes" : "no");
                assertThat(client.configGet("appendfsync")).containsEntry("appendfsync", "always");
                assertThat(client.configGet("save")).containsEntry("save", "");
                assertThat(client.configGet("maxmemory-policy")).containsEntry("maxmemory-policy", "noeviction");
                assertThat(handle.capabilities()).contains(BlobCapability.NON_EXPIRING_WRITES);
                var store = handle.store();
                etag = store.put(new BlobStore.PutSpec(NAMESPACE, "source", "application/x-protobuf",
                        Map.of("schema", "retained-definition", "owner", "account-a"), null), body).eTag();
                store.copy(NAMESPACE, "source", NAMESPACE, "copy");
                store.put(new BlobStore.PutSpec(NAMESPACE, "removed", "text/plain", null, null), body);
                attributes = client.hget(sourceKey.getBytes(StandardCharsets.UTF_8),
                        "attributes".getBytes(StandardCharsets.UTF_8));
                assertThat(attributes).isNotEmpty();
                assertThat(client.ttl(sourceKey)).isEqualTo(-1);
            }

            int restartedPort = killAndRestart(redis);

            try (var handle = open(redis.getHost(), restartedPort);
                    var client = new Jedis(redis.getHost(), restartedPort)) {
                var store = handle.store();
                if (persistent) {
                    for (String key : new String[]{"source", "copy"}) {
                        var restored = store.getBounded(NAMESPACE, key, null, body.length);
                        assertThat(restored.data()).isEqualTo(body);
                        assertThat(restored.contentType()).isEqualTo("application/x-protobuf");
                        assertThat(restored.eTag()).isEqualTo(etag);
                        String physical = new RedisObjectKeys(PREFIX).object(NAMESPACE, key);
                        assertThat(client.hget(physical.getBytes(StandardCharsets.UTF_8),
                                "attributes".getBytes(StandardCharsets.UTF_8))).isEqualTo(attributes);
                        assertThat(client.ttl(physical)).isEqualTo(-1);
                    }
                    // Establish durable existence before testing whether reclamation survives a crash.
                    assertThat(store.get(NAMESPACE, "removed").data()).isEqualTo(body);
                    assertThat(handle.reclaimer().reclaim(NAMESPACE, "removed")).isTrue();
                } else {
                    for (String key : new String[]{"source", "copy"}) {
                        assertThatThrownBy(() -> store.get(NAMESPACE, key))
                                .isInstanceOf(BlobStore.BlobNotFoundException.class);
                    }
                }
                assertThatThrownBy(() -> store.get(NAMESPACE, "removed"))
                        .isInstanceOf(BlobStore.BlobNotFoundException.class);
            }
            if (persistent) {
                int afterReclamation = killAndRestart(redis);
                try (var handle = open(redis.getHost(), afterReclamation)) {
                    assertThatThrownBy(() -> handle.store().get(NAMESPACE, "removed"))
                            .isInstanceOf(BlobStore.BlobNotFoundException.class);
                    for (String key : new String[]{"source", "copy"}) {
                        assertThat(handle.store().get(NAMESPACE, key).data()).isEqualTo(body);
                    }
                }
            }
        }
    }

    private static int killAndRestart(GenericContainer<?> redis) throws InterruptedException {
        // No SAVE, graceful shutdown, or explicit fsync after the adapter acknowledges writes.
        redis.getDockerClient().killContainerCmd(redis.getContainerId()).withSignal("KILL").exec();
        var stopped = redis.getDockerClient().inspectContainerCmd(redis.getContainerId()).exec();
        assertThat(stopped.getState().getRunning()).isFalse();
        assertThat(stopped.getState().getExitCodeLong()).isEqualTo(137L);
        redis.getDockerClient().startContainerCmd(redis.getContainerId()).exec();
        int restartedPort = port(redis);
        awaitReady(redis.getHost(), restartedPort);
        return restartedPort;
    }

    private static OpenedBlobStore open(String host, int port) {
        return BlobStores.discover().open("redis", Map.of("uri", "redis://" + host + ":" + port,
                "ttl-seconds", "0", "max-object-bytes", "1048576", "key-prefix", PREFIX));
    }

    private static int port(GenericContainer<?> container) {
        // Docker may assign a new host port on restart; do not reuse Testcontainers' cached inspection.
        var binding = container.getDockerClient().inspectContainerCmd(container.getContainerId()).exec()
                .getNetworkSettings().getPorts().getBindings()
                .get(new com.github.dockerjava.api.model.ExposedPort(6379))[0];
        return Integer.parseInt(binding.getHostPortSpec());
    }

    private static void awaitReady(String host, int port) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        JedisConnectionException last = null;
        do {
            try (var client = new Jedis(host, port, 500)) {
                assertThat(client.ping()).isEqualTo("PONG");
                return;
            } catch (JedisConnectionException starting) {
                last = starting;
            }
            Thread.sleep(25);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Redis did not become ready after process restart", last);
    }
}
