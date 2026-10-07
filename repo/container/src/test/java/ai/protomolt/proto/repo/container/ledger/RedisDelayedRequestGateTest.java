package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.redis.RedisBlobStore;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreConfig;
import ai.protomolt.proto.repo.blob.redis.RedisWritePolicy;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import static org.assertj.core.api.Assertions.*;

class RedisDelayedRequestGateTest {
    @Test void originalRequestCanArriveAfterCallerFailureAndConfirmedAbsence() throws Exception {
        try (var redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379)) {
            redis.start();
            String directUri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            try (var gate = new RedisDelayedRequestGate(redis.getHost(), redis.getMappedPort(6379));
                 var client = new RedisBlobStore(config(gate.uri()));
                 var direct = new RedisBlobStore(config(directUri));
                 var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                byte[] body = "delayed original payload".getBytes(StandardCharsets.UTF_8);
                var spec = new BlobStore.PutSpec("namespace", "delayed", "application/octet-stream", Map.of(), null);
                direct.put(new BlobStore.PutSpec("namespace", "neighbor", "text/plain", Map.of(), null), body);
                gate.arm(physicalKey("delayed"));
                client.put(new BlobStore.PutSpec("namespace", "unrelated", "text/plain", Map.of(), null), body);
                var write = executor.submit(() -> client.put(spec, body));
                assertThat(gate.awaitCaptured(Duration.ofSeconds(5))).isTrue();
                assertThatThrownBy(() -> write.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasRootCauseInstanceOf(java.net.SocketTimeoutException.class);
                assertThatThrownBy(() -> direct.getBounded("namespace", "delayed", null, 1024))
                        .isInstanceOf(BlobStore.BlobNotFoundException.class);
                direct.reclaim("namespace", "delayed");
                gate.deliver();
                assertThat(direct.getBounded("namespace", "delayed", null, 1024).data()).isEqualTo(body);
                assertThatThrownBy(gate::deliver).isInstanceOf(IllegalStateException.class);
                direct.reclaim("namespace", "delayed");
                assertThatThrownBy(() -> direct.getBounded("namespace", "delayed", null, 1024))
                        .isInstanceOf(BlobStore.BlobNotFoundException.class);
                assertThat(direct.getBounded("namespace", "neighbor", null, 1024).data()).isEqualTo(body);
            }
        }
    }

    private static String physicalKey(String key) {
        var encoder = java.util.Base64.getUrlEncoder().withoutPadding();
        return "protomolt:redis:v3-create-only:"
                + encoder.encodeToString("delayed-fixture".getBytes(StandardCharsets.UTF_8)) + ":"
                + encoder.encodeToString("namespace".getBytes(StandardCharsets.UTF_8)) + ":"
                + encoder.encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    private static RedisBlobStoreConfig config(String uri) {
        return new RedisBlobStoreConfig(uri, 0, 1024 * 1024, "delayed-fixture", RedisWritePolicy.CREATE_ONLY);
    }
}
