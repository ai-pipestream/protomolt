package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import redis.clients.jedis.Jedis;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class RedisStreamingIT {
    @Container static final GenericContainer<?> REDIS = RedisTestServers.container();

    @Test void streamsWholeObjectsAndRangesAndReportsLengthWithoutMaterializingGet() throws Exception {
        var uri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        try (var store = new RedisBlobStore(new RedisBlobStoreConfig(uri, 0, 0, UUID.randomUUID().toString()))) {
            var spec = new BlobStore.PutSpec("streaming", "object", "application/octet-stream", Map.of(), null);
            byte[] body = {0, 1, 2, 3, 4, 5};
            store.put(spec, new ByteArrayInputStream(body), body.length);
            assertThat(store.objectLength("streaming", "object")).isEqualTo(body.length);
            try (var input = store.openStream("streaming", "object", 0, -1)) {
                assertThat(input.readAllBytes()).containsExactly(body);
            }
            try (var input = store.openStream("streaming", "object", 2, 3)) {
                assertThat(input.readAllBytes()).containsExactly(2, 3, 4);
            }
        }
    }

    @Test void failedInputNeverChangesExistingObjectOrCreatesANewOne() {
        try (var store = store(0, RedisWritePolicy.REPLACE)) {
            var spec = spec("atomic", null);
            store.put(spec, new byte[]{9});
            for (int length : new int[]{1, 3}) {
                assertThatThrownBy(() ->
                        store.put(spec, new ByteArrayInputStream(new byte[]{1, 2}), length))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThat(store.get("streaming", "atomic", null).data()).containsExactly(9);
            }
            // Cross the socket/output window before failing: the server has already
            // received part of this command, but must not execute its Lua script.
            assertThatThrownBy(() -> store.put(spec, new ByteArrayInputStream(new byte[96 * 1024]), 128 * 1024))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(store.get("streaming", "atomic", null).data()).containsExactly(9);
            var badHash = spec("atomic", "00".repeat(32));
            assertThatThrownBy(() ->
                    store.put(badHash, new ByteArrayInputStream(new byte[96 * 1024]), 96 * 1024))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
            assertThat(store.get("streaming", "atomic", null).data()).containsExactly(9);
            var broken = new InputStream() {
                @Override public int read() throws IOException { throw new IOException("broken source"); }
            };
            assertThatThrownBy(() -> store.put(spec("absent", null), broken, 1))
                    .isInstanceOf(UncheckedIOException.class).hasRootCauseMessage("broken source");
            assertThatThrownBy(() -> store.objectLength("streaming", "absent"))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
    }

    @Test void emptyRangesAndCancellationPreserveStreamContract() throws Exception {
        try (var store = store(0, RedisWritePolicy.REPLACE)) {
            store.put(spec("ranges", null), new byte[]{1, 2, 3});
            for (long offset : new long[]{0, 3, Long.MAX_VALUE}) {
                try (var stream = store.openStream("streaming", "ranges", offset, 0)) {
                    assertThat(stream.read(new byte[0])).isZero();
                    assertThat(stream.read()).isEqualTo(-1);
                    assertThat(stream.read()).isEqualTo(-1);
                }
            }
            try (var stream = store.openStream("streaming", "ranges", 1, Long.MAX_VALUE)) {
                assertThat(stream.readAllBytes()).containsExactly(2, 3);
            }
            var stream = store.openStream("streaming", "ranges", 0, -1);
            assertThat(stream.read()).isEqualTo(1);
            stream.close();
            stream.close();
            assertThatThrownBy(stream::read).isInstanceOf(IOException.class);
            store.put(spec("empty", null), new ByteArrayInputStream(new byte[0]), 0);
            assertThat(store.objectLength("streaming", "empty")).isZero();
            try (var empty = store.openStream("streaming", "empty", 0, -1)) {
                assertThat(empty.read()).isEqualTo(-1);
            }
        }
    }

    @Test void streamedCreateOnlyPreservesAtomicConflictAndMetadata() {
        try (var store = store(0, RedisWritePolicy.CREATE_ONLY)) {
            var spec = new BlobStore.PutSpec("streaming", "created", "text/plain", Map.of("tag", "exact"), null);
            var written = store.put(spec, new ByteArrayInputStream(new byte[]{7}), 1);
            assertThatThrownBy(() ->
                    store.put(spec, new ByteArrayInputStream(new byte[]{8}), 1))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            var stored = store.get("streaming", "created", null);
            assertThat(stored.data()).containsExactly(7);
            assertThat(stored.eTag()).isEqualTo(written.eTag());
            assertThat(stored.contentType()).isEqualTo("text/plain");
        }
    }

    @Test void streamsObjectLargerThanTheEntireHeap() throws Exception {
        var log = Files.createTempFile("redis-transfer-heap-", ".log");
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx48m", "-cp", System.getProperty("protomolt.redis.transfer.classpath"),
                RedisTransferHeapProbe.class.getName(), uri(), UUID.randomUUID().toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertThat(process.waitFor(90, TimeUnit.SECONDS)).as("bounded heap transfer deadline").isTrue();
            assertThat(process.exitValue()).as(Files.readString(log)).isZero();
            assertThat(Files.readString(log)).contains("REDIS_BOUNDED_HEAP_TRANSFER_OK");
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
            Files.deleteIfExists(log);
        }
    }

    @Test void streamedTransfersHonorCredentialsDatabaseAndTtl() throws Exception {
        String user = "stream-" + UUID.randomUUID();
        String prefix = UUID.randomUUID().toString();
        try (var admin = new Jedis(URI.create(uri()))) {
            admin.aclSetUser(user, "on", ">transfer-test-password", "~*", "+@all");
            String authenticated = "redis://" + user + ":transfer-test-password@" + REDIS.getHost()
                    + ":" + REDIS.getMappedPort(6379) + "/2";
            try (var store = new RedisBlobStore(new RedisBlobStoreConfig(authenticated, 60, 0, prefix));
                 var database = new Jedis(URI.create(authenticated))) {
                store.put(spec("auth", null), new ByteArrayInputStream(new byte[]{3, 4}), 2);
                String key = new RedisObjectKeys(prefix, RedisWritePolicy.REPLACE).object("streaming", "auth");
                assertThat(admin.exists(key)).isFalse();
                assertThat(database.ttl(key)).isBetween(1L, 60L);
                try (var stream = store.openStream("streaming", "auth", 0, -1)) {
                    assertThat(stream.readAllBytes()).containsExactly(3, 4);
                }
                assertThat(store.objectLength("streaming", "auth")).isEqualTo(2);
                database.del(key);
            } finally { admin.aclDelUser(user); }
        }
    }

    @Test void streamPinsOneSnapshotAndStoreCloseAbandonsTransfers() throws Exception {
        var store = store(0, RedisWritePolicy.REPLACE);
        try {
            byte[] original = new byte[128 * 1024];
            Arrays.fill(original, (byte) 7);
            store.put(spec("snapshot", null), original);
            try (var stream = store.openStream("streaming", "snapshot", 0, -1)) {
                assertThat(stream.read()).isEqualTo(7);
                store.put(spec("snapshot", null), new byte[]{9});
                assertThat(stream.readAllBytes()).isEqualTo(Arrays.copyOfRange(original, 1, original.length));
            }
            store.put(spec("snapshot", null), original);
            try (var active = store.openStream("streaming", "snapshot", 0, -1)) {
                assertThat(active.read()).isEqualTo(7);
                store.close();
                assertThatThrownBy(active::readAllBytes).isInstanceOf(IOException.class);
            }
            assertThatThrownBy(() -> store.openStream("streaming", "snapshot", 0, -1))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.put(spec("after-close", null),
                    new ByteArrayInputStream(new byte[]{1}), 1)).isInstanceOf(IllegalStateException.class);
        } finally { store.close(); }
    }

    @Test void missingAndMalformedHashesAreNotSuccessfulStreams() {
        String prefix = UUID.randomUUID().toString();
        try (var store = new RedisBlobStore(new RedisBlobStoreConfig(uri(), 0, 0, prefix));
             var admin = new Jedis(URI.create(uri()))) {
            assertThatThrownBy(() -> store.openStream("streaming", "absent", 0, -1))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
            String key = new RedisObjectKeys(prefix, RedisWritePolicy.REPLACE).object("streaming", "malformed");
            admin.hset(key, "data", "not a complete object");
            assertThatThrownBy(() -> store.openStream("streaming", "malformed", 0, -1))
                    .isInstanceOf(BlobStoreException.class)
                    .hasMessageContaining("required metadata");
            admin.del(key);
        }
    }

    private static String uri() { return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379); }
    private static RedisBlobStore store(int ttl, RedisWritePolicy policy) {
        return new RedisBlobStore(new RedisBlobStoreConfig(uri(), ttl, 0, UUID.randomUUID().toString(), policy));
    }
    private static BlobStore.PutSpec spec(String key, String sha) {
        return new BlobStore.PutSpec("streaming", key, "application/octet-stream", Map.of(), sha);
    }
}
