package ai.pipestream.proto.repo.container.blob;

import ai.pipestream.proto.repo.container.codec.DocumentPartCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RedisBlobStore} against a real Redis (testcontainers
 * {@code redis:7-alpine} — no special module needed, a plain container with a
 * mapped port). Exercises the port contracts the in-memory fake cannot:
 * the client-side verified write, the two-entries-per-object key mapping,
 * the 1000-key pipelined {@code deleteAll}, SCAN listing across bucket
 * namespaces, and real TTL expiry. Skips cleanly without docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisBlobStoreIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    static RedisBlobStore store;

    @BeforeAll
    static void setUp() {
        store = new RedisBlobStore(new RedisBlobStoreConfig(
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                0, 0L, "it:"));
    }

    @AfterAll
    static void tearDown() {
        store.close();
    }

    @Test
    void putThenGetRoundTripsBytesAndContentType() {
        byte[] body = "hello redis".getBytes(StandardCharsets.UTF_8);
        BlobStore.PutResult put = store.put(new BlobStore.PutSpec("b1", "it/put-get",
                "text/plain", null, null), body);
        assertThat(put.eTag()).isNotBlank();
        assertThat(put.versionId()).isNull(); // Redis does not version

        BlobStore.GetResult got = store.get("b1", "it/put-get");
        assertThat(got.data()).isEqualTo(body);
        assertThat(got.contentType()).isEqualTo("text/plain");
        assertThat(got.versionId()).isNull();
    }

    @Test
    void verifiedPutWithMatchingChecksumSucceedsAndMismatchIsRejected() {
        byte[] body = "verified".getBytes(StandardCharsets.UTF_8);
        String sha = DocumentPartCodec.sha256Hex(body);
        store.put(new BlobStore.PutSpec("b1", "it/verified",
                "application/octet-stream", null, sha), body);
        assertThat(store.get("b1", "it/verified").data()).isEqualTo(body);

        // Client-side rejection (Redis has no server-side trailer): the
        // declared digest does not match the body, so nothing lands.
        assertThatThrownBy(() -> store.put(new BlobStore.PutSpec("b1", "it/bad-digest",
                "application/octet-stream", null, "0".repeat(64)), body))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("verified write rejected");
        assertThatThrownBy(() -> store.get("b1", "it/bad-digest"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test
    void copyDuplicatesTheObjectWithinRedis() {
        byte[] body = "copy me".getBytes(StandardCharsets.UTF_8);
        store.put(new BlobStore.PutSpec("b1", "it/copy-src", "text/plain", null, null), body);

        store.copy("b1", "it/copy-src", "b2", "it/copy-dst");

        BlobStore.GetResult got = store.get("b2", "it/copy-dst");
        assertThat(got.data()).isEqualTo(body);
        assertThat(got.contentType()).isEqualTo("text/plain");

        assertThatThrownBy(() -> store.copy("b1", "it/definitely-absent", "b2", "it/copy-dst-2"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class)
                .hasMessageContaining("it/definitely-absent");
    }

    @Test
    void deleteReportsWhetherTheObjectExisted() {
        store.put(new BlobStore.PutSpec("b1", "it/delete-me", "text/plain", null, null),
                "gone soon".getBytes(StandardCharsets.UTF_8));

        assertThat(store.delete("b1", "it/delete-me")).isTrue();
        assertThat(store.delete("b1", "it/delete-me")).isFalse();
        assertThatThrownBy(() -> store.headObject("b1", "it/delete-me"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test
    void deleteAllChunksBeyond1000KeysAndToleratesAbsentKeys() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 2100; i++) {
            keys.add("it/batch/%04d".formatted(i));
        }
        for (String key : keys) {
            store.put(new BlobStore.PutSpec("b1", key, "text/plain", null, null),
                    key.getBytes(StandardCharsets.UTF_8));
        }
        store.put(new BlobStore.PutSpec("b1", "it/batch/keep", "text/plain", null, null),
                "keep".getBytes(StandardCharsets.UTF_8));

        List<String> toDelete = new ArrayList<>(keys);
        toDelete.add("it/batch/never-existed"); // missing keys are success
        BlobStore.BatchDeleteResult result = store.deleteAll("b1", toDelete);

        assertThat(result.allSucceeded()).isTrue();
        assertThat(store.list("b1", "it/batch/"))
                .extracting(BlobStore.ListedObject::key)
                .containsExactly("it/batch/keep");
    }

    @Test
    void listStaysWithinTheBucketNamespaceAcrossTwoBuckets() {
        store.put(new BlobStore.PutSpec("ba", "it/list/a", "text/plain", null, null), new byte[1]);
        store.put(new BlobStore.PutSpec("ba", "it/list/b", "text/plain", null, null), new byte[2]);
        store.put(new BlobStore.PutSpec("bb", "it/list/a", "text/plain", null, null), new byte[3]);

        assertThat(store.list("ba", "it/list/"))
                .extracting(BlobStore.ListedObject::key)
                .containsExactlyInAnyOrder("it/list/a", "it/list/b");
        assertThat(store.list("bb", "it/list/"))
                .extracting(BlobStore.ListedObject::key)
                .containsExactly("it/list/a");
        // The $meta hashes never show up as objects.
        assertThat(store.list("ba", ""))
                .extracting(BlobStore.ListedObject::key)
                .noneMatch(k -> k.endsWith("$meta"));
    }

    @Test
    void headObjectAndGetOfAbsentKeyRaiseNotFound() {
        assertThatThrownBy(() -> store.headObject("b1", "it/no-such-object"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class)
                .hasMessageContaining("it/no-such-object");
        assertThatThrownBy(() -> store.get("b1", "it/no-such-get"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test
    void ttlExpiresTheObject() throws Exception {
        RedisBlobStoreConfig shortTtl = new RedisBlobStoreConfig(
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                1, 0L, "it:");
        try (RedisBlobStore expiring = new RedisBlobStore(shortTtl)) {
            expiring.put(new BlobStore.PutSpec("b1", "it/ttl", "text/plain", null, null),
                    "mayfly".getBytes(StandardCharsets.UTF_8));
            assertThat(expiring.get("b1", "it/ttl").data()).isNotEmpty();

            Thread.sleep(1500); // 1s TTL + slack, well under the 3s budget

            assertThatThrownBy(() -> expiring.get("b1", "it/ttl"))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
            assertThatThrownBy(() -> expiring.headObject("b1", "it/ttl"))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
    }
}
