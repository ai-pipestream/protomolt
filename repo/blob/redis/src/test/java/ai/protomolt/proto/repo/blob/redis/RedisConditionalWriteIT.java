package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.*;

/** Real Redis comparison/write races; no simulated successful provider. */
@Testcontainers
class RedisConditionalWriteIT {
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    private static RedisBlobStore store(String prefix) {
        return new RedisBlobStore(new RedisBlobStoreConfig(
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), 0, 0, prefix));
    }
    private static BlobStore.PutSpec spec() {
        return new BlobStore.PutSpec("namespace", "key", "application/octet-stream", Map.of(), null);
    }

    @Test void discoveredProviderQualifiesBothConditionalCapabilitiesAgainstRedis() throws Exception {
        var capabilities = java.util.Set.of(
                ai.protomolt.proto.repo.blob.spi.BlobCapability.ATOMIC_CONDITIONAL_WRITE,
                ai.protomolt.proto.repo.blob.spi.BlobCapability.AUTHORITATIVE_CONDITIONAL_READ);
        try (var opened = ai.protomolt.proto.repo.blob.spi.BlobStores.discover().open("redis", Map.of(
                "uri", "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                "ttl-seconds", "0", "max-object-bytes", "1024", "key-prefix", UUID.randomUUID().toString(), "write-policy", "replace"), capabilities)) {
            var result = opened.store().conditionalPut(spec(), new byte[]{4}, BlobStore.WriteCondition.absent());
            var read = opened.store().getForUpdate("namespace", "key");
            assertThat(read.data()).containsExactly(4);
            assertThat(read.eTag()).isEqualTo(result.eTag());
        }
    }

    @Test void absentRetryConflictsAndMatchingRetryCannotOverwriteNewerBytes() {
        try (var store = store(UUID.randomUUID().toString())) {
            var first = store.conditionalPut(spec(), new byte[]{1}, BlobStore.WriteCondition.absent());
            // A caller that did not receive this acknowledgment must reconcile; retry is not success.
            assertThatThrownBy(() -> store.conditionalPut(spec(), new byte[]{1}, BlobStore.WriteCondition.absent()))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            var snapshot = store.getForUpdate("namespace", "key");
            assertThat(snapshot.data()).containsExactly(1);
            assertThat(snapshot.eTag()).isEqualTo(first.eTag());
            store.conditionalPut(spec(), new byte[]{2}, BlobStore.WriteCondition.matching(snapshot.eTag()));
            assertThatThrownBy(() -> store.conditionalPut(spec(), new byte[]{3}, BlobStore.WriteCondition.matching(first.eTag())))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            assertThat(store.getForUpdate("namespace", "key").data()).containsExactly(2);
        }
    }

    @Test void simultaneousCreatesAndReplacementsHaveOneWinner() throws Exception {
        String prefix = UUID.randomUUID().toString();
        try (var first = store(prefix); var second = store(prefix);
                var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int round = 0; round < 2; round++) {
                var condition = round == 0 ? BlobStore.WriteCondition.absent()
                        : BlobStore.WriteCondition.matching(first.getForUpdate("namespace", "key").eTag());
                var ready = new CountDownLatch(2);
                var go = new CountDownLatch(1);
                var a = workers.submit(() -> compete(first, condition, new byte[]{11}, ready, go));
                var b = workers.submit(() -> compete(second, condition, new byte[]{22}, ready, go));
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                boolean wonA = a.get(10, TimeUnit.SECONDS), wonB = b.get(10, TimeUnit.SECONDS);
                assertThat(wonA ^ wonB).isTrue();
                assertThat(first.getForUpdate("namespace", "key").data()).containsExactly(wonA ? 11 : 22);
                // Ensure the next round's two candidate payloads differ from the old value.
                if (round == 0) first.put(spec(), new byte[]{33});
            }
        }
    }

    private static boolean compete(RedisBlobStore store, BlobStore.WriteCondition condition, byte[] body,
            CountDownLatch ready, CountDownLatch go) throws Exception {
        ready.countDown();
        if (!go.await(5, TimeUnit.SECONDS)) throw new AssertionError("Race start timed out");
        try { store.conditionalPut(spec(), body, condition); return true; }
        catch (BlobStore.BlobConflictException expected) { return false; }
    }

    @Test void conditionalBoundaryUsesExistingSpiLimitAndDoesNotMutateOnRefusal() {
        try (var store = store(UUID.randomUUID().toString())) {
            byte[] accepted = new byte[BlobStore.MAX_CONDITIONAL_BYTES];
            store.conditionalPut(spec(), accepted, BlobStore.WriteCondition.absent());
            assertThat(store.getForUpdate("namespace", "key").data()).hasSize(accepted.length);
            assertThatThrownBy(() -> store.conditionalPut(spec(), new byte[accepted.length + 1], BlobStore.WriteCondition.absent()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(store.getForUpdate("namespace", "key").data()).containsExactly(accepted);
            store.put(spec(), new byte[accepted.length + 1]);
            assertThatThrownBy(() -> store.getForUpdate("namespace", "key"))
                    .isInstanceOf(BlobStore.BlobReadLimitException.class);
        }
    }

    @Test void matchingMissingObjectConflictsWithoutCreatingIt() {
        try (var store = store(UUID.randomUUID().toString())) {
            assertThatThrownBy(() -> store.conditionalPut(spec(), new byte[]{1}, BlobStore.WriteCondition.matching("\"missing\"")))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            assertThatThrownBy(() -> store.get("namespace", "key")).isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
    }

    @Test void malformedStoredObjectsAreNotAbsenceOrReplaceableConflicts() {
        String prefix = UUID.randomUUID().toString();
        String key = new RedisObjectKeys(prefix).object("namespace", "key");
        try (var store = store(prefix);
                var admin = new redis.clients.jedis.Jedis(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            for (String field : new String[]{"data", "content_type", "etag", "last_modified_ms", "attributes"}) {
                store.put(spec(), new byte[]{7});
                admin.hdel(key, field);
                var before = admin.hgetAll(key);
                assertDataLoss(() -> store.conditionalPut(spec(), new byte[]{8}, BlobStore.WriteCondition.absent()));
                assertThat(admin.hgetAll(key)).isEqualTo(before);
                assertDataLoss(() -> store.getForUpdate("namespace", "key"));
            }
            store.put(spec(), new byte[]{7});
            admin.hset(key, "etag", "W/\"weak\"");
            assertDataLoss(() -> store.conditionalPut(spec(), new byte[]{8}, BlobStore.WriteCondition.matching("\"other\"")));
            assertDataLoss(() -> store.getForUpdate("namespace", "key"));
            admin.del(key); admin.set(key, "wrong-kind");
            assertDataLoss(() -> store.conditionalPut(spec(), new byte[]{8}, BlobStore.WriteCondition.absent()));
            assertDataLoss(() -> store.getForUpdate("namespace", "key"));
            assertThat(admin.get(key)).isEqualTo("wrong-kind");
        }
    }

    @Test void conflictPreservesTtlAndMetadataWhileSuccessfulReplacementAppliesConfiguredTtl() {
        String prefix = UUID.randomUUID().toString();
        String key = new RedisObjectKeys(prefix).object("namespace", "key");
        try (var store = new RedisBlobStore(new RedisBlobStoreConfig(
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), 120, 1024, prefix));
                var admin = new redis.clients.jedis.Jedis(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            var first = store.conditionalPut(spec(), new byte[]{1}, BlobStore.WriteCondition.absent());
            admin.pexpire(key, 30_000);
            var before = admin.hgetAll(key);
            assertThatThrownBy(() -> store.conditionalPut(spec(), new byte[]{2}, BlobStore.WriteCondition.absent()))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
            assertThat(admin.hgetAll(key)).isEqualTo(before);
            assertThat(admin.pttl(key)).isBetween(1L, 30_000L);
            store.conditionalPut(spec(), new byte[]{2}, BlobStore.WriteCondition.matching(first.eTag()));
            assertThat(admin.pttl(key)).isBetween(100_000L, 120_000L);
        }
    }

    @Test void lostCallerAcknowledgmentRequiresAuthoritativeReconciliation() {
        String prefix = UUID.randomUUID().toString();
        try (var store = store(prefix)) {
            // Controlled failure after the actual provider returned, before caller acknowledgment.
            assertThatThrownBy(() -> {
                store.conditionalPut(spec(), new byte[]{9}, BlobStore.WriteCondition.absent());
                throw new java.io.IOException("lost caller acknowledgment");
            }).isInstanceOf(java.io.IOException.class);
            assertThatThrownBy(() -> store.conditionalPut(spec(), new byte[]{9}, BlobStore.WriteCondition.absent()))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
        }
        try (var fresh = store(prefix)) {
            assertThat(fresh.getForUpdate("namespace", "key").data()).containsExactly(9);
        }
    }

    @Test void contentEtagIsNotAnEpochAndDoesNotDetectAba() {
        try (var store = store(UUID.randomUUID().toString())) {
            var first = store.conditionalPut(spec(), new byte[]{1}, BlobStore.WriteCondition.absent());
            store.put(spec(), new byte[]{2});
            store.put(spec(), new byte[]{1});
            assertThat(store.getForUpdate("namespace", "key").eTag()).isEqualTo(first.eTag());
            store.conditionalPut(spec(), new byte[]{3}, BlobStore.WriteCondition.matching(first.eTag()));
            assertThat(store.get("namespace", "key").data()).containsExactly(3);
        }
    }

    private static void assertDataLoss(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class,
                failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.blob.spi.BlobStoreException.Code.DATA_LOSS));
    }
}
