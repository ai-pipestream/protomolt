package ai.protomolt.proto.repo.blob.cache;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cache decorator composed with explicitly selected REAL providers through
 * packaged ServiceLoader discovery — no fake store anywhere:
 *
 * <ul>
 *   <li>LocalStack S3 backing + Redis front cache: write-through, read-through
 *       population, eviction on copy/delete, and a poisoned cache entry proving
 *       which path serves plain reads.</li>
 *   <li>Redis backing + Redis front cache (disjoint prefixes): the
 *       authoritative conditional read bypasses the cache, conditional writes
 *       land on the backing store and evict, and concurrent conditional writes
 *       through the decorator keep exactly one winner.</li>
 * </ul>
 */
@Testcontainers
class CacheProviderIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final String BUCKET = "cache-provider-it";

    static S3Client s3Admin;
    private OpenedBlobStore s3Handle;
    private OpenedBlobStore redisHandle;

    @BeforeAll
    static void createBucket() {
        s3Admin = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .region(Region.of(LOCALSTACK.getRegion()))
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true)
                .build();
        s3Admin.createBucket(b -> b.bucket(BUCKET));
    }

    @AfterEach
    void closeHandles() throws Exception {
        // Closing the handles releases the owned client/pool even on assertion failure.
        var s3 = s3Handle;
        var redis = redisHandle;
        s3Handle = null;
        redisHandle = null;
        try {
            if (s3 != null) s3.close();
        } finally {
            if (redis != null) redis.close();
        }
    }

    private static String redisUri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    private static Map<String, String> redisOptions(String prefix) {
        return Map.of("uri", redisUri(), "ttl-seconds", "0", "max-object-bytes", "0", "key-prefix", prefix);
    }

    private static Map<String, String> s3Options() {
        return Map.of(
                "endpoint", LOCALSTACK.getEndpoint().toString(),
                "region", LOCALSTACK.getRegion(),
                "access-key", LOCALSTACK.getAccessKey(),
                "secret-key", LOCALSTACK.getSecretKey(),
                "path-style", "true", "conditional-writes", "false");
    }

    private static BlobStore.PutSpec spec(String bucket, String key) {
        return new BlobStore.PutSpec(bucket, key, "application/octet-stream", Map.of(), null);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void s3BackingWithRedisFrontCacheWritesAndReadsThrough() throws Exception {
        var providers = BlobStores.discover();
        assertThat(providers.providerIds()).containsExactlyInAnyOrder("s3", "redis");
        s3Handle = providers.open("s3", s3Options());
        redisHandle = providers.open("redis", redisOptions("cache-it-" + UUID.randomUUID()));
        var backing = s3Handle.store();
        var cache = redisHandle.store();
        var decorator = new CachingBlobStore(backing, cache, 60, 1024 * 1024);

        // Write-through: the backing store lands first, the cache mirrors.
        decorator.put(spec(BUCKET, "entry"), bytes("authoritative"));
        assertThat(backing.get(BUCKET, "entry").data()).isEqualTo(bytes("authoritative"));
        assertThat(cache.get(BUCKET, "entry").data()).isEqualTo(bytes("authoritative"));
        assertThat(s3Admin.getObject(b -> b.bucket(BUCKET).key("entry")).readAllBytes())
                .isEqualTo(bytes("authoritative"));

        // A poisoned cache entry proves plain reads are genuinely served from
        // the cache; the backing store still holds the authoritative bytes.
        cache.put(spec(BUCKET, "entry"), bytes("poisoned"));
        assertThat(decorator.get(BUCKET, "entry").data()).isEqualTo(bytes("poisoned"));
        assertThat(backing.get(BUCKET, "entry").data()).isEqualTo(bytes("authoritative"));

        // Delete evicts the poisoned entry and removes the backing object.
        decorator.delete(BUCKET, "entry");
        assertThatThrownBy(() -> backing.get(BUCKET, "entry"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThatThrownBy(() -> cache.get(BUCKET, "entry"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);

        // Copy lands on the backing store and evicts any cached destination.
        decorator.put(spec(BUCKET, "source"), bytes("copied"));
        cache.put(spec(BUCKET, "destination"), bytes("stale-destination"));
        decorator.copy(BUCKET, "source", BUCKET, "destination");
        assertThat(backing.get(BUCKET, "destination").data()).isEqualTo(bytes("copied"));
        assertThat(decorator.get(BUCKET, "destination").data()).isEqualTo(bytes("copied"));

        decorator.close();
    }

    @Test
    void conditionalOperationsBypassAndEvictTheCache() throws Exception {
        var providers = BlobStores.discover();
        redisHandle = providers.open("redis", redisOptions("backing-it-" + UUID.randomUUID()));
        var cacheHandle = providers.open("redis", redisOptions("front-it-" + UUID.randomUUID()));
        try {
            var backing = redisHandle.store();
            var cache = cacheHandle.store();
            var decorator = new CachingBlobStore(backing, cache, 60, 1024 * 1024);

            decorator.put(spec("namespace", "key"), bytes("v1"));
            cache.put(spec("namespace", "key"), bytes("poisoned"));

            // The authoritative conditional read always bypasses the cache.
            var snapshot = decorator.getForUpdate("namespace", "key");
            assertThat(snapshot.data()).isEqualTo(bytes("v1"));
            assertThat(decorator.get("namespace", "key").data()).isEqualTo(bytes("poisoned"));

            // A conditional write lands on the backing store and evicts the
            // poisoned entry; the next plain read re-populates from backing.
            decorator.conditionalPut(spec("namespace", "key"), bytes("v2"),
                    BlobStore.WriteCondition.matching(snapshot.eTag()));
            assertThat(backing.getForUpdate("namespace", "key").data()).isEqualTo(bytes("v2"));
            assertThat(decorator.get("namespace", "key").data()).isEqualTo(bytes("v2"));

            decorator.close();
        } finally {
            cacheHandle.close();
        }
    }

    @Test
    void concurrentConditionalWritesThroughTheDecoratorHaveOneWinner() throws Exception {
        var providers = BlobStores.discover();
        redisHandle = providers.open("redis", redisOptions("race-backing-" + UUID.randomUUID()),
                java.util.Set.of(BlobCapability.ATOMIC_CONDITIONAL_WRITE,
                        BlobCapability.AUTHORITATIVE_CONDITIONAL_READ));
        var cacheHandle = providers.open("redis", redisOptions("race-front-" + UUID.randomUUID()));
        try {
            var decorator = new CachingBlobStore(redisHandle.store(), cacheHandle.store(), 60, 1024 * 1024);
            var spec = spec("namespace", "key");
            decorator.conditionalPut(spec, bytes("base"), BlobStore.WriteCondition.absent());
            var snapshot = decorator.getForUpdate("namespace", "key");

            int contenders = 4;
            var ready = new CountDownLatch(contenders);
            var go = new CountDownLatch(1);
            var successes = new ConcurrentLinkedQueue<String>();
            var conflicts = new ConcurrentLinkedQueue<String>();
            try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
                for (int index = 0; index < contenders; index++) {
                    String candidate = "candidate-" + index;
                    tasks.add(workers.submit((java.util.concurrent.Callable<Void>) () -> {
                        ready.countDown();
                        if (!go.await(10, TimeUnit.SECONDS)) throw new AssertionError("race start timed out");
                        try {
                            decorator.conditionalPut(spec, bytes(candidate),
                                    BlobStore.WriteCondition.matching(snapshot.eTag()));
                            successes.add(candidate);
                        } catch (BlobStore.BlobConflictException expected) {
                            conflicts.add(candidate);
                        }
                        return null;
                    }));
                }
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                for (var task : tasks) task.get(30, TimeUnit.SECONDS);
            }
            assertThat(successes).hasSize(1);
            assertThat(conflicts).hasSize(contenders - 1);
            // The winner's bytes read back from the authoritative path AND from
            // the re-populated cache; losers never became successful writes.
            assertThat(decorator.getForUpdate("namespace", "key").data())
                    .isEqualTo(bytes(successes.peek()));
            assertThat(decorator.get("namespace", "key").data())
                    .isEqualTo(bytes(successes.peek()));

            decorator.close();
        } finally {
            cacheHandle.close();
        }
    }
}
