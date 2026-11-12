package ai.protomolt.proto.repo.blob.cache;

import ai.protomolt.proto.repo.blob.redis.RedisTestServers;
import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
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
    static final GenericContainer<?> REDIS = RedisTestServers.container();

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
        return Map.of("uri", redisUri(), "ttl-seconds", "0", "max-object-bytes", "0", "key-prefix", prefix, "write-policy", "replace");
    }

    private static Map<String, String> s3Options() {
        return Map.ofEntries(
                Map.entry("endpoint", LOCALSTACK.getEndpoint().toString()),
                Map.entry("region", LOCALSTACK.getRegion()),
                Map.entry("access-key", LOCALSTACK.getAccessKey()),
                Map.entry("secret-key", LOCALSTACK.getSecretKey()),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000"));
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
        assertThat(providers.providerIds()).containsExactlyInAnyOrder("s3", "valkey", "redis");
        s3Handle = providers.open("s3", s3Options());
        redisHandle = providers.open("valkey", redisOptions("cache-it-" + UUID.randomUUID()));
        var backing = s3Handle.store();
        var cache = redisHandle.store();
        var decorator = new CachingBlobStore(backing, cache, 60, 1024 * 1024);

        // Write-through: the backing store lands first, the cache mirrors.
        decorator.put(spec(BUCKET, "entry"), bytes("authoritative"));
        assertThat(backing.get(BUCKET, "entry").data()).isEqualTo(bytes("authoritative"));
        assertThat(cache.get(BUCKET, "entry").data()).isEqualTo(bytes("authoritative"));
        assertThat(s3Admin.getObject(b -> b.bucket(BUCKET).key("entry")).readAllBytes())
                .isEqualTo(bytes("authoritative"));

        // A cache entry without the current authoritative identity must not be
        // served, even when its own checksum matches its poisoned bytes.
        cache.put(spec(BUCKET, "entry"), bytes("poisoned"));
        assertThat(decorator.get(BUCKET, "entry").data()).isEqualTo(bytes("authoritative"));
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
    void verifiedCacheMissHitAndCancellationFitSmallerHeapThanObject() throws Exception {
        var directory = java.nio.file.Files.createTempDirectory("cache-heap-probe-");
        var log = java.nio.file.Files.createTempFile("cache-heap-probe-", ".log");
        var process = new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx96m", "-Djava.io.tmpdir=" + directory, "-cp", System.getProperty("protomolt.cache.heap.classpath"),
                CacheHeapProbe.class.getName(), LOCALSTACK.getEndpoint().toString(), LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey(), redisUri(), UUID.randomUUID().toString(), BUCKET)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertThat(process.waitFor(120, TimeUnit.SECONDS)).as("cache heap probe deadline").isTrue();
            assertThat(process.exitValue()).as(java.nio.file.Files.readString(log)).isZero();
            assertThat(java.nio.file.Files.readString(log)).contains("CACHE_BOUNDED_HEAP_OK");
        } finally {
            if (process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
            try (var files = java.nio.file.Files.list(directory)) {
                for (var file : files.toList()) java.nio.file.Files.deleteIfExists(file);
            }
            java.nio.file.Files.deleteIfExists(directory);
            java.nio.file.Files.deleteIfExists(log);
        }
    }

    @Test
    void streamedRewriteIsVisibleAcrossCacheInstances() throws Exception {
        var providers = BlobStores.discover();
        s3Handle = providers.open("s3", s3Options());
        redisHandle = providers.open("valkey", redisOptions("rewrite-" + UUID.randomUUID()));
        var first = new CachingBlobStore(s3Handle.store(), redisHandle.store(), 60, 1024 * 1024);
        var second = new CachingBlobStore(s3Handle.store(), redisHandle.store(), 60, 1024 * 1024);
        String key = "rewrite-" + UUID.randomUUID();
        first.put(spec(BUCKET, key), bytes("old"));
        assertThat(second.get(BUCKET, key).data()).isEqualTo(bytes("old"));
        first.put(spec(BUCKET, key), new ByteArrayInputStream(bytes("new")), 3);
        assertThat(second.get(BUCKET, key).data()).isEqualTo(bytes("new"));
        try (var stream = second.openStream(BUCKET, key, 0, -1)) {
            assertThat(stream.readAllBytes()).isEqualTo(bytes("new"));
        }
        // A writer outside both decorators cannot evict their cache. Equal size
        // must not hide a changed authoritative ETag.
        s3Handle.store().put(spec(BUCKET, key), bytes("alt"));
        assertThat(second.get(BUCKET, key).data()).isEqualTo(bytes("alt"));
    }

    @Test
    void delayedOldReadCannotPoisonRewrittenKey() throws Exception {
        var providers = BlobStores.discover();
        s3Handle = providers.open("s3", s3Options());
        redisHandle = providers.open("valkey", redisOptions("late-fill-" + UUID.randomUUID()));
        var first = new CachingBlobStore(s3Handle.store(), redisHandle.store(), 60, 1024 * 1024);
        var second = new CachingBlobStore(s3Handle.store(), redisHandle.store(), 60, 1024 * 1024);
        String key = "late-fill-" + UUID.randomUUID();
        s3Handle.store().put(spec(BUCKET, key), bytes("old"));
        try (var oldSnapshot = first.openStream(BUCKET, key, 0, -1)) {
            // Opening the S3 response pins the old bytes; delay its cache fill
            // until after a successful write through another decorator.
            second.put(spec(BUCKET, key), new ByteArrayInputStream(bytes("new")), 3);
            assertThat(oldSnapshot.readAllBytes()).isEqualTo(bytes("old"));
        }
        assertThat(s3Handle.store().get(BUCKET, key).data()).isEqualTo(bytes("new"));
        assertThat(second.get(BUCKET, key).data()).isEqualTo(bytes("new"));
    }

    @Test
    void conditionalOperationsBypassAndEvictTheCache() throws Exception {
        var providers = BlobStores.discover();
        redisHandle = providers.open("valkey", redisOptions("backing-it-" + UUID.randomUUID()));
        var cacheHandle = providers.open("valkey", redisOptions("front-it-" + UUID.randomUUID()));
        try {
            var backing = redisHandle.store();
            var cache = cacheHandle.store();
            var decorator = new CachingBlobStore(backing, cache, 60, 1024 * 1024);

            decorator.put(spec("namespace", "key"), bytes("v1"));
            cache.put(spec("namespace", "key"), bytes("poisoned"));

            // The authoritative conditional read always bypasses the cache.
            var snapshot = decorator.getForUpdate("namespace", "key");
            assertThat(snapshot.data()).isEqualTo(bytes("v1"));
            assertThat(decorator.get("namespace", "key").data()).isEqualTo(bytes("v1"));

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
        redisHandle = providers.open("valkey", redisOptions("race-backing-" + UUID.randomUUID()),
                java.util.Set.of(BlobCapability.ATOMIC_CONDITIONAL_WRITE,
                        BlobCapability.AUTHORITATIVE_CONDITIONAL_READ));
        var cacheHandle = providers.open("valkey", redisOptions("race-front-" + UUID.randomUUID()));
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

    @Test
    void namespaceIsolationAndVerifiedStreamReadThroughOnS3AndValkey() throws Exception {
        var providers = BlobStores.discover();
        s3Handle = providers.open("s3", s3Options());
        redisHandle = providers.open("valkey", redisOptions("ns-it-" + UUID.randomUUID()));
        var backing = s3Handle.store();
        var cache = redisHandle.store();

        var driveA = new CachingBlobStore(backing, cache, 60, 1024 * 1024, "s3|http://endpoint-a:4566|us-east-1|pathStyle=true");
        var driveB = new CachingBlobStore(backing, cache, 60, 1024 * 1024, "s3|http://endpoint-b:4566|us-east-1|pathStyle=true");

        byte[] payloadA = bytes("streamed-drive-a-payload");
        String shaA = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payloadA));
        backing.put(spec(BUCKET, "shared-stream-key"), new ByteArrayInputStream(payloadA), payloadA.length);

        try (var stream = driveA.openVerifiedStream(BUCKET, "shared-stream-key", payloadA.length, shaA)) {
            assertThat(stream.readAllBytes()).isEqualTo(payloadA);
        }
        assertThat(cache.get(driveA.cacheBucket(BUCKET), "shared-stream-key").data()).isEqualTo(payloadA);
        assertThatThrownBy(() -> cache.get(driveB.cacheBucket(BUCKET), "shared-stream-key"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);

        // Poison driveA's cache entry; openVerifiedStream detects the SHA-256 mismatch,
        // evicts the corrupt entry, and falls back to authoritative S3.
        cache.put(spec(driveA.cacheBucket(BUCKET), "shared-stream-key"), bytes("corrupt-stream-payload!!"));
        try (var stream = driveA.openVerifiedStream(BUCKET, "shared-stream-key", payloadA.length, shaA)) {
            assertThat(stream.readAllBytes()).isEqualTo(payloadA);
        }
        assertThat(cache.get(driveA.cacheBucket(BUCKET), "shared-stream-key").data()).isEqualTo(payloadA);
    }

    @Test
    void boundedReadsAndHitsAnswerToAuthoritativeS3OverValkey() throws Exception {
        var providers = BlobStores.discover();
        s3Handle = providers.open("s3", s3Options());
        redisHandle = providers.open("valkey", redisOptions("bounded-it-" + UUID.randomUUID()));
        var backing = s3Handle.store();
        var cache = redisHandle.store();
        var store = new CachingBlobStore(backing, cache, 60, 32, "s3|bounded-it");

        byte[] small = bytes("bounded-small-payload");
        byte[] large = bytes("bounded-payload-larger-than-the-cache-ceiling");
        backing.put(spec(BUCKET, "bounded-small"), small);
        backing.put(spec(BUCKET, "bounded-large"), large);
        cache.delete(store.cacheBucket(BUCKET), "bounded-small");

        // Miss: S3 enforces the limit, the result populates Valkey; then a hit returns the same bytes.
        assertThat(store.getBounded(BUCKET, "bounded-small", null, small.length).data()).isEqualTo(small);
        assertThat(cache.get(store.cacheBucket(BUCKET), "bounded-small").data()).isEqualTo(small);
        assertThat(store.getBounded(BUCKET, "bounded-small", null, small.length).data()).isEqualTo(small);
        assertThatThrownBy(() -> store.getBounded(BUCKET, "bounded-small", null, small.length - 1))
                .isInstanceOf(BlobStore.BlobReadLimitException.class);

        // Over the cache ceiling: never cached, and the caller's limit still holds.
        assertThat(store.getBounded(BUCKET, "bounded-large", null, 1024).data()).isEqualTo(large);
        assertThatThrownBy(() -> cache.get(store.cacheBucket(BUCKET), "bounded-large"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThatThrownBy(() -> store.getBounded(BUCKET, "bounded-large", null, large.length - 1))
                .isInstanceOf(BlobStore.BlobReadLimitException.class);

        // Gone from S3 while still in Valkey: not found, and the entry is evicted.
        backing.delete(BUCKET, "bounded-small");
        assertThatThrownBy(() -> store.getBounded(BUCKET, "bounded-small", null, small.length))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThatThrownBy(() -> cache.get(store.cacheBucket(BUCKET), "bounded-small"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }
}
