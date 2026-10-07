package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
 * Conditional-write qualification against LocalStack S3 with bucket versioning
 * ENABLED, on the pinned localstack/localstack:4.13 image:
 *
 * <ul>
 *   <li>{@code If-None-Match: *} (write-if-absent): duplicate creates conflict
 *       explicitly and concurrent creates have exactly one winner whose bytes
 *       read back.</li>
 *   <li>{@code If-Match: <etag>} (write-if-matching): a stale precondition
 *       conflicts without mutating the object, and concurrent matching writes
 *       against one snapshot have exactly one winner.</li>
 * </ul>
 *
 * LocalStack 3.8 (still used by the unconditional S3 ITs) ignores
 * {@code If-Match}, so it cannot qualify matching writes; that is why this
 * suite pins 4.13.
 *
 * Provider selection goes through packaged ServiceLoader discovery, not direct
 * construction. Refused writes never mutate the object, and the provider's
 * opaque version identity reads back exact historical bytes.
 */
@Testcontainers
class S3LocalStackConditionalIT {

    private static final String BUCKET = "conditional-localstack-it";

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.13"))
                    .withServices("s3");

    static S3Client client;
    static OpenedBlobStore handle;
    static BlobStore store;

    @BeforeAll
    static void setUp() {
        client = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .region(Region.of(LOCALSTACK.getRegion()))
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true)
                .build();
        client.createBucket(b -> b.bucket(BUCKET));
        client.putBucketVersioning(b -> b.bucket(BUCKET).versioningConfiguration(v -> v.status(
                software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
        assertThat(client.getBucketVersioning(b -> b.bucket(BUCKET)).status()).isEqualTo(
                software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED);
        handle = BlobStores.discover().open("s3", Map.of(
                "endpoint", LOCALSTACK.getEndpoint().toString(),
                "region", LOCALSTACK.getRegion(),
                "access-key", LOCALSTACK.getAccessKey(),
                "secret-key", LOCALSTACK.getSecretKey(),
                "path-style", "true", "conditional-writes", "true"));
        assertThat(handle.capabilities()).contains(
                BlobCapability.ATOMIC_CONDITIONAL_WRITE, BlobCapability.AUTHORITATIVE_CONDITIONAL_READ);
        store = handle.store();
    }

    @AfterAll
    static void tearDown() throws Exception {
        try { if (handle != null) handle.close(); }
        finally { if (client != null) client.close(); }
    }

    @Test
    void absentWritesFenceDuplicateCreates() {
        var spec = spec("fencing/object");
        assertThatThrownBy(() -> store.getForUpdate(BUCKET, spec.key()))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);

        var created = store.conditionalPut(spec, bytes("snapshot-one"), BlobStore.WriteCondition.absent());
        assertThat(created.versionId()).isNotBlank().isNotEqualTo("null");
        var snapshot = store.getForUpdate(BUCKET, spec.key());
        assertThat(snapshot.data()).isEqualTo(bytes("snapshot-one"));
        assertThat(snapshot.eTag()).isEqualTo(created.eTag());

        // A duplicate create conflicts explicitly and never replaces the
        // committed bytes, no matter how often it is retried.
        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> store.conditionalPut(spec, bytes("impostor"),
                    BlobStore.WriteCondition.absent()))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
        }
        var current = store.getForUpdate(BUCKET, spec.key());
        assertThat(current.data()).isEqualTo(bytes("snapshot-one"));
        assertThat(current.versionId()).isEqualTo(created.versionId());
    }

    @Test
    void concurrentAbsentCreatesHaveExactlyOneWinner() throws Exception {
        var spec = spec("race/object");
        race(spec, BlobStore.WriteCondition.absent(), "create");
        var created = store.getForUpdate(BUCKET, spec.key());
        assertThat(new String(created.data(), StandardCharsets.UTF_8)).startsWith("create-");
    }

    @Test
    void staleMatchingWritesConflictWithoutMutation() {
        var spec = spec("matching-stale/object");
        store.conditionalPut(spec, bytes("base"), BlobStore.WriteCondition.absent());
        var snapshot = store.getForUpdate(BUCKET, spec.key());
        var meanwhile = store.put(spec, bytes("meanwhile"));

        // The stale writer loses explicitly on every retry and never lands.
        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> store.conditionalPut(spec, bytes("stale-writer"),
                    BlobStore.WriteCondition.matching(snapshot.eTag())))
                    .isInstanceOf(BlobStore.BlobConflictException.class);
        }
        var current = store.getForUpdate(BUCKET, spec.key());
        assertThat(current.data()).isEqualTo(bytes("meanwhile"));
        assertThat(current.versionId()).isEqualTo(meanwhile.versionId());

        // A writer holding the current ETag succeeds and reads back.
        var replaced = store.conditionalPut(spec, bytes("fresh-writer"),
                BlobStore.WriteCondition.matching(current.eTag()));
        var after = store.getForUpdate(BUCKET, spec.key());
        assertThat(after.data()).isEqualTo(bytes("fresh-writer"));
        assertThat(after.versionId()).isEqualTo(replaced.versionId()).isNotEqualTo(meanwhile.versionId());
    }

    @Test
    void concurrentMatchingWritesHaveExactlyOneWinner() throws Exception {
        var spec = spec("matching-race/object");
        store.conditionalPut(spec, bytes("base"), BlobStore.WriteCondition.absent());
        var snapshot = store.getForUpdate(BUCKET, spec.key());
        race(spec, BlobStore.WriteCondition.matching(snapshot.eTag()), "replace");
        var replaced = store.getForUpdate(BUCKET, spec.key());
        assertThat(new String(replaced.data(), StandardCharsets.UTF_8)).startsWith("replace-");
        assertThat(replaced.eTag()).isNotEqualTo(snapshot.eTag());
    }

    private static void race(BlobStore.PutSpec spec, BlobStore.WriteCondition condition,
            String prefix) throws Exception {
        int contenders = 8;
        var ready = new CountDownLatch(contenders);
        var go = new CountDownLatch(1);
        var successes = new ConcurrentLinkedQueue<String>();
        var conflicts = new ConcurrentLinkedQueue<String>();
        var failures = new ConcurrentLinkedQueue<Throwable>();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int index = 0; index < contenders; index++) {
                String candidate = prefix + "-" + index;
                tasks.add(workers.submit((java.util.concurrent.Callable<Void>) () -> {
                    ready.countDown();
                    if (!go.await(10, TimeUnit.SECONDS)) throw new AssertionError("race start timed out");
                    try {
                        store.conditionalPut(spec, bytes(candidate), condition);
                        successes.add(candidate);
                    } catch (BlobStore.BlobConflictException expected) {
                        conflicts.add(candidate);
                    } catch (RuntimeException unexpected) {
                        failures.add(unexpected);
                    }
                    return null;
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            for (var task : tasks) task.get(60, TimeUnit.SECONDS);
        }
        assertThat(failures).isEmpty();
        assertThat(successes).as("exactly one conditional write may win").hasSize(1);
        assertThat(conflicts).hasSize(contenders - 1);
        assertThat(store.getForUpdate(BUCKET, spec.key()).data())
                .isEqualTo(bytes(successes.peek()));
    }

    @Test
    void conditionalBoundaryUsesExistingSpiLimitAndDoesNotMutateOnRefusal() {
        var spec = spec("boundary/object");
        byte[] accepted = new byte[BlobStore.MAX_CONDITIONAL_BYTES];
        var created = store.conditionalPut(spec, accepted, BlobStore.WriteCondition.absent());
        assertThat(store.getForUpdate(BUCKET, spec.key()).data()).hasSize(accepted.length);

        assertThatThrownBy(() -> store.conditionalPut(spec, new byte[accepted.length + 1],
                BlobStore.WriteCondition.absent()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.get(BUCKET, spec.key(), created.versionId()).data()).hasSize(accepted.length);

        // An oversized object reachable through the unconditional API stays
        // visible to plain reads but is refused by the conditional read bound.
        store.put(spec, new byte[accepted.length + 1]);
        assertThat(store.get(BUCKET, spec.key()).data()).hasSize(accepted.length + 1);
        assertThatThrownBy(() -> store.getForUpdate(BUCKET, spec.key()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void backendVersionIdentityTracksWritesNotCallerRevisions() {
        var spec = spec("versioning/object");
        var first = store.put(spec, bytes("first"));
        var second = store.put(spec, bytes("second"));
        assertThat(first.versionId()).isNotBlank().isNotEqualTo("null");
        assertThat(second.versionId()).isNotBlank().isNotEqualTo("null").isNotEqualTo(first.versionId());
        // The provider's opaque version identity reads back exact bytes; it is
        // not a repository document revision number and must not be parsed.
        assertThat(store.get(BUCKET, spec.key(), first.versionId()).data()).isEqualTo(bytes("first"));
        assertThat(store.get(BUCKET, spec.key(), second.versionId()).data()).isEqualTo(bytes("second"));
        assertThat(store.get(BUCKET, spec.key()).data()).isEqualTo(bytes("second"));
    }

    private static BlobStore.PutSpec spec(String key) {
        return new BlobStore.PutSpec(BUCKET, key, "application/octet-stream", null, null);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
