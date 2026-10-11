package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.ExactVersionOutcome;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exact-version reclamation qualification against the deployment-pinned
 * RustFS image ({@code rustfs/rustfs:1.0.0-beta.11-preview.1}). LocalStack
 * proves correctness; this class proves the contract holds on a second,
 * non-LocalStack S3-compatible backend — including the NoSuchKey absence
 * classification, suspension refusal, and bounded timing observations while
 * one delete response is deliberately held by a raw TCP relay.
 */
@Testcontainers
class RustFsExactVersionReclaimerIT {
    private static final String IMAGE = "rustfs/rustfs:1.0.0-beta.11-preview.1";

    @Container static final GenericContainer<?> RUSTFS = new GenericContainer<>(
            DockerImageName.parse(IMAGE))
            .withCommand("/data")
            .withEnv("RUSTFS_VOLUMES", "/data")
            .withEnv("RUSTFS_ADDRESS", ":9000")
            .withEnv("RUSTFS_CONSOLE_ENABLE", "false")
            .withEnv("RUSTFS_ACCESS_KEY", "exact-version-test")
            .withEnv("RUSTFS_SECRET_KEY", "exact-version-test-secret")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/health").forPort(9000));

    static S3Client client;
    static S3BlobStore store;
    static ai.protomolt.proto.repo.blob.spi.ExactVersionReclaimer reclaimer;

    @BeforeAll static void boot() {
        client = S3Client.builder()
                .endpointOverride(java.net.URI.create("http://" + RUSTFS.getHost() + ":" + RUSTFS.getMappedPort(9000)))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create("exact-version-test", "exact-version-test-secret")))
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true).build();
        store = new S3BlobStore(client);
        reclaimer = new S3ExactVersionReclaimer(client);
    }

    @AfterAll static void close() { if (client != null) client.close(); }

    /**
     * Wrong static credentials: RustFS validates its configured access keys
     * and answers unknown ones with 403 InvalidAccessKeyId, so the adapter
     * must throw PERMISSION_DENIED/UNAUTHENTICATED — never an outcome, and
     * the target version must survive. LocalStack cannot qualify this: its
     * S3 data plane accepts any access key (verified by probe), so the
     * unreachable-endpoint half of the failure classification lives in
     * S3ExactVersionReclaimerIT instead.
     */
    @Test void wrongCredentialsRefuseWithClassification() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "guarded/auth";
        var first = put(bucket, key, bytes("rustfs must survive auth confusion"));
        try (S3Client wrong = S3Client.builder()
                .endpointOverride(java.net.URI.create("http://" + RUSTFS.getHost() + ":" + RUSTFS.getMappedPort(9000)))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create("not-the-rustfs-key", "not-the-rustfs-secret")))
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true).build()) {
            assertThatThrownBy(() -> new S3ExactVersionReclaimer(wrong).reclaim(bucket, key, first.versionId()))
                    .isInstanceOfSatisfying(BlobStoreException.class, failure -> assertThat(failure.code())
                            .isIn(BlobStoreException.Code.PERMISSION_DENIED, BlobStoreException.Code.UNAUTHENTICATED));
        }
        assertThat(store.get(bucket, key, first.versionId()).data()).isEqualTo(bytes("rustfs must survive auth confusion"));
    }

    @Test void twoVersionsOneKeyReclaimOnlyOld() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "doc/revision-a";
        byte[] firstBytes = bytes("rustfs first revision");
        byte[] secondBytes = bytes("rustfs second revision, longer");
        var first = put(bucket, key, firstBytes);
        var second = put(bucket, key, secondBytes);
        assertThat(first.versionId()).isNotBlank().isNotEqualTo("null");
        assertThat(second.versionId()).isNotBlank().isNotEqualTo("null").isNotEqualTo(first.versionId());

        assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.DELETED);

        var current = store.get(bucket, key);
        assertThat(current.data()).isEqualTo(secondBytes);
        assertThat(current.versionId()).isEqualTo(second.versionId());
        assertThat(store.get(bucket, key, second.versionId()).data()).isEqualTo(secondBytes);
        // RustFS answers NoSuchKey rather than NoSuchVersion for a deleted version id;
        // both must classify as verified exact-version absence.
        assertThatThrownBy(() -> store.get(bucket, key, first.versionId()))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(key));
        assertThat(listed.versions()).extracting(v -> v.key() + "@" + v.versionId())
                .containsExactly(key + "@" + second.versionId());
        assertThat(listed.deleteMarkers()).isEmpty();
    }

    @Test void repeatedReclaimIsIdempotent() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "doc/revision-b";
        var first = put(bucket, key, bytes("rustfs original"));
        var second = put(bucket, key, bytes("rustfs replacement"));

        assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.DELETED);
        assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.ABSENT);
        assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.ABSENT);

        assertThat(store.get(bucket, key).data()).isEqualTo(bytes("rustfs replacement"));
        assertThat(store.get(bucket, key).versionId()).isEqualTo(second.versionId());
        assertThat(client.listObjectVersions(r -> r.bucket(bucket).prefix(key)).versions()).hasSize(1);
    }

    @Test void deleteMarkerSurvivesExactReclaim() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        put(bucket, "marked/key", bytes("soon hidden"));
        client.deleteObject(b -> b.bucket(bucket).key("marked/key")); // key-only delete: delete marker
        var marker = client.listObjectVersions(r -> r.bucket(bucket).prefix("marked/key")).deleteMarkers().get(0);
        assertThat(marker.key()).isEqualTo("marked/key");
        assertThat(marker.isLatest()).isTrue();

        var first = put(bucket, "data/key", bytes("data v1"));
        var second = put(bucket, "data/key", bytes("data v2"));
        String before = snapshot(bucket, "");

        assertThat(reclaimer.reclaim(bucket, "data/key", first.versionId())).isEqualTo(ExactVersionOutcome.DELETED);

        List<String> beforeLines = new ArrayList<>(List.of(before.split("\n")));
        assertThat(beforeLines.removeIf(line -> line.startsWith("V data/key " + first.versionId() + " "))).isTrue();
        assertThat(snapshot(bucket, "")).isEqualTo(String.join("\n", beforeLines));
        assertThat(store.get(bucket, "data/key", second.versionId()).data()).isEqualTo(bytes("data v2"));
        var markers = client.listObjectVersions(r -> r.bucket(bucket).prefix("marked/key")).deleteMarkers();
        assertThat(markers).extracting(m -> m.versionId() + ":" + m.isLatest())
                .containsExactly(marker.versionId() + ":true");
        assertThatThrownBy(() -> store.get(bucket, "marked/key"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test void rustfsSuspendedRefusal() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        var first = put(bucket, "guarded/key", bytes("rustfs guarded v1"));
        var second = put(bucket, "guarded/key", bytes("rustfs guarded v2"));
        String intact = snapshot(bucket, "guarded/");

        try {
            client.putBucketVersioning(b -> b.bucket(bucket).versioningConfiguration(v -> v.status(BucketVersioningStatus.SUSPENDED)));
        } catch (S3Exception rejected) {
            throw new AssertionError("RustFS " + IMAGE + " rejected putBucketVersioning(SUSPENDED): "
                    + rejected.awsErrorDetails().errorCode() + " / " + rejected.awsErrorDetails().errorMessage());
        }
        var reported = client.getBucketVersioning(b -> b.bucket(bucket)).status();
        System.out.println("RUSTFS-TIMING image=" + IMAGE + " scenario=suspension reportedStatusAfterSuspend=" + reported);

        assertThatThrownBy(() -> reclaimer.reclaim(bucket, "guarded/key", first.versionId()))
                .isInstanceOfSatisfying(BlobStoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.FAILED_PRECONDITION));
        assertThat(snapshot(bucket, "guarded/")).isEqualTo(intact);
        // RustFS deviation, preserved as evidence: after suspension its plain
        // GET omits versionId (null) even for an object that carries a real
        // version id, so exact-version reads prove byte identity here.
        var currentAfterRefusal = store.get(bucket, "guarded/key");
        System.out.println("RUSTFS-TIMING image=" + IMAGE + " scenario=suspension plainGetVersionIdAfterSuspend="
                + currentAfterRefusal.versionId() + " (deviation: null on suspended buckets)");
        assertThat(currentAfterRefusal.data()).isEqualTo(bytes("rustfs guarded v2"));
        assertThat(store.get(bucket, "guarded/key", first.versionId()).data()).isEqualTo(bytes("rustfs guarded v1"));
        assertThat(store.get(bucket, "guarded/key", second.versionId()).data()).isEqualTo(bytes("rustfs guarded v2"));
    }

    @Test void rustfsHeldDeleteUnrelatedOpsProceedAndTiming() throws Exception {
        long scenarioStart = System.nanoTime();
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String heldKey = "held/target";
        var victim = put(bucket, heldKey, bytes("held delete victim"));
        var survivor = put(bucket, heldKey, bytes("held delete survivor"));
        int iterations = 30;
        List<String> heldTargets = precreateMixTargets(bucket, "mixheld", iterations);

        long[] heldOps;
        long heldNanos;
        ExactVersionOutcome heldOutcome;
        try (var relay = S3RequestRelay.to(RUSTFS.getHost(), RUSTFS.getMappedPort(9000));
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var release = new CountDownLatch(1);
            var cut = relay.cutResponse(
                    line -> line.startsWith("DELETE ") && line.contains("versionId=" + victim.versionId()), release);
            S3Client relayClient = relayClient(relay);
            var heldReclaimer = new S3ExactVersionReclaimer(relayClient);
            long heldStart = System.nanoTime();
            var held = workers.submit(() -> heldReclaimer.reclaim(bucket, heldKey, victim.versionId()));

            // The DELETE request is upstream; its response is held until released.
            assertThat(cut.await(30, TimeUnit.SECONDS)).as("held DELETE forwarded upstream").isTrue();
            heldOps = runMix(bucket, "mixheld", iterations, heldTargets);
            release.countDown();
            heldOutcome = held.get(30, TimeUnit.SECONDS);
            heldNanos = System.nanoTime() - heldStart;
            relayClient.close();
        }
        System.out.println("RUSTFS-TIMING image=" + IMAGE + " scenario=heldDelete outcome=" + heldOutcome
                + " heldDeleteMs=" + millis(heldNanos));
        assertThat(heldOutcome).isEqualTo(ExactVersionOutcome.UNCERTAIN);

        // Converge the same immutable target and prove isolation of everything else.
        assertThat(reclaimer.reclaim(bucket, heldKey, victim.versionId()))
                .isIn(ExactVersionOutcome.DELETED, ExactVersionOutcome.ABSENT);
        assertThat(store.get(bucket, heldKey).data()).isEqualTo(bytes("held delete survivor"));
        assertThat(store.get(bucket, heldKey).versionId()).isEqualTo(survivor.versionId());
        assertThatThrownBy(() -> store.get(bucket, heldKey, victim.versionId()))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(heldKey));
        assertThat(listed.versions()).extracting(v -> v.key() + "@" + v.versionId())
                .containsExactly(heldKey + "@" + survivor.versionId());

        List<String> baseTargets = precreateMixTargets(bucket, "mixbase", iterations);
        long[] baseOps = runMix(bucket, "mixbase", iterations, baseTargets);
        System.out.println("RUSTFS-TIMING image=" + IMAGE + " scenario=heldDelete unrelatedOps=" + iterations
                + " p50Ms=" + percentile50(heldOps) + " maxMs=" + maxMillis(heldOps));
        System.out.println("RUSTFS-TIMING image=" + IMAGE + " scenario=baseline unrelatedOps=" + iterations
                + " p50Ms=" + percentile50(baseOps) + " maxMs=" + maxMillis(baseOps));
        long totalMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scenarioStart);
        System.out.println("RUSTFS-TIMING image=" + IMAGE + " scenario=heldDelete totalMs=" + totalMs + " budgetMs=120000");
        assertThat(totalMs).as("bounded overall budget; timings are recorded evidence, not perf gates")
                .isLessThan(120_000);
    }

    @Test void rustfsOverlappingConverges() throws Exception {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "race/same-target";
        var first = put(bucket, key, bytes("rustfs raced away"));
        var second = put(bucket, key, bytes("rustfs raced survivor"));
        put(bucket, "race/neighbor", bytes("rustfs unrelated bytes"));

        int contenders = 4;
        var ready = new CountDownLatch(contenders);
        var go = new CountDownLatch(1);
        var outcomes = new ConcurrentLinkedQueue<ExactVersionOutcome>();
        var failures = new ConcurrentLinkedQueue<Throwable>();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<java.util.concurrent.Future<?>>();
            for (int index = 0; index < contenders; index++) {
                tasks.add(workers.submit((java.util.concurrent.Callable<Void>) () -> {
                    ready.countDown();
                    if (!go.await(10, TimeUnit.SECONDS)) throw new AssertionError("race start timed out");
                    try {
                        outcomes.add(reclaimer.reclaim(bucket, key, first.versionId()));
                    } catch (RuntimeException unexpected) {
                        failures.add(unexpected);
                    }
                    return null;
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            for (var task : tasks) task.get(30, TimeUnit.SECONDS);
        }
        assertThat(failures).isEmpty();
        assertThat(outcomes).hasSize(contenders)
                .allSatisfy(outcome -> assertThat(outcome).isIn(ExactVersionOutcome.DELETED, ExactVersionOutcome.ABSENT));
        assertThat(store.get(bucket, key).data()).isEqualTo(bytes("rustfs raced survivor"));
        assertThat(store.get(bucket, key).versionId()).isEqualTo(second.versionId());
        assertThat(store.get(bucket, "race/neighbor").data()).isEqualTo(bytes("rustfs unrelated bytes"));
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(key));
        assertThat(listed.versions()).extracting(v -> v.key() + "@" + v.versionId())
                .containsExactly(key + "@" + second.versionId());
    }

    /**
     * One timed iteration: an unrelated-key PUT, its read-back, and the
     * reclamation of another key's older version — all through the direct
     * client while the held delete is outstanding.
     */
    private static long[] runMix(String bucket, String prefix, int iterations, List<String> reclaimTargets) {
        long[] durations = new long[iterations];
        for (int index = 0; index < iterations; index++) {
            long start = System.nanoTime();
            String putKey = prefix + "/put-" + index;
            byte[] body = bytes("mix payload " + prefix + " " + index);
            store.put(new BlobStore.PutSpec(bucket, putKey, "application/octet-stream", null, null), body);
            assertThat(store.get(bucket, putKey).data()).isEqualTo(body);
            assertThat(reclaimer.reclaim(bucket, prefix + "/reclaim-" + index, reclaimTargets.get(index)))
                    .isEqualTo(ExactVersionOutcome.DELETED);
            durations[index] = System.nanoTime() - start;
        }
        return durations;
    }

    /** Creates, outside any timed section, one (v1, v2) pair per mix iteration; returns the v1 version ids. */
    private static List<String> precreateMixTargets(String bucket, String prefix, int iterations) {
        var targets = new ArrayList<String>();
        for (int index = 0; index < iterations; index++) {
            put(bucket, prefix + "/reclaim-" + index, bytes("mix old " + prefix + " " + index));
            var current = put(bucket, prefix + "/reclaim-" + index, bytes("mix new " + prefix + " " + index));
            targets.add(priorVersionId(bucket, prefix + "/reclaim-" + index, current.versionId()));
        }
        return targets;
    }

    /** The remaining version id of the key that is not the current one. */
    private static String priorVersionId(String bucket, String key, String currentVersionId) {
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(key));
        return listed.versions().stream()
                .filter(version -> !version.versionId().equals(currentVersionId))
                .map(version -> version.versionId())
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected a prior version under " + key));
    }

    private static S3Client relayClient(S3RequestRelay relay) {
        return S3Client.builder().endpointOverride(relay.endpoint()).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create("exact-version-test", "exact-version-test-secret")))
                .forcePathStyle(true)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(10)).socketTimeout(Duration.ofSeconds(60)))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(120))
                        .apiCallAttemptTimeout(Duration.ofSeconds(90))
                        .retryStrategy(r -> r.maxAttempts(1)))
                .build();
    }

    private static String percentile50(long[] nanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        return millis(sorted[sorted.length / 2]);
    }

    private static String maxMillis(long[] nanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        return millis(sorted[sorted.length - 1]);
    }

    private static String millis(long nanos) {
        return String.format(java.util.Locale.ROOT, "%.2f", nanos / 1_000_000.0);
    }

    private static String snapshot(String bucket, String prefix) {
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(prefix));
        var lines = new ArrayList<String>();
        for (var version : listed.versions()) {
            lines.add("V " + version.key() + " " + version.versionId() + " " + version.size() + " " + version.isLatest());
        }
        for (var marker : listed.deleteMarkers()) {
            lines.add("D " + marker.key() + " " + marker.versionId() + " " + marker.isLatest());
        }
        java.util.Collections.sort(lines);
        return String.join("\n", lines);
    }

    private static String bucket() {
        String name = "rustfs-exact-" + UUID.randomUUID();
        client.createBucket(r -> r.bucket(name));
        return name;
    }

    private static void versioning(String bucket, BucketVersioningStatus status) {
        client.putBucketVersioning(r -> r.bucket(bucket).versioningConfiguration(v -> v.status(status)));
        assertThat(client.getBucketVersioning(r -> r.bucket(bucket)).status()).isEqualTo(status);
    }

    private static BlobStore.PutResult put(String bucket, String key, byte[] body) {
        return store.put(new BlobStore.PutSpec(bucket, key, "application/octet-stream", null, null), body);
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
