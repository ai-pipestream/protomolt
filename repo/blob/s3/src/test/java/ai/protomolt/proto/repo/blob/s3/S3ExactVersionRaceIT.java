package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.ExactVersionOutcome;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrency and failure-injection scenarios against real LocalStack 3.8
 * traffic. The {@link S3RequestRelay} is a real socket relay over real
 * provider traffic: it holds a genuinely in-flight PUT so an exact deletion
 * must spare the version the PUT later creates, and it severs one
 * acknowledged DELETE so the UNCERTAIN classification is proven end to end.
 */
@Testcontainers
class S3ExactVersionRaceIT {
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static S3Client client;
    static S3BlobStore store;
    static ai.protomolt.proto.repo.blob.spi.ExactVersionReclaimer reclaimer;

    @BeforeAll static void boot() {
        client = S3Client.builder().endpointOverride(S3.getEndpoint()).region(Region.of(S3.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .forcePathStyle(true).build();
        store = new S3BlobStore(client);
        reclaimer = new S3ExactVersionReclaimer(client);
    }

    @AfterAll static void close() { if (client != null) client.close(); }

    @Test void heldPutSurvivesExactDeletion() throws Exception {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "held/blob";
        var first = put(bucket, key, bytes("committed before the race"));
        byte[] secondBytes = patterned(512 * 1024, (byte) 7);

        try (var relay = S3RequestRelay.to(S3.getEndpoint().getHost(), S3.getEndpoint().getPort());
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var release = new CountDownLatch(1);
            var held = relay.holdRequestBody(
                    line -> line.startsWith("PUT /" + bucket + "/" + key + " "), 16 * 1024, release);
            S3Client heldClient = relayClient(relay);

            var put = workers.submit(() -> heldClient.putObject(
                    b -> b.bucket(bucket).key(key).contentType("application/octet-stream")
                            .contentLength((long) secondBytes.length),
                    RequestBody.fromInputStream(new ByteArrayInputStream(secondBytes), secondBytes.length)));

            // The PUT is genuinely in flight: upstream has the head and a partial body only.
            assertThat(held.await(30, TimeUnit.SECONDS)).as("relay intercepted the in-flight PUT").isTrue();
            assertThat(relay.upstreamBodyBytes()).isBetween(1L, (long) secondBytes.length - 1);
            assertThat(relay.observedRequests()).anyMatch(line -> line.startsWith("PUT /" + bucket + "/" + key + " "));

            assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.DELETED);

            release.countDown();
            var second = put.get(30, TimeUnit.SECONDS);
            heldClient.close();
            assertThat(second.versionId()).isNotBlank().isNotEqualTo("null").isNotEqualTo(first.versionId());

            var current = store.get(bucket, key);
            assertThat(current.data()).isEqualTo(secondBytes);
            assertThat(current.versionId()).isEqualTo(second.versionId());
            assertThat(store.get(bucket, key, second.versionId()).data()).isEqualTo(secondBytes);
            assertThrownByVersionGone(bucket, key, first.versionId());
            var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(key));
            assertThat(listed.versions()).extracting(v -> v.key() + "@" + v.versionId())
                    .containsExactly(key + "@" + second.versionId());
            assertThat(listed.deleteMarkers()).isEmpty();
        }
    }

    /**
     * Overlapping calls for one immutable target. LocalStack 3.8 (moto) is NOT
     * qualified for the strict form of this guarantee: moto answers duplicate
     * concurrent versioned deletes with {@code 400 InvalidArgument "Invalid
     * version id specified"} once another caller removed the version, while
     * S3 answers 204 and the second caller converges. This test therefore
     * pins exactly what the adapter over LocalStack does guarantee — every
     * call either returns DELETED/ABSENT or throws the explicitly classified
     * INVALID_ARGUMENT refusal caused by that moto 400, never a bare outcome
     * and never any other exception — and that bounded retries of the same
     * target then converge to ABSENT for every caller. The strict
     * every-call-returns-DELETED-or-ABSENT-with-zero-exceptions form is
     * qualified on real RustFS in RustFsExactVersionReclaimerIT.
     */
    @Test void overlappingCallsConverge() throws Exception {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "race/same-target";
        var first = put(bucket, key, bytes("raced away"));
        var second = put(bucket, key, bytes("raced survivor"));
        put(bucket, "race/neighbor", bytes("unrelated bytes"));

        int contenders = 8;
        var ready = new CountDownLatch(contenders);
        var go = new CountDownLatch(1);
        var outcomes = new ConcurrentLinkedQueue<ExactVersionOutcome>();
        var motoDuplicateDeleteRefusals = new ConcurrentLinkedQueue<BlobStoreException>();
        var failures = new ConcurrentLinkedQueue<Throwable>();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<java.util.concurrent.Future<?>>();
            for (int index = 0; index < contenders; index++) {
                tasks.add(workers.submit((java.util.concurrent.Callable<Void>) () -> {
                    ready.countDown();
                    if (!go.await(10, TimeUnit.SECONDS)) throw new AssertionError("race start timed out");
                    try {
                        outcomes.add(reclaimer.reclaim(bucket, key, first.versionId()));
                    } catch (BlobStoreException refusal) {
                        if (refusal.code() == BlobStoreException.Code.INVALID_ARGUMENT
                                && causedByMotoDuplicateDelete400(refusal)) {
                            motoDuplicateDeleteRefusals.add(refusal);
                        } else {
                            failures.add(refusal);
                        }
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
        assertThat(outcomes).allSatisfy(outcome ->
                assertThat(outcome).isIn(ExactVersionOutcome.DELETED, ExactVersionOutcome.ABSENT));
        assertThat(outcomes.size() + motoDuplicateDeleteRefusals.size()).isEqualTo(contenders);
        System.out.println("LOCALSTACK-DEVIATION overlapping duplicate versioned deletes answered 400 "
                + "InvalidArgument 'Invalid version id specified' for " + motoDuplicateDeleteRefusals.size()
                + " of " + contenders + " callers (S3 answers 204); strict zero-exception convergence is "
                + "qualified on RustFS instead");

        // Bounded retry of the same immutable target converges for every caller.
        for (int retry = 0; retry < contenders; retry++) {
            assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.ABSENT);
        }
        assertThat(store.get(bucket, key).data()).isEqualTo(bytes("raced survivor"));
        assertThat(store.get(bucket, key).versionId()).isEqualTo(second.versionId());
        assertThat(store.get(bucket, "race/neighbor").data()).isEqualTo(bytes("unrelated bytes"));
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(key));
        assertThat(listed.versions()).extracting(v -> v.key() + "@" + v.versionId())
                .containsExactly(key + "@" + second.versionId());
    }

    /** True when the classified refusal was caused by moto's 400 InvalidArgument duplicate-delete answer. */
    private static boolean causedByMotoDuplicateDelete400(BlobStoreException refusal) {
        for (Throwable cause = refusal.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof S3Exception s3 && s3.statusCode() == 400
                    && s3.awsErrorDetails() != null && "InvalidArgument".equals(s3.awsErrorDetails().errorCode())) {
                return true;
            }
        }
        return false;
    }

    @Test void uncertainAckThenRetryConverges() throws Exception {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "uncertain/target";
        var first = put(bucket, key, bytes("delete me maybe"));
        var second = put(bucket, key, bytes("definitely keep me"));

        try (var relay = S3RequestRelay.to(S3.getEndpoint().getHost(), S3.getEndpoint().getPort())) {
            var cut = relay.cutResponse(
                    line -> line.startsWith("DELETE ") && line.contains("versionId=" + first.versionId()), null);
            S3Client relayClient = relayClient(relay);
            try {
                // The DELETE is fully forwarded upstream, then the connection is reset
                // before the provider's answer returns: the call cannot prove either
                // terminal state and must report UNCERTAIN, never ABSENT.
                assertThat(new S3ExactVersionReclaimer(relayClient).reclaim(bucket, key, first.versionId()))
                        .isEqualTo(ExactVersionOutcome.UNCERTAIN);
            } finally {
                relayClient.close();
            }
            assertThat(cut.await(10, TimeUnit.SECONDS)).as("the real DELETE reached the provider").isTrue();
            assertThat(relay.observedRequests()).anyMatch(line -> line.startsWith("DELETE ")
                    && line.contains("versionId=" + first.versionId()));
        }

        // Retry of the same immutable target converges to a terminal classification.
        assertThat(reclaimer.reclaim(bucket, key, first.versionId()))
                .isIn(ExactVersionOutcome.DELETED, ExactVersionOutcome.ABSENT);
        assertThrownByVersionGone(bucket, key, first.versionId());
        assertThat(store.get(bucket, key, second.versionId()).data()).isEqualTo(bytes("definitely keep me"));
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(key));
        assertThat(listed.versions()).extracting(v -> v.key() + "@" + v.versionId())
                .containsExactly(key + "@" + second.versionId());
        assertThat(listed.deleteMarkers()).isEmpty();
    }

    private static void assertThrownByVersionGone(String bucket, String key, String versionId) {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.get(bucket, key, versionId))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    /** Client pointed at the relay: one API attempt, generous budget for held traffic. */
    private static S3Client relayClient(S3RequestRelay relay) {
        return S3Client.builder().endpointOverride(relay.endpoint()).region(Region.of(S3.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .forcePathStyle(true)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(10)).socketTimeout(Duration.ofSeconds(60)))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(120))
                        .apiCallAttemptTimeout(Duration.ofSeconds(90))
                        .retryStrategy(r -> r.maxAttempts(1)))
                .build();
    }

    private static byte[] patterned(int size, byte seed) {
        byte[] body = new byte[size];
        for (int index = 0; index < size; index++) body[index] = (byte) (seed + index);
        return body;
    }

    private static String bucket() {
        String name = "race-" + UUID.randomUUID();
        client.createBucket(r -> r.bucket(name));
        return name;
    }

    private static void versioning(String bucket, BucketVersioningStatus status) {
        client.putBucketVersioning(r -> r.bucket(bucket).versioningConfiguration(v -> v.status(status)));
    }

    private static BlobStore.PutResult put(String bucket, String key, byte[] body) {
        return store.put(new BlobStore.PutSpec(bucket, key, "application/octet-stream", null, null), body);
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
