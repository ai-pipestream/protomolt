package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.ExactVersionOutcome;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exact-version reclamation correctness against real LocalStack 3.8: only the
 * addressed immutable version id is deleted, neighbors and delete markers
 * survive, refusal throws before any destructive effect, backend failures are
 * never classified as absence, and discovery advertises the capability only
 * together with a live port.
 */
@Testcontainers
class S3ExactVersionReclaimerIT {
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

    @Test void twoVersionsOneKeyReclaimOnlyOld() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "doc/revision-a";
        byte[] firstBytes = bytes("first revision bytes");
        byte[] secondBytes = bytes("second revision bytes, longer");
        var first = put(bucket, key, firstBytes);
        var second = put(bucket, key, secondBytes);

        assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.DELETED);

        var current = store.get(bucket, key);
        assertThat(current.data()).isEqualTo(secondBytes);
        assertThat(current.versionId()).isEqualTo(second.versionId());
        assertThat(store.get(bucket, key, second.versionId()).data()).isEqualTo(secondBytes);
        assertThatThrownBy(() -> store.get(bucket, key, first.versionId()))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThatThrownBy(() -> client.getObject(b -> b.bucket(bucket).key(key).versionId(first.versionId())))
                .isInstanceOfSatisfying(S3Exception.class, failure -> assertThat(failure.statusCode()).isEqualTo(404));
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(key));
        assertThat(listed.versions()).extracting(v -> v.key() + "@" + v.versionId())
                .containsExactly(key + "@" + second.versionId());
        assertThat(listed.deleteMarkers()).isEmpty();
    }

    @Test void neighborKeyAndDeleteMarkersSurvive() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        var first = put(bucket, "shared/a", bytes("shared-a-v1"));
        var second = put(bucket, "shared/a", bytes("shared-a-v2-longer"));
        var neighborOld = put(bucket, "shared/ab", bytes("nbr-old"));
        var neighborNew = put(bucket, "shared/ab", bytes("nbr-newer"));
        put(bucket, "shared/b", bytes("about to be marked"));
        client.deleteObject(b -> b.bucket(bucket).key("shared/b")); // key-only delete: delete marker
        var marker = client.listObjectVersions(r -> r.bucket(bucket).prefix("shared/b")).deleteMarkers().get(0);
        assertThat(marker.key()).isEqualTo("shared/b");
        assertThat(marker.isLatest()).isTrue();

        String before = snapshot(bucket, "shared/");
        assertThat(reclaimer.reclaim(bucket, "shared/a", first.versionId())).isEqualTo(ExactVersionOutcome.DELETED);

        // The only change under the prefix: v1 of shared/a is gone; everything else is byte/count identical.
        List<String> beforeLines = new ArrayList<>(List.of(before.split("\n")));
        assertThat(beforeLines.removeIf(line -> line.startsWith("V shared/a " + first.versionId() + " "))).isTrue();
        assertThat(snapshot(bucket, "shared/")).isEqualTo(String.join("\n", beforeLines));
        assertThat(store.get(bucket, "shared/a", second.versionId()).data()).isEqualTo(bytes("shared-a-v2-longer"));
        assertThat(store.get(bucket, "shared/ab", neighborOld.versionId()).data()).isEqualTo(bytes("nbr-old"));
        assertThat(store.get(bucket, "shared/ab", neighborNew.versionId()).data()).isEqualTo(bytes("nbr-newer"));
        var afterMarkers = client.listObjectVersions(r -> r.bucket(bucket).prefix("shared/b")).deleteMarkers();
        assertThat(afterMarkers).extracting(m -> m.versionId() + ":" + m.isLatest())
                .containsExactly(marker.versionId() + ":true");
        assertThatThrownBy(() -> store.get(bucket, "shared/b")).isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test void repeatedReclaimIsIdempotent() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "doc/revision-b";
        var first = put(bucket, key, bytes("original"));
        var second = put(bucket, key, bytes("replacement"));

        assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.DELETED);
        assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.ABSENT);
        assertThat(reclaimer.reclaim(bucket, key, first.versionId())).isEqualTo(ExactVersionOutcome.ABSENT);

        assertThat(store.get(bucket, key).data()).isEqualTo(bytes("replacement"));
        assertThat(store.get(bucket, key).versionId()).isEqualTo(second.versionId());
        assertThat(client.listObjectVersions(r -> r.bucket(bucket).prefix(key)).versions()).hasSize(1);
    }

    @Test void refusalWithoutEffects() {
        // Null/blank/literal-"null" versions: rejected before any provider I/O.
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        var first = put(bucket, "guarded/key", bytes("guarded v1"));
        var second = put(bucket, "guarded/key", bytes("guarded v2"));
        String intact = snapshot(bucket, "guarded/");
        for (String invalid : new String[] {null, "", "   ", "null"}) {
            assertThatThrownBy(() -> reclaimer.reclaim(bucket, "guarded/key", invalid))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(snapshot(bucket, "guarded/")).as("no effect after refusing version '%s'", invalid).isEqualTo(intact);
        }
        assertThat(store.get(bucket, "guarded/key", first.versionId()).data()).isEqualTo(bytes("guarded v1"));
        assertThat(store.get(bucket, "guarded/key", second.versionId()).data()).isEqualTo(bytes("guarded v2"));

        // Suspended versioning: FAILED_PRECONDITION, nothing deleted.
        versioning(bucket, BucketVersioningStatus.SUSPENDED);
        assertThatThrownBy(() -> reclaimer.reclaim(bucket, "guarded/key", first.versionId()))
                .isInstanceOfSatisfying(BlobStoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.FAILED_PRECONDITION));
        versioning(bucket, BucketVersioningStatus.ENABLED); // snapshot only compares version state; resume for clarity
        assertThat(snapshot(bucket, "guarded/")).isEqualTo(intact);
        assertThat(store.get(bucket, "guarded/key").versionId()).isEqualTo(second.versionId());
        assertThat(store.get(bucket, "guarded/key", first.versionId()).data()).isEqualTo(bytes("guarded v1"));
        assertThat(store.get(bucket, "guarded/key", second.versionId()).data()).isEqualTo(bytes("guarded v2"));

        // Never-versioned bucket: FAILED_PRECONDITION, the null version survives.
        String plain = bucket();
        put(plain, "object", bytes("plain bytes"));
        String plainSnapshot = snapshot(plain, "");
        assertThatThrownBy(() -> reclaimer.reclaim(plain, "object", "v-does-not-exist"))
                .isInstanceOfSatisfying(BlobStoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.FAILED_PRECONDITION));
        assertThat(snapshot(plain, "")).isEqualTo(plainSnapshot);
        assertThat(store.get(plain, "object").data()).isEqualTo(bytes("plain bytes"));

        // Missing bucket: NOT_FOUND, nothing is created.
        String missing = "missing-" + UUID.randomUUID();
        assertThatThrownBy(() -> reclaimer.reclaim(missing, "object", "v1"))
                .isInstanceOfSatisfying(BlobStoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BlobStoreException.Code.NOT_FOUND));
        assertThatThrownBy(() -> client.headBucket(b -> b.bucket(missing))).isInstanceOf(NoSuchBucketException.class);
        assertThat(snapshot(bucket, "guarded/")).isEqualTo(intact);
    }

    @Test void failureClassificationNotAbsence() throws Exception {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        String key = "guarded/failure";
        var first = put(bucket, key, bytes("must survive backend confusion"));

        // (b) Unreachable endpoint: UNCERTAIN, never ABSENT. Wrong static
        // credentials cannot be exercised against LocalStack: its S3 data
        // plane accepts any access key (verified by probe: requests signed
        // with unknown credentials reach bucket lookup and even explicit
        // Deny bucket policies are not enforced for these calls). That part
        // of the classification is qualified on real RustFS instead, which
        // answers unknown keys with 403 InvalidAccessKeyId; see
        // RustFsExactVersionReclaimerIT.wrongCredentialsRefuseWithClassification.
        int closedPort;
        try (var probe = new java.net.ServerSocket(0)) { closedPort = probe.getLocalPort(); }
        S3Client ghost = S3Client.builder().endpointOverride(URI.create("http://127.0.0.1:" + closedPort))
                .region(Region.of(S3.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .forcePathStyle(true)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofMillis(300)).socketTimeout(Duration.ofMillis(500)))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(2))
                        .apiCallAttemptTimeout(Duration.ofSeconds(1))
                        .retryStrategy(r -> r.maxAttempts(1)))
                .build();
        try {
            assertThat(new S3ExactVersionReclaimer(ghost).reclaim(bucket, key, first.versionId()))
                    .isEqualTo(ExactVersionOutcome.UNCERTAIN);
        } finally { ghost.close(); }
        assertThat(store.get(bucket, key, first.versionId()).data()).isEqualTo(bytes("must survive backend confusion"));
    }

    @Test void discoveryAdvertisesCapability() throws Exception {
        Map<String, String> options = Map.ofEntries(
                Map.entry("endpoint", S3.getEndpoint().toString()),
                Map.entry("region", S3.getRegion()),
                Map.entry("access-key", S3.getAccessKey()),
                Map.entry("secret-key", S3.getSecretKey()),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000"));
        try (var handle = BlobStores.discover().open("s3", options, Set.of(BlobCapability.EXACT_VERSION_RECLAMATION))) {
            assertThat(handle.capabilities()).contains(BlobCapability.EXACT_VERSION_RECLAMATION);
            var openedStore = (S3BlobStore) handle.store();
            String bucket = bucket();
            versioning(bucket, BucketVersioningStatus.ENABLED);
            String key = "discovered/key";
            var first = openedStore.put(new BlobStore.PutSpec(bucket, key, "application/octet-stream", null, null),
                    bytes("discovered old"));
            openedStore.put(new BlobStore.PutSpec(bucket, key, "application/octet-stream", null, null),
                    bytes("discovered current"));
            assertThat(handle.exactVersionReclaimer().reclaim(bucket, key, first.versionId()))
                    .isEqualTo(ExactVersionOutcome.DELETED);
            assertThat(openedStore.get(bucket, key).data()).isEqualTo(bytes("discovered current"));
            assertThatThrownBy(() -> openedStore.get(bucket, key, first.versionId()))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
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
        String name = "exact-" + UUID.randomUUID();
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
