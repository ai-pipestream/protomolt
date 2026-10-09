package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import static org.assertj.core.api.Assertions.*;

/** Actual versioned bytes and markers, including suspended versioning's null version. */
@Testcontainers
class S3ObjectReclaimerIT {
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static S3Client client;
    static S3BlobStore store;
    static S3ObjectReclaimer reclaimer;
    @BeforeAll static void boot() {
        client = S3Client.builder().endpointOverride(S3.getEndpoint()).region(Region.of(S3.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .forcePathStyle(true).build();
        store = new S3BlobStore(client);
        reclaimer = new S3ObjectReclaimer(client);
    }
    @AfterAll static void close() { if (client != null) client.close(); }

    @Test void reclaimsHistoricalVersionsAndMarkersWithoutTouchingPrefixNeighbours() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        put(bucket, "managed", 1);
        put(bucket, "managed", 2);
        put(bucket, "managed-neighbour", 3);
        store.delete(bucket, "managed");
        var before = client.listObjectVersions(r -> r.bucket(bucket).prefix("managed"));
        assertThat(before.versions().stream().filter(v -> v.key().equals("managed"))).hasSize(2);
        assertThat(before.deleteMarkers()).hasSize(1);
        assertThat(reclaimer.reclaim(bucket, "managed")).isTrue();
        assertAbsent(bucket, "managed");
        assertThat(store.get(bucket, "managed-neighbour").data()).containsExactly((byte) 3);
        assertThat(reclaimer.reclaim(bucket, "managed")).isTrue();
    }

    @Test void suspendedVersioningReclaimsNullVersionAndEarlierVersions() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        put(bucket, "managed", 1);
        versioning(bucket, BucketVersioningStatus.SUSPENDED);
        put(bucket, "managed", 2);
        assertThat(reclaimer.reclaim(bucket, "managed")).isTrue();
        assertAbsent(bucket, "managed");
    }

    @Test void unversionedBytesAndLateRecreationAreReclaimedOnEachPass() {
        String bucket = bucket();
        put(bucket, "managed", 1);
        assertThat(reclaimer.reclaim(bucket, "managed")).isTrue();
        assertAbsent(bucket, "managed");
        put(bucket, "managed", 2);
        assertThat(reclaimer.reclaim(bucket, "managed")).isTrue();
        assertAbsent(bucket, "managed");
    }

    @Test void moreThanOneBatchRequiresAnotherPassBeforeReportingAbsence() {
        String bucket = bucket();
        versioning(bucket, BucketVersioningStatus.ENABLED);
        for (int i = 0; i < 1001; i++) put(bucket, "managed", i);
        assertThat(reclaimer.reclaim(bucket, "managed")).isFalse();
        assertThat(client.listObjectVersions(r -> r.bucket(bucket).prefix("managed")).versions()).hasSize(1);
        assertThat(reclaimer.reclaim(bucket, "managed")).isTrue();
        assertAbsent(bucket, "managed");
    }

    @Test void failedListingDoesNotBecomeSuccessfulCleanup() {
        assertThatThrownBy(() -> reclaimer.reclaim("missing-" + UUID.randomUUID(), "managed"))
                .isInstanceOf(software.amazon.awssdk.services.s3.model.S3Exception.class);
    }

    @Test void absentKeyDoesNotScanEveryPageOfPrefixNeighbours() {
        String bucket = bucket();
        for (int i = 0; i < 1001; i++) put(bucket, "managed-neighbour-" + i, i);
        var lists = new java.util.concurrent.atomic.AtomicInteger();
        S3Client observed = (S3Client) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {S3Client.class}, (proxy, method, args) -> {
                    if (method.getName().equals("listObjectVersions")) lists.incrementAndGet();
                    try { return method.invoke(client, args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        assertThat(new S3ObjectReclaimer(observed).reclaim(bucket, "managed")).isTrue();
        assertThat(lists).hasValue(2);
        assertThat(store.get(bucket, "managed-neighbour-1000").data()).containsExactly((byte) 1000);
    }

    @Test void capabilityCannotBeAdvertisedWithoutSupplyingItsPort() {
        assertThatThrownBy(() -> new ai.protomolt.proto.repo.blob.spi.OpenedBlobStore(store, () -> {},
                java.util.Set.of(ai.protomolt.proto.repo.blob.spi.BlobCapability.PHYSICAL_RECLAMATION)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires a reclaimer port");
    }

    private static void assertAbsent(String bucket, String key) {
        var listed = client.listObjectVersions(r -> r.bucket(bucket).prefix(key));
        assertThat(listed.versions().stream().filter(v -> v.key().equals(key))).isEmpty();
        assertThat(listed.deleteMarkers().stream().filter(v -> v.key().equals(key))).isEmpty();
        assertThatThrownBy(() -> store.get(bucket, key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
    }
    private static String bucket() {
        String name = "reclaim-" + UUID.randomUUID();
        client.createBucket(r -> r.bucket(name));
        return name;
    }
    private static void versioning(String bucket, BucketVersioningStatus status) {
        client.putBucketVersioning(r -> r.bucket(bucket).versioningConfiguration(v -> v.status(status)));
    }
    private static void put(String bucket, String key, int value) {
        store.put(new BlobStore.PutSpec(bucket, key, "application/octet-stream", null, null), new byte[] {(byte) value});
    }
}
