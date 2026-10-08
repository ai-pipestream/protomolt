package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import static org.assertj.core.api.Assertions.*;

/** Provisioned namespaces are versioned, so committed objects carry a provider version id. */
@Testcontainers
class S3NamespaceProvisionerIT {
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static S3Client client;
    static S3NamespaceProvisioner provisioner;

    @BeforeAll static void boot() {
        client = S3Client.builder().endpointOverride(S3.getEndpoint()).region(Region.of(S3.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .forcePathStyle(true).build();
        provisioner = new S3NamespaceProvisioner(client);
    }
    @AfterAll static void close() { if (client != null) client.close(); }

    @Test void aNewNamespaceIsCreatedWithVersioningEnabled() {
        String bucket = bucket();
        provisioner.ensureNamespace(bucket);
        assertThat(status(bucket)).isEqualTo(BucketVersioningStatus.ENABLED);
        var written = new S3BlobStore(client).put(new BlobStore.PutSpec(bucket, "managed/object", "application/octet-stream", null, null), new byte[] {1});
        assertThat(written.versionId()).as("a provisioned namespace returns a provider version id").isNotBlank().isNotEqualTo("null");
    }

    @Test void anExistingUnversionedBucketIsAdoptedByEnablingVersioning() {
        String bucket = bucket();
        client.createBucket(b -> b.bucket(bucket));
        assertThat(status(bucket)).isNull();
        provisioner.ensureNamespace(bucket);
        assertThat(status(bucket)).isEqualTo(BucketVersioningStatus.ENABLED);
    }

    @Test void aSuspendedBucketIsReEnabledAndRepeatedCallsAreIdempotent() {
        String bucket = bucket();
        client.createBucket(b -> b.bucket(bucket));
        client.putBucketVersioning(b -> b.bucket(bucket).versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
        client.putBucketVersioning(b -> b.bucket(bucket).versioningConfiguration(v -> v.status(BucketVersioningStatus.SUSPENDED)));
        provisioner.ensureNamespace(bucket);
        provisioner.ensureNamespace(bucket);
        assertThat(status(bucket)).isEqualTo(BucketVersioningStatus.ENABLED);
    }

    private static BucketVersioningStatus status(String bucket) {
        return client.getBucketVersioning(b -> b.bucket(bucket)).status();
    }
    private static String bucket() {
        return "provision-" + UUID.randomUUID().toString().substring(0, 12);
    }
}
