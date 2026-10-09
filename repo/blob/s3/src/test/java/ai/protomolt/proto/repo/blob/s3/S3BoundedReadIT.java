package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.util.Map;
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
import static org.assertj.core.api.Assertions.*;

/** Successful reads use real versioned storage, including exact-boundary and empty objects. */
@Testcontainers
class S3BoundedReadIT {
    @Container static final LocalStackContainer BACKEND = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static S3Client client;
    static S3BlobStore store;
    static final String BUCKET = "bounded-read-it";

    @BeforeAll static void open() {
        client = S3Client.builder().endpointOverride(BACKEND.getEndpoint()).forcePathStyle(true)
                .region(Region.of(BACKEND.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        BACKEND.getAccessKey(), BACKEND.getSecretKey())))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
        store = new S3BlobStore(client);
        client.createBucket(b -> b.bucket(BUCKET));
        client.putBucketVersioning(b -> b.bucket(BUCKET).versioningConfiguration(
                v -> v.status(BucketVersioningStatus.ENABLED)));
    }
    @AfterAll static void close() { if (client != null) client.close(); }

    @Test void returnsExactRequestedVersionAndIdentityAtLimit() {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        var original = store.put(new BlobStore.PutSpec(BUCKET, "versioned", "application/octet-stream", Map.of(), null), bytes);
        store.put(new BlobStore.PutSpec(BUCKET, "versioned", "text/plain", Map.of(), null), new byte[20]);
        assertThat(original.versionId()).isNotBlank();
        var read = store.getBounded(BUCKET, "versioned", original.versionId(), bytes.length);
        assertThat(read.data()).isEqualTo(bytes);
        assertThat(read.versionId()).isEqualTo(original.versionId());
        assertThat(read.eTag()).isEqualTo(original.eTag());
        assertThat(read.contentType()).isEqualTo("application/octet-stream");
        assertThatThrownBy(() -> store.getBounded(BUCKET, "versioned", null, bytes.length))
                .isInstanceOf(BlobStore.BlobReadLimitException.class);
    }

    @Test void zeroLimitAllowsOnlyEmptyObjects() {
        store.put(new BlobStore.PutSpec(BUCKET, "empty", "application/octet-stream", Map.of(), null), new byte[0]);
        assertThat(store.getBounded(BUCKET, "empty", null, 0).data()).isEmpty();
        store.put(new BlobStore.PutSpec(BUCKET, "one-byte", "application/octet-stream", Map.of(), null), new byte[1]);
        assertThatThrownBy(() -> store.getBounded(BUCKET, "one-byte", null, 0))
                .isInstanceOf(BlobStore.BlobReadLimitException.class);
    }

    @Test void missingObjectRemainsDistinct() {
        assertThatThrownBy(() -> store.getBounded(BUCKET, "absent", null, 100))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test void negativeLimitFailsBeforeProviderRequest() {
        assertThatThrownBy(() -> store.getBounded("nonexistent", "absent", null, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
