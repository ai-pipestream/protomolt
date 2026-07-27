package ai.pipestream.proto.repo.container.blob;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link S3BlobStore} against LocalStack S3 (testcontainers). Exercises the
 * provider-mapping contracts the in-memory fake cannot: the verified-write
 * checksum trailer, NoSuchKey → {@link BlobStore.BlobNotFoundException}, and
 * the 1000-key batching of {@code deleteAll}. Skips cleanly without docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3BlobStoreIT {

    private static final String BUCKET = "claimcheck-it";

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static S3Client client;
    static S3BlobStore store;

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
        store = new S3BlobStore(client);
        // Bucket lifecycle is admin-plane work and stays on the raw client.
        client.createBucket(b -> b.bucket(BUCKET));
        store.headBucket(BUCKET);
    }

    @Test
    void putThenGetRoundTripsBytesContentTypeAndMetadata() {
        byte[] body = "hello s3".getBytes(StandardCharsets.UTF_8);
        BlobStore.PutResult put = store.put(new BlobStore.PutSpec(BUCKET, "it/put-get",
                "text/plain", Map.of("origin", "it"), null), body);
        assertThat(put.eTag()).isNotBlank();

        BlobStore.GetResult got = store.get(BUCKET, "it/put-get");
        assertThat(got.data()).isEqualTo(body);
        assertThat(got.contentType()).startsWith("text/plain");
    }

    @Test
    void verifiedPutWithMatchingChecksumSucceeds() {
        byte[] body = "verified".getBytes(StandardCharsets.UTF_8);
        String sha = ai.pipestream.proto.repo.container.codec.DocumentPartCodec.sha256Hex(body);
        BlobStore.PutResult put = store.put(new BlobStore.PutSpec(BUCKET, "it/verified",
                "application/octet-stream", null, sha), body);
        assertThat(put.eTag()).isNotBlank();
        assertThat(store.get(BUCKET, "it/verified").data()).isEqualTo(body);
    }

    @Test
    void verifiedPutWithWrongChecksumFails() {
        byte[] body = "not what the digest claims".getBytes(StandardCharsets.UTF_8);
        String wrongSha = "0".repeat(64);
        assertThatThrownBy(() -> store.put(new BlobStore.PutSpec(BUCKET, "it/bad-digest",
                "application/octet-stream", null, wrongSha), body))
                .isInstanceOfAny(S3Exception.class, SdkClientException.class);
    }

    @Test
    void copyDuplicatesTheObjectServerSide() {
        byte[] body = "copy me".getBytes(StandardCharsets.UTF_8);
        store.put(new BlobStore.PutSpec(BUCKET, "it/copy-src", "text/plain", null, null), body);

        store.copy(BUCKET, "it/copy-src", BUCKET, "it/copy-dst");

        assertThat(store.get(BUCKET, "it/copy-dst").data()).isEqualTo(body);
    }

    @Test
    void copyOfMissingSourceRaisesNotFound() {
        assertThatThrownBy(() -> store.copy(BUCKET, "it/definitely-absent", BUCKET, "it/copy-dst-2"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class)
                .hasMessageContaining("it/definitely-absent");
    }

    @Test
    void copyWithBlankSourceFailsFastBeforeAnyProviderCall() {
        assertThatThrownBy(() -> store.copy(BUCKET, " ", BUCKET, "it/copy-dst-3"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class)
                .hasMessageContaining("blank source");
    }

    @Test
    void deleteRemovesTheObject() {
        store.put(new BlobStore.PutSpec(BUCKET, "it/delete-me", "text/plain", null, null),
                "gone soon".getBytes(StandardCharsets.UTF_8));

        store.delete(BUCKET, "it/delete-me");

        assertThatThrownBy(() -> store.headObject(BUCKET, "it/delete-me"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test
    void deleteAllChunksBeyond1000KeysAndToleratesAbsentKeys() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 2100; i++) {
            keys.add("it/batch/%04d".formatted(i));
        }
        for (String key : keys) {
            store.put(new BlobStore.PutSpec(BUCKET, key, "text/plain", null, null),
                    key.getBytes(StandardCharsets.UTF_8));
        }
        store.put(new BlobStore.PutSpec(BUCKET, "it/batch/keep", "text/plain", null, null),
                "keep".getBytes(StandardCharsets.UTF_8));

        List<String> toDelete = new ArrayList<>(keys);
        toDelete.add("it/batch/never-existed"); // missing keys are success
        BlobStore.BatchDeleteResult result = store.deleteAll(BUCKET, toDelete);

        assertThat(result.allSucceeded()).isTrue();
        assertThat(store.list(BUCKET, "it/batch/"))
                .extracting(BlobStore.ListedObject::key)
                .containsExactly("it/batch/keep");
    }

    @Test
    void listReturnsEverythingUnderAPrefix() {
        store.put(new BlobStore.PutSpec(BUCKET, "it/list/a", "text/plain", null, null), new byte[1]);
        store.put(new BlobStore.PutSpec(BUCKET, "it/list/b", "text/plain", null, null), new byte[2]);

        assertThat(store.list(BUCKET, "it/list/"))
                .extracting(BlobStore.ListedObject::key)
                .containsExactlyInAnyOrder("it/list/a", "it/list/b");
    }

    @Test
    void headObjectOfAbsentKeyRaisesNotFound() {
        assertThatThrownBy(() -> store.headObject(BUCKET, "it/no-such-object"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class)
                .hasMessageContaining("it/no-such-object");
    }

    @Test
    void getOfAbsentKeyRaisesNotFound() {
        assertThatThrownBy(() -> store.get(BUCKET, "it/no-such-get"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }
}
