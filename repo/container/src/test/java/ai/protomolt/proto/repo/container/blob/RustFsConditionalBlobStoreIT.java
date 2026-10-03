package ai.protomolt.proto.repo.container.blob;

import ai.protomolt.proto.repo.blob.spi.BlobStore;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual conditional-write qualification against the deployment-pinned RustFS image. */
@Testcontainers(disabledWithoutDocker = true)
class RustFsConditionalBlobStoreIT {
    private static final String BUCKET = "conditional-it";

    @Container
    static final GenericContainer<?> RUSTFS = new GenericContainer<>(
            DockerImageName.parse("rustfs/rustfs:1.0.0-beta.11-preview.1"))
            .withCommand("/data")
            .withEnv("RUSTFS_VOLUMES", "/data")
            .withEnv("RUSTFS_ADDRESS", ":9000")
            .withEnv("RUSTFS_CONSOLE_ENABLE", "false")
            .withEnv("RUSTFS_ACCESS_KEY", "conditional-test")
            .withEnv("RUSTFS_SECRET_KEY", "conditional-test-secret")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/health").forPort(9000));

    static S3BlobStore store;
    static S3Client client;

    @BeforeAll
    static void setup() {
        client = S3Client.builder()
                .endpointOverride(java.net.URI.create("http://" + RUSTFS.getHost() + ":"
                        + RUSTFS.getMappedPort(9000)))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create("conditional-test", "conditional-test-secret")))
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true).build();
        client.createBucket(builder -> builder.bucket(BUCKET));
        store = new S3BlobStore(client, true);
    }

    @AfterAll
    static void closeClient() {
        if (client != null) client.close();
    }

    @Test
    void absentAndMatchingWritesFenceStaleSnapshots() {
        var spec = spec("fencing");
        assertThatThrownBy(() -> store.getForUpdate(BUCKET, "no-such-transcript"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        byte[] first = bytes("encrypted-snapshot-one");
        var created = store.conditionalPut(spec, first, BlobStore.WriteCondition.absent());
        var read = store.getForUpdate(BUCKET, spec.key());
        assertThat(read.data()).isEqualTo(first);
        assertThat(read.eTag()).isEqualTo(created.eTag());
        assertThatThrownBy(() -> store.conditionalPut(spec, bytes("other"),
                BlobStore.WriteCondition.absent()))
                .isInstanceOf(BlobStore.BlobConflictException.class);
        var replaced = store.conditionalPut(spec, bytes("encrypted-snapshot-two"),
                BlobStore.WriteCondition.matching(read.eTag()));
        assertThat(replaced.eTag()).isNotEqualTo(read.eTag());
        assertThatThrownBy(() -> store.conditionalPut(spec, bytes("delayed-old"),
                BlobStore.WriteCondition.matching(read.eTag())))
                .isInstanceOf(BlobStore.BlobConflictException.class);
        assertThat(store.getForUpdate(BUCKET, spec.key()).data())
                .isEqualTo(bytes("encrypted-snapshot-two"));
    }

    @Test
    void twoConcurrentCreatesAndTwoMatchingReplacementsHaveOneWinner() throws Exception {
        var spec = spec("race");
        race(spec, BlobStore.WriteCondition.absent(), "create");
        var read = store.getForUpdate(BUCKET, spec.key());
        race(spec, BlobStore.WriteCondition.matching(read.eTag()), "replace");
        var after = store.getForUpdate(BUCKET, spec.key());
        assertThat(new String(after.data(), StandardCharsets.UTF_8)).startsWith("replace-");
        assertThat(after.eTag()).isNotEqualTo(read.eTag());
    }

    private static void race(BlobStore.PutSpec spec, BlobStore.WriteCondition condition,
            String prefix) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> attempt(spec, condition, bytes(prefix + "-one"), start));
            var second = workers.submit(() -> attempt(spec, condition, bytes(prefix + "-two"), start));
            start.countDown();
            assertThat(first.get(30, TimeUnit.SECONDS) + second.get(30, TimeUnit.SECONDS)).isEqualTo(1);
        }
    }

    private static int attempt(BlobStore.PutSpec spec, BlobStore.WriteCondition condition,
            byte[] bytes, CountDownLatch start) throws Exception {
        start.await();
        try {
            store.conditionalPut(spec, bytes, condition);
            return 1;
        } catch (BlobStore.BlobConflictException conflict) {
            return 0;
        }
    }

    private static BlobStore.PutSpec spec(String key) {
        return new BlobStore.PutSpec(BUCKET, key, "application/octet-stream", null, null);
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
