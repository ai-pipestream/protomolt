package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * One byte of an object's data file is flipped on the pinned RustFS image's stopped volume,
 * the way the archive backup qualification's corrupt-payload case damages a restored volume.
 * The provider then answers every GET of that object with a complete 200 header set and a
 * body that ends before its declared length; HEAD still succeeds. The adapter reports that
 * end as UNAVAILABLE carrying the byte counts, never as DATA_LOSS, because a dropped
 * connection leaves the same wire picture. A closed port is the control: UNAVAILABLE without
 * byte counts.
 */
class RustFsDamagedObjectReadIT {
    static final String IMAGE = "rustfs/rustfs:1.0.0-beta.11-preview.1";
    private static final String ACCESS = "damaged-read-test";
    private static final String SECRET = "damaged-read-test-secret";
    private static final String BUCKET = "damaged-read-it";
    private static final String BIG_KEY = "entries/attachment.bin";
    private static final String SMALL_KEY = "entries/original";
    private static final int BIG_SIZE = 1_048_576;
    private static final String VOLUME = "protomolt-damaged-read-" + UUID.randomUUID();

    private static GenericContainer<?> rustfs;

    @BeforeAll static void createVolume() {
        DockerClientFactory.instance().client().createVolumeCmd().withName(VOLUME).exec();
        rustfs = start();
    }

    @AfterAll static void removeVolume() {
        try { if (rustfs != null) rustfs.stop(); }
        finally { DockerClientFactory.instance().client().removeVolumeCmd(VOLUME).exec(); }
    }

    @Test void damagedPartReadsAsUnavailableWithByteCountsAndNeverAsDataLoss() throws Exception {
        byte[] big = fixture("damaged object read fixture; synthetic bytes;", BIG_SIZE);
        byte[] small = fixture("intact sibling fixture;", 2048);
        String bigVersion, bigEtag;
        try (var client = client(rustfs)) {
            client.createBucket(builder -> builder.bucket(BUCKET));
            client.putBucketVersioning(builder -> builder.bucket(BUCKET)
                    .versioningConfiguration(configuration -> configuration.status(BucketVersioningStatus.ENABLED)));
            var store = new S3BlobStore(client);
            var put = store.put(new BlobStore.PutSpec(BUCKET, BIG_KEY, "application/octet-stream", null, sha256(big)), big);
            bigVersion = put.versionId(); bigEtag = put.eTag();
            assertThat(bigVersion).isNotBlank();
            store.put(new BlobStore.PutSpec(BUCKET, SMALL_KEY, "application/octet-stream", null, sha256(small)), small);
            assertThat(store.getBounded(BUCKET, BIG_KEY, bigVersion, BIG_SIZE).data()).isEqualTo(big);
            assertThat(store.get(BUCKET, BIG_KEY, bigVersion).data()).isEqualTo(big);
        }

        // Explicit damage outside any supported writer, on the stopped volume: one byte of the single data file behind the key.
        rustfs.stop();
        String injection = flipOneByte();
        System.out.println("RUSTFS_DAMAGE " + injection.replace('\n', ';'));
        assertThat(injection).contains("before=").contains("after=");
        rustfs = start();

        try (var client = client(rustfs)) {
            awaitServing(client);
            var store = new S3BlobStore(client);
            // HEAD gives no signal: the provider verifies the data block only when it serves it.
            var head = client.headObject(builder -> builder.bucket(BUCKET).key(BIG_KEY).versionId(bigVersion));
            assertThat(head.contentLength()).isEqualTo((long) BIG_SIZE);
            assertThat(head.eTag()).isEqualTo(bigEtag);

            var bounded = catchThrowable(() -> store.getBounded(BUCKET, BIG_KEY, bigVersion, BIG_SIZE));
            System.out.println("RUSTFS_DAMAGE bounded -> " + chain(bounded));
            assertThat(bounded).isInstanceOfSatisfying(BlobStoreException.class, failure -> {
                assertThat(failure.code()).as("short body is not provable damage").isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                assertThat(failure.getMessage()).contains("received 0 of " + BIG_SIZE + " declared bytes");
                assertThat(failure.getMessage()).doesNotContain(BUCKET, BIG_KEY, bigVersion, SECRET, rustfs.getHost());
                assertThat(causes(failure)).anyMatch(cause -> cause instanceof IOException);
            });

            var whole = catchThrowable(() -> store.get(BUCKET, BIG_KEY, bigVersion));
            System.out.println("RUSTFS_DAMAGE whole -> " + chain(whole));
            assertThat(whole).isInstanceOfSatisfying(BlobStoreException.class, failure -> {
                assertThat(failure.code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                assertThat(causes(failure)).as("the SDK retried the whole-object read and gave up")
                        .anyMatch(cause -> cause instanceof SdkClientException sdk && sdk.getMessage() != null && sdk.getMessage().contains("SDK Attempt Count"));
            });

            // The damage is confined to that object: the sibling reads byte-equal and the damaged object's second read is identical.
            assertThat(store.getBounded(BUCKET, SMALL_KEY, null, 2048).data()).isEqualTo(small);
            assertThatThrownBy(() -> store.getBounded(BUCKET, BIG_KEY, bigVersion, BIG_SIZE)).isInstanceOfSatisfying(BlobStoreException.class,
                    failure -> assertThat(failure.getMessage()).contains("received 0 of " + BIG_SIZE + " declared bytes"));
        }

        // Control: the same endpoint identity with nothing listening is the plain outage, without byte counts.
        rustfs.stop();
        try (var client = client(URI.create("http://" + rustfs.getHost() + ":" + mappedPortBeforeStop))) {
            assertThatThrownBy(() -> new S3BlobStore(client).getBounded(BUCKET, BIG_KEY, bigVersion, BIG_SIZE))
                    .isInstanceOfSatisfying(BlobStoreException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(BlobStoreException.Code.UNAVAILABLE);
                        assertThat(failure.getMessage()).doesNotContain("received");
                        assertThat(failure).hasCauseInstanceOf(SdkClientException.class);
                    });
        }
    }

    private static int mappedPortBeforeStop;

    private static GenericContainer<?> start() {
        var container = new GenericContainer<>(IMAGE)
                .withCommand("/data")
                .withEnv("RUSTFS_VOLUMES", "/data")
                .withEnv("RUSTFS_ADDRESS", ":9000")
                .withEnv("RUSTFS_CONSOLE_ENABLE", "false")
                .withEnv("RUSTFS_ACCESS_KEY", ACCESS)
                .withEnv("RUSTFS_SECRET_KEY", SECRET)
                .withExposedPorts(9000)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withBinds(new Bind(VOLUME, new Volume("/data"))))
                .waitingFor(Wait.forHttp("/health").forPort(9000));
        container.start();
        mappedPortBeforeStop = container.getMappedPort(9000);
        return container;
    }

    /** The volume's single data file larger than the inline threshold under the key, flipped at offset 4096. */
    private static String flipOneByte() {
        String script = "set -e; cd /data/" + BUCKET + "/" + BIG_KEY + "; files=$(find . -type f -size +524288c | sort); "
                + "count=$(printf '%s\\n' \"$files\" | grep -c . || true); "
                + "if [ \"$count\" -ne 1 ]; then echo \"expected exactly one data file, found $count\"; find . -type f -exec ls -l {} +; exit 3; fi; "
                + "f=\"$files\"; echo \"file=$f\"; sha256sum \"$f\"; v=$(dd if=\"$f\" bs=1 skip=4096 count=1 status=none | od -An -tu1 | tr -d ' '); "
                + "n=$(( (v ^ 1) & 255 )); printf \"$(printf '\\\\%03o' $n)\" | dd of=\"$f\" bs=1 seek=4096 conv=notrunc status=none; "
                + "sha256sum \"$f\"; echo \"offset=4096 before=$v after=$n\"";
        try (var helper = new GenericContainer<>(IMAGE)
                .withCreateContainerCmdModifier(command -> {
                    command.withEntrypoint("sh");
                    command.getHostConfig().withBinds(new Bind(VOLUME, new Volume("/data")));
                })
                .withCommand("-c", script)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(60)))) {
            helper.start();
            return helper.getLogs().strip();
        }
    }

    /** A restarted RustFS answers 503 with x-rustfs-readiness-pending until its subsystems are up; wait for a real answer. */
    private static void awaitServing(S3Client client) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            try { client.headObject(builder -> builder.bucket(BUCKET).key(SMALL_KEY)); return; }
            catch (S3Exception pending) {
                if (pending.statusCode() != 503 || System.nanoTime() > deadline) throw pending;
                Thread.sleep(250);
            }
        }
    }

    private static S3Client client(GenericContainer<?> container) {
        return client(URI.create("http://" + container.getHost() + ":" + container.getMappedPort(9000)));
    }

    private static S3Client client(URI endpoint) {
        return S3Client.builder().endpointOverride(endpoint)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS, SECRET)))
                .region(Region.US_EAST_1).httpClient(UrlConnectionHttpClient.create()).forcePathStyle(true).build();
    }

    private static byte[] fixture(String label, int size) {
        byte[] seed = label.getBytes(StandardCharsets.US_ASCII);
        byte[] bytes = new byte[size];
        for (int index = 0; index < size; index++) bytes[index] = seed[index % seed.length];
        return bytes;
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static List<Throwable> causes(Throwable failure) {
        var chain = new ArrayList<Throwable>();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) chain.add(cause);
        return chain;
    }

    private static String chain(Throwable failure) {
        var text = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (!text.isEmpty()) text.append(" <- ");
            text.append(cause.getClass().getSimpleName());
            if (cause instanceof BlobStoreException provider) text.append('(').append(provider.code()).append(')');
            text.append(": ").append(cause.getMessage());
        }
        return text.toString();
    }
}
