package ai.protomolt.proto.repo.stack;

import java.net.URI;
import org.junit.jupiter.api.BeforeAll;
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
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * The stack contract against RustFS (requirements §15 preamble: S3 correctness on
 * LocalStack, performance figures on RustFS). Qualifying the same contract here
 * keeps the RustFS performance claims on a store that also passes correctness.
 */
@Testcontainers
class RustFsStackContractIT extends AbstractStackContractIT {
    private static final String ACCESS_KEY = "rustfs-test";
    private static final String SECRET_KEY = "rustfs-test-secret";

    @Container static final GenericContainer<?> RUSTFS = new GenericContainer<>(
            DockerImageName.parse("rustfs/rustfs:1.0.0-beta.11-preview.1"))
            .withCommand("/data")
            .withEnv("RUSTFS_VOLUMES", "/data")
            .withEnv("RUSTFS_ADDRESS", ":9000")
            .withEnv("RUSTFS_CONSOLE_ENABLE", "false")
            .withEnv("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .withEnv("RUSTFS_SECRET_KEY", SECRET_KEY)
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/health").forPort(9000));

    private static final String BUCKET = "stack-contract";
    private static S3Client client;

    private static String endpointUrl() {
        return "http://" + RUSTFS.getHost() + ":" + RUSTFS.getMappedPort(9000);
    }

    @BeforeAll static void createBucket() {
        client = S3Client.builder().endpointOverride(URI.create(endpointUrl()))
                .region(Region.of("us-east-1")).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .httpClient(UrlConnectionHttpClient.create()).build();
        client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }

    @Override S3Client client() { return client; }
    @Override String bucket() { return BUCKET; }
    @Override String endpoint() { return endpointUrl(); }
    @Override String region() { return "us-east-1"; }
    @Override String accessKey() { return ACCESS_KEY; }
    @Override String secretKey() { return SECRET_KEY; }
}
