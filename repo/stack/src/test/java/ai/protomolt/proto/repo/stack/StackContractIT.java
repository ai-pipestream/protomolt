package ai.protomolt.proto.repo.stack;

import java.net.URI;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/** The stack contract against S3 on LocalStack. */
@Testcontainers
class StackContractIT extends AbstractStackContractIT {
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
            .withServices("s3");
    private static final String BUCKET = "stack-contract";
    private static S3Client client;

    @BeforeAll static void createBucket() {
        client = S3Client.builder().endpointOverride(URI.create(S3.getEndpoint().toString()))
                .region(Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .httpClient(UrlConnectionHttpClient.create()).build();
        client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }

    @Override S3Client client() { return client; }
    @Override String bucket() { return BUCKET; }
    @Override String endpoint() { return S3.getEndpoint().toString(); }
    @Override String region() { return S3.getRegion(); }
    @Override String accessKey() { return S3.getAccessKey(); }
    @Override String secretKey() { return S3.getSecretKey(); }
}
