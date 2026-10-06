package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.blob.s3.S3ObjectReclaimer;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import static org.assertj.core.api.Assertions.*;

/** Real SDK timeout followed by a real remote effect; no repository recovery claim yet. */
@Testcontainers
class DelayedS3PutGatewayIT {
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @Test void originalSignedPutArrivesAfterSdkCallAndInboundHandlerHaveStopped() throws Exception {
        String bucket = "delayed-put", key = "documents/" + UUID.randomUUID();
        byte[] body = "actual delayed provider bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (var direct = client(S3.getEndpoint(), Duration.ofSeconds(10));
             var gateway = new DelayedS3PutGateway(S3.getEndpoint(), "/" + bucket + "/" + key);
             var delayed = client(S3.getEndpoint(), Duration.ofSeconds(1), gateway.endpoint());
             var worker = Executors.newVirtualThreadPerTaskExecutor()) {
            direct.createBucket(b -> b.bucket(bucket));
            direct.putBucketVersioning(b -> b.bucket(bucket).versioningConfiguration(v -> v.status("Enabled")));
            var call = worker.submit(() -> new S3BlobStore(delayed).put(new BlobStore.PutSpec(bucket, key,
                    "application/octet-stream", Map.of(), DocumentPartCodec.sha256Hex(body)), body));
            gateway.awaitCaptured();
            assertThatThrownBy(() -> call.get(5, TimeUnit.SECONDS)).isInstanceOfSatisfying(ExecutionException.class,
                    failure -> assertThat(failure.getCause()).isInstanceOf(ApiCallTimeoutException.class));
            assertThat(call.isDone()).isTrue();
            gateway.disconnectClient();
            assertThat(gateway.requests()).isEqualTo(1);
            assertThatThrownBy(() -> direct.headObject(b -> b.bucket(bucket).key(key)))
                    .isInstanceOfSatisfying(S3Exception.class, failure -> assertThat(failure.statusCode()).isEqualTo(404));
            var reclaimer = new S3ObjectReclaimer(direct);
            // A prefix-sharing neighbor must survive both cleanup passes.
            String neighbor = key + "-neighbor";
            var neighborVersion = new S3BlobStore(direct).put(new BlobStore.PutSpec(bucket, neighbor,
                    "application/octet-stream", Map.of(), DocumentPartCodec.sha256Hex(body)), body).versionId();
            assertThat(reclaimer.reclaim(bucket, key)).isTrue();
            assertThat(new S3BlobStore(direct).getBounded(bucket, neighbor, neighborVersion, body.length).data()).containsExactly(body);
            assertThatThrownBy(() -> gateway.forwardTo(URI.create("https://127.0.0.1/")))
                    .hasMessageContaining("plain HTTP test upstream");
            var delivered = gateway.forwardTo(S3.getEndpoint());
            assertThat(delivered.status()).isEqualTo(200);
            assertThat(delivered.version()).isNotBlank();
            var result = new S3BlobStore(direct).getBounded(bucket, key, delivered.version(), body.length);
            assertThat(result.data()).containsExactly(body);
            assertThat(result.versionId()).isEqualTo(delivered.version());
            assertThat(reclaimer.reclaim(bucket, key)).isTrue();
            assertThatThrownBy(() -> direct.headObject(b -> b.bucket(bucket).key(key).versionId(delivered.version())))
                    .isInstanceOfSatisfying(S3Exception.class, failure -> assertThat(failure.statusCode()).isEqualTo(404));
            assertThat(new S3BlobStore(direct).getBounded(bucket, neighbor, neighborVersion, body.length).data()).containsExactly(body);
            var versions = direct.listObjectVersions(b -> b.bucket(bucket).prefix(key));
            assertThat(versions.versions()).noneMatch(v -> v.key().equals(key));
            assertThat(versions.deleteMarkers()).noneMatch(v -> v.key().equals(key));
            assertThat(gateway.requests()).isEqualTo(1);
            assertThatThrownBy(() -> gateway.forwardTo(S3.getEndpoint())).hasMessageContaining("one forwarding attempt");
        }
    }

    static S3Client client(URI endpoint, Duration timeout) {
        return client(endpoint, timeout, null);
    }
    static S3Client client(URI endpoint, Duration timeout, URI proxy) {
        var http = UrlConnectionHttpClient.builder().connectionTimeout(Duration.ofSeconds(1)).socketTimeout(Duration.ofSeconds(5));
        if (proxy != null) http.proxyConfiguration(c -> c.endpoint(proxy).nonProxyHosts(Set.of())
                .useSystemPropertyValues(false).useEnvironmentVariablesValues(false));
        return S3Client.builder().endpointOverride(endpoint).region(Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .serviceConfiguration(c -> c.chunkedEncodingEnabled(false).expectContinueEnabled(false))
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .httpClientBuilder(http)
                .overrideConfiguration(c -> c.apiCallTimeout(timeout).retryStrategy(r -> r.maxAttempts(1))).build();
    }
}
