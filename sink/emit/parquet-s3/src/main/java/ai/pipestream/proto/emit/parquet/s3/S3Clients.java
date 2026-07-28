package ai.pipestream.proto.emit.parquet.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

/**
 * {@link S3Client} factories for the stores this module targets - the client-plane
 * counterpart of {@code S3Catalogs} in protomolt-iceberg-s3, which configures the same
 * stores for Iceberg's {@code S3FileIO}. Every client uses the JDK HttpURLConnection
 * HTTP client, so no Netty or Apache HttpClient lands on the classpath.
 *
 * <p>The store is the operator's choice, configured here, never a request argument - the
 * same disk-footprint stance the rest of the toolkit takes.</p>
 */
public final class S3Clients {

    private S3Clients() {
    }

    /**
     * A client for a self-hosted, path-style S3 store reached at {@code endpoint} (e.g.
     * {@code http://localhost:9000} for RustFS or SeaweedFS) with static credentials.
     * For real AWS S3, prefer {@link #awsRegion}, which lets the SDK resolve the region's
     * endpoint and pick up ambient credentials.
     *
     * @param endpoint base URL of the store
     * @param region region the store reports (RustFS and SeaweedFS accept any value)
     * @param accessKeyId access key
     * @param secretAccessKey secret key
     * @return the configured client
     */
    public static S3Client pathStyle(String endpoint, String region,
                                     String accessKeyId, String secretAccessKey) {
        return pathStyleBuilder(endpoint, region)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
                .build();
    }

    /**
     * A client for a self-hosted, path-style S3 store whose credentials come from the AWS
     * SDK's default provider chain (env, profile, IMDS).
     *
     * @param endpoint base URL of the store
     * @param region region the store reports
     * @return the configured client
     */
    public static S3Client pathStyle(String endpoint, String region) {
        return pathStyleBuilder(endpoint, region).build();
    }

    /**
     * A client for real AWS S3 in {@code region}: no endpoint override and no static
     * credentials, so the AWS SDK's default provider chain (env, profile, IMDS, IRSA)
     * supplies them.
     *
     * @param region the AWS region
     * @return the configured client
     */
    public static S3Client awsRegion(String region) {
        return S3Client.builder()
                .region(Region.of(region))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    /**
     * A client for real AWS S3 in {@code region} with static credentials instead of the
     * default provider chain.
     *
     * @param region the AWS region
     * @param accessKeyId access key
     * @param secretAccessKey secret key
     * @return the configured client
     */
    public static S3Client awsRegion(String region, String accessKeyId, String secretAccessKey) {
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    private static software.amazon.awssdk.services.s3.S3ClientBuilder pathStyleBuilder(
            String endpoint, String region) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .forcePathStyle(true)
                .httpClientBuilder(UrlConnectionHttpClient.builder());
    }
}
