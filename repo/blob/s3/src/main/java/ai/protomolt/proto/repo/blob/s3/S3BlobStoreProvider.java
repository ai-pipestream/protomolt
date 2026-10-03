package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStoreProvider;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Explicit endpoint/static-credential factory for S3-compatible storage. */
public final class S3BlobStoreProvider implements BlobStoreProvider {
    private static final Set<String> OPTIONS = Set.of("endpoint", "region", "access-key",
            "secret-key", "path-style", "conditional-writes");
    @Override public String id() { return "s3"; }

    @Override public OpenedBlobStore open(Map<String, String> options) {
        if (!options.keySet().equals(OPTIONS)) {
            throw new IllegalArgumentException("S3 requires endpoint, region, access-key, secret-key, path-style and conditional-writes only");
        }
        for (String key : OPTIONS) {
            if (options.get(key) == null || options.get(key).isBlank()) {
                throw new IllegalArgumentException("Missing S3 option: " + key);
            }
        }
        URI endpoint;
        try { endpoint = URI.create(options.get("endpoint")); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("Invalid S3 endpoint URI"); }
        if (!("http".equals(endpoint.getScheme()) || "https".equals(endpoint.getScheme()))
                || endpoint.getHost() == null || endpoint.getUserInfo() != null
                || endpoint.getQuery() != null || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("S3 endpoint requires http/https and a host without user info, query or fragment");
        }
        boolean pathStyle = bool(options, "path-style");
        boolean conditional = bool(options, "conditional-writes");
        S3Client client = S3Client.builder().endpointOverride(endpoint)
                .region(Region.of(options.get("region")))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        options.get("access-key"), options.get("secret-key"))))
                .forcePathStyle(pathStyle).httpClientBuilder(UrlConnectionHttpClient.builder()).build();
        try {
            var capabilities = java.util.EnumSet.of(
                    ai.protomolt.proto.repo.blob.spi.BlobCapability.LIST,
                    ai.protomolt.proto.repo.blob.spi.BlobCapability.SERVER_SIDE_COPY,
                    ai.protomolt.proto.repo.blob.spi.BlobCapability.STREAMING_WRITE);
            if (conditional) {
                capabilities.add(ai.protomolt.proto.repo.blob.spi.BlobCapability.AUTHORITATIVE_CONDITIONAL_READ);
                capabilities.add(ai.protomolt.proto.repo.blob.spi.BlobCapability.ATOMIC_CONDITIONAL_WRITE);
            }
            return new OpenedBlobStore(new S3BlobStore(client, conditional), client, capabilities);
        } catch (RuntimeException | Error failure) {
            try { client.close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static boolean bool(Map<String, String> options, String key) {
        return switch (options.get(key)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(key + " must be true or false");
        };
    }
}
