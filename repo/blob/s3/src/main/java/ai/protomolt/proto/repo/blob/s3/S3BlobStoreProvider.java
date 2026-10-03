package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStoreProvider;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** S3 factory with explicitly selected static credentials or the AWS default chain. */
public final class S3BlobStoreProvider implements BlobStoreProvider {
    @Override public String id() { return "s3"; }

    @Override public OpenedBlobStore open(Map<String, String> options) {
        // The original explicit key-pair form remains supported.
        String mode = options.containsKey("credentials-mode") ? options.get("credentials-mode") : "static";
        if (!Set.of("static", "default-chain").contains(mode)) {
            throw new IllegalArgumentException("S3 credentials-mode must be static or default-chain");
        }
        var keys = new HashSet<>(Set.of("endpoint", "region", "path-style", "conditional-writes"));
        if (options.containsKey("credentials-mode")) keys.add("credentials-mode");
        if (mode.equals("static")) keys.addAll(Set.of("access-key", "secret-key"));
        if (!options.keySet().equals(keys)) throw new IllegalArgumentException("Invalid S3 option set");
        for (String key : keys) {
            if (options.get(key) == null || (!key.equals("endpoint") && options.get(key).isBlank())) {
                throw new IllegalArgumentException("Missing S3 option: " + key);
            }
        }
        URI endpoint = null;
        if (!options.get("endpoint").isEmpty()) {
            try { endpoint = URI.create(options.get("endpoint")); }
            catch (IllegalArgumentException e) { throw new IllegalArgumentException("Invalid S3 endpoint URI"); }
            if (!("http".equals(endpoint.getScheme()) || "https".equals(endpoint.getScheme()))
                    || endpoint.getHost() == null || endpoint.getUserInfo() != null
                    || endpoint.getQuery() != null || endpoint.getFragment() != null) {
                throw new IllegalArgumentException("S3 endpoint requires http/https and a host without user info, query or fragment");
            }
        }
        boolean pathStyle = bool(options, "path-style");
        boolean conditional = bool(options, "conditional-writes");
        Region region = Region.of(options.get("region"));
        AwsCredentialsProvider credentials = mode.equals("static")
                ? StaticCredentialsProvider.create(AwsBasicCredentials.create(options.get("access-key"), options.get("secret-key")))
                : DefaultCredentialsProvider.builder().build();
        S3Client acquired = null;
        try {
            var builder = S3Client.builder().region(region).credentialsProvider(credentials)
                    .forcePathStyle(pathStyle).httpClientBuilder(UrlConnectionHttpClient.builder());
            if (endpoint != null) builder.endpointOverride(endpoint);
            acquired = builder.build();
            S3Client client = acquired;
            var capabilities = EnumSet.of(BlobCapability.LIST, BlobCapability.SERVER_SIDE_COPY, BlobCapability.STREAMING_WRITE);
            if (conditional) capabilities.addAll(Set.of(BlobCapability.AUTHORITATIVE_CONDITIONAL_READ,
                    BlobCapability.ATOMIC_CONDITIONAL_WRITE));
            return new OpenedBlobStore(new S3BlobStore(client, conditional),
                    () -> close(client, credentials), capabilities, new S3NamespaceProvisioner(client));
        } catch (RuntimeException | Error failure) {
            try { close(acquired, credentials); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static void close(S3Client client, AwsCredentialsProvider credentials) throws Exception {
        Exception failure = null;
        try { if (client != null) client.close(); } catch (Exception e) { failure = e; }
        try { if (credentials instanceof AutoCloseable closeable) closeable.close(); }
        catch (Exception e) {
            if (failure == null) failure = e;
            else if (failure != e) failure.addSuppressed(e);
        }
        if (failure != null) throw failure;
    }

    private static boolean bool(Map<String, String> options, String key) {
        return switch (options.get(key)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(key + " must be true or false");
        };
    }
}
