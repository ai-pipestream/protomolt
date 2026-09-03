package ai.protomolt.proto.acquire.s3;

import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.acquire.pull.GrpcIntakeFeed;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.Map;
import java.util.Set;

/**
 * The S3 pull connector as a mountable role: wiring builds the source-side client from the
 * module config, opens the intake feed over the node's channels (in-process when the service is
 * co-mounted), and contributes the {@code pull-s3} verb for the registry's actions route.
 * Nothing runs on a schedule — a pull happens when the verb is called, and the caller owns
 * the watermark.
 */
public final class S3PullModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "acquire-s3";

    /** Environment variable naming the connector's intake API key. */
    public static final String ENV_API_KEY = "PROTOMOLT_ACQUIRE_API_KEY";

    /** Environment variable naming the source S3 endpoint (omit for AWS). */
    public static final String ENV_ENDPOINT = "PROTOMOLT_ACQUIRE_S3_ENDPOINT";

    /** Environment variable naming the source S3 region. */
    public static final String ENV_REGION = "PROTOMOLT_ACQUIRE_S3_REGION";

    /** Environment variable naming the source S3 access key (omit for the default chain). */
    public static final String ENV_ACCESS_KEY = "PROTOMOLT_ACQUIRE_S3_ACCESS_KEY";

    /** Environment variable naming the source S3 secret key. */
    public static final String ENV_SECRET_KEY = "PROTOMOLT_ACQUIRE_S3_SECRET_KEY";

    /**
     * Module configuration.
     *
     * @param endpoint the source S3 endpoint, or blank for AWS
     * @param region the source S3 region
     * @param accessKey static access key, or blank for the default provider chain
     * @param secretKey static secret key, or blank for the default provider chain
     * @param apiKey the intake API key the connector's identity rides
     */
    public record Config(String endpoint, String region, String accessKey, String secretKey,
                         String apiKey) {

        /** Reads the config from the environment, refusing missing identity by name. */
        public static Config fromEnvironment(Map<String, String> env) {
            String apiKey = env.getOrDefault(ENV_API_KEY, "");
            if (apiKey.isBlank()) {
                throw new IllegalArgumentException(
                        ENV_API_KEY + " is required: the connector's identity rides the"
                                + " intake API key");
            }
            String region = env.getOrDefault(ENV_REGION, "");
            if (region.isBlank()) {
                throw new IllegalArgumentException(ENV_REGION + " is required");
            }
            return new Config(
                    env.getOrDefault(ENV_ENDPOINT, ""),
                    region,
                    env.getOrDefault(ENV_ACCESS_KEY, ""),
                    env.getOrDefault(ENV_SECRET_KEY, ""),
                    apiKey);
        }
    }

    private final Config config;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public S3PullModule(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.config = config;
    }

    @Override
    public String role() {
        return ROLE;
    }

    @Override
    public Set<String> requires() {
        return Set.of("intake");
    }

    @Override
    public ServiceMount wire(NodeContext context) {
        S3Client s3 = client(config);
        GrpcIntakeFeed feed = new GrpcIntakeFeed(
                context.channels().targetOf("intake"), config.apiKey(),
                GrpcIntakeFeed.plaintextRequested(context.environment()));
        context.contributions().contribute(ProtoAction.class,
                new S3PullAction(new S3Pull(s3, feed)));
        return ServiceMount.inert(() -> {
            feed.close();
            s3.close();
        });
    }

    private static S3Client client(Config config) {
        var builder = S3Client.builder()
                .region(Region.of(config.region()))
                .httpClientBuilder(UrlConnectionHttpClient.builder());
        if (!config.endpoint().isBlank()) {
            builder.endpointOverride(URI.create(config.endpoint())).forcePathStyle(true);
        }
        if (!config.accessKey().isBlank()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.accessKey(), config.secretKey())));
        }
        return builder.build();
    }
}
