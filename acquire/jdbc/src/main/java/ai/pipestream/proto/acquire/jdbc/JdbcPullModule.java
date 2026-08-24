package ai.pipestream.proto.acquire.jdbc;

import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.acquire.pull.GrpcIntakeFeed;
import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;

import java.sql.DriverManager;
import java.util.Map;
import java.util.Set;

/**
 * The JDBC pull connector as a mountable role: the source database connection is module
 * configuration (a deployment fact), the query and watermark travel with each {@code pull-jdbc}
 * call. Wiring opens the intake feed over the node's channels and contributes the verb; there
 * is no schedule and no hidden cursor.
 */
public final class JdbcPullModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "acquire-jdbc";

    /** Environment variable naming the connector's intake API key. */
    public static final String ENV_API_KEY = "PROTOMOLT_ACQUIRE_API_KEY";

    /** Environment variable naming the source database JDBC URL. */
    public static final String ENV_URL = "PROTOMOLT_ACQUIRE_JDBC_URL";

    /** Environment variable naming the source database username. */
    public static final String ENV_USERNAME = "PROTOMOLT_ACQUIRE_JDBC_USERNAME";

    /** Environment variable naming the source database password. */
    public static final String ENV_PASSWORD = "PROTOMOLT_ACQUIRE_JDBC_PASSWORD";

    /**
     * Module configuration.
     *
     * @param jdbcUrl the source database URL
     * @param username the source database username
     * @param password the source database password
     * @param apiKey the intake API key the connector's identity rides
     */
    public record Config(String jdbcUrl, String username, String password, String apiKey) {

        /** Reads the config from the environment, refusing missing identity by name. */
        public static Config fromEnvironment(Map<String, String> env) {
            String apiKey = env.getOrDefault(ENV_API_KEY, "");
            if (apiKey.isBlank()) {
                throw new IllegalArgumentException(
                        ENV_API_KEY + " is required: the connector's identity rides the"
                                + " intake API key");
            }
            String url = env.getOrDefault(ENV_URL, "");
            if (url.isBlank()) {
                throw new IllegalArgumentException(ENV_URL + " is required");
            }
            return new Config(url,
                    env.getOrDefault(ENV_USERNAME, ""),
                    env.getOrDefault(ENV_PASSWORD, ""),
                    apiKey);
        }
    }

    private final Config config;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public JdbcPullModule(Config config) {
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
        GrpcIntakeFeed feed = new GrpcIntakeFeed(
                context.channels().targetOf("intake"), config.apiKey(),
                GrpcIntakeFeed.plaintextRequested(context.environment()));
        JdbcPull.ConnectionFactory connections = () -> DriverManager.getConnection(
                config.jdbcUrl(), config.username(), config.password());
        context.contributions().contribute(ProtoAction.class,
                new JdbcPullAction(new JdbcPull(connections, feed)));
        return ServiceMount.inert(feed);
    }
}
