package ai.pipestream.proto.search.door;

import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import io.grpc.Server;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * The search door as a mountable role. Wiring opens the document fetcher
 * over the repo role's channel, builds the Lucene store (resolving every
 * chunk lane's embedding provider — a subject naming an absent model fails
 * the mount), and publishes the door's in-process endpoint. Starting binds
 * the external Netty port.
 */
public final class SearchDoorModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "search";

    /**
     * Module configuration.
     *
     * @param grpcPort the external port (0 for ephemeral)
     * @param indexDir the root index directory
     * @param subjects the mapping subjects to serve, keyed by subject name
     */
    public record Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects) {

        /** Validates the configuration. */
        public Config {
            if (indexDir == null) {
                throw new IllegalArgumentException("indexDir must not be null");
            }
            if (subjects == null || subjects.isEmpty()) {
                throw new IllegalArgumentException(
                        "at least one served mapping subject is required");
            }
            subjects = Map.copyOf(subjects);
        }
    }

    private final Config config;
    private SearchDoorServices door;
    private Server inProcess;
    private Server netty;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public SearchDoorModule(Config config) {
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
        return Set.of("repo");
    }

    @Override
    public ServiceMount wire(NodeContext context) throws Exception {
        door = SearchDoorServices.build(
                new SearchDoorConfig(config.grpcPort(), config.indexDir(), config.subjects()),
                new GrpcDocumentFetcher(context.channels().targetOf("repo")));
        String name = ROLE + "-" + context.nodeId();
        inProcess = door.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);
        return new ServiceMount() {
            @Override
            public void start() throws Exception {
                netty = door.startNetty(config.grpcPort());
            }

            @Override
            public void close() {
                if (inProcess != null) {
                    inProcess.shutdownNow();
                }
                door.close();
            }
        };
    }

    /** The bound external gRPC port; only valid after start. */
    public int grpcPort() {
        if (netty == null) {
            throw new IllegalStateException("search module has not started");
        }
        return netty.getPort();
    }
}
