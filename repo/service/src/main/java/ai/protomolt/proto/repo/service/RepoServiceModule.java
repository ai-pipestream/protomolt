package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.composer.Channels;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import io.grpc.Server;

/**
 * The document store as a mountable role. Wiring starts the in-process
 * endpoint siblings resolve through {@link Channels}; starting binds the
 * external Netty port, seeds account drives, and begins the lifecycle
 * loops.
 */
public final class RepoServiceModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "repo";

    private final RepoServiceConfig config;
    private RepoServices services;
    private Server netty;

    /**
     * Creates the module.
     *
     * @param config the repo service configuration
     */
    public RepoServiceModule(RepoServiceConfig config) {
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
    public ServiceMount wire(NodeContext context) {
        services = RepoServices.build(config);
        String name = ROLE + "-" + context.nodeId();
        services.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);
        // The in-process transport needs no credential: it has no socket, and a caller
        // already inside the JVM is past every boundary a credential could draw. The TCP
        // listener is the one reachable from elsewhere, so that is the one guarded.
        String apiToken = context.environment().get("PROTOMOLT_API_TOKEN");
        String credential = apiToken == null || apiToken.isBlank() ? null : apiToken;
        return new ServiceMount() {
            @Override
            public void start() {
                netty = services.startNetty(config.grpcPort(), credential, null);
                services.seedAccountDrives();
                services.startLifecycle();
            }

            @Override
            public void close() {
                services.close();
            }
        };
    }

    /** The bound external gRPC port; only valid after start. */
    public int grpcPort() {
        if (netty == null) {
            throw new IllegalStateException("repo module has not started");
        }
        return netty.getPort();
    }
}
