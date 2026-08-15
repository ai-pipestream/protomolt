package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.composer.Channels;
import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
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
        return new ServiceMount() {
            @Override
            public void start() {
                netty = services.startNetty(config.grpcPort());
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
