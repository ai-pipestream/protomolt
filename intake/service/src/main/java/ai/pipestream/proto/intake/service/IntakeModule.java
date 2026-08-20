package ai.pipestream.proto.intake.service;

import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.intake.service.identity.ApiKeyIdentityResolver;
import io.grpc.Server;
import java.util.Set;

/**
 * The authenticated intake service as a mountable role. The repo target comes
 * from the node's channels (in-process when co-mounted, remote otherwise);
 * the key store either arrives explicitly or is selected from the
 * environment with the standing precedence (OIDC over JDBC over seeded).
 * Wiring also publishes the service's in-process endpoint, so co-mounted
 * modules (the pull connectors) feed it without a socket — authenticated
 * exactly like remote callers, the interceptor wraps every transport.
 */
public final class IntakeModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "intake";

    /**
     * Module configuration.
     *
     * @param grpcPort the gRPC port (0 for ephemeral)
     * @param httpPort the HTTP upload-lane port, or -1 to not serve HTTP
     * @param maxPayloadBytes the per-document payload cap
     * @param resolver the key store, or null to select from the environment
     */
    public record Config(int grpcPort, int httpPort, long maxPayloadBytes,
                         ApiKeyIdentityResolver resolver) {
    }

    private final Config config;
    private IntakeServices services;
    private Server inProcess;
    private Server netty;
    private IntakeHttpServer http;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public IntakeModule(Config config) {
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
        ApiKeyIdentityResolver resolver = config.resolver() != null
                ? config.resolver()
                : IntakeServiceMain.selectResolver(context.environment());
        services = IntakeServices.build(
                new IntakeServiceConfig(
                        config.grpcPort(),
                        context.channels().targetOf("repo"),
                        config.maxPayloadBytes()),
                resolver);
        String inProcessName = ROLE + "-" + context.nodeId();
        inProcess = services.startInProcess(inProcessName);
        context.channels().publishInProcess(ROLE, inProcessName);
        return new ServiceMount() {
            @Override
            public void start() throws Exception {
                netty = services.startNetty(config.grpcPort());
                if (config.httpPort() >= 0) {
                    http = services.startHttp(config.httpPort());
                }
            }

            @Override
            public void close() {
                if (http != null) {
                    http.close();
                }
                inProcess.shutdownNow();
                services.close();
            }
        };
    }

    /** The bound gRPC port; only valid after start. */
    public int grpcPort() {
        if (netty == null) {
            throw new IllegalStateException("intake module has not started");
        }
        return netty.getPort();
    }
}
