package ai.pipestream.proto.acquire.confluence;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.HealthStatusManager;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The standalone Confluence proxy entry point. All wiring lives in this one
 * class: config from the environment, the {@link ConfluenceGrpcService}
 * facade over the crawler core, and a Netty server with reflection and health
 * on. Every handler runs on a virtual-thread-per-task executor, so the
 * facade's plain blocking REST calls park instead of pinning carriers.
 *
 * <p>Environment:</p>
 * <ul>
 *   <li>{@code CONFLUENCE_BASE_URL}, {@code CONFLUENCE_EMAIL} (or
 *   {@code CONFLUENCE_USER}), {@code CONFLUENCE_API_TOKEN} (or
 *   {@code CONFLUENCE_TOKEN}) and the rest of the crawler config, per
 *   {@link ConfluenceConnectorConfig#fromEnvironment()}</li>
 *   <li>{@code CONFLUENCE_GRPC_PORT}: listen port, default 9095</li>
 *   <li>{@code CONFLUENCE_ATTACHMENT_MAX_BYTES}: inline attachment cap,
 *   default 25 MiB</li>
 * </ul>
 */
public final class ConfluenceProxyServer {

    /** Environment variable for the gRPC listen port. */
    public static final String ENV_GRPC_PORT = "CONFLUENCE_GRPC_PORT";
    /** Environment variable for the inline attachment size cap in bytes. */
    public static final String ENV_ATTACHMENT_MAX_BYTES = "CONFLUENCE_ATTACHMENT_MAX_BYTES";
    /** Default gRPC listen port. */
    public static final int DEFAULT_GRPC_PORT = 9095;

    private static final System.Logger LOG = System.getLogger(ConfluenceProxyServer.class.getName());

    private ConfluenceProxyServer() {
    }

    /**
     * Boots the proxy from the environment and blocks until shutdown.
     *
     * @param args ignored (configuration is env-driven)
     * @throws Exception on boot failure
     */
    public static void main(String[] args) throws Exception {
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.fromEnvironment();
        ConfluenceGrpcService service = new ConfluenceGrpcService(config,
                new ConfluenceClient(config),
                parseLong(System.getenv(ENV_ATTACHMENT_MAX_BYTES),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES));
        Server server = startNetty(service, parseInt(System.getenv(ENV_GRPC_PORT),
                DEFAULT_GRPC_PORT));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            try {
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }, "confluence-proxy-shutdown"));
        LOG.log(System.Logger.Level.INFO,
                "confluence-proxy listening on port {0} (base url {1}, spaces {2})",
                server.getPort(), config.baseUrl(),
                config.hasSpaceAllowlist() ? config.spaces() : "all");
        server.awaitTermination();
    }

    /**
     * Starts the given service on a Netty TCP server plus the gRPC
     * health-status and reflection services, handlers on virtual threads.
     * Health reports SERVING for the overall server once it is listening;
     * reflection stays on so descriptor-driven clients (grpcurl, protomolt's
     * reflect/grpc-invoke, stub generators) can discover the surface.
     *
     * @param service the facade to serve
     * @param port the listen port (0 = ephemeral; read the bound port back
     *        from the returned server)
     * @return the started server
     * @throws IOException when the port cannot be bound
     */
    public static Server startNetty(BindableService service, int port) throws IOException {
        HealthStatusManager health = new HealthStatusManager();
        Server server = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(service)
                .addService(health.getHealthService())
                .addService(ProtoReflectionService.newInstance())
                .build().start();
        health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        return server;
    }

    private static int parseInt(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long parseLong(String value, long fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
