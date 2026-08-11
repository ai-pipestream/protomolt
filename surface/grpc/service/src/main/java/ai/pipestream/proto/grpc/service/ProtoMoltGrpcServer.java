package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A ready-to-run gRPC server for {@code ProtoMoltService}: the catalog bound as the service,
 * server reflection enabled — so ProtoMolt's own {@code reflect} verb (or grpcurl) discovers
 * the server that hosts it. Each call runs on its own virtual thread so blocking action work,
 * including gRPC and model-provider calls, parks without occupying a platform worker.
 */
public final class ProtoMoltGrpcServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoMoltGrpcServer.class);

    private final Server server;
    private final ExecutorService executor;

    private ProtoMoltGrpcServer(Server server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
    }

    /** Starts the service on {@code port} (0 picks a free port), with no call credential. */
    public static ProtoMoltGrpcServer start(int port, ActionCatalog catalog) {
        return start(port, catalog, null);
    }

    /**
     * Starts the service on {@code port} (0 picks a free port). With a non-null
     * {@code apiToken}, every call — reflection included — must present it as
     * {@code api_token} metadata or an {@code authorization} bearer credential.
     */
    public static ProtoMoltGrpcServer start(int port, ActionCatalog catalog, String apiToken) {
        return start(null, port, catalog, apiToken);
    }

    /**
     * Starts the service bound to {@code host} (null, blank, or {@code 0.0.0.0} binds every
     * interface) — the gRPC listener honors the same bind address as the HTTP surfaces.
     */
    public static ProtoMoltGrpcServer start(String host, int port, ActionCatalog catalog,
                                            String apiToken) {
        Objects.requireNonNull(catalog, "catalog");
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            boolean wildcard = host == null || host.isBlank() || "0.0.0.0".equals(host);
            ServerBuilder<?> builder = (wildcard
                    ? ServerBuilder.forPort(port)
                    : io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forAddress(
                            new java.net.InetSocketAddress(host, port)))
                    .executor(executor)
                    .addService(ProtoMoltGrpcService.definition(catalog))
                    .addService(ProtoReflectionServiceV1.newInstance());
            if (apiToken != null) {
                builder.intercept(new ApiTokenServerInterceptor(apiToken));
            }
            Server server = builder.build().start();
            LOG.info("ProtoMoltService listening on port {} (reflection enabled{})",
                    server.getPort(), apiToken != null ? ", api_token required" : "");
            return new ProtoMoltGrpcServer(server, executor);
        } catch (IOException e) {
            executor.shutdownNow();
            throw new IllegalStateException("Failed to start the gRPC server on port " + port, e);
        } catch (RuntimeException | Error e) {
            executor.shutdownNow();
            throw e;
        }
    }

    public int port() {
        return server.getPort();
    }

    /** Blocks until the server terminates. */
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void close() {
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
            executor.shutdownNow();
        }
    }
}
