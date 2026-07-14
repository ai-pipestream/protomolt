package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A ready-to-run gRPC server for {@code ProtoMoltService}: the catalog bound as the service,
 * server reflection enabled — so ProtoMolt's own {@code reflect} verb (or grpcurl) discovers
 * the server that hosts it.
 */
public final class ProtoMoltGrpcServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoMoltGrpcServer.class);

    private final Server server;

    private ProtoMoltGrpcServer(Server server) {
        this.server = server;
    }

    /** Starts the service on {@code port} (0 picks a free port). */
    public static ProtoMoltGrpcServer start(int port, ActionCatalog catalog) {
        Objects.requireNonNull(catalog, "catalog");
        try {
            Server server = ServerBuilder.forPort(port)
                    .addService(ProtoMoltGrpcService.definition(catalog))
                    .addService(ProtoReflectionServiceV1.newInstance())
                    .build()
                    .start();
            LOG.info("ProtoMoltService listening on port {} (reflection enabled)", server.getPort());
            return new ProtoMoltGrpcServer(server);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start the gRPC server on port " + port, e);
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
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }
}
