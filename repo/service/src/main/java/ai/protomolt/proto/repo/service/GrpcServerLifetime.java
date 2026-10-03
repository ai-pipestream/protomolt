package ai.protomolt.proto.repo.service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Owns both a transport and the executor borrowed by gRPC. */
final class GrpcServerLifetime implements AutoCloseable {
    private final Server server;
    private final OwnedResources resources;

    private GrpcServerLifetime(Server server, OwnedResources resources) {
        this.server = server;
        this.resources = resources;
    }

    static GrpcServerLifetime start(ServerBuilder<?> builder) throws IOException {
        return start(builder, Executors.newVirtualThreadPerTaskExecutor());
    }

    // Transfers executor ownership, including when build or start fails.
    static GrpcServerLifetime start(ServerBuilder<?> builder, ExecutorService executor) throws IOException {
        var resources = new OwnedResources();
        resources.add(() -> stopExecutor(executor));
        try {
            Server server = builder.executor(executor).build();
            resources.add(() -> stopServer(server));
            server.start();
            return new GrpcServerLifetime(server, resources);
        } catch (IOException | RuntimeException | Error failure) {
            try { resources.close(); }
            catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    Server server() { return server; }

    private static void stopServer(Server server) throws InterruptedException {
        server.shutdown();
        try {
            if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                server.shutdownNow();
                if (!server.awaitTermination(10, TimeUnit.SECONDS))
                    throw new IllegalStateException("gRPC server did not terminate");
            }
        } catch (InterruptedException interrupted) {
            server.shutdownNow();
            throw interrupted;
        }
    }

    private static void stopExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(10, TimeUnit.SECONDS))
                    throw new IllegalStateException("gRPC executor did not terminate");
            }
        } catch (InterruptedException interrupted) {
            executor.shutdownNow();
            throw interrupted;
        }
    }

    @Override public void close() { resources.close(); }
}
