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
    private final ExecutorService executor;

    private GrpcServerLifetime(Server server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
    }

    static GrpcServerLifetime start(ServerBuilder<?> builder) throws IOException {
        return start(builder, Executors.newVirtualThreadPerTaskExecutor());
    }

    // Transfers executor ownership, including when build or start fails.
    static GrpcServerLifetime start(ServerBuilder<?> builder, ExecutorService executor) throws IOException {
        return start(builder, executor, lifetime -> {});
    }

    static GrpcServerLifetime start(ServerBuilder<?> builder, java.util.function.Consumer<GrpcServerLifetime> retain) throws IOException {
        return start(builder, Executors.newVirtualThreadPerTaskExecutor(), retain);
    }

    private static GrpcServerLifetime start(ServerBuilder<?> builder, ExecutorService executor,
            java.util.function.Consumer<GrpcServerLifetime> retain) throws IOException {
        var resources = new OwnedResources();
        resources.add(() -> ExecutorShutdown.stop(executor, java.time.Duration.ofSeconds(10)));
        try {
            Server server = builder.executor(executor).build();
            resources.add(() -> stopServer(server, java.time.Duration.ofSeconds(10)));
            var lifetime = new GrpcServerLifetime(server, executor);
            retain.accept(lifetime);
            server.start();
            return lifetime;
        } catch (IOException | RuntimeException | Error failure) {
            try { resources.close(); }
            catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    Server server() { return server; }

    private static void stopServer(Server server, java.time.Duration timeout) throws InterruptedException {
        server.shutdown();
        try {
            if (!server.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                server.shutdownNow();
                if (!server.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS))
                    throw new IllegalStateException("gRPC server did not terminate");
            }
        } catch (InterruptedException interrupted) {
            server.shutdownNow();
            throw interrupted;
        }
    }

    @Override public void close() { close(java.time.Duration.ofSeconds(10)); }

    synchronized void close(java.time.Duration timeout) {
        ShutdownBarrier.releaseAfter(java.util.List.of(
                () -> stopServer(server, timeout), () -> ExecutorShutdown.stop(executor, timeout)), () -> {});
    }
}
