package ai.pipestream.proto.intake.service;

import ai.pipestream.proto.intake.service.identity.ApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * One factory wires the intake stack: the repo-service channel, the
 * key-store resolver, the authenticated {@link IntakeGrpcService}, and the
 * servers it mounts on. There is no DI framework; this factory is the SPI.
 *
 * <p>The key store arrives as an {@link ApiKeyIdentityResolver} — the caller
 * chooses the store (Keycloak-backed, JDBC, in-memory) and this class never
 * learns which. Every mounted server carries the
 * {@link ApiKeyServerInterceptor}, so there is no unauthenticated path to the
 * service.
 */
public final class IntakeServices implements AutoCloseable {

    private final IntakeServiceConfig config;
    private final ApiKeyIdentityResolver resolver;
    private final ManagedChannel repoChannel;
    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final ServerServiceDefinition intakeService;
    private final List<IntakeHttpServer> httpServers = new ArrayList<>();
    private Server server;

    private IntakeServices(IntakeServiceConfig config, ApiKeyIdentityResolver resolver) {
        this.config = config;
        this.resolver = resolver;
        this.repoChannel = openRepoChannel(config.repoTarget());
        this.documents = DocumentServiceGrpc.newBlockingStub(repoChannel);
        IntakeGrpcService intake = new IntakeGrpcService(documents, config.maxPayloadBytes());
        this.intakeService =
                ServerInterceptors.intercept(intake, new ApiKeyServerInterceptor(resolver));
    }

    /**
     * Builds the stack.
     *
     * @param config service configuration
     * @param resolver the key store every call authenticates against
     */
    public static IntakeServices build(IntakeServiceConfig config, ApiKeyIdentityResolver resolver) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (resolver == null) {
            throw new IllegalArgumentException("resolver must not be null");
        }
        return new IntakeServices(config, resolver);
    }

    /** Starts the intake service on an in-process server named {@code name}. */
    public Server startInProcess(String name) throws IOException {
        server =
                InProcessServerBuilder.forName(name)
                        .executor(Executors.newVirtualThreadPerTaskExecutor())
                        .addService(intakeService)
                        .build()
                        .start();
        return server;
    }

    /** Starts the intake service on a Netty server with health and reflection. */
    public Server startNetty(int port) throws IOException {
        HealthStatusManager health = new HealthStatusManager();
        server =
                NettyServerBuilder.forPort(port)
                        .executor(Executors.newVirtualThreadPerTaskExecutor())
                        .addService(intakeService)
                        .addService(health.getHealthService())
                        .addService(ProtoReflectionServiceV1.newInstance())
                        .build()
                        .start();
        return server;
    }

    /**
     * Starts the HTTP upload lane ({@code POST /v1/intake:upload}, see
     * {@link IntakeHttpServer}) alongside the gRPC transports: the same
     * authenticated wrap-and-save path as the gRPC lanes, over a raw-binary
     * HTTP POST.
     *
     * @param port the listen port (0 = ephemeral; read the bound port back
     *        from the returned server)
     * @return the started HTTP server (also closed by {@link #close()})
     */
    public IntakeHttpServer startHttp(int port) {
        IntakeHttpServer http =
                new IntakeHttpServer(
                        new RawIngests(documents, config.maxPayloadBytes()), resolver);
        http.start(port);
        httpServers.add(http);
        return http;
    }

    /** The bound server, once one of the start methods has run. */
    public Server server() {
        return server;
    }

    @Override
    public void close() {
        for (IntakeHttpServer http : httpServers) {
            http.close();
        }
        if (server != null) {
            server.shutdownNow();
        }
        repoChannel.shutdownNow();
        try {
            repoChannel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ManagedChannel openRepoChannel(String target) {
        if (target.startsWith(IntakeServiceConfig.INPROCESS_TARGET_PREFIX)) {
            String name = target.substring(IntakeServiceConfig.INPROCESS_TARGET_PREFIX.length());
            return InProcessChannelBuilder.forName(name).build();
        }
        return NettyChannelBuilder.forTarget(target).usePlaintext().build();
    }
}
