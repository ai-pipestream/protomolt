package ai.pipestream.proto.parse.service;

import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * One factory wires the coordinator stack: the repo-service channel, the
 * routing rules, the parser registry, the
 * {@link ParseCoordinatorGrpcService}, and the servers it mounts on. There
 * is no DI framework; this factory is the SPI.
 *
 * <p>The rules and the parser fleet arrive built — the caller chooses where
 * they come from (env JSON, a file, code) and this class never learns which.
 */
public final class ParseCoordinatorServices implements AutoCloseable {

    private final ManagedChannel repoChannel;
    private final ParserRegistry parsers;
    private final ParseCoordinatorGrpcService coordinator;
    private Server server;

    private ParseCoordinatorServices(ParseCoordinatorConfig config,
            java.util.function.Supplier<RoutingRules> rules, ParserRegistry parsers) {
        this.repoChannel = openRepoChannel(config.repoTarget());
        this.parsers = parsers;
        DocumentServiceGrpc.DocumentServiceBlockingStub documents =
                DocumentServiceGrpc.newBlockingStub(repoChannel);
        this.coordinator = new ParseCoordinatorGrpcService(
                documents,
                rules,
                parsers,
                config.drive(),
                Duration.ofSeconds(config.parseDeadlineSeconds()));
    }

    /**
     * Builds the stack.
     *
     * @param config service configuration
     * @param rules the compiled routing-rule set
     * @param parsers the parser fleet; closed with this factory
     * @return the wired, not-yet-started stack
     */
    public static ParseCoordinatorServices build(
            ParseCoordinatorConfig config, RoutingRules rules, ParserRegistry parsers) {
        if (rules == null) {
            throw new IllegalArgumentException("rules must not be null");
        }
        return build(config, () -> rules, parsers);
    }

    /**
     * As {@link #build(ParseCoordinatorConfig, RoutingRules, ParserRegistry)}
     * with a live rule supplier: distributed config swaps the compiled set
     * and every later plan uses the new rules — the "config reload" the
     * routing header always promised, with no CRUD surface.
     *
     * @param config service configuration
     * @param rules the live rule-set supplier; read per plan
     * @param parsers the parser fleet; closed with this factory
     * @return the wired, not-yet-started stack
     */
    public static ParseCoordinatorServices build(ParseCoordinatorConfig config,
            java.util.function.Supplier<RoutingRules> rules, ParserRegistry parsers) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (rules == null) {
            throw new IllegalArgumentException("rules must not be null");
        }
        if (parsers == null) {
            throw new IllegalArgumentException("parsers must not be null");
        }
        return new ParseCoordinatorServices(config, rules, parsers);
    }

    /**
     * Starts the coordinator on an in-process server named {@code name}.
     *
     * @param name the in-process server name
     * @return the started server
     * @throws IOException when the server fails to bind
     */
    public Server startInProcess(String name) throws IOException {
        server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(coordinator)
                .build()
                .start();
        return server;
    }

    /**
     * Starts the coordinator on a Netty server with health and reflection.
     *
     * @param port the port to bind; {@code 0} picks a free port
     * @return the started server
     * @throws IOException when the server fails to bind
     */
    public Server startNetty(int port) throws IOException {
        HealthStatusManager health = new HealthStatusManager();
        server = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(coordinator)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        return server;
    }

    /** The bound server, once one of the start methods has run. */
    public Server server() {
        return server;
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdownNow();
        }
        parsers.close();
        repoChannel.shutdownNow();
        try {
            repoChannel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ManagedChannel openRepoChannel(String target) {
        if (target.startsWith(ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX)) {
            String name = target.substring(ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX.length());
            return InProcessChannelBuilder.forName(name).build();
        }
        return NettyChannelBuilder.forTarget(target).usePlaintext().build();
    }
}
