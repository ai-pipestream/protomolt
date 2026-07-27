package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.blob.PartStorage;
import ai.pipestream.proto.repo.container.blob.S3BlobStore;
import ai.pipestream.proto.repo.container.ledger.DocumentLedger;
import ai.pipestream.proto.repo.container.ledger.DriveLedger;
import ai.pipestream.proto.repo.container.ledger.LedgerDatabase;
import ai.pipestream.proto.repo.container.ledger.Tx;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.HealthStatusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The repo service set, transport-agnostic: one factory wires the whole
 * claim-check stack and hands out the gRPC services, so the same set can be
 * embedded in-JVM or run standalone — no DI framework, this factory is the
 * SPI.
 *
 * <p>Same-JVM embedding uses {@link #startInProcess(String)}: the in-process
 * transport is zero-copy (no sockets, no serialization round-trip) while
 * keeping full gRPC semantics (interceptors, deadlines, status codes), which
 * is how a host server mounts the repository alongside its own services and
 * how the integration tests boot the stack. Standalone deployment uses
 * {@link #startNetty(int)}: TCP via Netty, plus the gRPC health-status and
 * reflection services; {@link RepoServiceMain} is exactly that path driven
 * from the environment.
 *
 * <p>Boot order (in {@link #build(RepoServiceConfig)}): ledger database
 * (pool → Flyway migration → validated JPA mappings) → transaction wrapper +
 * ledgers → S3 client → blob store → part storage → the two gRPC service
 * impls. Construction fails fast (migrations, mapping validation) — nothing
 * starts lazily. Every server's call executor is a virtual-thread-per-task
 * executor: every handler is plain blocking code (JDBC, S3), and a blocked
 * call parks its virtual thread instead of a carrier, so no
 * offload/directExecutor tricks are needed.
 */
public final class RepoServices implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RepoServices.class);

    private final RepoServiceConfig config;
    private final LedgerDatabase database;
    private final Tx tx;
    private final DocumentLedger documentLedger;
    private final DriveLedger driveLedger;
    private final S3Client s3Client;
    private final S3BlobStore blobStore;
    private final PartStorage partStorage;
    private final List<BindableService> services;

    private final List<Server> servers = new CopyOnWriteArrayList<>();

    private RepoServices(RepoServiceConfig config) {
        this.config = config;
        this.database = new LedgerDatabase(config.ledger());
        this.tx = new Tx(database.entityManagerFactory());
        this.documentLedger = new DocumentLedger(tx);
        this.driveLedger = new DriveLedger(tx);
        this.s3Client = buildS3Client(config);
        this.blobStore = new S3BlobStore(s3Client);
        this.partStorage = new PartStorage();
        this.services = List.of(
                new DocumentGrpcService(documentLedger, driveLedger, tx, blobStore, partStorage),
                new DriveGrpcService(driveLedger, s3Client,
                        config.defaultBucketBase(), config.s3Region()));
    }

    /**
     * Wire every component the services need, in boot order.
     *
     * @param config the resolved service configuration
     * @return the wired service set, ready to serve over either transport
     */
    public static RepoServices build(RepoServiceConfig config) {
        return new RepoServices(config);
    }

    /**
     * The wired gRPC services (document + drive), for hosts that register
     * them on their own server builder.
     *
     * @return the service implementations, unmodifiable
     */
    public List<BindableService> services() {
        return services;
    }

    /**
     * Starts all services on an in-process server (same-JVM embedding).
     *
     * @param name the in-process transport name; clients reach it via
     *        {@code InProcessChannelBuilder.forName(name)}
     * @return the started server (also closed by {@link #close()})
     */
    public Server startInProcess(String name) {
        try {
            return registerAndStart(InProcessServerBuilder.forName(name)
                    // Virtual threads: every handler body is blocking JDBC/S3 —
                    // exactly what virtual threads are for.
                    .executor(Executors.newVirtualThreadPerTaskExecutor()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Starts all services on a Netty TCP server (standalone deployment), plus
     * the gRPC health-status and reflection services. Health reports SERVING
     * for the overall server once it is listening; reflection is enabled for
     * grpcurl-style tooling.
     *
     * @param port the listen port (0 = ephemeral)
     * @return the started server (also closed by {@link #close()})
     */
    public Server startNetty(int port) {
        try {
            HealthStatusManager health = new HealthStatusManager();
            var builder = NettyServerBuilder.forPort(port)
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .addService(health.getHealthService())
                    .addService(ProtoReflectionService.newInstance());
            Server server = registerAndStart(builder);
            health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
            LOG.info("repo-service listening on port {}", server.getPort());
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Server registerAndStart(io.grpc.ServerBuilder<?> builder) throws IOException {
        services.forEach(builder::addService);
        Server server = builder.build().start();
        servers.add(server);
        return server;
    }

    /** Stops all started servers, then closes the S3 client and the ledger database. */
    @Override
    public void close() {
        for (Server server : servers) {
            server.shutdown();
        }
        for (Server server : servers) {
            try {
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }
        servers.clear();
        s3Client.close();
        database.close();
        LOG.info("repo-service stopped");
    }

    private static S3Client buildS3Client(RepoServiceConfig config) {
        var builder = S3Client.builder()
                .region(Region.of(config.s3Region()))
                .httpClient(UrlConnectionHttpClient.create());
        if (config.hasStaticCredentials()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.s3AccessKey(), config.s3SecretKey())));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }
        if (config.s3Endpoint() != null) {
            // S3-compatible store (LocalStack, SeaweedFS, MinIO): path-style
            // addressing is required for endpoint overrides.
            builder.endpointOverride(URI.create(config.s3Endpoint()));
            builder.forcePathStyle(true);
        }
        return builder.build();
    }

    // Package-private accessors: the IT asserts on ledger state through the
    // same wired components (no mocks).

    DocumentLedger documentLedger() {
        return documentLedger;
    }

    DriveLedger driveLedger() {
        return driveLedger;
    }

    S3BlobStore blobStore() {
        return blobStore;
    }

    S3Client s3Client() {
        return s3Client;
    }
}
