package ai.pipestream.proto.account.service;

import ai.pipestream.proto.account.service.events.AccountEventRelay;
import ai.pipestream.proto.account.service.events.JdbcAccountEventOutbox;
import ai.pipestream.proto.account.service.identity.DirectAccountIdentityResolver;
import ai.pipestream.proto.account.service.identity.IdentityResolver;
import ai.pipestream.proto.account.service.provision.DriveProvisioner;
import ai.pipestream.proto.account.service.store.AccountDatabase;
import ai.pipestream.proto.account.service.store.AccountStore;
import ai.pipestream.proto.account.service.store.JdbcAccountStore;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import com.google.protobuf.Message;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.HealthStatusManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The account service set, transport-agnostic: one factory wires the whole
 * stack and hands out the gRPC services, so the same set can be embedded
 * in-JVM or run standalone — no DI framework, this factory is the SPI.
 *
 * <p>Same-JVM embedding uses {@link #startInProcess(String)}: the in-process
 * transport is zero-copy while keeping full gRPC semantics. Standalone
 * deployment uses {@link #startNetty(int)}: TCP via Netty, plus the gRPC
 * health-status and reflection services; {@link AccountServiceMain} is
 * exactly that path driven from the environment.
 *
 * <p>Boot order (in {@link #build(AccountServiceConfig)}): account database
 * (pool → Flyway migration) → store + outbox + relay → the repo-service
 * channel (plaintext {@code host:port}, or the in-process transport for the
 * {@code "inprocess:<name>"} target convention) and its DriveService stub →
 * the drive provisioner, identity resolver, and the gRPC service impl.
 * Construction fails fast (migrations) — nothing starts lazily. Every
 * server's call executor is a virtual-thread-per-task executor: every
 * handler is plain blocking code (JDBC, the repo RPC), and a blocked call
 * parks its virtual thread instead of a carrier.
 *
 * <p>Kafka eventing ({@code DOCUMENT_PLATFORM_ACCOUNT_KAFKA_BOOTSTRAP_SERVERS}):
 * the transactional outbox. Unset = no outbox, no relay, no producer, and
 * the commit points skip the outbox entirely (zero overhead).
 */
public final class AccountServices implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AccountServices.class);

    /** Event-relay drain batch size per loop iteration. */
    private static final int RELAY_BATCH_SIZE = 100;

    private final AccountServiceConfig config;
    private final AccountDatabase database;
    private final AccountStore store;
    private final IdentityResolver identityResolver;
    private final ManagedChannel repoChannel;
    private final DriveProvisioner driveProvisioner;
    private final JdbcAccountEventOutbox eventOutbox;
    private final AccountEventRelay eventRelay;
    private final KafkaProducer<String, Message> eventProducer;
    private final List<BindableService> services;

    private final List<Server> servers = new CopyOnWriteArrayList<>();
    private final List<Thread> lifecycleThreads = new CopyOnWriteArrayList<>();
    private volatile boolean lifecycleClosed;

    private AccountServices(AccountServiceConfig config) {
        this.config = config;
        this.database = new AccountDatabase(config.store());
        this.store = new JdbcAccountStore();
        this.identityResolver = new DirectAccountIdentityResolver();
        this.repoChannel = config.repoTargetIsInProcess()
                ? InProcessChannelBuilder.forName(config.repoTargetName()).build()
                : NettyChannelBuilder.forTarget(config.repoGrpcTarget()).usePlaintext().build();
        this.driveProvisioner =
                new DriveProvisioner(DriveServiceGrpc.newBlockingStub(repoChannel));
        // Kafka eventing (DOCUMENT_PLATFORM_ACCOUNT_KAFKA_BOOTSTRAP_SERVERS):
        // the transactional outbox. Unset = no outbox, no relay, no producer,
        // and the commit points skip the outbox entirely (zero overhead).
        this.eventOutbox = config.kafkaEnabled() ? new JdbcAccountEventOutbox(database) : null;
        this.eventRelay = eventOutbox != null ? new AccountEventRelay(eventOutbox) : null;
        this.eventProducer = eventOutbox != null
                ? AccountEventRelay.newProducer(config.kafkaBootstrapServers(),
                        config.schemaRegistryUrl()) : null;
        this.services = List.of(
                new AccountGrpcService(database, store, eventOutbox, driveProvisioner));
    }

    /**
     * Wire every component the services need, in boot order.
     *
     * @param config the resolved service configuration
     * @return the wired service set, ready to serve over either transport
     */
    public static AccountServices build(AccountServiceConfig config) {
        return new AccountServices(config);
    }

    /**
     * The wired gRPC services, for hosts that register them on their own
     * server builder.
     *
     * @return the service implementations, unmodifiable
     */
    public List<BindableService> services() {
        return services;
    }

    /**
     * The IdentityResolver this service set runs (the seam external identity
     * adapters slot into; no RPC consumes it yet).
     *
     * @return the identity resolver
     */
    public IdentityResolver identityResolver() {
        return identityResolver;
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
                    // Virtual threads: every handler body is blocking
                    // JDBC/RPC — exactly what virtual threads are for.
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
            LOG.info("account-service listening on port {}", server.getPort());
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

    /**
     * Starts the background event relay: a single virtual-thread loop that
     * drains the account-events outbox to Kafka — a non-empty drain loops
     * again immediately, an empty one backs off
     * {@code DOCUMENT_PLATFORM_ACCOUNT_RELAY_INTERVAL_MS} (default 5000). The
     * loop catches and logs per iteration — one bad record never kills it.
     * No-op when eventing is off or
     * {@code DOCUMENT_PLATFORM_ACCOUNT_LIFECYCLE_ENABLED=false}; the loop
     * stops in {@link #close()}.
     */
    public void startLifecycle() {
        if (!config.lifecycleEnabled()) {
            LOG.info("account lifecycle loops disabled ({})",
                    AccountServiceConfig.ENV_LIFECYCLE_ENABLED + "=false");
            return;
        }
        if (eventRelay == null) {
            LOG.info("account event relay not configured ({} unset)",
                    AccountServiceConfig.ENV_KAFKA_BOOTSTRAP_SERVERS);
            return;
        }
        Thread thread = Thread.ofVirtual().name("account-event-relay").start(() -> {
            while (!lifecycleClosed) {
                try {
                    int published = eventRelay.relayOnce(eventProducer, config.kafkaTopic(),
                            RELAY_BATCH_SIZE);
                    // Idle backoff: work left → drain again immediately;
                    // empty → wait.
                    if (published == 0) {
                        sleep(config.relayIntervalMs());
                    }
                } catch (RuntimeException e) {
                    LOG.warn("account-event-relay iteration failed (loop continues): {}",
                            e.getMessage(), e);
                    sleep(1000);
                }
            }
        });
        lifecycleThreads.add(thread);
        LOG.info("account lifecycle loops started (relay interval {} ms)",
                config.relayIntervalMs());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stops the lifecycle loops, then all started servers, then the producer, the repo channel and the database. */
    @Override
    public void close() {
        lifecycleClosed = true;
        for (Thread thread : lifecycleThreads) {
            thread.interrupt();
        }
        for (Thread thread : lifecycleThreads) {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        lifecycleThreads.clear();
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
        if (eventProducer != null) {
            eventProducer.close();
        }
        repoChannel.shutdownNow();
        database.close();
        LOG.info("account-service stopped");
    }

    // Package-private accessors: the IT asserts on store/outbox state through
    // the same wired components (no mocks).

    AccountStore accountStore() {
        return store;
    }

    AccountDatabase database() {
        return database;
    }

    JdbcAccountEventOutbox eventOutbox() {
        return eventOutbox;
    }

    AccountEventRelay eventRelay() {
        return eventRelay;
    }
}
