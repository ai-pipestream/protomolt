package ai.protomolt.proto.account.service;

import ai.protomolt.proto.account.service.events.AccountEventRelay;
import ai.protomolt.proto.account.service.events.JdbcAccountEventOutbox;
import ai.protomolt.proto.account.service.identity.DirectAccountIdentityResolver;
import ai.protomolt.proto.account.service.identity.IdentityResolver;
import ai.protomolt.proto.account.service.provision.DriveProvisioner;
import ai.protomolt.proto.account.service.store.AccountDatabase;
import ai.protomolt.proto.account.service.store.AccountStore;
import ai.protomolt.proto.account.service.store.JdbcAccountStore;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import com.google.protobuf.Message;
import io.grpc.BindableService;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.MetadataUtils;
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
 * transport is zero-copy while keeping full gRPC semantics, and its callers
 * sit inside the trust boundary with process authority. Standalone
 * deployment uses {@link #startNetty(int, String, CallerResolver)}: TCP via
 * Netty, plus the gRPC health-status and reflection services, every call
 * authenticated by the operator token or a credential the
 * {@link CallerResolver} names; {@link AccountServiceMain} is exactly that
 * path driven from the environment. A scoped caller reads only the accounts
 * {@link #withAccountBindings(AccountBindingResolver)} binds it to.
 *
 * <p>Boot order (in {@link #build(AccountServiceConfig)}): account database
 * (pool → Flyway migration) → store + outbox + relay → the repo-service
 * channel (plaintext {@code host:port}, or the in-process transport for the
 * {@code "inprocess:<name>"} target convention), presenting the operator
 * token as {@code api_token} metadata when one is given, and its DriveService
 * stub →
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

    /** Environment variable holding the operator token: {@value}. */
    public static final String ENV_API_TOKEN = "PROTOMOLT_API_TOKEN";

    /** Event-relay drain batch size per loop iteration. */
    private static final int RELAY_BATCH_SIZE = 100;

    private static final Metadata.Key<String> API_TOKEN_HEADER =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);

    private final AccountServiceConfig config;
    private final AccountDatabase database;
    private final AccountStore store;
    private final IdentityResolver identityResolver;
    private final ManagedChannel repoChannel;
    private final DriveProvisioner driveProvisioner;
    private final JdbcAccountEventOutbox eventOutbox;
    private final AccountEventRelay eventRelay;
    private final KafkaProducer<String, Message> eventProducer;
    private final AccountGrpcService accountService;
    private final List<BindableService> services;

    private final List<Server> servers = new CopyOnWriteArrayList<>();
    private final List<Thread> lifecycleThreads = new CopyOnWriteArrayList<>();
    private volatile boolean lifecycleClosed;

    private AccountServices(AccountServiceConfig config, String repoApiToken) {
        this.config = config;
        // Refused before the database pool opens, so a misconfiguration
        // leaves nothing to close.
        requireRepoCredential(config, repoApiToken);
        AccountDatabase openedDatabase = new AccountDatabase(config.store());
        ManagedChannel openedRepoChannel = null;
        KafkaProducer<String, Message> openedEventProducer = null;
        try {
            this.database = openedDatabase;
            this.store = new JdbcAccountStore();
            this.identityResolver = new DirectAccountIdentityResolver();
            openedRepoChannel = repoChannel(config, repoApiToken);
            this.repoChannel = openedRepoChannel;
            this.driveProvisioner =
                    new DriveProvisioner(DriveServiceGrpc.newBlockingStub(repoChannel));
            // Kafka eventing (DOCUMENT_PLATFORM_ACCOUNT_KAFKA_BOOTSTRAP_SERVERS):
            // the transactional outbox. Unset = no outbox, no relay, no producer,
            // and the commit points skip the outbox entirely (zero overhead).
            this.eventOutbox = config.kafkaEnabled() ? new JdbcAccountEventOutbox(database) : null;
            this.eventRelay = eventOutbox != null ? new AccountEventRelay(eventOutbox) : null;
            openedEventProducer = eventOutbox != null
                    ? AccountEventRelay.newProducer(config.kafkaBootstrapServers(),
                            config.schemaRegistryUrl()) : null;
            this.eventProducer = openedEventProducer;
            this.accountService = new AccountGrpcService(database, store, eventOutbox, driveProvisioner);
            this.services = List.of(accountService);
        } catch (RuntimeException | Error failure) {
            if (openedEventProducer != null) {
                try {
                    openedEventProducer.close();
                } catch (RuntimeException | Error suppressed) {
                    failure.addSuppressed(suppressed);
                }
            }
            if (openedRepoChannel != null) {
                openedRepoChannel.shutdownNow();
            }
            try {
                openedDatabase.close();
            } catch (RuntimeException | Error suppressed) {
                failure.addSuppressed(suppressed);
            }
            throw failure;
        }
    }

    /**
     * Wire every component the services need, in boot order, with no
     * credential for the repo channel. Only an in-process repo target
     * ({@code "inprocess:<name>"}) is accepted; a network repo target needs
     * {@link #build(AccountServiceConfig, String)}.
     *
     * @param config the resolved service configuration
     * @return the wired service set, ready to serve over either transport
     * @throws IllegalArgumentException when the repo target is a network target
     */
    public static AccountServices build(AccountServiceConfig config) {
        return new AccountServices(config, null);
    }

    /**
     * Wire every component the services need, in boot order, presenting
     * {@code repoApiToken} as {@code api_token} metadata on every repo call.
     * The repository host's TCP listener refuses a call without it, so a
     * network repo target requires a nonblank token; an in-process repo
     * target accepts null.
     *
     * @param config the resolved service configuration
     * @param repoApiToken the operator token the repository host accepts
     *        ({@value #ENV_API_TOKEN}), or null for an in-process repo target
     * @return the wired service set, ready to serve over either transport
     * @throws IllegalArgumentException when a network repo target has no token
     */
    public static AccountServices build(AccountServiceConfig config, String repoApiToken) {
        return new AccountServices(config, repoApiToken);
    }

    /**
     * Sets the account bindings scoped callers resolve through. Until set, a
     * scoped caller is bound to no account and every read refuses it.
     * Composition roots call this before serving.
     *
     * @param resolver the binding source
     * @return this service set, for chaining
     */
    public AccountServices withAccountBindings(AccountBindingResolver resolver) {
        accountService.useAccountBindings(resolver);
        return this;
    }

    private static void requireRepoCredential(AccountServiceConfig config, String repoApiToken) {
        if (repoApiToken != null && repoApiToken.isBlank()) {
            throw new IllegalArgumentException(
                    "the repo channel's operator token is blank: set " + ENV_API_TOKEN);
        }
        if (!config.repoTargetIsInProcess() && repoApiToken == null) {
            throw new IllegalArgumentException(AccountServiceConfig.ENV_REPO_GRPC_TARGET
                    + " names the network repo target '" + config.repoGrpcTarget()
                    + "', whose listener requires the operator token: set " + ENV_API_TOKEN);
        }
    }

    private static ManagedChannel repoChannel(AccountServiceConfig config, String repoApiToken) {
        if (config.repoTargetIsInProcess()) {
            var builder = InProcessChannelBuilder.forName(config.repoTargetName());
            if (repoApiToken != null) {
                builder.intercept(credentialInterceptor(repoApiToken));
            }
            return builder.build();
        }
        return NettyChannelBuilder.forTarget(config.repoGrpcTarget()).usePlaintext()
                .intercept(credentialInterceptor(repoApiToken)).build();
    }

    private static ClientInterceptor credentialInterceptor(String apiToken) {
        Metadata headers = new Metadata();
        headers.put(API_TOKEN_HEADER, apiToken);
        return MetadataUtils.newAttachHeadersInterceptor(headers);
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
     * Starts all services on an in-process server (same-JVM embedding). The
     * transport carries no credential: its callers are inside the trust
     * boundary and run with process authority.
     *
     * @param name the in-process transport name; clients reach it via
     *        {@code InProcessChannelBuilder.forName(name)}
     * @return the started server (also closed by {@link #close()})
     */
    public Server startInProcess(String name) {
        try {
            return registerAndStart(inProcessBuilder(name));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Starts all services on an in-process server behind the same credential
     * check as {@link #startNetty(int, String, CallerResolver)}: every call
     * presents the operator token or a credential {@code resolver} names.
     *
     * @param name the in-process transport name
     * @param apiToken the nonblank operator token, which runs with process authority
     * @param resolver resolves other credentials to scoped callers, or null
     *        when only the operator token authenticates
     * @return the started server (also closed by {@link #close()})
     * @throws IllegalArgumentException when {@code apiToken} is null or blank
     */
    public Server startInProcess(String name, String apiToken, CallerResolver resolver) {
        requireOperatorToken(apiToken);
        try {
            return registerAndStart(inProcessBuilder(name)
                    .intercept(new ApiTokenServerInterceptor(apiToken, resolver)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static InProcessServerBuilder inProcessBuilder(String name) {
        // Virtual threads: every handler body is blocking JDBC/RPC, exactly
        // what virtual threads are for.
        return InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Starts all services on a Netty TCP server (standalone deployment), plus
     * the gRPC health-status and reflection services. Every call, health and
     * reflection included, must present a credential in {@code api_token}
     * metadata (or {@code authorization: Bearer}): the operator token runs
     * with process authority, and a credential {@code resolver} names runs
     * as its scoped caller. Health reports SERVING for the overall server
     * once it is listening.
     *
     * @param port the listen port (0 = ephemeral)
     * @param apiToken the nonblank operator token ({@value #ENV_API_TOKEN})
     * @param resolver resolves other credentials to scoped callers, or null
     *        when only the operator token authenticates
     * @return the started server (also closed by {@link #close()})
     * @throws IllegalArgumentException when {@code apiToken} is null or blank
     */
    public Server startNetty(int port, String apiToken, CallerResolver resolver) {
        requireOperatorToken(apiToken);
        try {
            HealthStatusManager health = new HealthStatusManager();
            var builder = NettyServerBuilder.forPort(port)
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .addService(health.getHealthService())
                    .addService(ProtoReflectionServiceV1.newInstance())
                    .intercept(new ApiTokenServerInterceptor(apiToken, resolver));
            Server server = registerAndStart(builder);
            health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
            LOG.info("account-service listening on port {}", server.getPort());
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void requireOperatorToken(String apiToken) {
        if (apiToken == null || apiToken.isBlank()) {
            throw new IllegalArgumentException(
                    "the account transport requires a nonblank operator token (" + ENV_API_TOKEN + ")");
        }
    }

    private Server registerAndStart(ServerBuilder<?> builder) throws IOException {
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
