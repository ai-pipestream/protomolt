package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.engine.DriveProvisioner;

import ai.protomolt.proto.repo.spi.ArchiveRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.engine.ArchiveOperations;

import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.cache.CachingBlobStore;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.container.lifecycle.CoherenceProbe;
import ai.protomolt.proto.repo.container.lifecycle.EventRelay;
import ai.protomolt.proto.repo.container.lifecycle.JdbcEventOutbox;
import ai.protomolt.proto.repo.container.lifecycle.JdbcPurgeQueue;
import ai.protomolt.proto.repo.container.lifecycle.KafkaPurgeQueue;
import ai.protomolt.proto.repo.container.lifecycle.PurgeQueue;
import ai.protomolt.proto.repo.container.lifecycle.PurgeSweeper;
import ai.protomolt.proto.repo.container.lifecycle.S3Purger;
import ai.protomolt.proto.repo.container.lifecycle.StorageReconciler;
import ai.protomolt.proto.repo.blob.grpc.RemoteBlobStore;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
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
 * ledgers → S3 client → blob store (S3 direct, or the dogfood
 * {@code RemoteBlobStore} when {@code DOCUMENT_PLATFORM_BLOB_STORE} selects a
 * repo mode) → part storage → the two gRPC service impls. Construction fails
 * fast (migrations, mapping validation) — nothing starts lazily. Every
 * server's call executor is a virtual-thread-per-task executor: every handler
 * is plain blocking code (JDBC, S3), and a blocked call parks its virtual
 * thread instead of a carrier, so no offload/directExecutor tricks are
 * needed.
 *
 * <p>Seeded default account ({@code DOCUMENT_PLATFORM_SEED_ACCOUNT_ID}):
 * standalone deployments without an account-service name ONE seed account in
 * the environment, and {@link #seedAccountDrives()} idempotently ensures its
 * two provisioning-time drives ({@code intake} and {@code pipeline}) exist.
 * Seeding is deliberately NOT part of {@code build()}: {@link RepoServiceMain}
 * opts in after building, and embedded hosts ({@link #startInProcess(String)})
 * call the method themselves when they want it. Unset/blank = no seeding.
 *
 * <p>Bulk uploads are served by {@link #startHttp(int)}: the streaming HTTP
 * route whose body flows to object storage without buffering, next to the
 * unary gRPC API.
 */
public final class RepoServices implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RepoServices.class);

    private final RepoServiceConfig config;
    private final LedgerDatabase database;
    private final Tx tx;
    private final DocumentLedger documentLedger;
    private final DriveLedger driveLedger;
    private final PurgeQueue purgeQueue;
    private final OwnedResources owned = new OwnedResources();
    private final BlobStore blobStore;
    private final ManagedChannel remoteChannel;
    private final PartStorage partStorage;
    private final DocumentGrpcService documentService;
    private final ai.protomolt.proto.repo.spi.RawIngestionRepository rawIngestion;
    private final ai.protomolt.proto.repo.engine.RawObjectRecovery rawRecovery;
    private final ai.protomolt.proto.repo.container.ledger.DocumentAttemptRecoveryService documentRecovery;
    private boolean lifecycleStarted;
    private final ArchiveOperations archiveOperations;
    private final ManagedArchiveServices managedArchive;
    private final ManagedDocumentServices managedDocuments;
    private final DriveProvisioner driveProvisioner;
    private final ai.protomolt.proto.repo.spi.DriveRepository driveOperations;
    private final List<BindableService> services;
    private final S3Purger s3Purger;
    private final PurgeSweeper purgeSweeper;
    private final StorageReconciler storageReconciler;
    private final CoherenceProbe coherenceProbe;
    private final JdbcEventOutbox eventOutbox;
    private final EventRelay eventRelay;
    private final KafkaProducer<String, com.google.protobuf.Message> eventProducer;
    private final KafkaProducer<String, com.google.protobuf.Message> purgeProducer;
    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> purgeConsumer;

    private final List<GrpcServerLifetime> servers = new CopyOnWriteArrayList<>();
    private final List<UploadHttpServer> httpServers = new CopyOnWriteArrayList<>();
    private final List<Thread> lifecycleThreads = new CopyOnWriteArrayList<>();
    private volatile boolean lifecycleClosed;

    private RepoServices(RepoServiceConfig config) {
        this(config, BridgeEngine.standard());
    }

    private RepoServices(RepoServiceConfig config, BridgeEngine bridges) {
        this(config, bridges, ai.protomolt.proto.repo.blob.spi.BlobStores.discover());
    }

    RepoServices(RepoServiceConfig config, BridgeEngine bridges, ai.protomolt.proto.repo.blob.spi.BlobStores providers) {
        ManagedArchiveServices startingArchive = null;
        try {
            this.config = config;
            if (config.managedStorage().retentionQualified()
                    && (!config.lifecycleEnabled() || !("s3".equals(config.blobStore()) || "s3-redis-cache".equals(config.blobStore()))))
                throw new IllegalArgumentException("Managed storage requires an S3 backing store and enabled lifecycle recovery");
            if ((RepoServiceConfig.BLOB_STORE_REPO.equals(config.blobStore())
                    || RepoServiceConfig.BLOB_STORE_REPO_INPROCESS.equals(config.blobStore()))
                    && config.repoBucketBindings().isEmpty())
                throw new IllegalArgumentException(RepoServiceConfig.ENV_REPO_BUCKET_BINDINGS + " is required for remote storage");
            this.database = owned.add(new LedgerDatabase(config.ledger()));
            this.tx = new Tx(database.entityManagerFactory());
            this.documentLedger = new DocumentLedger(tx);
            String selectedDriveProvider = RepoServiceConfig.BLOB_STORE_S3_REDIS_CACHE.equals(config.blobStore())
                    ? "s3" : config.blobStore();
            var driveGate = new SelectedDriveBackend(config);
            this.driveLedger = new DriveLedger(tx, driveGate);
            // Purge-queue selection (DOCUMENT_PLATFORM_PURGE_QUEUE): "jdbc"
            // claims rows straight from document_purges; "kafka" keeps the row as
            // the ledger of record and distributes claims through the purge topic
            // (the config already failed fast when kafka is selected without
            // bootstrap servers).
            if (RepoServiceConfig.PURGE_QUEUE_KAFKA.equals(config.purgeQueue())) {
                this.purgeProducer = owned.add(KafkaPurgeQueue.newProducer(config.kafkaBootstrapServers(),
                        config.schemaRegistryUrl()));
                this.purgeConsumer = owned.add(KafkaPurgeQueue.newConsumer(config.kafkaBootstrapServers(),
                        PURGE_CONSUMER_GROUP));
                this.purgeQueue = KafkaPurgeQueue.create(tx, purgeProducer, purgeConsumer,
                        config.kafkaPurgeTopic(), PURGE_POLL_TIMEOUT);
            } else {
                this.purgeProducer = null;
                this.purgeConsumer = null;
                this.purgeQueue = new JdbcPurgeQueue(tx);
            }
            ai.protomolt.proto.repo.blob.spi.NamespaceProvisioner namespaces;
            java.util.Set<ai.protomolt.proto.repo.blob.spi.BlobCapability> managedCapabilities = java.util.Set.of();
            ai.protomolt.proto.repo.blob.spi.ObjectReclaimer managedReclaimer = null;
            ai.protomolt.proto.repo.blob.spi.OpenedBlobStore managedBacking = null;
            switch (config.blobStore()) {
                case RepoServiceConfig.BLOB_STORE_S3 -> {
                    var selected = owned.add(providers.open("s3", s3Options(config)));
                    this.blobStore = selected.store();
                    managedCapabilities = selected.capabilities();
                    managedReclaimer = selected.reclaimer();
                    managedBacking = selected;
                    namespaces = selected::ensureNamespace;
                    this.remoteChannel = null;
                }
                case RepoServiceConfig.BLOB_STORE_REDIS -> {
                    var selected = owned.add(providers.open("redis", redisOptions(config)));
                    this.blobStore = selected.store();
                    namespaces = selected::ensureNamespace;
                    this.remoteChannel = null;
                }
                case RepoServiceConfig.BLOB_STORE_S3_REDIS_CACHE -> {
                    var backing = owned.add(providers.open("s3", s3Options(config)));
                    var cache = owned.add(providers.open("redis", redisOptions(config)));
                    this.blobStore = new CachingBlobStore(backing.store(), cache.store(),
                            config.redisTtlSeconds(), config.redisMaxObjectBytes());
                    managedCapabilities = backing.capabilities();
                    managedReclaimer = ((CachingBlobStore) blobStore).reclaimer(backing.reclaimer());
                    managedBacking = backing;
                    namespaces = backing::ensureNamespace;
                    this.remoteChannel = null;
                }
                default -> {
                    this.remoteChannel = RepoServiceConfig.BLOB_STORE_REPO.equals(config.blobStore())
                            ? NettyChannelBuilder.forTarget(config.repoTarget()).usePlaintext().build()
                            : InProcessChannelBuilder.forName(config.repoTarget()).build();
                    owned.add(() -> {
                        remoteChannel.shutdownNow();
                        if (!remoteChannel.awaitTermination(10, TimeUnit.SECONDS))
                            throw new IllegalStateException("Repository client channel did not terminate");
                    });
                    this.blobStore = new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(remoteChannel),
                            config.repoBucketBindings(), java.time.Duration.ofSeconds(30));
                    namespaces = blobStore::headBucket;
                }
            }
            this.partStorage = new PartStorage();
            // Kafka eventing (DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS): the
            // transactional outbox. Unset = no outbox, no relay, no producer, and
            // the commit points skip the outbox entirely (zero overhead).
            this.eventOutbox = config.kafkaEnabled() ? new JdbcEventOutbox(tx) : null;
            this.eventRelay = eventOutbox != null ? new EventRelay(eventOutbox) : null;
            this.eventProducer = eventOutbox != null
                    ? owned.add(EventRelay.newProducer(config.kafkaBootstrapServers(),
                            config.schemaRegistryUrl())) : null;
            String generation = config.managedStorage().retentionQualified() ? config.managedStorage().backendGeneration() : null;
            var documents = new ai.protomolt.proto.repo.engine.DocumentOperations(documentLedger, driveLedger, tx,
                    blobStore, partStorage, purgeQueue, eventOutbox, generation);
            this.documentService = new DocumentGrpcService(documents,
                    new ai.protomolt.proto.repo.engine.BlobOperations(blobStore, driveLedger));
            var archiveLedger = new ai.protomolt.proto.repo.container.archive.ArchiveLedger(tx);
            if (generation != null) {
                if (!managedCapabilities.containsAll(java.util.Set.of(
                        ai.protomolt.proto.repo.blob.spi.BlobCapability.STREAMING_WRITE,
                        ai.protomolt.proto.repo.blob.spi.BlobCapability.NON_EXPIRING_WRITES,
                        ai.protomolt.proto.repo.blob.spi.BlobCapability.PHYSICAL_RECLAMATION)))
                    throw new IllegalArgumentException("Selected backing provider cannot support managed ingestion and reclamation");
                var profiles = new ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger(tx);
                var profile = new ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger.Profile(
                        providers.managedIdentity("s3", s3Options(config)), config.managedStorage().storageRealm());
                profiles.bind(generation, profile);
                var reclaimer = java.util.Objects.requireNonNull(managedReclaimer);
                this.documentRecovery = new ai.protomolt.proto.repo.container.ledger.DocumentAttemptRecoveryService(
                        tx, generation, profile, java.util.Objects.requireNonNull(managedBacking), reclaimer);
                this.rawRecovery = new ai.protomolt.proto.repo.engine.RawObjectRecovery(documentLedger.rawObjects(), profiles,
                        (originalGeneration, originalProfile) -> {
                            if (!generation.equals(originalGeneration) || !profile.equals(originalProfile))
                                throw new IllegalStateException("Original managed backend is not configured on this host");
                            return reclaimer;
                        });
                this.rawIngestion = new ai.protomolt.proto.repo.engine.RawIngestionOperations(documents, documentLedger,
                        driveLedger, blobStore, generation, managedCapabilities);
                this.managedArchive = startingArchive = new ManagedArchiveServices(tx, archiveLedger, blobStore,
                        managedCapabilities, generation, profile, reclaimer);
            } else {
                this.rawIngestion = null;
                this.rawRecovery = null;
                this.documentRecovery = null;
                this.managedArchive = null;
            }
            this.driveProvisioner = new DriveProvisioner(driveLedger,
                    namespaces,
                    config.defaultBucketBase(), config.s3Region(), selectedDriveProvider, driveGate);
            this.archiveOperations = new ArchiveOperations(
                    archiveLedger, driveLedger, blobStore, bridges,
                    managedArchive == null ? null : managedArchive.reader,
                    managedArchive == null ? null : managedArchive.writer);
            this.driveOperations = new ai.protomolt.proto.repo.engine.DriveOperations(driveLedger, driveProvisioner);
            var configuredServices = new java.util.ArrayList<BindableService>(List.of(
                    documentService,
                    new ArchiveGrpcService(archiveOperations),
                    new DriveGrpcService(driveOperations)));
            if (managedArchive != null) configuredServices.add(new ArchiveMutationGrpcService(managedArchive.mutations));
            this.services = List.copyOf(configuredServices);
            // The lifecycle engine (two-phase delete): stateless workers over the
            // same ledgers/queue, driven by startLifecycle()'s loops or, in tests,
            // by hand via the accessors below.
            this.s3Purger = new S3Purger(tx, documentLedger, driveLedger, purgeQueue, eventOutbox);
            this.purgeSweeper = new PurgeSweeper(tx, documentLedger, driveLedger, purgeQueue);
            this.storageReconciler = new StorageReconciler(documentLedger);
            this.coherenceProbe = new CoherenceProbe(documentLedger, driveLedger);
            // Register the native reader last: no later constructor step may fail
            // after this component acquires its durable lifecycle identity.
            this.managedDocuments = generation == null ? null : new ManagedDocumentServices(tx, driveLedger,
                    generation, new ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger(tx).find(generation).orElseThrow(),
                    managedBacking, config.kafkaEnabled());
        } catch (RuntimeException | Error failure) {
            if (startingArchive != null) {
                // Construction has not exposed services or started workers. Preserve
                // its registered reader identity before releasing the borrowed ledger.
                try {
                    startingArchive.reader.close();
                    if (!startingArchive.reader.awaitIdle(java.time.Duration.ofSeconds(5)))
                        throw new IllegalStateException("Fresh archive reader did not drain after startup failure");
                    startingArchive.reader.attestLocalQuiescence();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure.addSuppressed(interrupted);
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                }
            }
            try { owned.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
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
     * Builds the service set with an explicit bridge engine. The default
     * engine runs what needs no other service; a host that can reach a
     * parser supplies one that also runs the text and OCR bridges.
     *
     * @param config the service configuration
     * @param bridges the bridges this host can execute
     * @return the service set
     */
    public static RepoServices build(RepoServiceConfig config, BridgeEngine bridges) {
        return new RepoServices(config, bridges);
    }

    /** Archive operations sharing this composition's storage lifetime. */
    public ArchiveRepository archiveRepository() {
        requireOpen();
        if (managedArchive != null) startLifecycle();
        return archiveOperations;
    }

    /** Identified mutations require qualified storage and running recovery. */
    public ai.protomolt.proto.repo.spi.ArchiveMutationRepository archiveMutationRepository() {
        requireOpen();
        if (managedArchive == null) throw new IllegalStateException("Managed archive storage is not configured");
        startLifecycle();
        return managedArchive.mutations;
    }

    /** Drive operations sharing this composition's storage lifetime. */
    public ai.protomolt.proto.repo.spi.DriveRepository driveRepository() {
        requireOpen();
        return driveOperations;
    }

    /** Shared document operations; this composition retains ownership of storage resources. */
    public ai.protomolt.proto.repo.spi.DocumentRepository repository() {
        requireOpen();
        if (documentRecovery != null) startLifecycle();
        return documentService.repository();
    }

    /**
     * The wired gRPC services for hosts that register them on their own builder.
     * Qualified managed storage also mounts ArchiveMutationService. Starts managed
     * recovery before returning; the embedding host must close this composition
     * if its own server startup fails. The mutation adapter requires an explicit
     * authenticated CallerContexts entry.
     *
     * @return the service implementations, unmodifiable
     */
    public List<BindableService> services() {
        requireOpen();
        if (managedArchive != null) startLifecycle();
        return services;
    }

    /**
     * Starts all services on an in-process server (same-JVM embedding).
     *
     * @param name the in-process transport name; clients reach it via
     *        {@code InProcessChannelBuilder.forName(name)}
     * @return the started server (also closed by {@link #close()})
     */
    public synchronized Server startInProcess(String name) {
        requireOpen();
        RemoteRouting.rejectInProcess(config, name);
        try {
            return registerAndStart(InProcessServerBuilder.forName(name)
                    .maxInboundMessageSize(10 * 1024 * 1024));
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
        return startNetty(port, null, null);
    }

    /**
     * Starts all services on a Netty TCP server, requiring a call credential when
     * {@code apiToken} is set.
     *
     * <p>The repository holds every account's documents and every claim-check blob, so an
     * unauthenticated listener is a read and write path to all of them. With a token, every
     * call must present it in {@code api_token} metadata (or {@code authorization: Bearer});
     * reflection and health are covered too, because reflection enumerates the very RPCs
     * being guarded. With a {@link CallerResolver}, a credential the mounted access policy
     * names runs as its principal instead of with process authority.
     *
     * <p>Without a token the listener stays open, which is the trusted-network deployment
     * this service has always supported: a repository reachable only from inside the node's
     * network, with authentication enforced at the surfaces in front of it. That default is
     * unchanged so an existing deployment does not break, but a repository reachable from
     * anywhere else should set a token.
     *
     * @param port the listen port (0 = ephemeral)
     * @param apiToken the operator credential every call must present, or null to serve open
     * @param resolver resolves a policy-named credential to its principal; requires a token
     * @return the started server (also closed by {@link #close()})
     */
    public synchronized Server startNetty(int port, String apiToken, CallerResolver resolver) {
        requireOpen();
        if (apiToken == null && resolver != null) {
            throw new IllegalArgumentException(
                    "an access-policy resolver requires the operator api token");
        }
        try {
            HealthStatusManager health = new HealthStatusManager();
            var builder = NettyServerBuilder.forPort(port)
                    .maxInboundMessageSize(10 * 1024 * 1024)
                    .addService(health.getHealthService())
                    .addService(ProtoReflectionService.newInstance());
            if (apiToken != null) {
                builder.intercept(new ApiTokenServerInterceptor(apiToken, resolver));
            }
            Server server = registerAndStart(builder, started -> RemoteRouting.rejectTcp(config, started.getPort()));
            health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
            LOG.info("repo-service listening on port {}", server.getPort());
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Server registerAndStart(io.grpc.ServerBuilder<?> builder) throws IOException {
        return registerAndStart(builder, server -> {});
    }

    private Server registerAndStart(io.grpc.ServerBuilder<?> builder,
                                    java.util.function.Consumer<Server> routingCheck) throws IOException {
        GrpcServerLifetime transport = null;
        try {
            services().forEach(builder::addService);
            transport = GrpcServerLifetime.start(builder, servers::add);
            routingCheck.accept(transport.server());
            return transport.server();
        } catch (IOException | RuntimeException | Error failure) {
            // services() may have started durable recovery before binding.
            // A failed listener must not leave that composition running unseen.
            try { close(); }
            catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /**
     * Starts the streaming HTTP upload server ({@code POST
     * /v1/documents:upload}, see {@link UploadHttpServer}) alongside the gRPC
     * transports. This is where bulk bytes belong: the body streams to object
     * storage without ever being buffered in memory, unlike the unary blob
     * RPCs.
     *
     * @param port the listen port (0 = ephemeral; read the bound port back
     *        from the returned server)
     * @return the started HTTP server (also closed by {@link #close()})
     */
    public UploadHttpServer startHttp(int port) {
        return startHttp(port, null);
    }

    /**
     * Starts the streaming HTTP upload server, requiring a credential when
     * {@code apiToken} is set. The route writes into any account's drive, so it takes the
     * same credential as the gRPC surface rather than a second one; without a token it
     * serves open, the trusted-network default this server has always had.
     *
     * @param port the listen port (0 = ephemeral; read the bound port back
     *        from the returned server)
     * @param apiToken the credential every request must present, or null to serve open
     * @return the started HTTP server (also closed by {@link #close()})
     */
    public synchronized UploadHttpServer startHttp(int port, String apiToken) {
        requireOpen();
        if (rawIngestion != null) startLifecycle();
        UploadHttpServer http = new UploadHttpServer(rawIngestion, apiToken, archiveOperations);
        httpServers.add(http);
        try {
            http.start(port);
            return http;
        } catch (RuntimeException | Error failure) {
            try { close(); }
            catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /**
     * Seeded default account ({@code DOCUMENT_PLATFORM_SEED_ACCOUNT_ID}):
     * idempotently ensures the seed account's two provisioning-time drives
     * exist — {@code intake} ({@code INTAKE}) and {@code pipeline}
     * ({@code PIPELINE}) — through the same {@link DriveProvisioner} the gRPC
     * {@code CreateDrive} path uses, logging each drive as created vs. found.
     * {@code account_id} stays required on every request; this only
     * pre-creates the drives a standalone deployment would otherwise have to
     * provision by hand. No-op when the variable is unset. Opt-in:
     * {@link RepoServiceMain} calls this after {@link #build(RepoServiceConfig)}
     * and before serving; embedded hosts call it themselves when they want it.
     */
    public void seedAccountDrives() {
        String accountId = config.seedAccountId();
        if (accountId == null) {
            return;
        }
        DriveRecord intake = driveProvisioner.ensureDrive(accountId, "intake",
                DriveType.DRIVE_TYPE_INTAKE);
        DriveRecord pipeline = driveProvisioner.ensureDrive(accountId, "pipeline",
                DriveType.DRIVE_TYPE_PIPELINE);
        LOG.info("Seed account '{}' drives ready: intake (id={}), pipeline (id={})",
                accountId, intake.driveId, pipeline.driveId);
    }

    /**
     * Starts the background purge lifecycle: two single virtual-thread loops —
     * the purger ({@code drainOnce} against the purge queue; a non-empty drain
     * loops again immediately, an empty one backs off
     * {@code DOCUMENT_PLATFORM_PURGE_INTERVAL_MS}, default 5000) and the
     * sweeper ({@code sweepOnce} every {@code DOCUMENT_PLATFORM_SWEEP_INTERVAL_MS},
     * default 60000). When {@code DOCUMENT_PLATFORM_RECONCILE_ENABLED} is set,
     * a third slow loop reconciles every drive (dry-run logging unless
     * {@code DOCUMENT_PLATFORM_RECONCILE_DRY_RUN=false}). When
     * {@code DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS} is set, a fourth loop
     * relays the document-events outbox to Kafka (same drain/backoff shape as
     * the purger). Every loop catches
     * and logs per iteration — one bad record never kills a loop. No-op when
     * {@code DOCUMENT_PLATFORM_LIFECYCLE_ENABLED=false}; the loops stop in
     * {@link #close()}.
     */
    public synchronized void startLifecycle() {
        requireOpen();
        if (lifecycleStarted) return;
        if (!config.lifecycleEnabled()) {
            LOG.info("repo lifecycle loops disabled ({})",
                    RepoServiceConfig.ENV_LIFECYCLE_ENABLED + "=false");
            return;
        }
        try {
            if (managedDocuments != null) {
                startLifecycleThread("repo-document-read-reconciliation", () -> {
                    managedDocuments.publication.tick();
                    sleep(config.sweepIntervalMs());
                });
            }
            if (managedArchive != null) {
                startLifecycleThread("repo-archive-mutation-recovery", () -> {
                    managedArchive.recoverMutations(java.time.Instant.now().minusMillis(config.purgeIntervalMs()), 100);
                    sleep(config.purgeIntervalMs());
                });
                startLifecycleThread("repo-archive-reconciliation", () -> {
                    managedArchive.reconcile(java.time.Instant.now().minusMillis(config.reconcileMinAgeMs()), 100);
                    sleep(config.sweepIntervalMs());
                });
            }
            if (rawRecovery != null) {
                startLifecycleThread("repo-raw-recovery", () -> {
                    var cutoff = java.time.Instant.now().minus(java.time.Duration.ofHours(1));
                    for (var candidate : documentLedger.rawObjects().cleanupCandidates(cutoff, 100)) {
                        if (lifecycleClosed || Thread.currentThread().isInterrupted()) break;
                        try { rawRecovery.recover(candidate.rawId, cutoff); }
                        catch (RuntimeException failure) {
                            LOG.warn("Managed raw cleanup failed for {}; durable retry remains pending", candidate.rawId, failure);
                        }
                    }
                    sleep(config.sweepIntervalMs());
                });
            }
            if (documentRecovery != null) {
                startLifecycleThread("repo-document-attempt-recovery", () -> {
                    var results = documentRecovery.reconcilePass(java.time.Duration.ofHours(1),
                            java.time.Duration.ofMinutes(10), 100);
                    for (var result : results) {
                        if (result.failure() != null)
                            LOG.warn("Document attempt cleanup failed for {}; durable retry remains pending", result.attemptId(), result.failure());
                        else if (result.outcome() == ai.protomolt.proto.repo.container.ledger.DocumentAttemptRecoveryService.Outcome.RETRY
                                || result.outcome() == ai.protomolt.proto.repo.container.ledger.DocumentAttemptRecoveryService.Outcome.LOST_CLAIM)
                            LOG.warn("Document attempt cleanup for {} returned {}; another pass is required", result.attemptId(), result.outcome());
                    }
                    sleep(config.sweepIntervalMs());
                });
            }
            startLifecycleThread("repo-purger", () -> {
                int purged = s3Purger.drainOnce(blobStore, PURGE_BATCH_SIZE);
                // Idle backoff: work left → drain again immediately; empty → wait.
                if (purged == 0) {
                    sleep(config.purgeIntervalMs());
                }
            });
            startLifecycleThread("repo-purge-sweeper", () -> {
                purgeSweeper.sweepOnce();
                sleep(config.sweepIntervalMs());
            });
            if (eventRelay != null) {
                startLifecycleThread("repo-event-relay", () -> {
                    int published = eventRelay.relayOnce(eventProducer, config.kafkaTopic(),
                            RELAY_BATCH_SIZE);
                    // Idle backoff: work left → drain again immediately; empty → wait.
                    if (published == 0) {
                        sleep(config.purgeIntervalMs());
                    }
                });
            }
            if (config.reconcileEnabled()) {
                startLifecycleThread("repo-storage-reconciler", () -> {
                    reconcileAllDrives();
                    sleep(config.sweepIntervalMs());
                });
            }
            LOG.info("repo lifecycle loops started (purge interval {} ms, sweep interval {} ms,"
                            + " reconcile {})", config.purgeIntervalMs(), config.sweepIntervalMs(),
                    config.reconcileEnabled()
                            ? "enabled (dryRun=" + config.reconcileDryRun() + ")" : "disabled");
            lifecycleStarted = true;
        } catch (RuntimeException | Error failure) {
            // A partial worker set must not leave a usable upload composition.
            try { close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Purge drain batch size per loop iteration. */
    private static final int PURGE_BATCH_SIZE = 100;

    /** Event-relay drain batch size per loop iteration. */
    private static final int RELAY_BATCH_SIZE = 100;

    /** The purger fleet's consumer group (Kafka purge queue only). */
    private static final String PURGE_CONSUMER_GROUP = "repo-purger";

    /** The Kafka purge queue's consumer poll budget per claim. */
    private static final java.time.Duration PURGE_POLL_TIMEOUT = java.time.Duration.ofSeconds(1);

    /** One reconcile pass over every drive's bucket scope. */
    private void reconcileAllDrives() {
        for (DriveRecord drive : driveLedger.listAll(1000)) {
            try {
                storageReconciler.reconcile(blobStore, drive.bucket, drive.prefix,
                        java.time.Duration.ofMillis(config.reconcileMinAgeMs()),
                        config.reconcileDryRun());
            } catch (RuntimeException e) {
                LOG.warn("Reconcile of drive '{}' (bucket {}) failed: {}",
                        drive.name, drive.bucket, e.getMessage());
            }
        }
    }

    /** Runs {@code iteration} forever (until close), catching everything per iteration. */
    private void startLifecycleThread(String name, Runnable iteration) {
        Thread thread = Thread.ofVirtual().name(name).start(() -> {
            while (!lifecycleClosed) {
                try {
                    iteration.run();
                } catch (RuntimeException e) {
                    LOG.warn("{} iteration failed (loop continues): {}", name, e.getMessage(), e);
                    sleep(1000);
                }
            }
        });
        lifecycleThreads.add(thread);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stops lifecycle workers and transports before releasing providers, messaging clients and the ledger. */
    @Override
    public synchronized void close() {
        close(java.time.Duration.ofSeconds(10));
    }

    synchronized void close(java.time.Duration timeout) {
        java.util.Objects.requireNonNull(timeout);
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("Shutdown timeout must be positive");
        lifecycleClosed = true;
        if (managedDocuments != null) managedDocuments.publication.close();
        if (managedArchive != null) managedArchive.reader.close();
        LifecycleShutdown.stopBeforeRelease(lifecycleThreads, timeout, () -> releaseAfterWorkersStop(timeout));
    }

    private void releaseAfterWorkersStop(java.time.Duration timeout) {
        var transports = new java.util.ArrayList<AutoCloseable>();
        transports.addAll(httpServers);
        transports.addAll(servers);
        ShutdownBarrier.releaseAfter(transports, () -> {
            if (managedDocuments != null) managedDocuments.drain(timeout);
            if (managedArchive != null) {
                try {
                    if (!managedArchive.reader.awaitIdle(timeout))
                        throw new IllegalStateException("Managed archive reads still active; shared resources retained");
                    managedArchive.reader.attestLocalQuiescence();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Archive reader drain interrupted; shared resources retained", interrupted);
                }
            }
            lifecycleThreads.clear();
            httpServers.clear();
            servers.clear();
            owned.close();
        });
        LOG.info("repo-service stopped");
    }

    private void requireOpen() {
        if (lifecycleClosed) throw new IllegalStateException("repository services are closed");
    }

    private static java.util.Map<String, String> redisOptions(RepoServiceConfig config) {
        return java.util.Map.of("uri", config.redisUri(), "ttl-seconds", Integer.toString(config.redisTtlSeconds()),
                "max-object-bytes", Long.toString(config.redisMaxObjectBytes()), "key-prefix", "");
    }

    private static java.util.Map<String, String> s3Options(RepoServiceConfig config) {
        var options = new java.util.HashMap<String, String>();
        options.put("endpoint", config.s3Endpoint() == null ? "" : config.s3Endpoint());
        options.put("region", config.s3Region());
        options.put("path-style", Boolean.toString(config.s3Endpoint() != null));
        options.put("conditional-writes", Boolean.toString(config.s3ConditionalWrites()));
        options.put("credentials-mode", config.hasStaticCredentials() ? "static" : "default-chain");
        if (config.hasStaticCredentials()) {
            options.put("access-key", config.s3AccessKey());
            options.put("secret-key", config.s3SecretKey());
        }
        return options;
    }

    // Package-private accessors: the IT asserts on ledger state through the
    // same wired components (no mocks).

    DocumentLedger documentLedger() {
        return documentLedger;
    }

    ai.protomolt.proto.repo.container.ledger.DocumentPublicationRuntime documentPublication() {
        requireOpen();
        if (managedDocuments == null) throw new IllegalStateException("Managed document storage is not configured");
        startLifecycle();
        return managedDocuments.publication;
    }

    ai.protomolt.proto.repo.spi.HistoricalDocumentRepository documentHistory() {
        requireOpen();
        if (managedDocuments == null) throw new IllegalStateException("Managed document storage is not configured");
        startLifecycle();
        return managedDocuments.history;
    }

    DriveLedger driveLedger() {
        return driveLedger;
    }

    BlobStore blobStore() {
        return blobStore;
    }

    javax.sql.DataSource ledgerDataSource() {
        return database.dataSource();
    }


    PurgeQueue purgeQueue() {
        return purgeQueue;
    }

    S3Purger s3Purger() {
        return s3Purger;
    }

    PurgeSweeper purgeSweeper() {
        return purgeSweeper;
    }

    StorageReconciler storageReconciler() {
        return storageReconciler;
    }

    CoherenceProbe coherenceProbe() {
        return coherenceProbe;
    }

    JdbcEventOutbox eventOutbox() {
        return eventOutbox;
    }

    EventRelay eventRelay() {
        return eventRelay;
    }

    KafkaProducer<String, com.google.protobuf.Message> eventProducer() {
        return eventProducer;
    }
}
