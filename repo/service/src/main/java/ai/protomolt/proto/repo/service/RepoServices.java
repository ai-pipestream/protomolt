package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.blob.CachingBlobStore;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.blob.RedisBlobStore;
import ai.protomolt.proto.repo.container.blob.RedisBlobStoreConfig;
import ai.protomolt.proto.repo.container.blob.S3BlobStore;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
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
import ai.protomolt.proto.repo.service.client.RemoteBlobStore;
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
    private final S3Client s3Client;
    private final BlobStore blobStore;
    private final ManagedChannel remoteChannel;
    private final PartStorage partStorage;
    private final DocumentGrpcService documentService;
    private final ArchiveOperations archiveOperations;
    private final DriveProvisioner driveProvisioner;
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

    private final List<Server> servers = new CopyOnWriteArrayList<>();
    private final List<UploadHttpServer> httpServers = new CopyOnWriteArrayList<>();
    private final List<Thread> lifecycleThreads = new CopyOnWriteArrayList<>();
    private volatile boolean lifecycleClosed;

    private RepoServices(RepoServiceConfig config) {
        this.config = config;
        this.database = new LedgerDatabase(config.ledger());
        this.tx = new Tx(database.entityManagerFactory());
        this.documentLedger = new DocumentLedger(tx);
        this.driveLedger = new DriveLedger(tx);
        // Purge-queue selection (DOCUMENT_PLATFORM_PURGE_QUEUE): "jdbc"
        // claims rows straight from document_purges; "kafka" keeps the row as
        // the ledger of record and distributes claims through the purge topic
        // (the config already failed fast when kafka is selected without
        // bootstrap servers).
        if (RepoServiceConfig.PURGE_QUEUE_KAFKA.equals(config.purgeQueue())) {
            this.purgeProducer = KafkaPurgeQueue.newProducer(config.kafkaBootstrapServers(),
                    config.schemaRegistryUrl());
            this.purgeConsumer = KafkaPurgeQueue.newConsumer(config.kafkaBootstrapServers(),
                    PURGE_CONSUMER_GROUP);
            this.purgeQueue = KafkaPurgeQueue.create(tx, purgeProducer, purgeConsumer,
                    config.kafkaPurgeTopic(), PURGE_POLL_TIMEOUT);
        } else {
            this.purgeProducer = null;
            this.purgeConsumer = null;
            this.purgeQueue = new JdbcPurgeQueue(tx);
        }
        this.s3Client = buildS3Client(config);
        // Blob-store selection (DOCUMENT_PLATFORM_BLOB_STORE): "s3" is the
        // direct object-storage path; "repo"/"repo-inprocess" dogfood the
        // service's own blob API — bytes delegate to another repo-service
        // (netty or in-process target) via RemoteBlobStore. Note the target
        // must be a DIFFERENT service set: pointing a repo mode back at this
        // one would recurse PutBlob into itself. "redis" keeps objects in
        // Redis outright; "s3-redis-cache" puts an expendable Redis
        // read-through/write-through cache in front of the S3 store of truth.
        switch (config.blobStore()) {
            case RepoServiceConfig.BLOB_STORE_S3 -> {
                this.blobStore = new S3BlobStore(s3Client);
                this.remoteChannel = null;
            }
            case RepoServiceConfig.BLOB_STORE_REDIS -> {
                this.blobStore = new RedisBlobStore(redisConfig(config));
                this.remoteChannel = null;
            }
            case RepoServiceConfig.BLOB_STORE_S3_REDIS_CACHE -> {
                this.blobStore = new CachingBlobStore(new S3BlobStore(s3Client),
                        new RedisBlobStore(redisConfig(config)),
                        config.redisTtlSeconds(), config.redisMaxObjectBytes());
                this.remoteChannel = null;
            }
            default -> {
                this.remoteChannel = RepoServiceConfig.BLOB_STORE_REPO.equals(config.blobStore())
                        ? NettyChannelBuilder.forTarget(config.repoTarget()).usePlaintext().build()
                        : InProcessChannelBuilder.forName(config.repoTarget()).build();
                this.blobStore = new RemoteBlobStore(
                        DocumentServiceGrpc.newBlockingStub(remoteChannel), config.repoDrive());
            }
        }
        this.partStorage = new PartStorage();
        // Kafka eventing (DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS): the
        // transactional outbox. Unset = no outbox, no relay, no producer, and
        // the commit points skip the outbox entirely (zero overhead).
        this.eventOutbox = config.kafkaEnabled() ? new JdbcEventOutbox(tx) : null;
        this.eventRelay = eventOutbox != null ? new EventRelay(eventOutbox) : null;
        this.eventProducer = eventOutbox != null
                ? EventRelay.newProducer(config.kafkaBootstrapServers(),
                        config.schemaRegistryUrl()) : null;
        this.documentService = new DocumentGrpcService(documentLedger, driveLedger, tx,
                blobStore, partStorage, purgeQueue, eventOutbox);
        this.driveProvisioner = new DriveProvisioner(driveLedger, s3Client,
                config.defaultBucketBase(), config.s3Region());
        this.archiveOperations = new ArchiveOperations(
                new ai.protomolt.proto.repo.container.archive.ArchiveLedger(tx),
                driveLedger, blobStore);
        this.services = List.of(
                documentService,
                new ArchiveGrpcService(archiveOperations),
                new DriveGrpcService(driveLedger, s3Client,
                        config.defaultBucketBase(), config.s3Region()));
        // The lifecycle engine (two-phase delete): stateless workers over the
        // same ledgers/queue, driven by startLifecycle()'s loops or, in tests,
        // by hand via the accessors below.
        this.s3Purger = new S3Purger(tx, documentLedger, driveLedger, purgeQueue, eventOutbox);
        this.purgeSweeper = new PurgeSweeper(tx, documentLedger, driveLedger, purgeQueue);
        this.storageReconciler = new StorageReconciler(documentLedger);
        this.coherenceProbe = new CoherenceProbe(documentLedger, driveLedger);
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
     * The wired gRPC services (document + archive + drive), for hosts that register
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
    public Server startNetty(int port, String apiToken, CallerResolver resolver) {
        if (apiToken == null && resolver != null) {
            throw new IllegalArgumentException(
                    "an access-policy resolver requires the operator api token");
        }
        try {
            HealthStatusManager health = new HealthStatusManager();
            var builder = NettyServerBuilder.forPort(port)
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .addService(health.getHealthService())
                    .addService(ProtoReflectionService.newInstance());
            if (apiToken != null) {
                builder.intercept(new ApiTokenServerInterceptor(apiToken, resolver));
            }
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
    public UploadHttpServer startHttp(int port, String apiToken) {
        UploadHttpServer http = new UploadHttpServer(documentService, driveLedger,
                blobStore, apiToken, archiveOperations);
        http.start(port);
        httpServers.add(http);
        return http;
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
    public void startLifecycle() {
        if (!config.lifecycleEnabled()) {
            LOG.info("repo lifecycle loops disabled ({})",
                    RepoServiceConfig.ENV_LIFECYCLE_ENABLED + "=false");
            return;
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

    /** Stops the lifecycle loops, then all started servers, then closes the S3 client and the ledger database. */
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
        for (UploadHttpServer http : httpServers) {
            http.close();
        }
        httpServers.clear();
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
        if (purgeConsumer != null) {
            purgeConsumer.close();
        }
        if (purgeProducer != null) {
            purgeProducer.close();
        }
        if (remoteChannel != null) {
            remoteChannel.shutdownNow();
        }
        // Close Redis pools (direct redis store, or the cache inside the
        // caching decorator — CachingBlobStore.close closes both arms that
        // are closeable; S3BlobStore is not).
        if (blobStore instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.warn("blob store close failed", e);
            }
        }
        s3Client.close();
        database.close();
        LOG.info("repo-service stopped");
    }

    private static RedisBlobStoreConfig redisConfig(RepoServiceConfig config) {
        return new RedisBlobStoreConfig(config.redisUri(), config.redisTtlSeconds(),
                config.redisMaxObjectBytes(), "");
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

    BlobStore blobStore() {
        return blobStore;
    }

    S3Client s3Client() {
        return s3Client;
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
