package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.engine.DriveProvisioner;

import ai.protomolt.proto.repo.spi.ArchiveRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.engine.ArchiveOperations;

import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.OidcConsoleSessions;
import ai.protomolt.proto.authz.RequestCallerResolver;
import ai.protomolt.proto.http.connect.ConnectGateway;
import ai.protomolt.proto.http.connect.ConnectLimits;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.v1.DriveType;
import com.sun.net.httpserver.HttpHandler;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.services.HealthStatusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

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
 * {@link #startNetty(int, String, CallerResolver)}: TCP via Netty, plus the gRPC health-status and
 * reflection services; {@link RepoServiceMain} is exactly that path driven
 * from the environment.
 *
 * <p>Boot order (in {@link #build(RepoServiceConfig)}): ledger database
 * (pool → Flyway migration → validated JPA mappings) → transaction wrapper +
 * drive ledger → blob provider (S3, Redis, or S3 with Redis cache) → the
 * parts-based catalog and v2 accession service alongside the frozen archive
 * and drive services. Construction fails fast (migrations, mapping
 * validation) — nothing starts lazily. Every server's call executor is a
 * virtual-thread-per-task executor: every handler is plain blocking code
 * (JDBC, S3), and a blocked call parks its virtual thread instead of a
 * carrier, so no offload/directExecutor tricks are needed.
 *
 * <p>Seeded default account ({@code DOCUMENT_PLATFORM_SEED_ACCOUNT_ID}):
 * standalone deployments without an account-service name ONE seed account in
 * the environment, and {@link #seedAccountDrives()} idempotently ensures its
 * two provisioning-time drives ({@code intake} and {@code pipeline}) exist.
 * Seeding is deliberately NOT part of {@code build()}: {@link RepoServiceMain}
 * opts in after building, and embedded hosts ({@link #startInProcess(String)})
 * call the method themselves when they want it. Unset/blank = no seeding.
 *
 * <p>Bulk uploads are served by {@link #startHttp(int, String, RepositoryConsole)}: the streaming HTTP
 * route whose body flows to object storage without buffering, next to the
 * unary gRPC API.
 */
public final class RepoServices implements AutoCloseable {
    private final ReaderHostOptions readerHost;
    private boolean readerHostFenced;

    private static final Logger LOG = LoggerFactory.getLogger(RepoServices.class);
    /** The Connect gateway's message bound, the same as the gRPC transports' inbound limit. */
    private static final int CONNECT_MESSAGE_BYTES = 10 * 1024 * 1024;

    private final RepoServiceConfig config;
    private final LedgerDatabase database;
    private final Tx tx;
    private final DriveLedger driveLedger;
    private final OwnedResources owned = new OwnedResources();
    private final BlobStore blobStore;
    private boolean lifecycleStarted;
    private final UnaryRequestAdmission archiveIngress;
    private final ai.protomolt.proto.repo.engine.ArchivePutAdmission archiveAdmission;
    private final ai.protomolt.proto.repo.engine.ArchiveGetAdmission archiveReadAdmission;
    private final ArchiveOperations archiveOperations;
    /** The host read byte budget shared by v2 document reads and streamed rendition reads. */
    private final ReadByteBudget readBudget;
    private final ManagedArchiveServices managedArchive;
    private final DriveProvisioner driveProvisioner;
    private final ai.protomolt.proto.repo.spi.DriveRepository driveOperations;
    private final List<BindableService> services;

    // The parts-based v2 stack, present only in the full document profile: the catalog
    // lives in its own schema on the ledger database, and v2 drives resolve per account
    // through the drive ledger onto this host's blob store.
    private final ai.protomolt.proto.repo.catalog.PostgresCatalog catalog;
    private final ai.protomolt.proto.repo.accession.DocumentUploads v2Uploads;
    private final ai.protomolt.proto.repo.accession.DocumentSaves.Drives v2Drives;
    private final ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Resolver v2Resolver;
    private final ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Limits v2Limits;
    private final ai.protomolt.proto.repo.accession.OutboxRelay v2OutboxRelay;
    // One caller mapping for the document, drive and archive services; account
    // bindings are set before serving (withAccountBindings).
    private final RepoCallerBindings callerBindings = new RepoCallerBindings();
    // The catalog supplier reads the field this constructor assigns later; the
    // rebinding touches the catalog only once the services are serving.
    private final RepoStepSaveRebinding stepSave = new RepoStepSaveRebinding(this::stepGrantCatalog, callerBindings);

    private ai.protomolt.proto.repo.catalog.PostgresCatalog stepGrantCatalog() {
        return catalog;
    }

    /** Schema the parts-based catalog migrates into when co-hosted with the ledger. */
    private static final String V2_CATALOG_SCHEMA = "repo_catalog";
    /** Schemas admitted beyond the repository contract, keyed by type URL; set before serving. */
    private volatile java.util.Map<String, ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition>
            admittedSchemas = java.util.Map.of();

    /**
     * Admits the message types of descriptors co-mounted modules contributed (the parse
     * module's fleet document model), so typed {@code Any} content of those types
     * resolves at admission instead of refusing. Composition roots call this after
     * {@code build} and before any transport starts serving.
     *
     * @param contributed the contributed file descriptors
     * @return this instance, for chaining
     */
    public RepoServices withAdmittedSchemas(java.util.List<com.google.protobuf.Descriptors.FileDescriptor> contributed) {
        java.util.Objects.requireNonNull(contributed);
        if (contributed.isEmpty()) {
            return this;
        }
        var byTypeUrl = new java.util.HashMap<String, ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition>();
        for (var file : contributed) {
            for (var type : file.getMessageTypes()) {
                registerAdmitted(type, byTypeUrl);
            }
        }
        this.admittedSchemas = java.util.Map.copyOf(byTypeUrl);
        return this;
    }

    /**
     * Binds scoped callers to the accounts and ACL identities {@code resolver} names. The
     * document, drive and archive services all read the same binding. Until a composition
     * root calls this, a scoped caller is bound to no account and every account-addressed
     * call refuses it; process authority is unaffected. Composition roots call this after
     * {@code build} and before any transport starts serving.
     *
     * @param resolver the account-binding source
     * @return this instance, for chaining
     */
    public RepoServices withAccountBindings(AccountBindingResolver resolver) {
        callerBindings.use(resolver);
        return this;
    }

    private static void registerAdmitted(com.google.protobuf.Descriptors.Descriptor type,
            java.util.Map<String, ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition> out) {
        var closure = ai.protomolt.proto.descriptors.DescriptorFingerprints.closure(type);
        var bytes = closure.toByteString();
        String artifactSha;
        try {
            artifactSha = java.util.HexFormat.of().formatHex(
                    java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
        var metadata = ai.protomolt.proto.repo.v1.RepositorySchemaAsset.newBuilder()
                .setTypeUrl("type.googleapis.com/" + type.getFullName())
                .setArtifactSha256(artifactSha)
                .setSchema(ai.protomolt.proto.repo.v1.PublicationSchemaCondition.newBuilder()
                        .setTypeName(type.getFullName())
                        .setDescriptorFingerprint(ai.protomolt.proto.descriptors.DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(ai.protomolt.proto.repo.v1.SchemaCompilationProvenance.newBuilder()
                        .setOrigin(ai.protomolt.proto.repo.v1.SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(ai.protomolt.proto.repo.v1.SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("Descriptor contributed by a co-mounted module")
                        .setAdmissionRuntime(ai.protomolt.proto.repo.admission.BuiltinDocumentSchema.admissionRuntime()))
                .build();
        out.put(metadata.getTypeUrl(),
                new ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition(metadata, bytes, java.util.Optional.empty()));
        for (var nested : type.getNestedTypes()) {
            registerAdmitted(nested, out);
        }
    }

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
        this(config, bridges, providers, null, null);
    }

    /** Internal local qualification path; transport allocation is not yet bounded. */
    RepoServices(RepoServiceConfig config, BridgeEngine bridges,
            ai.protomolt.proto.repo.blob.spi.BlobStores providers, BoundedArchiveProfile bounded) {
        this(config, bridges, providers, java.util.Objects.requireNonNull(bounded), null);
    }

    RepoServices(RepoServiceConfig config, BridgeEngine bridges,
            ai.protomolt.proto.repo.blob.spi.BlobStores providers, BoundedArchiveProfile bounded,
            ReaderHostOptions readerHost) {
        if (RepoServiceConfig.BLOB_STORE_REPO.equals(config.blobStore())
                || RepoServiceConfig.BLOB_STORE_REPO_INPROCESS.equals(config.blobStore()))
            throw new IllegalArgumentException("Remote repository blob storage is retired: " + config.blobStore());
        this.readerHost = readerHost;
        if (readerHost != null && !config.managedStorage().retentionQualified())
            throw new IllegalArgumentException("Host-bound readers require qualified managed storage");
        Tx registeredHostTx = null;
        this.readBudget = new ReadByteBudget(config.readBudgetBytes());
        this.archiveAdmission = bounded == null ? null : bounded.openAdmission(config);
        this.archiveReadAdmission = bounded == null ? null : new ai.protomolt.proto.repo.engine.ArchiveGetAdmission(
                bounded.readLimits(), bounded.budget(), bounded.maxActive());
        this.archiveIngress = bounded == null ? null : new UnaryRequestAdmission(bounded.budget(),
                bounded.limits().maxRequestBytes(), bounded.maxActive(), BoundedArchiveMethods.UNARY,
                BoundedArchiveMethods.readResponseLimits(bounded.readLimits().maxResponseBytes()));
        ManagedArchiveServices startingArchive = null;
        try {
            this.config = config;
            if (bounded == null && config.managedStorage().retentionQualified()
                    && (!config.lifecycleEnabled() || !("s3".equals(config.blobStore()) || "s3-redis-cache".equals(config.blobStore()))))
                throw new IllegalArgumentException("Managed storage requires an S3 backing store and enabled lifecycle recovery");
            this.database = owned.add(new LedgerDatabase(config.ledger()));
            this.tx = new Tx(database.entityManagerFactory());
            if (readerHost != null) {
                ai.protomolt.proto.repo.container.ledger.ReaderHostExecutions.register(tx,
                        readerHost.execution(), readerHost.hostIdentity(), readerHost.bootIdentity());
                // Only an acknowledged fresh registration belongs to this constructor.
                // A duplicate or uncertain reply must not fence another live execution.
                registeredHostTx = tx;
            }
            String selectedDriveProvider = RepoServiceConfig.BLOB_STORE_S3_REDIS_CACHE.equals(config.blobStore())
                    ? "s3" : config.blobStore();
            var driveGate = new SelectedDriveBackend(config);
            this.driveLedger = new DriveLedger(tx, driveGate);
            var storage = RepoStorageWiring.wire(config, providers, bounded != null, owned, tx, readerHost);
            this.blobStore = storage.blobStore();
            this.managedArchive = startingArchive = storage.managedArchive();
            this.driveProvisioner = new DriveProvisioner(driveLedger,
                    storage.namespaces(),
                    config.defaultBucketBase(), config.s3Region(), selectedDriveProvider, driveGate);
            this.archiveOperations = new ArchiveOperations(
                    storage.archiveLedger(), driveLedger, blobStore, bridges,
                    managedArchive == null ? null : managedArchive.reader,
                    managedArchive == null ? null : managedArchive.writer, archiveAdmission, archiveReadAdmission);
            this.driveOperations = new ai.protomolt.proto.repo.engine.DriveOperations(driveLedger, driveProvisioner);
            RepoV2Wiring.Wired v2 = bounded == null
                    ? RepoV2Wiring.wire(config, owned, driveLedger, blobStore, selectedDriveProvider,
                            V2_CATALOG_SCHEMA, () -> admittedSchemas, stepSave::bindCaller, readBudget)
                    : null;
            this.catalog = v2 == null ? null : v2.catalog();
            this.v2Uploads = v2 == null ? null : v2.uploads();
            this.v2Drives = v2 == null ? null : v2.drives();
            this.v2Resolver = v2 == null ? null : v2.resolver();
            this.v2Limits = v2 == null ? null : v2.limits();
            this.v2OutboxRelay = v2 == null ? null : v2.relay();
            var configuredServices = new java.util.ArrayList<BindableService>(List.of(
                    new ArchiveGrpcService(archiveOperations, callerBindings, readBudget),
                    new DriveGrpcService(driveOperations, callerBindings)));
            if (v2 != null) {
                var accessionImpl = v2.accession();
                var stepSaveInterceptor = stepSave.interceptor();
                configuredServices.add(new BindableService() {
                    @Override public io.grpc.ServerServiceDefinition bindService() {
                        return io.grpc.ServerInterceptors.intercept(accessionImpl, stepSaveInterceptor);
                    }
                });
            }
            if (managedArchive != null) configuredServices.add(new ArchiveMutationGrpcService(managedArchive.mutations));
            this.services = List.copyOf(configuredServices);
        } catch (RuntimeException | Error failure) {
            if (archiveIngress != null) archiveIngress.close();
            if (archiveAdmission != null) archiveAdmission.close();
            if (archiveReadAdmission != null) archiveReadAdmission.close();
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
            if (registeredHostTx != null) {
                try { fenceReaderHost(registeredHostTx, java.time.Duration.ofSeconds(5)); }
                catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
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

    /** Bounded archive profile with the same supervisor identity rules as a hosted build. */
    public static RepoServices buildBoundedArchiveHosted(RepoServiceConfig config, BoundedArchiveOptions options,
            ReaderHostOptions host) {
        return new RepoServices(java.util.Objects.requireNonNull(config), BridgeEngine.standard(),
                ai.protomolt.proto.repo.blob.spi.BlobStores.discover(),
                java.util.Objects.requireNonNull(options).profile(), java.util.Objects.requireNonNull(host));
    }

    /**
     * Builds managed archives with explicit bounded Redis storage and shared input
     * admission. Requires retention-qualified Redis, zero TTL, a finite object cap
     * within 9 MiB and lifecycle recovery. Redis persistence and eviction settings
     * remain the operator's responsibility. This profile supports local archive
     * operations and {@link #startBoundedArchiveNetty}; document operations, bridge
     * generation, streaming uploads and the general transport methods are unavailable.
     */
    public static RepoServices buildBoundedArchive(RepoServiceConfig config, BoundedArchiveOptions options) {
        return new RepoServices(java.util.Objects.requireNonNull(config), BridgeEngine.standard(),
                ai.protomolt.proto.repo.blob.spi.BlobStores.discover(),
                java.util.Objects.requireNonNull(options).profile());
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

    /**
     * The parts-based v2 store as the saves run over it: the catalog, the
     * per-account drive resolution, and the schema admission the host applies.
     */
    public record V2Composition(ai.protomolt.proto.repo.catalog.PostgresCatalog catalog,
            ai.protomolt.proto.repo.accession.DocumentSaves.Drives drives,
            ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Resolver resolver,
            ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Limits limits,
            ai.protomolt.proto.repo.admission.DocumentSchemas schemas) {
    }

    /**
     * The v2 composition for a host that writes through its own
     * {@link ai.protomolt.proto.repo.accession.DocumentSaves} over this store,
     * such as the dev corpus seeder, which commits under a stepped clock. The
     * saves it builds see the same catalog, drives and admission as this host's
     * transports; nothing is copied.
     *
     * @return the catalog, drives and admission of this composition
     */
    public V2Composition v2Composition() {
        requireOpen();
        requireFullProfile();
        return new V2Composition(catalog, v2Drives, v2Resolver, v2Limits,
                ai.protomolt.proto.repo.admission.DocumentSchemas.compiled());
    }

    /** Drive operations sharing this composition's storage lifetime. */
    public ai.protomolt.proto.repo.spi.DriveRepository driveRepository() {
        requireOpen();
        return driveOperations;
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
        requireFullProfile();
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
        return startInProcess(name, null, null);
    }

    /** Same credential boundary as Netty, including scoped credential resolution. */
    public synchronized Server startInProcess(String name, String apiToken, CallerResolver resolver) {
        requireOpen();
        requireTransportAuthentication(apiToken, resolver);
        try {
            var builder = InProcessServerBuilder.forName(name).maxInboundMessageSize(10 * 1024 * 1024);
            if (apiToken != null) builder.intercept(new ApiTokenServerInterceptor(apiToken, resolver));
            return registerAndStart(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Starts all services on a Netty TCP server with a required operator credential.
     *
     * <p>The repository holds every account's documents and every claim-check blob, so an
     * unauthenticated listener is a read and write path to all of them. With a token, every
     * call must present it in {@code api_token} metadata (or {@code authorization: Bearer});
     * reflection and health are covered too, because reflection enumerates the very RPCs
     * being guarded. With a {@link CallerResolver}, a credential the mounted access policy
     * names runs as its principal instead of with process authority.
     *
     * @param port the listen port (0 = ephemeral)
     * @param apiToken nonblank operator credential; policy-resolved credentials may also authenticate
     * @param resolver resolves a policy-named credential to its principal; requires a token
     * @return the started server (also closed by {@link #close()})
     */
    public synchronized Server startNetty(int port, String apiToken, CallerResolver resolver) {
        requireOpen();
        RepositoryNetworkAuthentication.requireOperatorToken(apiToken);
        requireTransportAuthentication(apiToken, resolver);
        try {
            HealthStatusManager health = new HealthStatusManager();
            var builder = NettyServerBuilder.forPort(port)
                    .maxInboundMessageSize(10 * 1024 * 1024)
                    .addService(health.getHealthService())
                    .addService(ProtoReflectionServiceV1.newInstance());
            builder.intercept(new ApiTokenServerInterceptor(apiToken, resolver));
            Server server = registerAndStart(builder);
            health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
            LOG.info("repo-service listening on port {}", server.getPort());
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Starts the bounded archive-only Netty listener. Requires a nonblank operator
     * token granting process-level access across accounts. The listener is plaintext;
     * use a trusted network or terminate TLS before remote access. Every listener
     * shares this host's input and GetEntry response budget. Decoded heap and network
     * buffers are separate. No document, drive, bridge or streaming RPC is exposed.
     */
    public synchronized Server startBoundedArchiveNetty(int port, String apiToken) {
        requireOpen();
        if (archiveIngress == null) throw new IllegalStateException("Bounded archive profile is not configured");
        if (apiToken == null || apiToken.isBlank()) throw new IllegalArgumentException("Bounded archive transport requires an API token");
        try {
            startLifecycle();
            var builder = NettyServerBuilder.forPort(port).addService(new ArchiveGrpcService(archiveOperations, readBudget));
            archiveIngress.configure(builder);
            // Last registered runs first: authentication precedes request admission.
            builder.intercept(new ApiTokenServerInterceptor(apiToken, null));
            return GrpcServerLifetime.start(builder, servers::add).server();
        } catch (IOException failure) {
            try { close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw new UncheckedIOException(failure);
        } catch (RuntimeException | Error failure) {
            try { close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void requireTransportAuthentication(String apiToken, CallerResolver resolver) {
        if (apiToken == null && resolver != null)
            throw new IllegalArgumentException("an access-policy resolver requires the operator api token");
    }

    private Server registerAndStart(io.grpc.ServerBuilder<?> builder) throws IOException {
        GrpcServerLifetime transport = null;
        try {
            services().forEach(builder::addService);
            transport = GrpcServerLifetime.start(builder, servers::add);
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
     * Starts the streaming HTTP upload server with a required operator credential.
     * This administrative route writes into any account's drive. Scoped upload
     * authorization is a separate capability. The parts-based upload handler owns
     * the document route; the archive rendition route stays mounted alongside it.
     * The same port serves {@code console} at {@link RepositoryConsole#PATH}.
     *
     * @param port the listen port (0 = ephemeral; read the bound port back
     *        from the returned server)
     * @param apiToken nonblank operator credential every upload request must present
     * @param console the console bundle served at {@link RepositoryConsole#PATH}
     * @return the started HTTP server (also closed by {@link #close()})
     */
    public synchronized UploadHttpServer startHttp(int port, String apiToken,
            RepositoryConsole console) {
        return startHttp(port, apiToken, console, Map.of());
    }

    /**
     * {@link #startHttp(int, String, RepositoryConsole)} plus {@code routes}, extra context
     * path to handler mounted on the same port before it starts ({@link UploadHttpServer}
     * names the paths it refuses).
     *
     * @param port the listen port (0 = ephemeral)
     * @param apiToken nonblank operator credential every upload request must present
     * @param console the console bundle served at {@link RepositoryConsole#PATH}
     * @param routes extra routes; each handler owns its credential check
     * @return the started HTTP server (also closed by {@link #close()})
     */
    public synchronized UploadHttpServer startHttp(int port, String apiToken,
            RepositoryConsole console, Map<String, HttpHandler> routes) {
        requireOpen();
        requireFullProfile();
        RepositoryNetworkAuthentication.requireOperatorToken(apiToken);
        if (managedArchive != null) startLifecycle();
        return startHttp(port, new UploadHttpServer(v2Uploads,
                () -> new RepositoryCaller("http-upload", true), apiToken, archiveOperations,
                console, routes));
    }

    /**
     * {@link #startHttp(int, String, RepositoryConsole, Map)} plus the Connect gateway at
     * {@link UploadHttpServer#CONNECT_PATH}: the {@link ConsoleProcedures} of this host's
     * services, each request's caller resolved by {@code requestCallers} and run under the
     * same caller context the gRPC transport sets. The started server exposes the same
     * resolver through {@link UploadHttpServer#callers()} for other routes on the port.
     *
     * @param port the listen port (0 = ephemeral)
     * @param apiToken nonblank operator credential every upload request must present
     * @param console the console bundle served at {@link RepositoryConsole#PATH}
     * @param routes extra routes; each handler owns its credential check
     * @param requestCallers resolves the caller of every Connect request; hosts compose
     *        {@code RequestCallerResolver.firstOf(List.of(bearer(apiToken, policy), sessions))}
     * @return the started HTTP server (also closed by {@link #close()})
     */
    public synchronized UploadHttpServer startHttp(int port, String apiToken,
            RepositoryConsole console, Map<String, HttpHandler> routes,
            RequestCallerResolver requestCallers) {
        requireOpen();
        requireFullProfile();
        RepositoryNetworkAuthentication.requireOperatorToken(apiToken);
        Objects.requireNonNull(requestCallers, "requestCallers");
        if (managedArchive != null) startLifecycle();
        List<ServerServiceDefinition> definitions = services.stream()
                .map(BindableService::bindService)
                .toList();
        ConnectGateway gateway = new ConnectGateway(definitions, ConsoleProcedures.ALL, requestCallers,
                new ConnectLimits(CONNECT_MESSAGE_BYTES, CONNECT_MESSAGE_BYTES));
        UploadHttpServer http;
        try {
            http = new UploadHttpServer(v2Uploads,
                    () -> new RepositoryCaller("http-upload", true), apiToken, archiveOperations,
                    console, routes, gateway, requestCallers, callerBindings::bind);
        } catch (RuntimeException | Error failure) {
            try { gateway.close(); }
            catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
        return startHttp(port, http);
    }

    /**
     * A host's request resolver for the HTTP port: the bearer credentials its gRPC listener
     * accepts (the operator token, plus the credential stores when the host has any), then
     * the console session when sign-in is configured. The bearer form is asked first: an
     * explicit token is a deliberate per-request credential and needs no session lookup.
     *
     * @param apiToken the operator credential
     * @param credentialStores resolves non-operator credentials, or null when the host
     *        authenticates the operator token only (as {@link #startNetty} takes it)
     * @param sessions the console session store, or empty when sign-in is not configured
     * @return the resolver
     */
    public static RequestCallerResolver requestCallers(String apiToken, CallerResolver credentialStores,
            Optional<OidcConsoleSessions> sessions) {
        Objects.requireNonNull(sessions, "sessions");
        RequestCallerResolver bearer = credentialStores == null
                ? RequestCallerResolver.bearer(apiToken)
                : RequestCallerResolver.bearer(apiToken, credentialStores);
        return sessions.<RequestCallerResolver>map(store -> RequestCallerResolver.firstOf(List.of(bearer, store)))
                .orElse(bearer);
    }

    private UploadHttpServer startHttp(int port, UploadHttpServer http) {
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
     * The console's sign-in routes over {@code sessions}, for {@link #startHttp(int, String,
     * RepositoryConsole, java.util.Map)}: the status answer names the accounts this host's
     * bindings map the session's principal to. A host without sign-in mounts the
     * not-configured handler, which answers 503 naming the variables.
     *
     * @param sessions the session store, or empty when sign-in is not configured
     * @return the route map, one entry at {@link ConsoleSignInHandler#PATH}
     */
    public java.util.Map<String, com.sun.net.httpserver.HttpHandler> consoleSignInRoutes(
            java.util.Optional<ai.protomolt.proto.authz.OidcConsoleSessions> sessions) {
        ConsoleSignInHandler handler = sessions
                .map(store -> ConsoleSignInHandler.over(store, callerBindings::boundAccountIds))
                .orElseGet(ConsoleSignInHandler::notConfigured);
        return java.util.Map.of(ConsoleSignInHandler.PATH, handler);
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
     * Starts the background lifecycle loops the wired profile needs: managed archive
     * mutation recovery and reconciliation, and the v2 outbox relay when Kafka
     * eventing is configured. Every loop catches and logs per iteration — one bad
     * record never kills a loop. No-op when
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
            if (v2OutboxRelay != null) {
                startLifecycleThread("repo-v2-outbox-relay", () -> {
                    try {
                        int relayed = v2OutboxRelay.relayOnce(RELAY_BATCH_SIZE);
                        // Idle backoff: work left → relay again immediately; empty → wait.
                        if (relayed == 0) {
                            sleep(config.purgeIntervalMs());
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            LOG.info("repo lifecycle loops started (purge interval {} ms, sweep interval {} ms)",
                    config.purgeIntervalMs(), config.sweepIntervalMs());
            lifecycleStarted = true;
        } catch (RuntimeException | Error failure) {
            // A partial worker set must not leave a usable upload composition.
            try { close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Outbox-relay drain batch size per loop iteration. */
    private static final int RELAY_BATCH_SIZE = 100;

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
        if (archiveIngress != null) archiveIngress.close();
        if (archiveAdmission != null) archiveAdmission.close();
        if (archiveReadAdmission != null) archiveReadAdmission.close();
        if (managedArchive != null) managedArchive.reader.close();
        LifecycleShutdown.stopBeforeRelease(lifecycleThreads, timeout, () -> releaseAfterWorkersStop(timeout));
    }

    private void releaseAfterWorkersStop(java.time.Duration timeout) {
        long started = System.nanoTime();
        // Bounded admission is already closed. Keep transport/executor alive for
        // accepted calls; transport shutdown can otherwise forcibly cancel them.
        // A timeout retains every resource so the owner can retry draining.
        if (archiveIngress != null) awaitArchiveIdle(timeout);
        var transports = new java.util.ArrayList<AutoCloseable>();
        transports.addAll(httpServers);
        transports.addAll(servers);
        ShutdownBarrier.releaseAfter(transports, () -> {
            awaitArchiveIdle(timeout);
            if (managedArchive != null) managedArchive.reader.attestLocalQuiescence();
            if (readerHost != null && !readerHostFenced) {
                fenceReaderHost(tx, timeout.minusNanos(System.nanoTime() - started));
                readerHostFenced = true;
            }
            lifecycleThreads.clear();
            httpServers.clear();
            servers.clear();
            owned.close();
        });
        LOG.info("repo-service stopped");
    }

    private void fenceReaderHost(Tx transaction, java.time.Duration remaining) {
        long milliseconds = Math.min(5000, remaining.toMillis());
        if (milliseconds < 1)
            throw new IllegalStateException("Reader host fence deadline expired; shared resources retained");
        var limit = java.time.Duration.ofMillis(milliseconds);
        var bounded = transaction.withTimeouts(new ai.protomolt.proto.repo.container.ledger.SqlTimeouts(limit, limit));
        ai.protomolt.proto.repo.container.ledger.ReaderHostExecutions.fence(bounded, readerHost.execution());
    }

    private void awaitArchiveIdle(java.time.Duration timeout) {
        String phase = "Archive RPC";
        try {
            if (archiveIngress != null && !archiveIngress.awaitIdle(timeout))
                throw new RepositoryDrainTimeoutException(RepositoryDrainTimeoutException.Phase.ARCHIVE_RPC,
                        "Archive RPCs still active; shared resources retained");
            phase = "Archive put";
            if (archiveAdmission != null && !archiveAdmission.awaitIdle(timeout))
                throw new RepositoryDrainTimeoutException(RepositoryDrainTimeoutException.Phase.ARCHIVE_PUT,
                        "Archive puts still active; shared resources retained");
            phase = "Archive reader";
            if (archiveReadAdmission != null && !archiveReadAdmission.awaitIdle(timeout))
                throw new RepositoryDrainTimeoutException(RepositoryDrainTimeoutException.Phase.ARCHIVE_READ,
                        "Archive response construction still active; shared resources retained");
            if (managedArchive != null && !managedArchive.reader.awaitIdle(timeout))
                throw new RepositoryDrainTimeoutException(RepositoryDrainTimeoutException.Phase.ARCHIVE_READ,
                        "Managed archive reads still active; shared resources retained");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(phase + " drain interrupted; shared resources retained", interrupted);
        }
    }

    private void requireFullProfile() {
        if (archiveAdmission != null) throw new UnsupportedOperationException(
                "General transport admission and document operations are unavailable in the bounded archive profile");
    }

    private void requireOpen() {
        if (lifecycleClosed) throw new IllegalStateException("repository services are closed");
    }

    // Package-private accessors: the IT asserts on ledger state through the
    // same wired components (no mocks).

    DriveLedger driveLedger() {
        return driveLedger;
    }

    BlobStore blobStore() {
        return blobStore;
    }

    javax.sql.DataSource ledgerDataSource() {
        return database.dataSource();
    }
}
