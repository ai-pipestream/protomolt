package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.CredentialBinding;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaArtifactCache;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.PurgeAuthorization;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.repo.v2.StepWriterScope;
import ai.protomolt.proto.repo.v2.WriterCredential;
import ai.protomolt.proto.repo.v2.WriterGrant;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessServerBuilder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

/**
 * Shared v2 catalog, admission, S3 stack and in-process gRPC {@link AccessionGrpcService}
 * fixture backed by Testcontainers PostgreSQL and LocalStack S3.
 *
 * <p>Provides a baseline fixture for intake and pull-connector integration tests, a
 * parse-coordinator configuration with step and seed writer grants and built-in
 * {@link SearchMetadata} schema resolution, and a {@link RegistrySchemaResolver}
 * option for registry-backed schema admission integration tests.
 */
public final class V2RepositoryFixture implements AutoCloseable {

    /** Default drive name registered on the fixture. */
    public static final String DEFAULT_DRIVE = "intake";

    /** Default S3 object key prefix within the fixture bucket. */
    public static final String DEFAULT_OBJECT_PREFIX = "objects";

    /** Default Hikari pool size for the fixture catalog. */
    public static final int DEFAULT_CATALOG_POOL_SIZE = 4;

    /** Default credential issuer for parse-coordinator fixture setup. */
    public static final String PARSE_ISSUER = "d8-test";

    /** Default step module and service principal for parse-coordinator fixture setup. */
    public static final String PARSE_PRINCIPAL = "parse-coordinator";

    /** Default intake datasource ID for parse-coordinator seed grants. */
    public static final String PARSE_DATASOURCE = "ds-parse";

    /** Canonical type URL for {@link SearchMetadata}. */
    public static final String SEARCH_METADATA_TYPE_URL =
            "type.googleapis.com/" + SearchMetadata.getDescriptor().getFullName();

    /** Explicit fixture resource budget for candidate and schema admission. */
    public static final DocumentSchemaAdmission.Limits DEFAULT_LIMITS =
            new DocumentSchemaAdmission.Limits(
                    16,
                    64L * 1024 * 1024,
                    64,
                    4L * 1024 * 1024,
                    32,
                    32L * 1024 * 1024,
                    64 * 1024 * 1024);

    /** Default descriptor artifact cache limits when constructing an owned {@link RegistrySchemaResolver}. */
    public static final DocumentSchemaArtifactCache.Limits DEFAULT_CACHE_LIMITS =
            new DocumentSchemaArtifactCache.Limits(32_000_000L, 64, 16_000_000);

    private static final int DEFAULT_MAX_CONCURRENT_ATTEMPTS = 4;
    private static final int DEFAULT_MAX_ARTIFACTS_PER_ATTEMPT = 64;
    private static final Duration RESOLVER_DRAIN_TIMEOUT = Duration.ofSeconds(5);

    /** The PostgreSQL catalog backing the fixture. */
    public final PostgresCatalog catalog;

    /** The S3-backed stack bound to the default drive. */
    public final BlobStoreStack stack;

    /** The S3 client connected to the LocalStack endpoint. */
    public final S3Client client;

    /** The document saves coordinator. */
    public final DocumentSaves saves;

    /** The document reads coordinator. */
    public final DocumentReads reads;

    /** The started in-process gRPC server hosting {@link AccessionGrpcService}. */
    public final Server server;

    private final RegistrySchemaResolver ownedRegistryResolver;

    /**
     * Creates and starts a baseline v2 repository fixture with an empty schema resolver
     * and unrestricted caller delegation.
     *
     * @param postgres the running PostgreSQL container
     * @param localstack the running LocalStack S3 container
     * @param name the bucket, stack ID, and in-process gRPC server name
     * @throws Exception if catalog initialization, bucket creation, or server startup fails
     */
    public V2RepositoryFixture(
            PostgreSQLContainer postgres, LocalStackContainer localstack, String name)
            throws Exception {
        this(builder(postgres, localstack, name));
    }

    /**
     * Creates and starts a v2 repository fixture configured for {@code parse-coordinator}
     * integration tests in the given account.
     *
     * @param postgres the running PostgreSQL container
     * @param localstack the running LocalStack S3 container
     * @param name the bucket, stack ID, and in-process gRPC server name
     * @param account the account ID authorized for the seed intake and parse step grants
     * @throws Exception if catalog initialization, bucket creation, or server startup fails
     */
    public V2RepositoryFixture(
            PostgreSQLContainer postgres,
            LocalStackContainer localstack,
            String name,
            String account)
            throws Exception {
        this(builder(postgres, localstack, name).withParseCoordinator(account));
    }

    private V2RepositoryFixture(Builder builder) throws Exception {
        String jdbcUrl =
                builder.isolatedDatabase
                        ? createDatabase(builder.postgres, "v2_fixture_" + UUID.randomUUID().toString().replace('-', '_'))
                        : builder.postgres.getJdbcUrl();
        this.catalog =
                new PostgresCatalog(
                        new CatalogConfig(
                                jdbcUrl,
                                builder.postgres.getUsername(),
                                builder.postgres.getPassword(),
                                DEFAULT_CATALOG_POOL_SIZE));
        this.client =
                S3Client.builder()
                        .endpointOverride(builder.localstack.getEndpoint())
                        .region(Region.of(builder.localstack.getRegion()))
                        .forcePathStyle(true)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(
                                                builder.localstack.getAccessKey(),
                                                builder.localstack.getSecretKey())))
                        .httpClient(UrlConnectionHttpClient.create())
                        .build();
        ensureBucket(this.client, builder.name);
        String id =
                this.catalog.registerStack(
                        StackRecord.newBuilder()
                                .setStackId(builder.name)
                                .setProvider("s3")
                                .setEndpoint(builder.localstack.getEndpoint().toString())
                                .setNamespace(builder.name)
                                .build());
        this.stack =
                new BlobStoreStack(
                        id, new S3BlobStore(this.client), builder.name, DEFAULT_OBJECT_PREFIX);
        var drives = new DocumentSaves.Drives(builder.driveName, Map.of(builder.driveName, this.stack));
        this.ownedRegistryResolver = builder.ownedRegistryResolver;
        this.saves =
                new DocumentSaves(
                        this.catalog,
                        drives,
                        builder.resolver,
                        builder.limits,
                        bytes -> () -> {},
                        Clock.systemUTC(),
                        builder.schemas);
        this.reads =
                new DocumentReads(
                        this.catalog,
                        drives,
                        builder.limits,
                        bytes -> () -> {},
                        Clock.systemUTC());
        if (builder.catalogInitializer != null) {
            builder.catalogInitializer.accept(this.catalog);
        }
        var service =
                new AccessionGrpcService(
                        this.saves,
                        new DocumentUploads(this.saves),
                        this.reads,
                        new DocumentDeletes(this.catalog, Clock.systemUTC()),
                        new DocumentLists(this.catalog),
                        new DocumentPurges(
                                this.catalog,
                                drives,
                                (identity, tenant) -> new PurgeAuthorization.Decision(false, false),
                                Clock.systemUTC()),
                        builder.callerBindings);
        var serverBuilder = InProcessServerBuilder.forName(builder.name);
        if (builder.interceptor != null) {
            serverBuilder.addService(ServerInterceptors.intercept(service, builder.interceptor));
        } else {
            serverBuilder.addService(service);
        }
        this.server = serverBuilder.build().start();
    }

    /**
     * Creates a new fluent {@link Builder} for configuring a {@link V2RepositoryFixture}.
     *
     * @param postgres the running PostgreSQL container
     * @param localstack the running LocalStack S3 container
     * @param name the bucket, stack ID, and in-process gRPC server name
     * @return a new builder
     */
    public static Builder builder(
            PostgreSQLContainer postgres, LocalStackContainer localstack, String name) {
        return new Builder(postgres, localstack, name);
    }

    /**
     * Adapts a {@link RegistrySchemaResolver} and per-occurrence {@link RegistrySchemaResolver.Selector}
     * into a {@link DocumentSchemaAdmission.Resolver} suitable for {@link DocumentSaves}.
     *
     * <p>Each occurrence opens a thread-confined {@link RegistrySchemaResolver.Attempt}, authorizes and
     * selects metadata through {@code selector}, and loads or reuses the pinned descriptor set bytes from
     * the resolver's cache and backing {@link SchemaRegistryStore}. Unregistered types (where {@code selector}
     * returns {@code null} or the descriptor artifact is absent from the store) throw
     * {@link DocumentCandidateAdmission.SchemaMissing}, which {@link DocumentSaves} records as
     * {@code REJECTION_REASON_SCHEMA_MISSING}. Store outages propagate as
     * {@link DocumentCandidateAdmission.SchemaUnavailable}.
     *
     * @param registryResolver the host-scoped registry schema resolver
     * @param selector the occurrence selector supplying schema asset metadata and optional source bytes
     * @return the admission resolver backed by {@code registryResolver}
     */
    public static DocumentSchemaAdmission.Resolver registryResolver(
            RegistrySchemaResolver registryResolver, RegistrySchemaResolver.Selector selector) {
        Objects.requireNonNull(registryResolver, "registryResolver");
        Objects.requireNonNull(selector, "selector");
        return occurrence -> {
            try (var attempt =
                    registryResolver.open(
                            occ -> {
                                var selected = selector.select(occ);
                                if (selected == null) {
                                    throw new DocumentCandidateAdmission.SchemaMissing(occ.typeUrl());
                                }
                                return selected;
                            },
                            () -> {})) {
                return attempt.select(occurrence);
            } catch (RegistrySchemaResolver.MissingDescriptor missing) {
                throw new DocumentCandidateAdmission.SchemaMissing(occurrence.typeUrl());
            } catch (RegistryStoreException unavailable) {
                throw new DocumentCandidateAdmission.SchemaUnavailable(
                        "Schema registry unavailable", unavailable);
            }
        };
    }

    /**
     * Returns the built-in {@link DocumentSchemaAdmission.Resolver} that resolves only
     * {@link SearchMetadata} and returns {@code null} for all other type URLs.
     *
     * @return a resolver for {@link SearchMetadata}
     */
    public static DocumentSchemaAdmission.Resolver searchMetadataResolver() {
        var definition = BuiltinDocumentSchema.definition(SearchMetadata.getDescriptor());
        return selection ->
                SEARCH_METADATA_TYPE_URL.equals(selection.typeUrl()) ? definition : null;
    }

    /**
     * Returns the PostgreSQL catalog.
     *
     * @return the catalog
     */
    public PostgresCatalog catalog() {
        return catalog;
    }

    /**
     * Returns the S3-backed blob store stack.
     *
     * @return the stack
     */
    public BlobStoreStack stack() {
        return stack;
    }

    /**
     * Returns the S3 client connected to LocalStack.
     *
     * @return the S3 client
     */
    public S3Client client() {
        return client;
    }

    /**
     * Returns the document saves coordinator.
     *
     * @return the document saves coordinator
     */
    public DocumentSaves saves() {
        return saves;
    }

    /**
     * Returns the document reads coordinator.
     *
     * @return the document reads coordinator
     */
    public DocumentReads reads() {
        return reads;
    }

    /**
     * Returns the in-process gRPC server.
     *
     * @return the gRPC server
     */
    public Server server() {
        return server;
    }

    /**
     * Reads the retained raw blob bytes for the first blob reference in the given
     * document revision's manifest directly from the backing {@link BlobStoreStack}.
     *
     * @param address the document node address
     * @param revision the revision number
     * @return the raw blob bytes stored in the stack
     * @throws Exception if the manifest or blob reference is missing or cannot be read
     */
    public byte[] retainedBlob(NodeAddress address, long revision) throws Exception {
        var ref =
                catalog.manifest(address, revision).orElseThrow().getPartsList().stream()
                        .filter(p -> p.getBlobRefsCount() > 0)
                        .findFirst()
                        .orElseThrow()
                        .getBlobRefs(0)
                        .getRef();
        try (var input = stack.open(ref)) {
            return input.readAllBytes();
        }
    }

    @Override
    public void close() {
        server.shutdownNow();
        if (ownedRegistryResolver != null) {
            ownedRegistryResolver.close();
            try {
                ownedRegistryResolver.awaitLoads(RESOLVER_DRAIN_TIMEOUT);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        catalog.close();
        client.close();
    }

    private static void ensureBucket(S3Client client, String bucket) {
        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (NoSuchBucketException missing) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    private static String createDatabase(PostgreSQLContainer postgres, String dbName)
            throws Exception {
        try (Connection connection =
                DriverManager.getConnection(
                        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            connection.createStatement().execute("CREATE DATABASE " + dbName);
        }
        return postgres.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + dbName + "$1");
    }

    /**
     * Fluent builder for {@link V2RepositoryFixture}.
     */
    public static final class Builder {
        private final PostgreSQLContainer postgres;
        private final LocalStackContainer localstack;
        private final String name;
        private String driveName = DEFAULT_DRIVE;
        private boolean isolatedDatabase;
        private DocumentSchemaAdmission.Limits limits = DEFAULT_LIMITS;
        private DocumentSchemaAdmission.Resolver resolver = selection -> null;
        private DocumentSchemas schemas = DocumentSchemas.compiled();
        private RegistrySchemaResolver ownedRegistryResolver;
        private Consumer<PostgresCatalog> catalogInitializer;
        private Function<AuthenticatedCaller, RepositoryCaller> callerBindings =
                authentication ->
                        new RepositoryCaller(
                                authentication.caller().name(),
                                authentication.caller().unrestricted());
        private ServerInterceptor interceptor;

        private Builder(
                PostgreSQLContainer postgres, LocalStackContainer localstack, String name) {
            this.postgres = Objects.requireNonNull(postgres, "postgres");
            this.localstack = Objects.requireNonNull(localstack, "localstack");
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Fixture name must not be blank");
            }
            this.name = name;
        }

        /**
         * Configures the default drive name mapped to the fixture's S3 stack.
         *
         * @param driveName the drive name (defaults to {@link #DEFAULT_DRIVE})
         * @return this builder
         */
        public Builder driveName(String driveName) {
            if (driveName == null || driveName.isBlank()) {
                throw new IllegalArgumentException("Drive name must not be blank");
            }
            this.driveName = driveName;
            return this;
        }

        /**
         * Creates a fresh PostgreSQL database inside the container for this fixture instance
         * so multiple fixtures sharing a single container have isolated catalog tables.
         *
         * @return this builder
         */
        public Builder isolatedDatabase() {
            this.isolatedDatabase = true;
            return this;
        }

        /**
         * Sets custom admission resource limits.
         *
         * @param limits the admission limits
         * @return this builder
         */
        public Builder limits(DocumentSchemaAdmission.Limits limits) {
            this.limits = Objects.requireNonNull(limits, "limits");
            return this;
        }

        /**
         * Sets the {@link DocumentSchemaAdmission.Resolver} used by {@link DocumentSaves}.
         *
         * @param resolver the schema resolver
         * @return this builder
         */
        public Builder resolver(DocumentSchemaAdmission.Resolver resolver) {
            this.resolver = Objects.requireNonNull(resolver, "resolver");
            return this;
        }

        /**
         * Configures {@link DocumentSaves} to resolve {@code Any} occurrences through a
         * caller-managed {@link RegistrySchemaResolver}.
         *
         * @param registryResolver the registry schema resolver
         * @param selector the occurrence selector
         * @return this builder
         */
        public Builder registryResolver(
                RegistrySchemaResolver registryResolver,
                RegistrySchemaResolver.Selector selector) {
            this.resolver = V2RepositoryFixture.registryResolver(registryResolver, selector);
            return this;
        }

        /**
         * Constructs an owned {@link RegistrySchemaResolver} over {@code store} with
         * {@link #DEFAULT_CACHE_LIMITS} and wires it into {@link DocumentSaves}.
         * The owned resolver is closed and drained when {@link V2RepositoryFixture#close()} runs.
         *
         * @param store the schema registry store supplying descriptor set artifacts
         * @param selector the occurrence selector
         * @return this builder
         */
        public Builder registryResolver(
                SchemaRegistryStore store, RegistrySchemaResolver.Selector selector) {
            Objects.requireNonNull(store, "store");
            Objects.requireNonNull(selector, "selector");
            var owned =
                    new RegistrySchemaResolver(
                            store,
                            DEFAULT_CACHE_LIMITS,
                            DEFAULT_MAX_CONCURRENT_ATTEMPTS,
                            DEFAULT_MAX_ARTIFACTS_PER_ATTEMPT);
            this.ownedRegistryResolver = owned;
            this.resolver = V2RepositoryFixture.registryResolver(owned, selector);
            return this;
        }

        /**
         * Sets the {@link DocumentSchemas} supplier for the container {@code Document} schema.
         *
         * @param schemas the container document schemas supplier
         * @return this builder
         */
        public Builder schemas(DocumentSchemas schemas) {
            this.schemas = Objects.requireNonNull(schemas, "schemas");
            return this;
        }

        /**
         * Configures the built-in {@link SearchMetadata} schema resolver.
         *
         * @return this builder
         */
        public Builder withSearchMetadataSchema() {
            this.resolver = searchMetadataResolver();
            return this;
        }

        /**
         * Registers a callback invoked after the {@link PostgresCatalog} is created to seed
         * credentials or writer grants.
         *
         * @param catalogInitializer the catalog initializer
         * @return this builder
         */
        public Builder afterCatalogInit(Consumer<PostgresCatalog> catalogInitializer) {
            this.catalogInitializer = Objects.requireNonNull(catalogInitializer, "catalogInitializer");
            return this;
        }

        /**
         * Sets the function mapping an {@link AuthenticatedCaller} to a {@link RepositoryCaller}.
         *
         * @param callerBindings the caller binding function
         * @return this builder
         */
        public Builder callerBindings(
                Function<AuthenticatedCaller, RepositoryCaller> callerBindings) {
            this.callerBindings = Objects.requireNonNull(callerBindings, "callerBindings");
            return this;
        }

        /**
         * Attaches a {@link ServerInterceptor} to the in-process gRPC service.
         *
         * @param interceptor the server interceptor
         * @return this builder
         */
        public Builder interceptor(ServerInterceptor interceptor) {
            this.interceptor = Objects.requireNonNull(interceptor, "interceptor");
            return this;
        }

        /**
         * Applies the {@code parse-coordinator} integration test configuration for {@code account}:
         * resolves {@link SearchMetadata} as a built-in schema, registers a {@code d8-test}
         * writer credential with {@code parse} step and {@code seed} intake grants, and attaches
         * an in-process caller context interceptor for {@code parse-coordinator}.
         *
         * @param account the account ID to authorize
         * @return this builder
         */
        public Builder withParseCoordinator(String account) {
            if (account == null || account.isBlank()) {
                throw new IllegalArgumentException("Account must not be blank");
            }
            this.resolver = searchMetadataResolver();
            var credentialId = UUID.randomUUID();
            this.catalogInitializer =
                    catalog -> {
                        catalog.registerCredential(PARSE_ISSUER, credentialId, PARSE_PRINCIPAL);
                        var credential =
                                WriterCredential.newBuilder()
                                        .setIssuer(PARSE_ISSUER)
                                        .setCredentialId(credentialId.toString());
                        catalog.installGrant(
                                WriterGrant.newBuilder()
                                        .setGrantId("parse")
                                        .setAccountId(account)
                                        .setCredential(credential)
                                        .setStep(
                                                StepWriterScope.newBuilder()
                                                        .setModuleId(PARSE_PRINCIPAL))
                                        .build());
                        catalog.installGrant(
                                WriterGrant.newBuilder()
                                        .setGrantId("seed")
                                        .setAccountId(account)
                                        .setCredential(credential)
                                        .setAllowCreate(true)
                                        .setIntake(
                                                IntakeOrigin.newBuilder()
                                                        .setDatasourceId(PARSE_DATASOURCE))
                                        .build());
                    };
            var caller = Caller.scoped(PARSE_PRINCIPAL, Set.of());
            var authentication =
                    new AuthenticatedCaller(
                            caller,
                            Optional.of(
                                    new CredentialBinding(PARSE_ISSUER, credentialId, 1)));
            this.callerBindings =
                    auth ->
                            new RepositoryCaller(
                                    auth.caller().name(),
                                    false,
                                    Set.of(account),
                                    Set.of(
                                            Principal.newBuilder()
                                                    .setIdentityType("service")
                                                    .setIdentity(PARSE_PRINCIPAL)
                                                    .build()),
                                    Optional.of(
                                            new RepositoryCredentialBinding(
                                                    PARSE_ISSUER, credentialId, 1)));
            // Fixed, trusted in-process fixture authentication, not a client-controlled identity header.
            this.interceptor =
                    new ServerInterceptor() {
                        @Override
                        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                                ServerCall<ReqT, RespT> call,
                                Metadata headers,
                                ServerCallHandler<ReqT, RespT> next) {
                            return Contexts.interceptCall(
                                    Context.current()
                                            .withValue(CallerContexts.CALLER, caller)
                                            .withValue(
                                                    CallerContexts.AUTHENTICATED_CALLER,
                                                    authentication),
                                    call,
                                    headers,
                                    next);
                        }
                    };
            return this;
        }

        /**
         * Builds and starts the {@link V2RepositoryFixture}.
         *
         * @return the started fixture
         * @throws Exception if setup fails
         */
        public V2RepositoryFixture build() throws Exception {
            return new V2RepositoryFixture(this);
        }
    }
}
