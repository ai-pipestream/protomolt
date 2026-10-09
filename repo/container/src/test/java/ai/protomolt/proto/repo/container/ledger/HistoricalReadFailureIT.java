package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.engine.DocumentHistoricalOperations;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.service.DocumentHistoryGrpcService;
import ai.protomolt.proto.repo.service.DocumentHistoryMaterializationGrpcService;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.HistoricalMaterializationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentHistoryMaterializationServiceGrpc;
import ai.protomolt.proto.repo.v1.DocumentHistoryServiceGrpc;
import ai.protomolt.proto.repo.v1.HistoricalDocumentReadMode;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.DocumentPublicationPart;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import ai.protomolt.proto.repo.v1.DocumentPublicationRowKind;
import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import ai.protomolt.proto.repo.v1.DocumentRevisionCondition;
import ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence;
import ai.protomolt.proto.repo.v1.DocumentSchemaPolicy;
import ai.protomolt.proto.repo.v1.DocumentSchemaPolicyLimits;
import ai.protomolt.proto.repo.v1.DocumentSchemaPolicyMode;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.HistoricalMaterializationLimits;
import ai.protomolt.proto.repo.v1.HistoricalOccurrenceSelection;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.PublicationUpload;
import ai.protomolt.proto.repo.v1.ReadHistoricalOccurrenceRequest;
import ai.protomolt.proto.repo.v1.ReadHistoricalOccurrenceResponse;
import ai.protomolt.proto.repo.v1.ReadRevisionRequest;
import ai.protomolt.proto.repo.v1.ReadRevisionResponse;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import jakarta.persistence.LockModeType;
import java.io.InputStream;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Real PostgreSQL and a real S3 provider (LocalStack, then the pinned RustFS image) behind the
 * public repository path. Provider refusals must reach library and in-process gRPC callers as
 * the same repository-domain classification. The recording wrapper around the real adapter is
 * the only synthetic element; every case names whether its failure is real or injected.
 */
@Testcontainers
class HistoricalReadFailureIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final DocumentRevisionAssembly.Limits ASSEMBLY = new DocumentRevisionAssembly.Limits(1_000_000, 100, 100, 100, 100_000);
    private static final HistoricalMaterializationRepository.Limits MATERIALIZATION =
            new HistoricalMaterializationRepository.Limits(4_000_000, 4_000_000, 16_000_000, 64, 4_000_000, 64);
    private static final Metadata.Key<String> IDENTITY = Metadata.Key.of("test-authenticated-identity", Metadata.ASCII_STRING_MARSHALLER);
    private static LedgerDatabase database;
    private static Tx tx;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
    }
    @AfterAll static void close() throws Exception { if (database != null) database.close(); }

    /** Expected outward classification; {@code provider} is the adapter code expected on the cause chain, or null. */
    private record Expected(RepositoryException.Code code, String message, BlobStoreException.Code provider) {}

    private record Request(String bucket, String key, String versionId) {}

    /** Synthetic over real: records and optionally rewrites, intercepts, fails or alters bounded reads of the real adapter. */
    private static final class RecordingStore implements BlobStore {
        private final BlobStore delegate;
        private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
        private volatile UnaryOperator<Request> rewrite = request -> request;
        private volatile Consumer<Request> before = request -> {};
        private volatile Function<Request, RuntimeException> failure = request -> null;
        private volatile UnaryOperator<GetResult> after = result -> result;
        RecordingStore(BlobStore delegate) { this.delegate = delegate; }
        void reset() { rewrite = request -> request; before = request -> {}; failure = request -> null; after = result -> result; }
        List<Request> requests() { synchronized (requests) { return List.copyOf(requests); } }
        @Override public GetResult getBounded(String bucket, String key, String versionId, int maxBytes) {
            var request = rewrite.apply(new Request(bucket, key, versionId));
            requests.add(request);
            before.accept(request);
            var injected = failure.apply(request);
            if (injected != null) throw injected;
            return after.apply(delegate.getBounded(request.bucket(), request.key(), request.versionId(), maxBytes));
        }
        @Override public PutResult put(PutSpec spec, byte[] body) { return delegate.put(spec, body); }
        @Override public PutResult put(PutSpec spec, InputStream body, long length) { return delegate.put(spec, body, length); }
        @Override public GetResult get(String bucket, String key, String versionId) { return delegate.get(bucket, key, versionId); }
        @Override public GetResult getForUpdate(String bucket, String key) { return delegate.getForUpdate(bucket, key); }
        @Override public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
            return delegate.conditionalPut(spec, body, condition);
        }
        @Override public void copy(String sourceBucket, String sourceKey, String bucket, String key) { delegate.copy(sourceBucket, sourceKey, bucket, key); }
        @Override public boolean delete(String bucket, String key) { return delegate.delete(bucket, key); }
        @Override public BatchDeleteResult deleteAll(String bucket, List<String> keys) { return delegate.deleteAll(bucket, keys); }
        @Override public List<ListedObject> list(String bucket, String prefix) { return delegate.list(bucket, prefix); }
        @Override public void headBucket(String bucket) { delegate.headBucket(bucket); }
        @Override public void headObject(String bucket, String key) { delegate.headObject(bucket, key); }
    }

    /** Sequential in-process gRPC over the same engine; the identity header is test authentication only. */
    private static final class Transport implements AutoCloseable {
        final Server server;
        final ManagedChannel channel;
        final PayloadBudget responses = new PayloadBudget(32L * 1024 * 1024);
        final AtomicReference<CountDownLatch> terminal = new AtomicReference<>(new CountDownLatch(0));
        Transport(DocumentHistoricalOperations history, String account) throws Exception {
            Function<Caller, RepositoryCaller> bindings = authenticated -> switch (authenticated.name()) {
                case "member" -> new RepositoryCaller("member", false, Set.of(account), Set.of());
                case "foreign" -> new RepositoryCaller("foreign", false, Set.of("elsewhere-" + account), Set.of());
                default -> throw new AssertionError("Unexpected authenticated fixture identity");
            };
            String name = "historical-failures-" + UUID.randomUUID();
            ServerInterceptor auth = new ServerInterceptor() {
                @Override public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                    String principal = headers.get(IDENTITY);
                    var finished = new CountDownLatch(1);
                    terminal.set(finished);
                    var listener = principal == null ? next.startCall(call, headers)
                            : Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER, Caller.scoped(principal, Set.of())), call, headers, next);
                    return new io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener<Q>(listener) {
                        @Override public void onComplete() {
                            try { super.onComplete(); } finally { finished.countDown(); }
                        }
                        @Override public void onCancel() {
                            try { super.onCancel(); } finally { finished.countDown(); }
                        }
                    };
                }
            };
            server = InProcessServerBuilder.forName(name)
                    .addService(ServerInterceptors.intercept(new DocumentHistoryGrpcService(history, bindings, responses, 1), auth))
                    .addService(ServerInterceptors.intercept(new DocumentHistoryMaterializationGrpcService(history, bindings, MATERIALIZATION, responses, 1), auth))
                    .build().start();
            channel = InProcessChannelBuilder.forName(name).maxInboundMessageSize(8 * 1024 * 1024).build();
        }
        DocumentHistoryServiceGrpc.DocumentHistoryServiceBlockingStub history(String principal, Duration deadline) {
            awaitTerminal();
            var headers = new Metadata(); headers.put(IDENTITY, principal);
            return DocumentHistoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        DocumentHistoryMaterializationServiceGrpc.DocumentHistoryMaterializationServiceBlockingStub selected(String principal, Duration deadline) {
            awaitTerminal();
            var headers = new Metadata(); headers.put(IDENTITY, principal);
            return DocumentHistoryMaterializationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        // The client can receive completion before the server releases its single-call slot.
        // Terminal listener callbacks run after the producer and include its close/cancel handler.
        void awaitTerminal() {
            try {
                assertThat(terminal.get().await(10, TimeUnit.SECONDS))
                        .as("previous server call reached its terminal callback").isTrue();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted waiting for server call completion", interrupted);
            }
        }
        @Override public void close() throws Exception {
            channel.shutdownNow(); server.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest @ValueSource(strings = {"localstack", "rustfs"})
    void providerRefusalsReachTheRepositoryApiAsDomainCodes(String kind) throws Exception {
        try (var backend = new AssessmentStorageBackend(kind)) {
            backend.start();
            String endpoint = backend.getEndpoint().toString();
            String suffix = UUID.randomUUID().toString().substring(0, 8);
            String namespace = "history-failure-" + kind + "-" + suffix;
            String generation = "history-failure-" + kind + "-" + suffix;
            String account = "account-" + kind + "-" + suffix;
            try (var opened = BlobStores.discover().open("s3", options(endpoint, backend.getRegion(), backend.getAccessKey(), backend.getSecretKey(), 10_000))) {
                opened.ensureNamespace(namespace);
                var profile = new ManagedBackendLedger.Profile(S3BackendIdentity.of(endpoint, backend.getRegion(), true), "history-failure-realm");
                new ManagedBackendLedger(tx).bind(generation, profile);
                run(kind, backend, opened, namespace, generation, account, profile);
            }
        }
    }

    private void run(String kind, AssessmentStorageBackend backend, OpenedBlobStore opened, String namespace,
            String generation, String account, ManagedBackendLedger.Profile profile) throws Exception {
        String endpoint = backend.getEndpoint().toString();
        var drives = new DriveLedger(tx);
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = account; drive.name = "native-" + drive.driveId;
        drive.driveType = "CUSTOM"; drive.provider = "s3"; drive.bucket = namespace; drives.insert(drive);
        new DocumentSchemaPolicies(tx).activate(DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder()
                .setEncodingVersion(1).setAccountId(account).setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED)
                .setAnyResolvedSchema(true).setValidationProfile("protomolt-retained-schema-admission/v1")
                .setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(32).setMaxFragmentBytes(4_000_000).setMaxRoots(100)
                        .setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000)).build(),
                () -> {}), 0, () -> {});
        var security = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ)).build();
        var ownership = OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("source").setSecurity(security).build();
        var address = NodeAddress.newBuilder().setAccountId(account).setDocId("failure-document").setGraphId("graph").setGraphAddressId("node").build();
        var document = Document.newBuilder().setDocId(address.getDocId()).setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("retained provider payload"), "type.test")).build();
        var member = DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(drive.driveId.toString()).setOwnership(ownership)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(address));
        var bodies = new HashMap<DocumentPublicationRuntime.PayloadKey, PartObject>();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            bodies.put(new DocumentPublicationRuntime.PayloadKey("member", member.getPartsCount()), part);
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(part.bytes().length).setSha256(part.sha256()).setContentType("application/protobuf")));
        }
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId(account)
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var placements = Map.of(drive.driveId, new DocumentPublicationRuntime.Placement(drives.findById(drive.driveId).orElseThrow(), generation, profile));
        var payloadDefinition = DocumentSchemaRetentionFixture.definition(StringValue.getDescriptor());
        var resolutions = new AtomicInteger();
        DocumentPublicationRuntime.SchemaScopes scopes = (caller, selected, control) -> new DocumentSchemaAdmission.Resolution() {
            @Override public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) {
                resolutions.incrementAndGet(); return payloadDefinition;
            }
            @Override public void close() {}
        };

        var budget = new PayloadBudget(8_000_000);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var recording = new RecordingStore(opened.store());
        var active = new AtomicReference<BlobStore>(recording);
        var resolverCalls = Collections.synchronizedList(new ArrayList<String>());
        var readerCaller = new RepositoryCaller("member", false, Set.of(account), Set.of());
        var foreign = new RepositoryCaller("foreign", false, Set.of("elsewhere-" + account), Set.of());
        DocumentPublicationResult result;
        try (var reader = new DocumentPartReader((selectedGeneration, selectedProfile) -> {
                resolverCalls.add(selectedGeneration + "|" + selectedProfile);
                return active.get();
            }, 4, 1_000_000, budget)) {
            var runtime = new DocumentPublicationRuntime(tx, drives, reads, reader, budget,
                    (selectedGeneration, selectedProfile) -> new DocumentPublicationRuntime.Backend(profile.identity(), opened),
                    ASSEMBLY, new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5)), 4, Duration.ofMillis(25), LEASE, 4, 4_000_000, 100, false);
            try {
                result = runtime.executeScoped(ADMIN, command, placements, bodies, Map.of(), Map.of("member", DocumentPublicationRuntime.Mode.TYPED),
                        Optional.of(DocumentSchemaRetentionFixture.definition(Document.getDescriptor())), scopes, RepositoryReadControl.NONE);
                assertThat(result.getMembersCount()).isEqualTo(1);
                assertThat(resolutions.get()).isEqualTo(1);
                assertThat(resolverCalls).as("publication of fresh uploads resolves no historical backend").isEmpty();
                var published = result.getMembers(0);
                var revision = UUID.fromString(published.getRevisionId());
                var selection = selection(revision);
                var history = new DocumentHistoricalOperations(reads, reader, budget);
                try (var transport = new Transport(history, account)) {
                    var cases = new Cases(kind, backend, namespace, history, transport, reads, budget, readerCaller, address, revision, selection,
                            recording, resolverCalls, generation + "|" + profile, resolutions, List.of(endpoint, backend.getSecretKey(), backend.getAccessKey(), namespace));
                    // Successful retained reads on the real provider; raw, validated and selected through both transports.
                    var baseline = cases.baseline();
                    var recorded = recording.requests();
                    assertThat(recorded).isNotEmpty();
                    assertThat(recorded).allSatisfy(request -> {
                        assertThat(request.bucket()).isEqualTo(namespace);
                        assertThat(request.versionId()).as("every retained read names its exact provider version").isNotBlank();
                    });

                    // Authorization precedes provider access, even while the provider would refuse.
                    recording.failure = request -> new BlobStoreException(BlobStoreException.Code.UNAVAILABLE, "injected while unauthorized", null);
                    int before = recording.requests().size();
                    cases.expectUnauthorized(foreign, "foreign");
                    cases.expectCancelledBeforeProvider();
                    assertThat(recording.requests()).as("unauthorized and pre-cancelled reads never reach the provider").hasSize(before);
                    recording.reset();

                    // Injected: every adapter code through the wrapper over the real adapter.
                    for (var code : BlobStoreException.Code.values()) {
                        recording.failure = request -> new BlobStoreException(code, "injected provider " + code + " for " + request.key(), null);
                        cases.expect("injected " + code, expected(code));
                        recording.reset();
                    }

                    // Injected: bytes or identity altered by the wrapper after a real read.
                    recording.after = fetched -> {
                        var altered = fetched.data().clone(); altered[0] ^= 0x7f;
                        return new BlobStore.GetResult(altered, fetched.contentType(), fetched.eTag(), fetched.versionId());
                    };
                    cases.expect("altered bytes", new Expected(RepositoryException.Code.DATA_LOSS,
                            "Document part disagrees with its published byte or provider identity", null));
                    recording.reset();
                    recording.after = fetched -> new BlobStore.GetResult(fetched.data(), fetched.contentType(), fetched.eTag(), "other-" + UUID.randomUUID());
                    cases.expect("altered version identity", new Expected(RepositoryException.Code.DATA_LOSS,
                            "Document part disagrees with its published byte or provider identity", null));
                    recording.reset();

                    // Injected: READ access revoked while the provider fails; the ledger refusal wins over the provider detail.
                    recording.before = request -> security(address, Access.ACCESS_DENY);
                    recording.failure = request -> new BlobStoreException(BlobStoreException.Code.UNAVAILABLE, "injected after revocation", null);
                    cases.expect("revoked during provider failure", new Expected(RepositoryException.Code.NOT_FOUND, "Document is unavailable", null));
                    recording.reset();
                    security(address, Access.ACCESS_READ);
                    cases.baselineEquals(baseline);

                    // Injected timing: cancellation and expiry observed after real provider bytes arrived.
                    cases.expectCancelledDuringProvider();
                    cases.expectExpiredDuringProvider();

                    // Real: the host credential is refused by RustFS (403 signature mismatch). LocalStack accepts any static credential.
                    if (kind.equals("rustfs")) {
                        try (var wrong = BlobStores.discover().open("s3", options(endpoint, backend.getRegion(), backend.getAccessKey(), "not-" + backend.getSecretKey(), 10_000))) {
                            var refused = new RecordingStore(wrong.store());
                            active.set(refused);
                            cases.expect("wrong credential", new Expected(RepositoryException.Code.FAILED_PRECONDITION,
                                    "Original document backend refused the historical read", BlobStoreException.Code.PERMISSION_DENIED));
                            assertThat(refused.requests()).isNotEmpty();
                        } finally { active.set(recording); }
                        cases.baselineEquals(baseline);
                    }

                    // Real: connection refused at a closed loopback port, then the bound endpoint again.
                    int closedPort;
                    try (var socket = new ServerSocket(0)) { closedPort = socket.getLocalPort(); }
                    try (var dead = BlobStores.discover().open("s3", options("http://127.0.0.1:" + closedPort, backend.getRegion(), backend.getAccessKey(), backend.getSecretKey(), 1_000))) {
                        active.set(new RecordingStore(dead.store()));
                        cases.expect("unreachable endpoint", new Expected(RepositoryException.Code.UNAVAILABLE,
                                "Original document backend is unreachable", BlobStoreException.Code.UNAVAILABLE));
                    } finally { active.set(recording); }
                    cases.baselineEquals(baseline);

                    // Real: an unknown version id at the real provider; the wrapper rewrites only the requested version.
                    recording.rewrite = request -> new Request(request.bucket(), request.key(), UUID.randomUUID().toString());
                    cases.expect("unknown version id", unknownVersion(kind));
                    recording.reset();

                    // Real: the exact recorded version deleted while a newer version exists under the same key.
                    var target = recorded.stream().filter(request -> request.key().equals(baseline.manifestKey(selection.revisionOrdinal()))).findFirst().orElseThrow();
                    byte[] newer = ("newer bytes under " + target.key()).getBytes();
                    try (var admin = S3Client.builder().endpointOverride(backend.getEndpoint()).region(Region.of(backend.getRegion())).forcePathStyle(true)
                            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(backend.getAccessKey(), backend.getSecretKey()))).build()) {
                        var put = admin.putObject(b -> b.bucket(namespace).key(target.key()), RequestBody.fromBytes(newer));
                        assertThat(put.versionId()).isNotEqualTo(target.versionId());
                        admin.deleteObject(b -> b.bucket(namespace).key(target.key()).versionId(target.versionId()));
                        assertThat(admin.getObjectAsBytes(b -> b.bucket(namespace).key(target.key())).asByteArray())
                                .as("the provider now serves the newer version for an unversioned read").isEqualTo(newer);
                    }
                    int sinceDeletion = recording.requests().size();
                    cases.expect("deleted exact version", new Expected(RepositoryException.Code.DATA_LOSS,
                            "Published document part is missing from its original backend", null));
                    assertThat(recording.requests().subList(sinceDeletion, recording.requests().size()))
                            .as("the newer version is never requested in place of the recorded one")
                            .allSatisfy(request -> assertThat(request.versionId()).isNotNull())
                            .filteredOn(request -> request.key().equals(target.key()))
                            .isNotEmpty().allSatisfy(request -> assertThat(request.versionId()).isEqualTo(target.versionId()));

                    assertThat(resolverCalls).as("only the recorded generation and profile are resolved").isNotEmpty().containsOnly(generation + "|" + profile);
                    assertThat(resolutions.get()).as("no registry or schema resolution happens during historical reads").isEqualTo(1);
                    assertThat(runtime.executeScoped(ADMIN, command, Map.of(), Map.of(), Map.of(), Map.of(), Optional.empty(),
                            (caller, selected, control) -> { throw new AssertionError("Terminal replay must not resolve schemas"); },
                            RepositoryReadControl.NONE)).as("receipt replay after provider failures").isEqualTo(result);
                    assertThat(new DocumentPublicationReplay(tx).observe(ADMIN, command).result()).contains(result);
                }
            } finally {
                boolean stopped = false;
                for (int pass = 0; pass < 3 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(5));
                assertThat(stopped).isTrue();
            }
            reader.close();
            assertThat(reader.awaitIdle(Duration.ofSeconds(5))).as("no provider worker or read slot remains held").isTrue();
        }
        assertThat(reads.outstandingReads()).isZero();
        assertThat(budget.reservedBytes()).isZero();
        assertThat(count("document_read_pins")).isZero();
    }

    /** One revision's reads through both transports with the release checks every case repeats. */
    private final class Cases {
        final String kind; final AssessmentStorageBackend backend; final String namespace;
        final DocumentHistoricalOperations history; final Transport transport; final DocumentReadLedger reads; final PayloadBudget budget;
        final RepositoryCaller member; final NodeAddress address; final UUID revision; final HistoricalMaterializationRepository.Selection selection;
        final RecordingStore recording; final List<String> resolverCalls; final String expectedResolver; final AtomicInteger resolutions;
        final List<String> secrets;
        Cases(String kind, AssessmentStorageBackend backend, String namespace, DocumentHistoricalOperations history, Transport transport,
                DocumentReadLedger reads, PayloadBudget budget, RepositoryCaller member, NodeAddress address, UUID revision,
                HistoricalMaterializationRepository.Selection selection, RecordingStore recording, List<String> resolverCalls,
                String expectedResolver, AtomicInteger resolutions, List<String> secrets) {
            this.kind = kind; this.backend = backend; this.namespace = namespace; this.history = history; this.transport = transport;
            this.reads = reads; this.budget = budget; this.member = member; this.address = address; this.revision = revision;
            this.selection = selection; this.recording = recording; this.resolverCalls = resolverCalls; this.expectedResolver = expectedResolver;
            this.resolutions = resolutions; this.secrets = secrets;
        }

        record Baseline(List<byte[]> fragments, Document document, Any original, Map<Integer, String> manifestKeys) {
            String manifestKey(int ordinal) { return manifestKeys.get(ordinal); }
        }

        Baseline baseline() throws Exception {
            var fragments = new ArrayList<byte[]>();
            var keys = new HashMap<Integer, String>();
            int callsBefore = recording.requests().size();
            try (var raw = history.readRaw(member, address, revision, RepositoryReadControl.NONE)) {
                for (var fragment : raw.fragments()) {
                    var bytes = new byte[fragment.bytes().remaining()]; fragment.bytes().get(bytes); fragments.add(bytes);
                    keys.put(fragment.revisionOrdinal(), raw.manifest().getParts(fragment.revisionOrdinal()).getObjectKey());
                }
                int callsAfterRead = recording.requests().size();
                raw.authorizeDelivery(RepositoryReadControl.NONE);
                raw.fragments();
                assertThat(recording.requests()).as("no deferred provider read behind a returned raw result").hasSize(callsAfterRead);
            }
            assertThat(recording.requests()).as("closing a raw result performs no provider call").hasSize(callsBefore + fragments.size());
            Document document;
            try (var validated = history.readValidated(member, address, revision, RepositoryReadControl.NONE)) {
                document = validated.document();
                validated.authorizeDelivery(RepositoryReadControl.NONE);
                assertThat(validated.policySha256()).isNotBlank();
            }
            Any original;
            try (var materialized = history.readMaterialized(member, address, revision, selection, MATERIALIZATION, RepositoryReadControl.NONE)) {
                int calls = recording.requests().size();
                var view = materialized.view(RepositoryReadControl.NONE);
                original = view.original();
                assertThat(view.value().getDescriptorForType().getFullName()).isEqualTo(StringValue.getDescriptor().getFullName());
                materialized.view(RepositoryReadControl.NONE);
                assertThat(recording.requests()).as("views reauthorize through SQL only").hasSize(calls);
            }
            assertThat(original.unpack(StringValue.class).getValue()).isEqualTo("retained provider payload");
            assertThat(document.getStructuredData()).isEqualTo(original);
            var baseline = new Baseline(fragments, document, original, keys);
            baselineEquals(baseline);
            return baseline;
        }

        void baselineEquals(Baseline baseline) throws Exception {
            try (var raw = history.readRaw(member, address, revision, RepositoryReadControl.NONE)) {
                assertThat(raw.fragments()).hasSize(baseline.fragments().size());
                for (int index = 0; index < baseline.fragments().size(); index++) {
                    var bytes = new byte[raw.fragments().get(index).bytes().remaining()]; raw.fragments().get(index).bytes().get(bytes);
                    assertThat(bytes).isEqualTo(baseline.fragments().get(index));
                }
            }
            try (var validated = history.readValidated(member, address, revision, RepositoryReadControl.NONE)) {
                assertThat(validated.document()).isEqualTo(baseline.document());
            }
            try (var materialized = history.readMaterialized(member, address, revision, selection, MATERIALIZATION, RepositoryReadControl.NONE)) {
                assertThat(materialized.view(RepositoryReadControl.NONE).original()).isEqualTo(baseline.original());
            }
            ReadRevisionResponse raw = transport.history("member", Duration.ofSeconds(10)).readRevision(request(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW));
            assertThat(raw.getRaw().getFragmentsCount()).isEqualTo(baseline.fragments().size());
            for (int index = 0; index < baseline.fragments().size(); index++)
                assertThat(raw.getRaw().getFragments(index).getContent().toByteArray()).isEqualTo(baseline.fragments().get(index));
            ReadRevisionResponse validated = transport.history("member", Duration.ofSeconds(10)).readRevision(request(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED));
            assertThat(validated.getValidated().getDocument()).isEqualTo(baseline.document());
            ReadHistoricalOccurrenceResponse selected = transport.selected("member", Duration.ofSeconds(10)).readHistoricalOccurrence(selectedRequest());
            assertThat(selected.getOriginal()).isEqualTo(baseline.original());
            released();
        }

        void expect(String label, Expected expected) throws Exception {
            for (var read : List.of("raw", "validated", "materialized")) {
                var raised = catchThrowable(() -> library(read, member, RepositoryReadControl.NONE));
                if (!(raised instanceof RepositoryException)) System.out.println("HISTORICAL_READ_LEAK " + kind + " " + label + " " + read + " -> " + chain(raised));
                assertThat(raised).as("%s %s library read on %s", label, read, kind).isInstanceOf(RepositoryException.class);
                var failure = (RepositoryException) raised;
                assertThat(failure.code()).as("%s %s library code on %s", label, read, kind).isEqualTo(expected.code());
                assertThat(failure.getMessage()).as("%s %s library message on %s", label, read, kind).isEqualTo(expected.message());
                hygiene(label + " " + read, failure);
                if (expected.provider() != null) {
                    var provider = providerCause(failure);
                    assertThat(provider).as("%s %s keeps the adapter failure on its cause chain", label, read).isNotNull();
                    assertThat(provider.code()).isEqualTo(expected.provider());
                }
                var status = catchThrowable(() -> wire(read, "member", Duration.ofSeconds(20)));
                assertThat(status).as("%s %s gRPC read on %s", label, read, kind).isInstanceOf(StatusRuntimeException.class);
                var observed = ((StatusRuntimeException) status).getStatus();
                assertThat(observed.getCode()).as("%s %s gRPC code on %s", label, read, kind).isEqualTo(status(expected.code()));
                assertThat(observed.getDescription()).as("%s %s gRPC description on %s", label, read, kind).isEqualTo(expected.message());
            }
            released();
        }

        void expectUnauthorized(RepositoryCaller caller, String principal) throws Exception {
            for (var read : List.of("raw", "validated", "materialized")) {
                assertThatThrownBy(() -> library(read, caller, RepositoryReadControl.NONE)).isInstanceOfSatisfying(RepositoryException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND);
                    assertThat(failure.getMessage()).isEqualTo("Document is unavailable");
                });
                assertThatThrownBy(() -> wire(read, principal, Duration.ofSeconds(10))).isInstanceOfSatisfying(StatusRuntimeException.class,
                        failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
            }
            released();
        }

        void expectCancelledBeforeProvider() throws Exception {
            var cancelled = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return true; }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            for (var read : List.of("raw", "validated", "materialized"))
                assertThatThrownBy(() -> library(read, member, cancelled)).isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            released();
        }

        void expectCancelledDuringProvider() throws Exception {
            var flag = new AtomicBoolean();
            var control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return flag.get(); }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            recording.before = request -> flag.set(true);
            for (var read : List.of("raw", "validated", "materialized")) {
                flag.set(false);
                int calls = recording.requests().size();
                assertThatThrownBy(() -> library(read, member, control)).isInstanceOfSatisfying(RepositoryException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED);
                    assertThat(failure.getMessage()).isEqualTo("Document read cancelled");
                });
                assertThat(recording.requests().size()).as("the provider was reached before cancellation").isGreaterThan(calls);
            }
            recording.reset();
            released();
            // gRPC: the real provider answers only after the client deadline passed; the server then
            // releases its snapshot, budget, pins and the single call slot.
            var gate = new AtomicReference<CountDownLatch>();
            recording.before = request -> {
                try {
                    if (!gate.get().await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Provider gate timed out");
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
            };
            for (var read : List.of("raw", "validated", "materialized")) {
                gate.set(new CountDownLatch(1));
                try {
                    assertThatThrownBy(() -> wire(read, "member", Duration.ofMillis(400))).isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED));
                } finally { gate.get().countDown(); }
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (System.nanoTime() < deadline && (budget.reservedBytes() != 0 || transport.responses.reservedBytes() != 0
                        || reads.releaseDrained(64) > 0 || reads.outstandingReads() != 0)) Thread.sleep(20);
                released();
                wire(read, "member", Duration.ofSeconds(10));
            }
            recording.reset();
            released();
        }

        void expectExpiredDuringProvider() throws Exception {
            var expired = new AtomicBoolean();
            var control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return false; }
                @Override public long remainingNanos() { return expired.get() ? 0 : Long.MAX_VALUE; }
            };
            recording.before = request -> expired.set(true);
            for (var read : List.of("raw", "validated", "materialized")) {
                expired.set(false);
                assertThatThrownBy(() -> library(read, member, control)).isInstanceOfSatisfying(RepositoryException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RepositoryException.Code.DEADLINE_EXCEEDED);
                    assertThat(failure.getMessage()).isEqualTo("Document read deadline exceeded");
                });
            }
            recording.reset();
            released();
        }

        private void library(String read, RepositoryCaller caller, RepositoryReadControl control) {
            switch (read) {
                case "raw" -> { try (var raw = history.readRaw(caller, address, revision, control)) { raw.fragments(); raw.authorizeDelivery(control); } }
                case "validated" -> { try (var validated = history.readValidated(caller, address, revision, control)) { validated.document(); validated.authorizeDelivery(control); } }
                default -> { try (var materialized = history.readMaterialized(caller, address, revision, selection, MATERIALIZATION, control)) { materialized.view(control); } }
            }
        }

        private Object wire(String read, String principal, Duration deadline) {
            return switch (read) {
                case "raw" -> transport.history(principal, deadline).readRevision(request(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW));
                case "validated" -> transport.history(principal, deadline).readRevision(request(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED));
                default -> transport.selected(principal, deadline).readHistoricalOccurrence(selectedRequest());
            };
        }

        private ReadRevisionRequest request(HistoricalDocumentReadMode mode) {
            return ReadRevisionRequest.newBuilder().setAddress(address).setRevisionId(revision.toString()).setMode(mode).build();
        }

        private ReadHistoricalOccurrenceRequest selectedRequest() {
            return ReadHistoricalOccurrenceRequest.newBuilder().setAddress(address).setRevisionId(revision.toString())
                    .setSelection(HistoricalOccurrenceSelection.newBuilder().setRevisionOrdinal(selection.revisionOrdinal())
                            .setRootSha256(selection.rootSha256()).setPathSha256(selection.pathSha256()))
                    .setLimits(HistoricalMaterializationLimits.newBuilder().setMaxFragmentBytes(4_000_000).setMaxEvidenceBytes(4_000_000)
                            .setMaxRetainedBytes(16_000_000).setMaxReferences(64).setMaxDecodedBytes(4_000_000).setMaxBoundaries(64)).build();
        }

        private void hygiene(String label, RepositoryException failure) {
            for (var secret : secrets)
                assertThat(failure.getMessage()).as("%s message must not disclose %s", label, secret).doesNotContain(secret);
            assertThat(failure.getMessage()).doesNotContain("s3://", "127.0.0.1", "documents/");
        }

        /**
         * Budgets, pins and outstanding captures are back to zero after every case. A library
         * failure releases its lease before it leaves the engine; after a transport call the
         * server thread closes the engine result and its response snapshot after the client
         * already holds the answer, so both budget checks are bounded rather than instantaneous.
         */
        private void released() throws Exception {
            transport.awaitTerminal();
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while ((budget.reservedBytes() != 0 || transport.responses.reservedBytes() != 0) && System.nanoTime() < deadline) Thread.sleep(10);
            assertThat(budget.reservedBytes()).as("payload budget released").isZero();
            assertThat(transport.responses.reservedBytes()).as("response budget released").isZero();
            reads.releaseDrained(64);
            assertThat(reads.outstandingReads()).as("no outstanding historical capture").isZero();
            assertThat(count("document_read_pins")).as("no SQL read pin remains").isZero();
            assertThat(resolverCalls).containsOnly(expectedResolver);
        }
    }

    private static Expected expected(BlobStoreException.Code code) {
        return switch (code) {
            case PERMISSION_DENIED, UNAUTHENTICATED -> new Expected(RepositoryException.Code.FAILED_PRECONDITION, "Original document backend refused the historical read", code);
            case UNAVAILABLE -> new Expected(RepositoryException.Code.UNAVAILABLE, "Original document backend is unreachable", code);
            case DEADLINE_EXCEEDED -> new Expected(RepositoryException.Code.UNAVAILABLE, "Original document backend did not answer in time", code);
            case RESOURCE_EXHAUSTED -> new Expected(RepositoryException.Code.RESOURCE_EXHAUSTED, "Original document backend read capacity is exhausted", code);
            case CANCELLED -> new Expected(RepositoryException.Code.CANCELLED, "Historical read cancelled or expired", code);
            case NOT_FOUND -> new Expected(RepositoryException.Code.DATA_LOSS, "Published document part is missing from its original backend", code);
            case DATA_LOSS -> new Expected(RepositoryException.Code.DATA_LOSS, "Historical document part is damaged at its original backend", code);
            case FAILED_PRECONDITION -> new Expected(RepositoryException.Code.FAILED_PRECONDITION, "Original document backend cannot serve the retained revision", code);
            case INVALID_ARGUMENT -> new Expected(RepositoryException.Code.FAILED_PRECONDITION, "Original document backend rejected the retained object coordinates", code);
            case UNIMPLEMENTED -> new Expected(RepositoryException.Code.UNSUPPORTED, "Original document backend does not support historical reads", code);
            case ABORTED -> new Expected(RepositoryException.Code.CONFLICT, "Original document backend aborted the historical read", code);
            case INTERNAL -> new Expected(RepositoryException.Code.INTERNAL, "Original document backend failed internally", code);
            case UNKNOWN, ALREADY_EXISTS, OUT_OF_RANGE -> new Expected(RepositoryException.Code.UNKNOWN, "Original document backend rejected the historical read", code);
        };
    }

    /** The pinned images answer an unknown version id differently; each answer is a real provider response. */
    private static Expected unknownVersion(String kind) {
        return switch (kind) {
            case "localstack" -> new Expected(RepositoryException.Code.DATA_LOSS, "Published document part is missing from its original backend", null);
            case "rustfs" -> new Expected(RepositoryException.Code.DATA_LOSS, "Published document part is missing from its original backend", null);
            default -> throw new IllegalArgumentException(kind);
        };
    }

    private static Status.Code status(RepositoryException.Code code) {
        return switch (code) {
            case CONFLICT -> Status.Code.ABORTED;
            case UNSUPPORTED -> Status.Code.UNIMPLEMENTED;
            default -> Status.Code.valueOf(code.name());
        };
    }

    private static String chain(Throwable failure) {
        var text = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            text.append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append(" <- ");
        return text.toString();
    }

    private static BlobStoreException providerCause(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof BlobStoreException provider) return provider;
        return null;
    }

    private static void security(NodeAddress address, Access access) {
        tx.inTransaction(em -> {
            var row = em.find(DocumentRecord.class, DocumentIds.nodeId(address), LockModeType.PESSIMISTIC_WRITE);
            row.writeSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                    .setIdentityType("public").setIdentity("public").setAccess(access)).build());
        });
    }

    private static HistoricalMaterializationRepository.Selection selection(UUID revision) throws Exception {
        var row = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT revision_ordinal,encode(root_locator_sha256,'hex'),evidence_bytes
                FROM document_revision_schema_evidence WHERE revision_id=:revision ORDER BY revision_ordinal LIMIT 1
                """).setParameter("revision", revision).getSingleResult());
        var evidence = DocumentRootSchemaEvidence.parseFrom((byte[]) row[2]);
        var path = evidence.getOccurrencesList().stream().filter(value -> value.getStepsCount() == 1).findFirst().orElseThrow();
        return new HistoricalMaterializationRepository.Selection(((Number) row[0]).intValue(), (String) row[1],
                DocumentPartCodec.sha256Hex(path.toByteArray()));
    }

    private static long count(String table) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table).getSingleResult()).longValue());
    }

    private static Map<String, String> options(String endpoint, String region, String accessKey, String secretKey, int connectionTimeoutMs) {
        return Map.ofEntries(
                Map.entry("endpoint", endpoint),
                Map.entry("region", region),
                Map.entry("access-key", accessKey),
                Map.entry("secret-key", secretKey),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "60000"),
                Map.entry("api-attempt-timeout-ms", "20000"),
                Map.entry("connection-timeout-ms", Integer.toString(connectionTimeoutMs)),
                Map.entry("socket-timeout-ms", "20000"));
    }
}
