package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.container.ledger.DocumentPublicationRuntime;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.container.ledger.Tx;
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
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.PublicationUpload;
import ai.protomolt.proto.repo.v1.ReadHistoricalOccurrenceRequest;
import ai.protomolt.proto.repo.v1.ReadRevisionRequest;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
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
 * The production host over real PostgreSQL and the pinned RustFS image. The provider endpoint
 * is reached through an in-test TCP proxy that forwards bytes unchanged, so the bound endpoint
 * identity can become unreachable and reachable again without changing the backend generation.
 * Every provider failure here is a real provider or socket response.
 */
@Testcontainers
class HistoricalReadFailureHostIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final RustFsBenchmarkStore STORAGE = new RustFsBenchmarkStore();
    private static final RepositoryCaller ADMIN = new RepositoryCaller("schema-owner", true);
    private static final HistoricalMaterializationRepository.Limits MATERIALIZATION =
            new HistoricalMaterializationRepository.Limits(4_000_000, 4_000_000, 16_000_000, 64, 4_000_000, 64);
    private static final Metadata.Key<String> TOKEN = Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);

    /** Loopback forwarder; closing the listener yields a real connection refusal at the same endpoint identity. */
    static final class Proxy implements AutoCloseable {
        private final String targetHost; private final int targetPort; private final int port;
        private final List<Socket> sockets = Collections.synchronizedList(new ArrayList<>());
        private volatile ServerSocket listener;
        Proxy(String targetHost, int targetPort) throws IOException {
            this.targetHost = targetHost; this.targetPort = targetPort;
            try (var reserved = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) { port = reserved.getLocalPort(); }
        }
        int port() { return port; }
        synchronized void start() throws IOException {
            var opened = new ServerSocket();
            opened.setReuseAddress(true);
            opened.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            listener = opened;
            Thread.ofVirtual().start(() -> accept(opened));
        }
        private void accept(ServerSocket opened) {
            while (!opened.isClosed()) {
                try {
                    var client = opened.accept();
                    var upstream = new Socket(targetHost, targetPort);
                    sockets.add(client); sockets.add(upstream);
                    Thread.ofVirtual().start(() -> pump(client, upstream));
                    Thread.ofVirtual().start(() -> pump(upstream, client));
                } catch (IOException closed) { return; }
            }
        }
        private static void pump(Socket from, Socket to) {
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                in.transferTo(out);
                to.shutdownOutput();
            } catch (IOException ended) {
                try { to.close(); } catch (IOException ignored) { /* peer already gone */ }
            }
        }
        synchronized void stop() throws IOException {
            var opened = listener; listener = null;
            if (opened != null) opened.close();
            synchronized (sockets) { for (var socket : sockets) socket.close(); sockets.clear(); }
        }
        @Override public void close() throws IOException { stop(); }
    }

    private record Expected(RepositoryException.Code code, String message, BlobStoreException.Code provider) {}
    private record Published(NodeAddress address, UUID revision, HistoricalMaterializationRepository.Selection selection,
            DocumentPublicationCommand command, DocumentPublicationResult result, String namespace) {}

    @Test void hostTranslatesRealProviderRefusalsIdenticallyForLibraryAndTransportCallers() throws Exception {
        String generation = "history-host-" + UUID.randomUUID();
        try (var proxy = new Proxy(STORAGE.getHost(), STORAGE.getMappedPort(9000))) {
            proxy.start();
            String endpoint = "http://127.0.0.1:" + proxy.port();
            var config = config(endpoint, STORAGE.getSecretKey(), generation);
            var schemas = new Schemas();
            String account = "account-" + UUID.randomUUID();
            var access = new HistoricalReadAccess(caller -> switch (caller.name()) {
                case "member" -> new RepositoryCaller("member", false, Set.of(account), Set.of());
                case "foreign" -> new RepositoryCaller("foreign", false, Set.of("elsewhere"), Set.of());
                default -> throw new AssertionError("Unexpected authenticated fixture identity " + caller.name());
            }, 32L * 1024 * 1024, 2).withMaterialization(MATERIALIZATION);
            List<String> secrets = List.of(endpoint, STORAGE.getSecretKey(), STORAGE.getAccessKey());
            Published intact, doomed;
            try (var database = new LedgerDatabase(config.ledger())) {
                var tx = new Tx(database.entityManagerFactory());
                try (var host = RepoServices.build(config, BridgeEngine.standard(), access, schemas)) {
                    intact = publish(host, tx, generation, account, "intact", endpoint);
                    doomed = publish(host, tx, generation, account, "doomed", endpoint);
                    assertThat(schemas.opened.get()).isEqualTo(2);
                    try (var transport = new Transport(host)) {
                        var member = new RepositoryCaller("member", false, Set.of(account), Set.of());
                        var foreign = new RepositoryCaller("foreign", false, Set.of("elsewhere"), Set.of());
                        var intactBaseline = baseline(host, transport, member, intact);
                        var doomedBaseline = baseline(host, transport, member, doomed);

                        // Real: connection refused at the bound endpoint; an unauthorized caller is still refused by the ledger first.
                        proxy.stop();
                        expectUnauthorized(host, transport, foreign, intact);
                        expect(host, transport, member, intact, "unreachable", new Expected(RepositoryException.Code.UNAVAILABLE,
                                "Original document backend is unreachable", BlobStoreException.Code.UNAVAILABLE), secrets);
                        proxy.start();
                        baselineEquals(host, transport, member, intact, intactBaseline);

                        // Real: the exact recorded version is deleted while a newer version exists under the key.
                        try (var admin = S3Client.builder().endpointOverride(STORAGE.getEndpoint()).region(Region.of(STORAGE.getRegion())).forcePathStyle(true)
                                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(STORAGE.getAccessKey(), STORAGE.getSecretKey()))).build()) {
                            String key = doomedBaseline.manifestKeys().get(doomed.selection().revisionOrdinal());
                            var versions = admin.listObjectVersions(b -> b.bucket(doomed.namespace()).prefix(key)).versions().stream()
                                    .filter(version -> version.key().equals(key)).toList();
                            assertThat(versions).hasSize(1);
                            String recorded = versions.getFirst().versionId();
                            var put = admin.putObject(b -> b.bucket(doomed.namespace()).key(key), RequestBody.fromBytes(("newer " + key).getBytes()));
                            assertThat(put.versionId()).isNotEqualTo(recorded);
                            admin.deleteObject(b -> b.bucket(doomed.namespace()).key(key).versionId(recorded));
                            assertThat(admin.getObjectAsBytes(b -> b.bucket(doomed.namespace()).key(key)).asByteArray()).isEqualTo(("newer " + key).getBytes());
                        }
                        expect(host, transport, member, doomed, "deleted exact version", new Expected(RepositoryException.Code.DATA_LOSS,
                                "Published document part is missing from its original backend", null), secrets);
                        baselineEquals(host, transport, member, intact, intactBaseline);
                        assertThat(replay(host, intact)).isEqualTo(intact.result());
                        assertThat(replay(host, doomed)).isEqualTo(doomed.result());
                        assertThat(schemas.opened.get()).as("no schema resolution during historical reads").isEqualTo(2);
                    }
                }
                assertThat(count("ACTIVE")).isZero();

                // Real: a second host over the same ledger and generation presents a credential RustFS refuses (403).
                var wrongSchemas = new Schemas();
                try (var host = RepoServices.build(config(endpoint, "not-" + STORAGE.getSecretKey(), generation), BridgeEngine.standard(), access, wrongSchemas);
                        var transport = new Transport(host)) {
                    var member = new RepositoryCaller("member", false, Set.of(account), Set.of());
                    expect(host, transport, member, intact, "wrong credential", new Expected(RepositoryException.Code.FAILED_PRECONDITION,
                            "Original document backend refused the historical read", BlobStoreException.Code.PERMISSION_DENIED), secrets);
                    assertThat(replay(host, intact)).as("receipt replay needs no provider access").isEqualTo(intact.result());
                    assertThat(wrongSchemas.opened.get()).isZero();
                }
                assertThat(count("ACTIVE")).isZero();
            }
        }
    }

    private record Baseline(List<byte[]> fragments, Document document, Any original, Map<Integer, String> manifestKeys) {}

    private static Baseline baseline(RepoServices host, Transport transport, RepositoryCaller member, Published published) throws Exception {
        var fragments = new ArrayList<byte[]>(); var keys = new HashMap<Integer, String>();
        try (var raw = host.historicalRepository().readRaw(member, published.address(), published.revision(), RepositoryReadControl.NONE)) {
            for (var fragment : raw.fragments()) {
                var bytes = new byte[fragment.bytes().remaining()]; fragment.bytes().get(bytes); fragments.add(bytes);
                keys.put(fragment.revisionOrdinal(), raw.manifest().getParts(fragment.revisionOrdinal()).getObjectKey());
            }
            raw.authorizeDelivery(RepositoryReadControl.NONE);
        }
        Document document;
        try (var validated = host.historicalRepository().readValidated(member, published.address(), published.revision(), RepositoryReadControl.NONE)) {
            document = validated.document();
        }
        Any original;
        try (var materialized = host.historicalMaterializationRepository().readMaterialized(member, published.address(), published.revision(),
                published.selection(), MATERIALIZATION, RepositoryReadControl.NONE)) {
            original = materialized.view(RepositoryReadControl.NONE).original();
        }
        assertThat(document.getStructuredData()).isEqualTo(original);
        var baseline = new Baseline(fragments, document, original, keys);
        baselineEquals(host, transport, member, published, baseline);
        return baseline;
    }

    private static void baselineEquals(RepoServices host, Transport transport, RepositoryCaller member, Published published, Baseline baseline) throws Exception {
        try (var raw = host.historicalRepository().readRaw(member, published.address(), published.revision(), RepositoryReadControl.NONE)) {
            assertThat(raw.fragments()).hasSize(baseline.fragments().size());
            for (int index = 0; index < baseline.fragments().size(); index++) {
                var bytes = new byte[raw.fragments().get(index).bytes().remaining()]; raw.fragments().get(index).bytes().get(bytes);
                assertThat(bytes).isEqualTo(baseline.fragments().get(index));
            }
        }
        try (var validated = host.historicalRepository().readValidated(member, published.address(), published.revision(), RepositoryReadControl.NONE)) {
            assertThat(validated.document()).isEqualTo(baseline.document());
        }
        try (var materialized = host.historicalMaterializationRepository().readMaterialized(member, published.address(), published.revision(),
                published.selection(), MATERIALIZATION, RepositoryReadControl.NONE)) {
            assertThat(materialized.view(RepositoryReadControl.NONE).original()).isEqualTo(baseline.original());
        }
        var raw = transport.history("member-key").readRevision(request(published, HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW));
        assertThat(raw.getRaw().getFragmentsCount()).isEqualTo(baseline.fragments().size());
        for (int index = 0; index < baseline.fragments().size(); index++)
            assertThat(raw.getRaw().getFragments(index).getContent().toByteArray()).isEqualTo(baseline.fragments().get(index));
        var validated = transport.history("member-key").readRevision(request(published, HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED));
        assertThat(validated.getValidated().getDocument()).isEqualTo(baseline.document());
        assertThat(transport.selected("member-key").readHistoricalOccurrence(selectedRequest(published)).getOriginal()).isEqualTo(baseline.original());
    }

    private static void expect(RepoServices host, Transport transport, RepositoryCaller member, Published published, String label,
            Expected expected, List<String> secrets) {
        for (var read : List.of("raw", "validated", "materialized")) {
            var raised = catchThrowable(() -> library(host, read, member, published));
            if (!(raised instanceof RepositoryException)) System.out.println("HISTORICAL_READ_LEAK host " + label + " " + read + " -> " + chain(raised));
            assertThat(raised).as("%s %s library read", label, read).isInstanceOf(RepositoryException.class);
            var failure = (RepositoryException) raised;
            assertThat(failure.code()).as("%s %s library code", label, read).isEqualTo(expected.code());
            assertThat(failure.getMessage()).as("%s %s library message", label, read).isEqualTo(expected.message());
            for (var secret : secrets) assertThat(failure.getMessage()).doesNotContain(secret);
            assertThat(failure.getMessage()).doesNotContain("s3://", "127.0.0.1", published.namespace());
            if (expected.provider() != null) {
                BlobStoreException provider = null;
                for (Throwable cause = failure; cause != null && provider == null; cause = cause.getCause())
                    if (cause instanceof BlobStoreException candidate) provider = candidate;
                assertThat(provider).as("%s %s keeps the adapter failure internally", label, read).isNotNull();
                assertThat(provider.code()).isEqualTo(expected.provider());
            }
            var status = catchThrowable(() -> wire(transport, read, "member-key", published));
            assertThat(status).as("%s %s gRPC read", label, read).isInstanceOf(StatusRuntimeException.class);
            var observed = ((StatusRuntimeException) status).getStatus();
            assertThat(observed.getCode()).as("%s %s gRPC code", label, read).isEqualTo(wire(expected.code()));
            assertThat(observed.getDescription()).as("%s %s gRPC description", label, read).isEqualTo(expected.message());
        }
    }

    private static void expectUnauthorized(RepoServices host, Transport transport, RepositoryCaller foreign, Published published) {
        for (var read : List.of("raw", "validated", "materialized")) {
            assertThatThrownBy(() -> library(host, read, foreign, published)).isInstanceOfSatisfying(RepositoryException.class, failure -> {
                assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND);
                assertThat(failure.getMessage()).isEqualTo("Document is unavailable");
            });
            assertThatThrownBy(() -> wire(transport, read, "foreign-key", published)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
        }
    }

    private static void library(RepoServices host, String read, RepositoryCaller caller, Published published) {
        switch (read) {
            case "raw" -> { try (var raw = host.historicalRepository().readRaw(caller, published.address(), published.revision(), RepositoryReadControl.NONE)) {
                raw.fragments(); raw.authorizeDelivery(RepositoryReadControl.NONE); } }
            case "validated" -> { try (var validated = host.historicalRepository().readValidated(caller, published.address(), published.revision(), RepositoryReadControl.NONE)) {
                validated.document(); validated.authorizeDelivery(RepositoryReadControl.NONE); } }
            default -> { try (var materialized = host.historicalMaterializationRepository().readMaterialized(caller, published.address(), published.revision(),
                    published.selection(), MATERIALIZATION, RepositoryReadControl.NONE)) { materialized.view(RepositoryReadControl.NONE); } }
        }
    }

    private static Object wire(Transport transport, String read, String token, Published published) {
        return switch (read) {
            case "raw" -> transport.history(token).readRevision(request(published, HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW));
            case "validated" -> transport.history(token).readRevision(request(published, HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED));
            default -> transport.selected(token).readHistoricalOccurrence(selectedRequest(published));
        };
    }

    private static Status.Code wire(RepositoryException.Code code) {
        return switch (code) {
            case CONFLICT -> Status.Code.ABORTED;
            case UNSUPPORTED -> Status.Code.UNIMPLEMENTED;
            default -> Status.Code.valueOf(code.name());
        };
    }

    private static ReadRevisionRequest request(Published published, HistoricalDocumentReadMode mode) {
        return ReadRevisionRequest.newBuilder().setAddress(published.address()).setRevisionId(published.revision().toString()).setMode(mode).build();
    }

    private static ReadHistoricalOccurrenceRequest selectedRequest(Published published) {
        var selection = published.selection();
        return ReadHistoricalOccurrenceRequest.newBuilder().setAddress(published.address()).setRevisionId(published.revision().toString())
                .setSelection(HistoricalOccurrenceSelection.newBuilder().setRevisionOrdinal(selection.revisionOrdinal())
                        .setRootSha256(selection.rootSha256()).setPathSha256(selection.pathSha256()))
                .setLimits(HistoricalMaterializationLimits.newBuilder().setMaxFragmentBytes(4_000_000).setMaxEvidenceBytes(4_000_000)
                        .setMaxRetainedBytes(16_000_000).setMaxReferences(64).setMaxDecodedBytes(4_000_000).setMaxBoundaries(64)).build();
    }

    private static DocumentPublicationResult replay(RepoServices host, Published published) throws Exception {
        return host.publishDocument(ADMIN, published.command(), Map.of(), Map.of(), Map.of(), Map.of(), Optional.empty(), RepositoryReadControl.NONE);
    }

    /** Authenticated in-process transport of the host; tokens are synthetic test credentials. */
    private static final class Transport implements AutoCloseable {
        final ManagedChannel channel;
        Transport(RepoServices host) {
            String name = "history-host-" + UUID.randomUUID();
            CallerResolver credentials = token -> switch (token) {
                case "member-key" -> Optional.of(Caller.scoped("member", Set.of()));
                case "foreign-key" -> Optional.of(Caller.scoped("foreign", Set.of()));
                default -> Optional.empty();
            };
            host.startInProcess(name, "synthetic-host-operator-key", credentials);
            channel = InProcessChannelBuilder.forName(name).maxInboundMessageSize(8 * 1024 * 1024).build();
        }
        DocumentHistoryServiceGrpc.DocumentHistoryServiceBlockingStub history(String token) {
            var headers = new Metadata(); headers.put(TOKEN, token);
            return DocumentHistoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        DocumentHistoryMaterializationServiceGrpc.DocumentHistoryMaterializationServiceBlockingStub selected(String token) {
            var headers = new Metadata(); headers.put(TOKEN, token);
            return DocumentHistoryMaterializationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        @Override public void close() throws Exception {
            channel.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    /** Registry-free schema access: the definition is supplied directly and every open is counted. */
    private static final class Schemas implements ManagedSchemaAccess {
        final AtomicInteger opened = new AtomicInteger();
        final DocumentSchemaAdmission.Definition definition = definition(StringValue.getDescriptor());
        @Override public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member, RepositoryReadControl control) {
            opened.incrementAndGet();
            return new DocumentSchemaAdmission.Resolution() {
                @Override public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) { return definition; }
                @Override public void close() {}
            };
        }
        @Override public void close() {}
        @Override public boolean awaitIdle(Duration timeout) { return true; }
    }


    private static RepoServiceConfig config(String endpoint, String secret, String generation) {
        return new RepoServiceConfig(0, new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                endpoint, STORAGE.getRegion(), STORAGE.getAccessKey(), secret, "history-host", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy(generation, "history-host-realm", true));
    }

    private static Published publish(RepoServices host, Tx tx, String generation, String account, String docId, String endpoint) throws Exception {
        String namespace = "history-host-" + UUID.randomUUID();
        try (var backing = BlobStores.discover().open("s3", Map.ofEntries(
                Map.entry("endpoint", endpoint), Map.entry("region", STORAGE.getRegion()), Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"), Map.entry("access-key", STORAGE.getAccessKey()), Map.entry("secret-key", STORAGE.getSecretKey()),
                Map.entry("credentials-mode", "static"), Map.entry("api-call-timeout-ms", "300000"), Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"), Map.entry("socket-timeout-ms", "60000")))) { backing.ensureNamespace(namespace); }
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = account; drive.name = "history-" + docId; drive.driveType = "PIPELINE"; drive.bucket = namespace;
        host.driveLedger().insert(drive);
        var profile = new ManagedBackendLedger(tx).find(generation).orElseThrow();
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder()
                .setEncodingVersion(1).setAccountId(account).setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED).setAnyResolvedSchema(true)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(32).setMaxFragmentBytes(4_000_000).setMaxRoots(100)
                        .setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000)).build(), () -> {});
        // Trusted fixture administration through the guarded policy catalog; the host exposes no policy administration API.
        boolean activated = tx.readOnly(em -> !em.createNativeQuery("SELECT 1 FROM document_schema_policy_current WHERE account_id=:account")
                .setParameter("account", account).getResultList().isEmpty());
        if (!activated) tx.inTransaction(em -> {
            em.createNativeQuery("SELECT lock_document_schema_policy_account(:account,true)").setParameter("account", account).getSingleResult();
            em.createNativeQuery("""
                    INSERT INTO document_schema_policies(account_id,policy_sha256,policy_codec,policy_version,policy_bytes)
                    VALUES(:account,:sha,:codec,:version,:bytes)
                    """).setParameter("account", account).setParameter("sha", HexFormat.of().parseHex(policy.sha256()))
                    .setParameter("codec", DocumentAdmissionPolicy.CODEC).setParameter("version", DocumentAdmissionPolicy.VERSION)
                    .setParameter("bytes", policy.bytes().toByteArray()).executeUpdate();
            em.createNativeQuery("INSERT INTO document_schema_policy_current(account_id,policy_sha256,policy_revision) VALUES(:account,:sha,1)")
                    .setParameter("account", account).setParameter("sha", HexFormat.of().parseHex(policy.sha256())).executeUpdate();
        });
        var security = DocumentSecurity.newBuilder()
                .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
                .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_WRITE)).build();
        var ownership = OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("source").setSecurity(security).build();
        var address = NodeAddress.newBuilder().setAccountId(account).setDocId(docId).setGraphId("graph").setGraphAddressId("node").build();
        var document = Document.newBuilder().setDocId(docId).setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("retained host payload " + docId), "type.test")).build();
        var member = DocumentPublicationMember.newBuilder().setMemberId("document").setDriveId(drive.driveId.toString()).setOwnership(ownership)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(address));
        var bodies = new HashMap<DocumentPublicationRuntime.PayloadKey, PartObject>();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            bodies.put(new DocumentPublicationRuntime.PayloadKey("document", member.getPartsCount()), part);
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(part.bytes().length).setSha256(DocumentPartCodec.sha256Hex(part.bytes())).setContentType("application/protobuf")));
        }
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId(account)
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var result = host.publishDocument(ADMIN, command, Map.of(drive.driveId, new DocumentPublicationRuntime.Placement(drive, generation, profile)), bodies,
                Map.of(), Map.of("document", DocumentPublicationRuntime.Mode.TYPED), Optional.of(definition(Document.getDescriptor())), RepositoryReadControl.NONE);
        assertThat(result.getMembersCount()).isEqualTo(1);
        var revision = UUID.fromString(result.getMembers(0).getRevisionId());
        var row = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT revision_ordinal,encode(root_locator_sha256,'hex'),evidence_bytes
                FROM document_revision_schema_evidence WHERE revision_id=:revision ORDER BY revision_ordinal LIMIT 1
                """).setParameter("revision", revision).getSingleResult());
        var evidence = DocumentRootSchemaEvidence.parseFrom((byte[]) row[2]);
        var path = evidence.getOccurrencesList().stream().filter(value -> value.getStepsCount() == 1).findFirst().orElseThrow();
        var selection = new HistoricalMaterializationRepository.Selection(((Number) row[0]).intValue(), (String) row[1],
                DocumentPartCodec.sha256Hex(path.toByteArray()));
        return new Published(address, revision, selection, command, result, namespace);
    }

    private static DocumentSchemaAdmission.Definition definition(Descriptors.Descriptor type) {
        var closure = DescriptorFingerprints.closure(type); var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl("type.test/" + type.getFullName())
                .setArtifactSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName()).setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder().setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN).setUnknownCompilerReason("fixture compiler unknown")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"))).build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    private static String chain(Throwable failure) {
        var text = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            text.append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append(" <- ");
        return text.toString();
    }

    private static long count(String state) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.prepareStatement("SELECT count(*) FROM repository_reader_incarnations WHERE state=?")) {
            query.setString(1, state);
            try (var result = query.executeQuery()) { result.next(); return result.getLong(1); }
        }
    }
}
