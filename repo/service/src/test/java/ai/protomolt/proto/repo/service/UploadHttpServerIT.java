package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.RenditionUploads;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.authz.RequestCallerResolver;
import ai.protomolt.proto.repo.archive.DocumentArchiveBindings;
import ai.protomolt.proto.repo.archive.RenditionReads;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.ValidationMode;
import ai.protomolt.proto.repo.v2.WriteAction;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * End-to-end integration test of the streaming HTTP upload route against
 * REAL infrastructure: testcontainers PostgreSQL + LocalStack S3, the full
 * service stack booted through {@link RepoServices}, and a real JDK
 * {@code HttpServer} on an ephemeral port driven by {@code java.net.http}.
 * The parts-based v2 handler owns the document route in every profile.
 */
@Testcontainers
class UploadHttpServerIT {

    private static final String ACCOUNT = "acct-http";
    private static final String DATASOURCE = "ds-http";
    private static final String DRIVE = "upload";
    private static final String OPERATOR_TOKEN = "synthetic-http-operator-key";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static UploadHttpServer http;
    static RepoServiceConfig config;
    static HttpClient client;

    @BeforeAll
    static void boot() {
        config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-http-docs",
                0, null, null, 0, 0L);
        services = RepoServices.build(config.withManagedStorage(new ManagedStoragePolicy("http-test-v1", "test-realm", true)));
        services.startInProcess("it-http");
        http = services.startHttp(0, "synthetic-http-operator-key", RepositoryConsole.notConfigured()); // ephemeral port
        channel = InProcessChannelBuilder.forName("it-http").build();
        DriveServiceGrpc.DriveServiceBlockingStub drives = DriveServiceGrpc.newBlockingStub(channel);
        drives.createDrive(CreateDriveRequest.newBuilder()
                .setName(DRIVE)
                .setAccountId(ACCOUNT)
                .build());
        client = HttpClient.newHttpClient();
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test void sameGenerationCannotBeReboundToAnotherStorageRealm() {
        assertThatThrownBy(() -> RepoServices.build(config.withManagedStorage(
                new ManagedStoragePolicy("http-test-v1", "different-realm", true))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("another physical profile");
    }

    @Test void defaultProfileServesUploadsThroughTheV2Handler() throws Exception {
        try (var disabled = RepoServices.build(config)) {
            var endpoint = disabled.startHttp(0, "synthetic-http-operator-key", RepositoryConsole.notConfigured());
            var body = new byte[] {1};
            String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
            var request = HttpRequest.newBuilder(URI.create("http://localhost:" + endpoint.port()
                    + UploadHttpServer.UPLOAD_PATH + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                    + "&drive=" + DRIVE + "&filename=disabled.bin&doc_id=disabled-upload"
                    + "&operation_id=op-disabled-upload"))
                    .header("api_token", "synthetic-http-operator-key")
                    .header("X-Content-Sha256", sha256)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            // The parts-based handler owns the route in the default profile.
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
            assertThat(response.body()).contains("\"doc_id\":\"disabled-upload\"", "\"revision\":1",
                    "\"replayed\":false", "\"sha256\":\"" + sha256 + "\"");
        }
        // A fresh cold host (empty v2Stacks cache) resolves the persisted stackId by drive primary key and reads back.
        try (var coldHost = RepoServices.build(config)) {
            coldHost.startInProcess("it-http-default-cold");
            var v2Channel = InProcessChannelBuilder.forName("it-http-default-cold").build();
            try {
                var readBack = ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(v2Channel)
                        .getDocument(ai.protomolt.proto.repo.v2.GetDocumentRequest.newBuilder()
                                .setAddress(ai.protomolt.proto.repo.v1.NodeAddress.newBuilder()
                                        .setAccountId(ACCOUNT).setDocId("disabled-upload")
                                        .setGraphId("intake:" + ACCOUNT).setGraphAddressId(DATASOURCE))
                                .build());
                assertThat(readBack.getManifest().getRevision()).isEqualTo(1);
            } finally {
                v2Channel.shutdownNow();
            }
        }
    }

    @Test
    void archiveUploadRouteStreamsMultiChunkRenditionThroughDocumentArchiveAndReplaysIdempotently() throws Exception {
        String archiveName = "contracts-v2";
        String entryId = "entry-http-v2-1";
        ArchiveCallerHandoff.callWithCaller(new RepositoryCaller("test-admin", true), null, () ->
                services.documentArchive().createArchive(CreateArchiveRequest.newBuilder()
                        .setArchive(Archive.newBuilder()
                                .setName(archiveName)
                                .setAccountId(ACCOUNT)
                                .setDriveName(DRIVE)
                                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)
                                .setDescription("v2 HTTP upload archive")
                                .build())
                        .build()));

        // Multi-MiB payload (~33 64-KiB chunks) exercising flow-controlled streaming.
        byte[] payload = new byte[2 * 1024 * 1024 + 321];
        new Random(42L).nextBytes(payload);
        String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        String queryPath = UploadHttpServer.ARCHIVE_UPLOAD_PATH
                + "?account_id=" + ACCOUNT
                + "&archive=" + archiveName
                + "&entry_id=" + entryId
                + "&rendition=original";
        URI uploadUri = URI.create("http://localhost:" + http.port() + queryPath);

        var unsupported = client.send(HttpRequest.newBuilder(URI.create(uploadUri + "&filename=ignored.pdf"))
                .header("api_token", OPERATOR_TOKEN).header("X-Content-Sha256", sha256)
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(unsupported.statusCode()).isEqualTo(400);
        assertThat(com.google.rpc.Status.parseFrom(unsupported.body()).getMessage()).contains("PutEntry");

        HttpRequest firstRequest = HttpRequest.newBuilder(uploadUri)
                .header("api_token", OPERATOR_TOKEN)
                .header("Content-Type", "application/pdf")
                .header("X-Content-Sha256", sha256)
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();
        HttpResponse<byte[]> firstResponse = client.send(firstRequest, HttpResponse.BodyHandlers.ofByteArray());
        assertThat(firstResponse.statusCode()).isEqualTo(200);
        assertThat(firstResponse.headers().firstValue("Content-Type")).contains("application/protobuf");
        assertThat(firstResponse.headers().firstValue("X-Replayed")).contains("false");

        UploadRenditionResponse firstProto = UploadRenditionResponse.parseFrom(firstResponse.body());
        assertThat(firstProto.getEntryUuid()).isEmpty();
        assertThat(firstProto.getVersion()).isEqualTo(1L);
        assertThat(firstProto.getSizeBytes()).isEqualTo(payload.length);
        assertThat(firstProto.getSha256()).isEqualTo(sha256);
        assertThat(firstProto.getRootChecksum()).isNotBlank();
        assertThat(firstProto.getReplayed()).isFalse();
        String derivedOperationId = firstProto.getOperationId();
        assertThat(derivedOperationId).isNotBlank();
        assertThat(firstResponse.headers().firstValue("X-Operation-Id")).contains(derivedOperationId);
        assertThat(UUID.fromString(derivedOperationId).version()).isEqualTo(5);

        // Read rendition bytes back through ReadRendition on DocumentArchive.
        byte[] readBack = readRenditionBytes(ACCOUNT, archiveName, entryId, "original");
        assertThat(readBack).isEqualTo(payload);

        // Retry the exact same HTTP request with X-Content-Sha256 and blank operation_id:
        // deterministic UUIDv5 operation_id replays the committed write at the header without
        // waiting for the request body (tested over a raw socket sending 0 body bytes).
        RawHttpResponse replayRaw = sendRawHttpHeadersWithoutBody(
                http.port(),
                queryPath,
                Map.of(
                        "api_token", OPERATOR_TOKEN,
                        "Content-Type", "application/pdf",
                        "X-Content-Sha256", sha256,
                        "Content-Length", Integer.toString(payload.length)),
                new byte[0],
                false);
        assertThat(replayRaw.statusCode()).as(replayRaw.bodyUtf8()).isEqualTo(200);
        assertThat(replayRaw.connectionHeader()).isEqualToIgnoringCase("close");

        UploadRenditionResponse retryProto = UploadRenditionResponse.parseFrom(replayRaw.bodyBytes());
        assertThat(retryProto.getEntryUuid()).isEqualTo(firstProto.getEntryUuid());
        assertThat(retryProto.getVersion()).isEqualTo(1L);
        assertThat(retryProto.getOperationId()).isEqualTo(derivedOperationId);
        assertThat(retryProto.getReplayed()).isTrue();
    }

    @Test
    void archiveUploadRejectsDigestMismatchTruncatedBodyAndExpectedVersionConflictWithoutCommitting() throws Exception {
        String archiveName = "failures-v2";
        ArchiveCallerHandoff.callWithCaller(new RepositoryCaller("test-admin", true), null, () ->
                services.documentArchive().createArchive(CreateArchiveRequest.newBuilder()
                        .setArchive(Archive.newBuilder()
                                .setName(archiveName)
                                .setAccountId(ACCOUNT)
                                .setDriveName(DRIVE)
                                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)
                                .setDescription("v2 failure tests archive")
                                .build())
                        .build()));

        // 1. Digest mismatch (X-Content-Sha256 does not match streamed bytes) -> 400 and no version committed.
        byte[] actualBytes = "actual rendition bytes".getBytes(StandardCharsets.UTF_8);
        String wrongSha256 = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest("wrong rendition bytes".getBytes(StandardCharsets.UTF_8)));
        URI mismatchUri = URI.create("http://localhost:" + http.port()
                + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                + "?account_id=" + ACCOUNT
                + "&archive=" + archiveName
                + "&entry_id=entry-mismatch"
                + "&operation_id=op-mismatch-1");
        HttpResponse<String> mismatchResponse = client.send(
                HttpRequest.newBuilder(mismatchUri)
                        .header("api_token", OPERATOR_TOKEN)
                        .header("Content-Type", "text/plain")
                        .header("X-Content-Sha256", wrongSha256)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(actualBytes))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(mismatchResponse.statusCode()).as(mismatchResponse.body()).isEqualTo(400);
        StatusRuntimeException mismatchLookup = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> ArchiveCallerHandoff.callWithCaller(
                        new RepositoryCaller("test-admin", true), null, () ->
                                services.documentArchive().getEntry(GetEntryRequest.newBuilder()
                                        .setAddress(EntryAddress.newBuilder()
                                                .setAccountId(ACCOUNT)
                                                .setArchive(archiveName)
                                                .setEntryId("entry-mismatch"))
                                        .build())));
        assertThat(mismatchLookup.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);

        // 2. Truncated body (fewer bytes sent than Content-Length before EOF) -> 400 and no version committed.
        byte[] partialBytes = "truncated".getBytes(StandardCharsets.UTF_8);
        RawHttpResponse truncatedResponse = sendRawHttpHeadersWithoutBody(
                http.port(),
                UploadHttpServer.ARCHIVE_UPLOAD_PATH
                        + "?account_id=" + ACCOUNT
                        + "&archive=" + archiveName
                        + "&entry_id=entry-truncated"
                        + "&operation_id=op-truncated-1",
                Map.of(
                        "api_token", OPERATOR_TOKEN,
                        "Content-Type", "text/plain",
                        "Content-Length", "4096"),
                partialBytes,
                true);
        assertThat(truncatedResponse.statusCode()).as(truncatedResponse.bodyUtf8()).isEqualTo(400);
        StatusRuntimeException truncatedLookup = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> ArchiveCallerHandoff.callWithCaller(
                        new RepositoryCaller("test-admin", true), null, () ->
                                services.documentArchive().getEntry(GetEntryRequest.newBuilder()
                                        .setAddress(EntryAddress.newBuilder()
                                                .setAccountId(ACCOUNT)
                                                .setArchive(archiveName)
                                                .setEntryId("entry-truncated"))
                                        .build())));
        assertThat(truncatedLookup.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);

        // 3. expected_version conflict -> 409 Conflict and no new version committed.
        String validSha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(actualBytes));
        URI conflictEntryUri = URI.create("http://localhost:" + http.port()
                + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                + "?account_id=" + ACCOUNT
                + "&archive=" + archiveName
                + "&entry_id=entry-conflict");
        HttpResponse<byte[]> v1Response = client.send(
                HttpRequest.newBuilder(conflictEntryUri)
                        .header("api_token", OPERATOR_TOKEN)
                        .header("Content-Type", "text/plain")
                        .header("X-Content-Sha256", validSha256)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(actualBytes))
                        .build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(v1Response.statusCode()).isEqualTo(200);
        assertThat(UploadRenditionResponse.parseFrom(v1Response.body()).getVersion()).isEqualTo(1L);

        byte[] v2Bytes = "conflicting rendition bytes".getBytes(StandardCharsets.UTF_8);
        String v2Sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(v2Bytes));
        URI wrongExpectedVersionUri = URI.create("http://localhost:" + http.port()
                + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                + "?account_id=" + ACCOUNT
                + "&archive=" + archiveName
                + "&entry_id=entry-conflict"
                + "&expected_version=99");
        HttpResponse<String> conflictResponse = client.send(
                HttpRequest.newBuilder(wrongExpectedVersionUri)
                        .header("api_token", OPERATOR_TOKEN)
                        .header("Content-Type", "text/plain")
                        .header("X-Content-Sha256", v2Sha256)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(v2Bytes))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(conflictResponse.statusCode()).as(conflictResponse.body()).isEqualTo(409);

        var stillV1 = ArchiveCallerHandoff.callWithCaller(
                new RepositoryCaller("test-admin", true), null, () ->
                        services.documentArchive().getEntry(GetEntryRequest.newBuilder()
                                .setAddress(EntryAddress.newBuilder()
                                        .setAccountId(ACCOUNT)
                                        .setArchive(archiveName)
                                        .setEntryId("entry-conflict"))
                                .build()));
        assertThat(stillV1.getManifest().getVersion()).isEqualTo(1L);
    }

    @Test
    void archiveUploadResolvesCallersThroughRequestCallerResolverAndEnforcesPermissions() throws Exception {
        String archiveName = "scoped-archive";
        try (var scopedHost = RepoServices.build(config)) {
            ArchiveCallerHandoff.callWithCaller(new RepositoryCaller("test-admin", true), null, () ->
                    scopedHost.documentArchive().createArchive(CreateArchiveRequest.newBuilder()
                            .setArchive(Archive.newBuilder()
                                    .setName(archiveName)
                                    .setAccountId(ACCOUNT)
                                    .setDriveName(DRIVE)
                                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)
                                    .setDescription("Scoped caller archive")
                                    .build())
                            .build()));

            AccountBinding aliceBinding = AccountBinding.newBuilder()
                    .setPrincipal("alice-writer")
                    .addAccountIds(ACCOUNT)
                    .addAclIdentities(ai.protomolt.proto.authz.AclIdentity.newBuilder()
                            .setIdentityType("user").setIdentity("alice").build())
                    .addIntakeWriters(IntakeWriterScope.newBuilder()
                            .setAccountId(ACCOUNT)
                            .setDatasourceId("ds-archive")
                            .setAllowCreate(true)
                            .build())
                    .build();
            AccountBinding charlieBinding = AccountBinding.newBuilder()
                    .setPrincipal("charlie-writer")
                    .addAccountIds(ACCOUNT)
                    .addAclIdentities(ai.protomolt.proto.authz.AclIdentity.newBuilder()
                            .setIdentityType("user").setIdentity("charlie").build())
                    .addIntakeWriters(IntakeWriterScope.newBuilder()
                            .setAccountId(ACCOUNT)
                            .setDatasourceId("ds-archive")
                            .setAllowCreate(true)
                            .build())
                    .build();
            AccountBinding noIdentityWriterBinding = AccountBinding.newBuilder()
                    .setPrincipal("noid-writer")
                    .addAccountIds(ACCOUNT)
                    .addIntakeWriters(IntakeWriterScope.newBuilder()
                            .setAccountId(ACCOUNT)
                            .setDatasourceId("ds-archive")
                            .setAllowCreate(true)
                            .build())
                    .build();
            AccountBinding bobBinding = AccountBinding.newBuilder()
                    .setPrincipal("bob-reader")
                    .addAccountIds(ACCOUNT)
                    .addAclIdentities(ai.protomolt.proto.authz.AclIdentity.newBuilder()
                            .setIdentityType("user").setIdentity("bob").build())
                    .build();
            AccountBindingResolver bindings = authCaller -> switch (authCaller.caller().name()) {
                case "alice-writer" -> Optional.of(aliceBinding);
                case "charlie-writer" -> Optional.of(charlieBinding);
                case "noid-writer" -> Optional.of(noIdentityWriterBinding);
                case "bob-reader" -> Optional.of(bobBinding);
                default -> Optional.empty();
            };
            scopedHost.withAccountBindings(bindings);

            RequestCallerResolver resolver = RequestCallerResolver.bearer(OPERATOR_TOKEN, token -> switch (token) {
                case "token-alice" -> Optional.of(Caller.scoped("alice-writer", Set.of()));
                case "token-charlie" -> Optional.of(Caller.scoped("charlie-writer", Set.of()));
                case "token-noid" -> Optional.of(Caller.scoped("noid-writer", Set.of()));
                case "token-bob" -> Optional.of(Caller.scoped("bob-reader", Set.of()));
                case "token-outage" ->
                        throw new IllegalStateException("credential store unreachable");
                default -> Optional.empty();
            });

            UploadHttpServer scopedHttp = scopedHost.startHttp(
                    0, OPERATOR_TOKEN, RepositoryConsole.notConfigured(), Map.of(), resolver);
            byte[] payload = "scoped caller rendition".getBytes(StandardCharsets.UTF_8);
            String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
            String queryPath = UploadHttpServer.ARCHIVE_UPLOAD_PATH
                    + "?account_id=" + ACCOUNT
                    + "&archive=" + archiveName
                    + "&entry_id=scoped-entry-1";
            URI uri = URI.create("http://localhost:" + scopedHttp.port() + queryPath);

            // 1. Unauthenticated request is refused with 401 before reading body.
            HttpResponse<String> unauthenticated = client.send(
                    HttpRequest.newBuilder(uri)
                            .header("Content-Type", "text/plain")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(unauthenticated.statusCode()).isEqualTo(401);

            // 2. Credential store outage returns 503.
            HttpResponse<String> outage = client.send(
                    HttpRequest.newBuilder(uri)
                            .header("Authorization", "Bearer token-outage")
                            .header("Content-Type", "text/plain")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(outage.statusCode()).isEqualTo(503);

            // 3. Scoped caller without write permission on the archive's datasource is refused with 403
            // immediately at header time without draining a 10 MiB body (0 body bytes sent on socket).
            RawHttpResponse forbiddenRaw = sendRawHttpHeadersWithoutBody(
                    scopedHttp.port(),
                    queryPath,
                    Map.of(
                            "Authorization", "Bearer token-bob",
                            "Content-Type", "text/plain",
                            "X-Content-Sha256", sha256,
                            "Content-Length", "10485760"),
                    new byte[0],
                    false);
            assertThat(forbiddenRaw.statusCode()).as(forbiddenRaw.bodyUtf8()).isEqualTo(403);
            assertThat(forbiddenRaw.connectionHeader()).isEqualToIgnoringCase("close");

            // 4. Scoped caller with intake writer permission succeeds with 200 and stamps its ACL identity.
            HttpResponse<byte[]> allowed = client.send(
                    HttpRequest.newBuilder(uri)
                            .header("Authorization", "Bearer token-alice")
                            .header("Content-Type", "text/plain")
                            .header("X-Content-Sha256", sha256)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(allowed.statusCode()).isEqualTo(200);
            assertThat(UploadRenditionResponse.parseFrom(allowed.body()).getVersion()).isEqualTo(1L);

            // 5. A second account member (charlie-writer) cannot read or overwrite alice's entry.
            AuthenticatedCaller charlieAuth = AuthenticatedCaller.unbound(Caller.scoped("charlie-writer", Set.of()));
            RepositoryCaller charlieRepoCaller = new RepositoryCaller(
                    "charlie-writer",
                    false,
                    Set.of(ACCOUNT),
                    Set.of(Principal.newBuilder().setIdentityType("user").setIdentity("charlie").build()));
            StatusRuntimeException charlieReadDenied = catchThrowableOfType(
                    StatusRuntimeException.class,
                    () -> ArchiveCallerHandoff.callWithCaller(charlieRepoCaller, charlieAuth, () ->
                            scopedHost.documentArchive().getEntry(GetEntryRequest.newBuilder()
                                    .setAddress(EntryAddress.newBuilder()
                                            .setAccountId(ACCOUNT)
                                            .setArchive(archiveName)
                                            .setEntryId("scoped-entry-1"))
                                    .build())));
            assertThat(charlieReadDenied.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

            byte[] charliePayload = "charlie overwrite attempt".getBytes(StandardCharsets.UTF_8);
            String charlieSha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(charliePayload));
            HttpResponse<String> charlieOverwrite = client.send(
                    HttpRequest.newBuilder(uri)
                            .header("Authorization", "Bearer token-charlie")
                            .header("Content-Type", "text/plain")
                            .header("X-Content-Sha256", charlieSha)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(charliePayload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(charlieOverwrite.statusCode()).as(charlieOverwrite.body()).isEqualTo(403);

            // 6. A scoped caller presenting no ACL identities (noid-writer) succeeds in uploading its
            // rendition (200 OK) while writing zero public rules: other account members (bob-reader
            // and charlie-writer) are denied read/write access on noid-entry-1.
            URI noIdUri = URI.create("http://localhost:" + scopedHttp.port()
                    + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                    + "?account_id=" + ACCOUNT
                    + "&archive=" + archiveName
                    + "&entry_id=noid-entry-1");
            HttpResponse<byte[]> noIdUpload = client.send(
                    HttpRequest.newBuilder(noIdUri)
                            .header("Authorization", "Bearer token-noid")
                            .header("Content-Type", "text/plain")
                            .header("X-Content-Sha256", sha256)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(noIdUpload.statusCode()).isEqualTo(200);
            assertThat(UploadRenditionResponse.parseFrom(noIdUpload.body()).getVersion()).isEqualTo(1L);
            AuthenticatedCaller bobAuth = AuthenticatedCaller.unbound(Caller.scoped("bob-reader", Set.of()));
            RepositoryCaller bobRepoCaller = new RepositoryCaller(
                    "bob-reader",
                    false,
                    Set.of(ACCOUNT),
                    Set.of(Principal.newBuilder().setIdentityType("user").setIdentity("bob").build()));
            StatusRuntimeException bobReadNoIdDenied = catchThrowableOfType(
                    StatusRuntimeException.class,
                    () -> ArchiveCallerHandoff.callWithCaller(bobRepoCaller, bobAuth, () ->
                            scopedHost.documentArchive().getEntry(GetEntryRequest.newBuilder()
                                    .setAddress(EntryAddress.newBuilder()
                                            .setAccountId(ACCOUNT)
                                            .setArchive(archiveName)
                                            .setEntryId("noid-entry-1"))
                                    .build())));
            assertThat(bobReadNoIdDenied.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            StatusRuntimeException charlieReadNoIdDenied = catchThrowableOfType(
                    StatusRuntimeException.class,
                    () -> ArchiveCallerHandoff.callWithCaller(charlieRepoCaller, charlieAuth, () ->
                            scopedHost.documentArchive().getEntry(GetEntryRequest.newBuilder()
                                    .setAddress(EntryAddress.newBuilder()
                                            .setAccountId(ACCOUNT)
                                            .setArchive(archiveName)
                                            .setEntryId("noid-entry-1"))
                                    .build())));
            assertThat(charlieReadNoIdDenied.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

            // 7. Archive definitions grant public READ only, never public WRITE: a scoped caller
            // (alice-writer) cannot overwrite the archive-def document via DocumentService.saveDocument.
            AuthenticatedCaller aliceAuth = scopedHttp.callers().resolve(ai.protomolt.proto.authz.RequestHeaders.of(
                    Map.of("Authorization", java.util.List.of("Bearer token-alice")))).orElseThrow();
            RepositoryCaller aliceRepoCaller = new RepositoryCaller(
                    "alice-writer",
                    false,
                    Set.of(ACCOUNT),
                    Set.of(Principal.newBuilder().setIdentityType("user").setIdentity("alice").build()));
            scopedHost.startInProcess("it-http-scoped-docs");
            ManagedChannel scopedDocsChannel = InProcessChannelBuilder.forName("it-http-scoped-docs")
                    .directExecutor()
                    .build();
            try {
                DocumentServiceGrpc.DocumentServiceBlockingStub docs = DocumentServiceGrpc.newBlockingStub(
                        new RenditionUploads(scopedHost.documentArchive()).channel());
                var definition = ArchiveCallerHandoff.callWithCaller(aliceRepoCaller, aliceAuth,
                        () -> docs.getDocument(GetDocumentRequest.newBuilder()
                                .setAddress(DocumentArchiveBindings.definitionAddress(ACCOUNT, archiveName)).build()));
                assertThat(definition.getDocument().getOwnership().getSecurity().getPermissionsList())
                        .isNotEmpty().allSatisfy(rule -> assertThat(rule.getAccess()).isEqualTo(Access.ACCESS_READ));
                StatusRuntimeException defOverwriteDenied = catchThrowableOfType(
                        StatusRuntimeException.class,
                        () -> ArchiveCallerHandoff.callWithCaller(aliceRepoCaller, aliceAuth, () ->
                                docs.saveDocument(SaveDocumentRequest.newBuilder()
                                        .setOperationId("def-overwrite-attempt-" + UUID.randomUUID())
                                        .setTarget(DocumentTarget.newBuilder()
                                                .setAddress(DocumentArchiveBindings.definitionAddress(ACCOUNT, archiveName)))
                                        .setExpected(ExpectedRevision.newBuilder().setRevision(1L))
                                        .setDocument(Document.newBuilder()
                                                .setDocId(archiveName)
                                                .setOwnership(OwnershipContext.newBuilder()
                                                        .setAccountId(ACCOUNT)
                                                        .setDatasourceId("ds-archive")))
                                        .addWrites(PartWrite.newBuilder()
                                                .setSelector(PartSelector.newBuilder()
                                                        .setPart(DocumentPart.DOCUMENT_PART_CORE))
                                                .setAction(WriteAction.WRITE_ACTION_PUT))
                                        .setOwner(SaveOwner.newBuilder()
                                                .setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-archive")))
                                        .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED)
                                        .setDrive("intake")
                                        .build())));
                assertThat(defOverwriteDenied.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            } finally {
                scopedDocsChannel.shutdownNow();
            }

            var entry = ArchiveCallerHandoff.callWithCaller(
                    new RepositoryCaller("test-admin", true), null, () ->
                            scopedHost.documentArchive().getEntry(GetEntryRequest.newBuilder()
                                    .setAddress(EntryAddress.newBuilder()
                                            .setAccountId(ACCOUNT)
                                            .setArchive(archiveName)
                                            .setEntryId("scoped-entry-1")
                                            .build())
                                    .build()));
            assertThat(entry.getManifest().getVersion()).isEqualTo(1L);
            assertThat(entry.getManifest().getRenditionsList()).singleElement().satisfies(rend ->
                    assertThat(rend.getSha256()).isEqualTo(sha256));
        }
    }

    private record RawHttpResponse(int statusCode, String connectionHeader, byte[] bodyBytes) {
        String bodyUtf8() {
            return new String(bodyBytes, StandardCharsets.UTF_8);
        }
    }

    private static RawHttpResponse sendRawHttpHeadersWithoutBody(
            int port,
            String pathAndQuery,
            Map<String, String> headers,
            byte[] partialBody,
            boolean shutdownOutputAfterPartialBody) throws Exception {
        try (Socket socket = new Socket("localhost", port)) {
            socket.setSoTimeout(5_000);
            OutputStream out = socket.getOutputStream();
            StringBuilder request = new StringBuilder();
            request.append("POST ").append(pathAndQuery).append(" HTTP/1.1\r\n");
            request.append("Host: localhost:").append(port).append("\r\n");
            for (Map.Entry<String, String> header : headers.entrySet()) {
                request.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
            }
            request.append("\r\n");
            out.write(request.toString().getBytes(StandardCharsets.US_ASCII));
            if (partialBody.length > 0) {
                out.write(partialBody);
            }
            out.flush();
            if (shutdownOutputAfterPartialBody) {
                socket.shutdownOutput();
            }

            InputStream in = socket.getInputStream();
            String statusLine = readAsciiLine(in);
            assertThat(statusLine).isNotNull();
            int firstSpace = statusLine.indexOf(' ');
            int secondSpace = statusLine.indexOf(' ', firstSpace + 1);
            int statusCode = Integer.parseInt(
                    secondSpace > firstSpace
                            ? statusLine.substring(firstSpace + 1, secondSpace)
                            : statusLine.substring(firstSpace + 1));
            String connectionHeader = "";
            int responseContentLength = 0;
            String line;
            while ((line = readAsciiLine(in)) != null && !line.isEmpty()) {
                int colon = line.indexOf(':');
                if (colon > 0) {
                    String name = line.substring(0, colon).trim();
                    String val = line.substring(colon + 1).trim();
                    if (name.equalsIgnoreCase("Connection")) {
                        connectionHeader = val;
                    } else if (name.equalsIgnoreCase("Content-Length")) {
                        responseContentLength = Integer.parseInt(val);
                    }
                }
            }
            byte[] bodyBytes = in.readNBytes(responseContentLength);
            return new RawHttpResponse(statusCode, connectionHeader, bodyBytes);
        }
    }

    private static String readAsciiLine(InputStream in) throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                byte[] bytes = buf.toByteArray();
                int len = bytes.length > 0 && bytes[bytes.length - 1] == '\r'
                        ? bytes.length - 1
                        : bytes.length;
                return new String(bytes, 0, len, StandardCharsets.US_ASCII);
            }
            buf.write(b);
        }
        return buf.size() == 0 ? null : buf.toString(StandardCharsets.US_ASCII);
    }

    private static byte[] readRenditionBytes(String accountId, String archive, String entryId, String rendition)
            throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        CompletableFuture<byte[]> done = new CompletableFuture<>();
        RenditionReads reader = new RenditionReads(services.documentArchive());
        ArchiveCallerHandoff.callWithCaller(new RepositoryCaller("test-reader", true), null, () -> {
            reader.read(
                    ReadRenditionRequest.newBuilder()
                            .setAddress(EntryAddress.newBuilder()
                                    .setAccountId(accountId)
                                    .setArchive(archive)
                                    .setEntryId(entryId)
                                    .build())
                            .setRendition(rendition)
                            .build(),
                    new StreamObserver<>() {
                        @Override
                        public void onNext(ReadRenditionResponse value) {
                            if (value.hasData()) {
                                try {
                                    value.getData().writeTo(buffer);
                                } catch (java.io.IOException e) {
                                    done.completeExceptionally(e);
                                }
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            done.completeExceptionally(t);
                        }

                        @Override
                        public void onCompleted() {
                            done.complete(buffer.toByteArray());
                        }
                    });
            return null;
        });
        return done.get();
    }
}
