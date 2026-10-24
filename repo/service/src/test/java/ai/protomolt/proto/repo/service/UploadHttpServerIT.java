package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.authz.RequestCallerResolver;
import ai.protomolt.proto.repo.archive.RenditionReads;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    private static final ObjectMapper JSON = new ObjectMapper();

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
    void archiveUploadRouteStreamsRenditionThroughDocumentArchiveAndReplaysIdempotently() throws Exception {
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

        byte[] payload = "archive rendition streamed over HTTP into DocumentArchive".getBytes(StandardCharsets.UTF_8);
        String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        URI uploadUri = URI.create("http://localhost:" + http.port()
                + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                + "?account_id=" + ACCOUNT
                + "&archive=" + archiveName
                + "&entry_id=" + entryId
                + "&rendition=original"
                + "&filename=contract.pdf"
                + "&title=Master+Agreement"
                + "&source_uri=https://example.test/contract.pdf");

        HttpRequest firstRequest = HttpRequest.newBuilder(uploadUri)
                .header("api_token", OPERATOR_TOKEN)
                .header("Content-Type", "application/pdf")
                .header("X-Content-Sha256", sha256)
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();
        HttpResponse<String> firstResponse = client.send(firstRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(firstResponse.statusCode()).as(firstResponse.body()).isEqualTo(200);

        JsonNode firstJson = JSON.readTree(firstResponse.body());
        assertThat(firstJson.has("entry_uuid")).isTrue();
        assertThat(firstJson.path("entry_uuid").asText()).isEmpty();
        assertThat(firstJson.path("version").asLong()).isEqualTo(1L);
        assertThat(firstJson.path("rendition").asText()).isEqualTo("original");
        assertThat(firstJson.path("size_bytes").asLong()).isEqualTo(payload.length);
        assertThat(firstJson.path("sha256").asText()).isEqualTo(sha256);
        assertThat(firstJson.path("root_checksum").asText()).isNotBlank();
        assertThat(firstJson.path("deduplicated").asBoolean()).isFalse();
        assertThat(firstJson.path("replayed").asBoolean()).isFalse();
        String derivedOperationId = firstJson.path("operation_id").asText();
        assertThat(derivedOperationId).isNotBlank();
        assertThat(UUID.fromString(derivedOperationId).version()).isEqualTo(5);

        // Read rendition bytes back through ReadRendition on DocumentArchive.
        byte[] readBack = readRenditionBytes(ACCOUNT, archiveName, entryId, "original");
        assertThat(readBack).isEqualTo(payload);

        // Retry the exact same HTTP request with X-Content-Sha256 and blank operation_id:
        // deterministic UUIDv5 operation_id replays the committed write without creating version 2.
        HttpRequest retryRequest = HttpRequest.newBuilder(uploadUri)
                .header("api_token", OPERATOR_TOKEN)
                .header("Content-Type", "application/pdf")
                .header("X-Content-Sha256", sha256)
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();
        HttpResponse<String> retryResponse = client.send(retryRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(retryResponse.statusCode()).as(retryResponse.body()).isEqualTo(200);

        JsonNode retryJson = JSON.readTree(retryResponse.body());
        assertThat(retryJson.path("entry_uuid").asText()).isEqualTo(firstJson.path("entry_uuid").asText());
        assertThat(retryJson.path("version").asLong()).isEqualTo(1L);
        assertThat(retryJson.path("operation_id").asText()).isEqualTo(derivedOperationId);
        assertThat(retryJson.path("replayed").asBoolean()).isTrue();
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
            AccountBinding bobBinding = AccountBinding.newBuilder()
                    .setPrincipal("bob-reader")
                    .addAccountIds(ACCOUNT)
                    .addAclIdentities(ai.protomolt.proto.authz.AclIdentity.newBuilder()
                            .setIdentityType("user").setIdentity("bob").build())
                    .build();
            AccountBindingResolver bindings = authCaller -> switch (authCaller.caller().name()) {
                case "alice-writer" -> Optional.of(aliceBinding);
                case "bob-reader" -> Optional.of(bobBinding);
                default -> Optional.empty();
            };
            scopedHost.withAccountBindings(bindings);

            RequestCallerResolver resolver = RequestCallerResolver.bearer(OPERATOR_TOKEN, token -> switch (token) {
                case "token-alice" -> Optional.of(Caller.scoped("alice-writer", Set.of()));
                case "token-bob" -> Optional.of(Caller.scoped("bob-reader", Set.of()));
                case "token-outage" ->
                        throw new IllegalStateException("credential store unreachable");
                default -> Optional.empty();
            });

            UploadHttpServer scopedHttp = scopedHost.startHttp(
                    0, OPERATOR_TOKEN, RepositoryConsole.notConfigured(), Map.of(), resolver);
            byte[] payload = "scoped caller rendition".getBytes(StandardCharsets.UTF_8);
            String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
            URI uri = URI.create("http://localhost:" + scopedHttp.port()
                    + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                    + "?account_id=" + ACCOUNT
                    + "&archive=" + archiveName
                    + "&entry_id=scoped-entry-1"
                    + "&filename=scoped.txt");

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

            // 3. Scoped caller without write permission on the archive's datasource is refused with 403.
            HttpResponse<String> forbidden = client.send(
                    HttpRequest.newBuilder(uri)
                            .header("Authorization", "Bearer token-bob")
                            .header("Content-Type", "text/plain")
                            .header("X-Content-Sha256", sha256)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(forbidden.statusCode()).as(forbidden.body()).isEqualTo(403);

            // 4. Scoped caller with intake writer permission succeeds with 200 and records its grant evidence.
            HttpResponse<String> allowed = client.send(
                    HttpRequest.newBuilder(uri)
                            .header("Authorization", "Bearer token-alice")
                            .header("Content-Type", "text/plain")
                            .header("X-Content-Sha256", sha256)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(allowed.statusCode()).as(allowed.body()).isEqualTo(200);

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
