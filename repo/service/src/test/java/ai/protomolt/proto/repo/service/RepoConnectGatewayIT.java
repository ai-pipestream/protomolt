package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.authz.RequestCallerResolver;
import ai.protomolt.proto.http.connect.ConnectGateway;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.ListDrivesRequest;
import ai.protomolt.proto.repo.v1.ListDrivesResponse;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetManifestRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadBlobResponse;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The console's Connect calls on the repository HTTP port, against PostgreSQL and LocalStack S3
 * and the production {@link RepoServices} composition: a document seeded through the upload
 * route is listed and its blob streamed over binary Connect, every one of the
 * {@link ConsoleProcedures} runs under the caller the request resolves to, and nothing outside
 * that list is reachable.
 */
@Testcontainers
class RepoConnectGatewayIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final String OPERATOR = "synthetic-connect-operator-token";
    private static final String ALICE = "synthetic-connect-alice-credential";
    private static final String CAROL = "synthetic-connect-carol-credential";
    private static final String ACCOUNT = "connect-account-a";
    private static final String DATASOURCE = "ds-connect";
    private static final String DRIVE = "intake";
    private static final String DOC = "connect-upload";
    private static final String BLOB = "original";
    private static final byte[] BODY = "bytes streamed through the Connect gateway".getBytes(StandardCharsets.UTF_8);

    static RepoServices services;
    static ManagedChannel channel;
    static String base;
    static final HttpClient CLIENT = HttpClient.newHttpClient();

    @BeforeAll
    static void boot() throws Exception {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-connect",
                0,
                null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("connect-it-original", "connect-it-realm", true));
        AccessPolicyCallers policy = new AccessPolicyCallers(AccessPolicy.newBuilder()
                .addPrincipals(principal("alice", ALICE))
                .addPrincipals(principal("carol", CAROL))
                .build());
        AccountBindings bindings = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice").addAccountIds(ACCOUNT))
                .build();
        services = RepoServices.build(config).withAccountBindings(AccountBindingResolver.of(bindings));
        int grpcPort = services.startNetty(0, OPERATOR, policy).getPort();
        UploadHttpServer http = services.startHttp(0, OPERATOR, RepositoryConsole.notConfigured(), Map.of(),
                RepoServices.requestCallers(OPERATOR, policy, Optional.empty()));
        base = "http://127.0.0.1:" + http.port();
        channel = NettyChannelBuilder.forAddress("localhost", grpcPort).usePlaintext().build();

        as(OPERATOR, DriveServiceGrpc.newBlockingStub(channel)).createDrive(CreateDriveRequest.newBuilder()
                .setName(DRIVE).setAccountId(ACCOUNT).setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
        as(OPERATOR, DriveServiceGrpc.newBlockingStub(channel)).createDrive(CreateDriveRequest.newBuilder()
                .setName("archive-drive").setAccountId(ACCOUNT).setDriveType(DriveType.DRIVE_TYPE_CUSTOM).build());
        as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(channel)).createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder().setName("docs").setAccountId(ACCOUNT).setDriveName("archive-drive")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
        as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(channel)).putEntry(PutEntryRequest.newBuilder()
                .setAddress(entry())
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original").setMediaType("text/plain"))
                        .setData(ByteString.copyFromUtf8("archived body")))
                .build());

        String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(BODY));
        HttpResponse<String> upload = CLIENT.send(HttpRequest.newBuilder(URI.create(base
                        + UploadHttpServer.UPLOAD_PATH + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                        + "&drive=" + DRIVE + "&filename=connect.txt&doc_id=" + DOC + "&blob_id=" + BLOB
                        + "&operation_id=op-" + DOC))
                .header("api_token", OPERATOR)
                .header("X-Content-Sha256", sha256)
                .POST(HttpRequest.BodyPublishers.ofByteArray(BODY)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(upload.statusCode()).as(upload.body()).isEqualTo(200);
    }

    @AfterAll
    static void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (services != null) services.close();
    }

    // ------------------------------------------------------------------ the console path

    @Test
    void aConnectClientListsTheSeededDocumentAndStreamsItsBlob() throws Exception {
        HttpResponse<byte[]> listed = unary(OPERATOR, DocumentServiceGrpc.getListDocumentsMethod().getFullMethodName(),
                ListDocumentsRequest.newBuilder().setAccountId(ACCOUNT).setPageSize(100).build());
        assertThat(listed.statusCode()).as(text(listed)).isEqualTo(200);
        assertThat(listed.headers().firstValue("Content-Type")).hasValue(ConnectGateway.UNARY_CONTENT_TYPE);
        assertThat(ListDocumentsResponse.parseFrom(listed.body()).getDocumentsList())
                .extracting(summary -> summary.getAddress().getDocId()).contains(DOC);

        HttpResponse<byte[]> streamed = stream(OPERATOR, DocumentServiceGrpc.getReadBlobMethod().getFullMethodName(),
                ReadBlobRequest.newBuilder().setAddress(address()).setBlobId(BLOB).build());
        assertThat(streamed.statusCode()).isEqualTo(200);
        assertThat(streamed.headers().firstValue("Content-Type")).hasValue(ConnectGateway.STREAM_CONTENT_TYPE);
        List<byte[]> frames = frames(streamed.body());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (byte[] frame : frames.subList(0, frames.size() - 1)) {
            ReadBlobResponse response = ReadBlobResponse.parseFrom(frame);
            if (response.hasData()) {
                bytes.writeBytes(response.getData().toByteArray());
            }
        }
        assertThat(bytes.toByteArray()).isEqualTo(BODY);
        assertThat(new String(frames.getLast(), StandardCharsets.UTF_8)).isEqualTo("{}");
    }

    @Test
    void aScopedCallerSeesItsAccountThroughTheGatewayAsOverGrpc() throws Exception {
        HttpResponse<byte[]> drives = unary(ALICE, DriveServiceGrpc.getListDrivesMethod().getFullMethodName(),
                ListDrivesRequest.newBuilder().setAccountId(ACCOUNT).build());
        assertThat(drives.statusCode()).as(text(drives)).isEqualTo(200);
        assertThat(ListDrivesResponse.parseFrom(drives.body()).getDrivesList()).isNotEmpty();

        HttpResponse<byte[]> otherAccount = unary(ALICE, DriveServiceGrpc.getListDrivesMethod().getFullMethodName(),
                ListDrivesRequest.newBuilder().setAccountId("connect-account-b").build());
        assertThat(otherAccount.statusCode()).isEqualTo(403);
        assertThat(text(otherAccount)).contains("\"code\":\"permission_denied\"");

        HttpResponse<byte[]> anonymous = unary(null, DriveServiceGrpc.getListDrivesMethod().getFullMethodName(),
                ListDrivesRequest.newBuilder().setAccountId(ACCOUNT).build());
        assertThat(anonymous.statusCode()).isEqualTo(401);
        assertThat(text(anonymous)).contains("\"code\":\"unauthenticated\"");
    }

    @Test
    void everyConsoleProcedureRunsUnderTheResolvedCaller() throws Exception {
        Map<String, Message> requests = consoleRequests();
        assertThat(requests.keySet()).isEqualTo(ConsoleProcedures.ALL);
        Set<String> streaming = Set.of(
                DocumentServiceGrpc.getReadDocumentMethod().getFullMethodName(),
                DocumentServiceGrpc.getReadBlobMethod().getFullMethodName(),
                ArchiveServiceGrpc.getReadRenditionMethod().getFullMethodName());
        for (Map.Entry<String, Message> call : requests.entrySet()) {
            String procedure = call.getKey();
            // carol authenticates but is bound to no account: every listed call reaches its
            // service, which refuses her by account rather than the gateway refusing the request.
            if (streaming.contains(procedure)) {
                List<byte[]> frames = frames(stream(CAROL, procedure, call.getValue()).body());
                assertThat(new String(frames.getLast(), StandardCharsets.UTF_8)).as(procedure)
                        .contains("\"code\":\"permission_denied\"", "bound to no account");
            } else {
                HttpResponse<byte[]> refused = unary(CAROL, procedure, call.getValue());
                assertThat(refused.statusCode()).as(procedure + " " + text(refused)).isEqualTo(403);
                assertThat(text(refused)).as(procedure).contains("bound to no account");
            }
        }
    }

    @Test
    void aMethodOutsideTheConsoleListIsUnreachableEvenForTheOperator() throws Exception {
        for (String procedure : List.of(
                DocumentServiceGrpc.getPurgeDocumentMethod().getFullMethodName(),
                DriveServiceGrpc.getCreateDriveMethod().getFullMethodName(),
                ArchiveServiceGrpc.getPutEntryMethod().getFullMethodName())) {
            HttpResponse<byte[]> refused = unary(OPERATOR, procedure, ListDrivesRequest.getDefaultInstance());
            assertThat(refused.statusCode()).as(procedure).isEqualTo(404);
            assertThat(text(refused)).contains("\"code\":\"unimplemented\"", procedure);
        }
    }

    // ------------------------------------------------------------------ fixtures

    private static Map<String, Message> consoleRequests() {
        Map<String, Message> requests = new LinkedHashMap<>();
        requests.put(DocumentServiceGrpc.getListDocumentsMethod().getFullMethodName(),
                ListDocumentsRequest.newBuilder().setAccountId(ACCOUNT).setPageSize(10).build());
        requests.put(DocumentServiceGrpc.getGetManifestMethod().getFullMethodName(),
                GetManifestRequest.newBuilder().setAddress(address()).build());
        requests.put(DocumentServiceGrpc.getGetDocumentMethod().getFullMethodName(),
                GetDocumentRequest.newBuilder().setAddress(address()).build());
        requests.put(DocumentServiceGrpc.getListRevisionsMethod().getFullMethodName(),
                ListRevisionsRequest.newBuilder().setAddress(address()).setPageSize(10).build());
        requests.put(DocumentServiceGrpc.getReadDocumentMethod().getFullMethodName(),
                ReadDocumentRequest.newBuilder().setAddress(address()).build());
        requests.put(DocumentServiceGrpc.getGetSchemaDescriptorMethod().getFullMethodName(),
                GetSchemaDescriptorRequest.newBuilder().setAddress(address())
                        .setDescriptorSha256("0".repeat(64)).build());
        requests.put(DocumentServiceGrpc.getReadBlobMethod().getFullMethodName(),
                ReadBlobRequest.newBuilder().setAddress(address()).setBlobId(BLOB).build());
        requests.put(DocumentServiceGrpc.getGetOperationOutcomeMethod().getFullMethodName(),
                GetOperationOutcomeRequest.newBuilder().setAccountId(ACCOUNT).setOperationId("op-" + DOC).build());
        requests.put(DocumentServiceGrpc.getDeleteDocumentMethod().getFullMethodName(),
                DeleteDocumentRequest.newBuilder().setOperationId("op-delete-" + DOC).setAddress(address())
                        .setExpected(ExpectedRevision.newBuilder().setRevision(1)).build());
        requests.put(DocumentServiceGrpc.getListLinksMethod().getFullMethodName(),
                ai.protomolt.proto.repo.v2.ListLinksRequest.newBuilder().setAddress(address()).build());
        requests.put(DocumentServiceGrpc.getResolveLinkMethod().getFullMethodName(),
                ai.protomolt.proto.repo.v2.ResolveLinkRequest.newBuilder().setAddress(address())
                        .setFieldPath("structured_data.related").setPageSize(10).build());
        requests.put(DocumentServiceGrpc.getListBacklinksMethod().getFullMethodName(),
                ai.protomolt.proto.repo.v2.ListBacklinksRequest.newBuilder().setTarget(address()).setPageSize(10).build());
        requests.put(DriveServiceGrpc.getListDrivesMethod().getFullMethodName(),
                ListDrivesRequest.newBuilder().setAccountId(ACCOUNT).build());
        requests.put(ArchiveServiceGrpc.getListArchivesMethod().getFullMethodName(),
                ListArchivesRequest.newBuilder().setAccountId(ACCOUNT).setLimit(10).build());
        requests.put(ArchiveServiceGrpc.getGetArchiveMethod().getFullMethodName(),
                GetArchiveRequest.newBuilder().setAccountId(ACCOUNT).setArchive("docs").build());
        requests.put(ArchiveServiceGrpc.getGetArchiveStatsMethod().getFullMethodName(),
                GetArchiveStatsRequest.newBuilder().setAccountId(ACCOUNT).setArchive("docs").build());
        requests.put(ArchiveServiceGrpc.getListEntriesMethod().getFullMethodName(),
                ListEntriesRequest.newBuilder().setAccountId(ACCOUNT).setArchive("docs").setLimit(10).build());
        requests.put(ArchiveServiceGrpc.getGetEntryManifestMethod().getFullMethodName(),
                GetEntryManifestRequest.newBuilder().setAddress(entry()).build());
        requests.put(ArchiveServiceGrpc.getListVersionsMethod().getFullMethodName(),
                ListVersionsRequest.newBuilder().setAddress(entry()).setLimit(10).build());
        requests.put(ArchiveServiceGrpc.getReadRenditionMethod().getFullMethodName(),
                ReadRenditionRequest.newBuilder().setAddress(entry()).setRendition("original").build());
        return requests;
    }

    private static Principal principal(String name, String credential) {
        return Principal.newBuilder().setName(name)
                .addCredentialSha256(AccessPolicyCallers.sha256Hex(credential))
                .addScopes("search-query").build();
    }

    private static <S extends AbstractStub<S>> S as(String credential, S stub) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), credential);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static NodeAddress address() {
        return NodeAddress.newBuilder().setAccountId(ACCOUNT).setDocId(DOC)
                .setGraphId("intake:" + ACCOUNT).setGraphAddressId(DATASOURCE).build();
    }

    private static EntryAddress entry() {
        return EntryAddress.newBuilder().setAccountId(ACCOUNT).setArchive("docs").setEntryId("entry-1").build();
    }

    // ------------------------------------------------------------------ a minimal Connect client

    private static HttpResponse<byte[]> unary(String credential, String procedure, Message request) throws Exception {
        return post(credential, procedure, ConnectGateway.UNARY_CONTENT_TYPE, request.toByteArray());
    }

    private static HttpResponse<byte[]> stream(String credential, String procedure, Message request) throws Exception {
        byte[] message = request.toByteArray();
        ByteArrayOutputStream envelope = new ByteArrayOutputStream();
        envelope.write(0);
        envelope.write(message.length >>> 24);
        envelope.write(message.length >>> 16);
        envelope.write(message.length >>> 8);
        envelope.write(message.length);
        envelope.writeBytes(message);
        return post(credential, procedure, ConnectGateway.STREAM_CONTENT_TYPE, envelope.toByteArray());
    }

    private static HttpResponse<byte[]> post(String credential, String procedure, String contentType, byte[] body)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + ConnectGateway.PATH + "/" + procedure))
                .header("Content-Type", contentType)
                .header("Connect-Protocol-Version", "1")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (credential != null) {
            request.header("Authorization", "Bearer " + credential);
        }
        return CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    /** Envelope payloads in order; the last is the end-of-stream record. */
    private static List<byte[]> frames(byte[] body) throws IOException {
        List<byte[]> frames = new ArrayList<>();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
        while (in.available() > 0) {
            in.readUnsignedByte();
            byte[] payload = new byte[in.readInt()];
            in.readFully(payload);
            frames.add(payload);
        }
        return frames;
    }

    private static String text(HttpResponse<byte[]> response) {
        return new String(response.body(), StandardCharsets.UTF_8);
    }
}
