package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AclIdentity;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.GetDriveRequest;
import ai.protomolt.proto.repo.v1.ListDrivesRequest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.StreamedBody;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import ai.protomolt.proto.repo.v2.UploadHeader;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Scoped callers on the repository host's TCP listener, end to end against real
 * infrastructure (PostgreSQL + LocalStack S3, the production {@link RepoServices}
 * composition, the production credential interceptor, no mocks). An access policy
 * authenticates three principals; account bindings bind {@code alice} to account A with
 * an ACL identity, {@code bob} to account B, and leave {@code carol} unbound. The
 * operator seeds drives, archives and documents in both accounts, then each principal
 * reads through the document, drive and archive services: its own account answers,
 * every other account refuses by name, and writes stay process authority.
 */
@Testcontainers
class ScopedCallersIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final String OPERATOR = "synthetic-operator-token";
    private static final String ALICE = "synthetic-alice-credential";
    private static final String BOB = "synthetic-bob-credential";
    private static final String CAROL = "synthetic-carol-credential";
    private static final String ACCOUNT_A = "scoped-account-a";
    private static final String ACCOUNT_B = "scoped-account-b";
    private static final String DATASOURCE = "ds-scoped";
    private static final String ALICE_UPN = "alice@example.com";

    static RepoServices services;
    static ManagedChannel channel;
    static String driveOfB;

    @BeforeAll
    static void boot() {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-scoped",
                0,
                null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("scoped-it-original", "scoped-it-realm", true));
        var policy = AccessPolicy.newBuilder()
                .addPrincipals(principal("alice", ALICE))
                .addPrincipals(principal("bob", BOB))
                .addPrincipals(principal("carol", CAROL))
                .build();
        var bindings = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice").addAccountIds(ACCOUNT_A)
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("user-principal-name").setIdentity(ALICE_UPN)))
                .addBindings(AccountBinding.newBuilder().setPrincipal("bob").addAccountIds(ACCOUNT_B))
                .build();
        services = RepoServices.build(config).withAccountBindings(AccountBindingResolver.of(bindings));
        int port = services.startNetty(0, OPERATOR, new AccessPolicyCallers(policy)).getPort();
        channel = NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build();

        var operatorDrives = as(OPERATOR, DriveServiceGrpc.newBlockingStub(channel));
        var operatorArchives = as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(channel));
        var operatorDocuments = as(OPERATOR, DocumentServiceGrpc.newBlockingStub(channel));
        for (String account : List.of(ACCOUNT_A, ACCOUNT_B)) {
            operatorDrives.createDrive(CreateDriveRequest.newBuilder().setName("intake")
                    .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
            operatorDrives.createDrive(CreateDriveRequest.newBuilder().setName("archive-drive")
                    .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_CUSTOM).build());
            operatorArchives.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setName("docs").setAccountId(account).setDriveName("archive-drive")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            operatorArchives.putEntry(PutEntryRequest.newBuilder().setAddress(entry(account))
                    .addRenditions(rendition("original", "text/plain", "body of " + account)).build());
        }
        // Account membership alone is not an entry grant. Give Alice explicit READ
        // access to the seeded entry so the positive archive assertions exercise ACLs.
        setEntryReader(entry(ACCOUNT_A), ALICE_UPN);
        driveOfB = operatorDrives.getDrive(GetDriveRequest.newBuilder()
                .setAccountId(ACCOUNT_B).setName("intake").build()).getDrive().getDriveId();
        // Account A: one document alice's ACL identity may read, one only an
        // identity she does not hold may read. Account B: one public document.
        operatorDocuments.saveDocument(save(ACCOUNT_A, "doc-for-alice",
                rule("user-principal-name", ALICE_UPN)));
        operatorDocuments.saveDocument(save(ACCOUNT_A, "doc-for-someone-else",
                rule("user-principal-name", "dave@example.com")));
        operatorDocuments.saveDocument(save(ACCOUNT_B, "doc-public-in-b", rule("public", "public")));
    }

    @AfterAll
    static void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (services != null) services.close();
    }

    // ------------------------------------------------------------------
    // Authentication
    // ------------------------------------------------------------------

    @Test
    void aMissingCredentialIsUnauthenticated() {
        assertCode(() -> DriveServiceGrpc.newBlockingStub(channel).listDrives(drives(ACCOUNT_A)),
                Status.Code.UNAUTHENTICATED);
    }

    @Test
    void anUnknownCredentialIsUnauthenticatedEverywhereIncludingHealth() {
        String unknown = "synthetic-unknown-credential";
        assertCode(() -> as(unknown, DriveServiceGrpc.newBlockingStub(channel)).listDrives(drives(ACCOUNT_A)),
                Status.Code.UNAUTHENTICATED);
        assertCode(() -> as(unknown, DocumentServiceGrpc.newBlockingStub(channel)).listDocuments(list(ACCOUNT_A)),
                Status.Code.UNAUTHENTICATED);
        assertCode(() -> as(unknown, HealthGrpc.newBlockingStub(channel)).check(HealthCheckRequest.getDefaultInstance()),
                Status.Code.UNAUTHENTICATED);
    }

    @Test
    void anUnboundPrincipalAuthenticatesButReachesNoAccount() {
        assertThat(as(CAROL, HealthGrpc.newBlockingStub(channel)).check(HealthCheckRequest.getDefaultInstance())
                .getStatus().name()).isEqualTo("SERVING");
        assertDenied(() -> as(CAROL, DriveServiceGrpc.newBlockingStub(channel)).listDrives(drives(ACCOUNT_A)),
                "The caller is bound to no account");
        assertDenied(() -> as(CAROL, ArchiveServiceGrpc.newBlockingStub(channel)).listArchives(
                ListArchivesRequest.newBuilder().setAccountId(ACCOUNT_A).build()), "The caller is bound to no account");
        assertDenied(() -> as(CAROL, DocumentServiceGrpc.newBlockingStub(channel)).listDocuments(list(ACCOUNT_A)),
                "The caller is bound to no account");
    }

    // ------------------------------------------------------------------
    // Documents
    // ------------------------------------------------------------------

    @Test
    void documentListingsShowOnlyTheBoundAccountRowsTheCallersIdentitiesMayRead() {
        var alice = as(ALICE, DocumentServiceGrpc.newBlockingStub(channel));
        assertThat(alice.listDocuments(list(ACCOUNT_A)).getDocumentsList())
                .extracting(DocumentSummary::getAddress).extracting(NodeAddress::getDocId)
                .containsExactlyInAnyOrder("doc-for-alice", "entry-1");
        assertThat(alice.getDocument(get(ACCOUNT_A, "doc-for-alice")).getDocument().getSearchMetadata().getTitle())
                .isEqualTo("doc-for-alice");
        assertDenied(() -> alice.listDocuments(list(ACCOUNT_B)),
                "The caller is not bound to account '" + ACCOUNT_B + "'");
        assertDenied(() -> alice.getDocument(get(ACCOUNT_B, "doc-public-in-b")),
                "The caller is not bound to account '" + ACCOUNT_B + "'");
    }

    @Test
    void aPublicDocumentStaysInsideItsAccount() {
        var bob = as(BOB, DocumentServiceGrpc.newBlockingStub(channel));
        assertThat(bob.listDocuments(list(ACCOUNT_B)).getDocumentsList())
                .extracting(DocumentSummary::getAddress).extracting(NodeAddress::getDocId)
                .containsExactly("doc-public-in-b");
        assertDenied(() -> bob.getDocument(get(ACCOUNT_A, "doc-for-alice")),
                "The caller is not bound to account '" + ACCOUNT_A + "'");
    }

    @Test
    void aBoundCallerWithoutAWriterGrantCannotSave() {
        var alice = as(ALICE, DocumentServiceGrpc.newBlockingStub(channel));
        assertCode(() -> alice.saveDocument(save(ACCOUNT_A, "doc-by-alice", rule("public", "public"))),
                Status.Code.PERMISSION_DENIED);
    }

    // ------------------------------------------------------------------
    // Drives
    // ------------------------------------------------------------------

    @Test
    void driveReadsAnswerForTheBoundAccountOnly() {
        var alice = as(ALICE, DriveServiceGrpc.newBlockingStub(channel));
        assertThat(alice.listDrives(drives(ACCOUNT_A)).getDrivesList())
                .extracting(drive -> drive.getAccountId() + "/" + drive.getName())
                .containsExactlyInAnyOrder(ACCOUNT_A + "/archive-drive", ACCOUNT_A + "/intake");
        assertThat(alice.getDrive(GetDriveRequest.newBuilder().setAccountId(ACCOUNT_A).setName("intake").build())
                .getDrive().getAccountId()).isEqualTo(ACCOUNT_A);
        assertDenied(() -> alice.listDrives(drives(ACCOUNT_B)),
                "The caller is not bound to account '" + ACCOUNT_B + "'");
        assertDenied(() -> alice.getDrive(GetDriveRequest.newBuilder().setAccountId(ACCOUNT_B).setName("intake").build()),
                "The caller is not bound to account '" + ACCOUNT_B + "'");
    }

    @Test
    void aDriveIdOutsideTheBoundAccountsAnswersAsMissing() {
        var alice = as(ALICE, DriveServiceGrpc.newBlockingStub(channel));
        var missing = UUID.randomUUID().toString();
        String unboundMessage = statusOf(() -> alice.getDrive(GetDriveRequest.newBuilder().setDriveId(driveOfB).build()))
                .getDescription();
        String missingMessage = statusOf(() -> alice.getDrive(GetDriveRequest.newBuilder().setDriveId(missing).build()))
                .getDescription();
        assertCode(() -> alice.getDrive(GetDriveRequest.newBuilder().setDriveId(driveOfB).build()), Status.Code.NOT_FOUND);
        assertThat(unboundMessage).isEqualTo(missingMessage.replace(missing, driveOfB));
        assertThat(as(BOB, DriveServiceGrpc.newBlockingStub(channel))
                .getDrive(GetDriveRequest.newBuilder().setDriveId(driveOfB).build()).getDrive().getAccountId())
                .isEqualTo(ACCOUNT_B);
    }

    @Test
    void driveWritesStayProcessAuthority() {
        assertDenied(() -> as(ALICE, DriveServiceGrpc.newBlockingStub(channel)).createDrive(CreateDriveRequest.newBuilder()
                        .setName("alice-drive").setAccountId(ACCOUNT_A).setDriveType(DriveType.DRIVE_TYPE_CUSTOM).build()),
                "Repository account policy is not configured for this principal");
    }

    // ------------------------------------------------------------------
    // Archives
    // ------------------------------------------------------------------

    @Test
    void everyArchiveReadAnswersForTheBoundAccount() {
        var alice = as(ALICE, ArchiveServiceGrpc.newBlockingStub(channel));
        assertThat(alice.getArchive(GetArchiveRequest.newBuilder().setAccountId(ACCOUNT_A).setArchive("docs").build())
                .getArchive().getAccountId()).isEqualTo(ACCOUNT_A);
        assertThat(alice.listArchives(ListArchivesRequest.newBuilder().setAccountId(ACCOUNT_A).build()).getArchivesList())
                .extracting(Archive::getName).containsExactly("docs");
        assertThat(alice.getArchiveStats(GetArchiveStatsRequest.newBuilder().setAccountId(ACCOUNT_A).setArchive("docs").build())
                .getStats().getEntries()).isEqualTo(1);
        assertThat(alice.listEntries(ListEntriesRequest.newBuilder().setAccountId(ACCOUNT_A).setArchive("docs").build())
                .getEntriesCount()).isEqualTo(1);
        assertThat(alice.getEntry(GetEntryRequest.newBuilder().setAddress(entry(ACCOUNT_A)).build())
                .getRenditions(0).getData().toStringUtf8()).isEqualTo("body of " + ACCOUNT_A);
        assertThat(alice.getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(entry(ACCOUNT_A)).build())
                .getManifest().getVersion()).isEqualTo(2);
        assertThat(alice.listVersions(ListVersionsRequest.newBuilder().setAddress(entry(ACCOUNT_A)).build())
                .getSummariesCount()).isEqualTo(2);
    }

    @Test
    void everyArchiveReadRefusesAnotherAccountByName() {
        var alice = as(ALICE, ArchiveServiceGrpc.newBlockingStub(channel));
        String refusal = "The caller is not bound to account '" + ACCOUNT_B + "'";
        for (ThrowingCallable read : List.<ThrowingCallable>of(
                () -> alice.getArchive(GetArchiveRequest.newBuilder().setAccountId(ACCOUNT_B).setArchive("docs").build()),
                () -> alice.listArchives(ListArchivesRequest.newBuilder().setAccountId(ACCOUNT_B).build()),
                () -> alice.getArchiveStats(GetArchiveStatsRequest.newBuilder().setAccountId(ACCOUNT_B).setArchive("docs").build()),
                () -> alice.listEntries(ListEntriesRequest.newBuilder().setAccountId(ACCOUNT_B).setArchive("docs").build()),
                () -> alice.getEntry(GetEntryRequest.newBuilder().setAddress(entry(ACCOUNT_B)).build()),
                () -> alice.getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(entry(ACCOUNT_B)).build()),
                () -> alice.listVersions(ListVersionsRequest.newBuilder().setAddress(entry(ACCOUNT_B)).build()))) {
            assertDenied(read, refusal);
        }
    }

    @Test
    void archiveWritesStayProcessAuthority() {
        var alice = as(ALICE, ArchiveServiceGrpc.newBlockingStub(channel));
        assertDenied(() -> alice.putEntry(PutEntryRequest.newBuilder().setAddress(entry(ACCOUNT_A))
                        .addRenditions(rendition("original", "text/plain", "alice was here")).build()),
                "Repository account policy is not configured for this principal");
        assertDenied(() -> alice.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                        .setName("alice-archive").setAccountId(ACCOUNT_A).setDriveName("archive-drive")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build()),
                "Repository account policy is not configured for this principal");
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static Principal principal(String name, String credential) {
        return Principal.newBuilder().setName(name)
                .addCredentialSha256(AccessPolicyCallers.sha256Hex(credential))
                .addScopes("search-query").build();
    }

    // ------------------------------------------------------------------
    // Uploads: identity comes from the transport credential, and every
    // refusal is the first answer, before a byte moves
    // ------------------------------------------------------------------

    @Test
    void anAnonymousUploadIsRefusedInTheFirstAnswer() throws Exception {
        var upload = new Upload(DocumentServiceGrpc.newStub(channel));
        upload.send(header(ACCOUNT_A, "doc-anonymous-upload", intakeOwner()));
        assertThat(upload.failure().getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
        assertThat(upload.answers).isEmpty();
    }

    @Test
    void aScopedCallerWithoutWriterAuthorityIsRefusedInTheFirstAnswer() throws Exception {
        var upload = new Upload(as(ALICE, DocumentServiceGrpc.newStub(channel)));
        upload.send(header(ACCOUNT_A, "doc-alice-upload", intakeOwner()));
        assertThat(upload.failure().getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(upload.answers).isEmpty();
    }

    @Test
    void aScopedCallerCannotUploadIntoAnAccountItIsNotBoundTo() throws Exception {
        var upload = new Upload(as(ALICE, DocumentServiceGrpc.newStub(channel)));
        upload.send(header(ACCOUNT_B, "doc-alice-into-b", intakeOwner()));
        assertThat(upload.failure().getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(upload.answers).isEmpty();
    }

    @Test
    void theOperatorCredentialAsABearerTokenUploadsOnlyAsAnExplicitIntakeOwner() throws Exception {
        var bearer = new Metadata();
        bearer.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + OPERATOR);
        var operator = DocumentServiceGrpc.newStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearer));
        // Its own account, so the listing assertions above stay exact.
        String account = "scoped-account-upload";
        as(OPERATOR, DriveServiceGrpc.newBlockingStub(channel)).createDrive(CreateDriveRequest.newBuilder()
                .setName("intake").setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());

        var stepOwned = new Upload(operator);
        stepOwned.send(header(account, "doc-operator-step", SaveOwner.newBuilder()
                .setStep(ai.protomolt.proto.repo.v1.WriteProvenance.newBuilder().setModuleId("module-1")).build()));
        assertThat(stepOwned.failure().getStatus().getDescription())
                .isEqualTo("Process authority may save only as an intake owner");
        assertThat(stepOwned.answers).isEmpty();

        var upload = new Upload(operator);
        upload.send(header(account, "doc-operator-upload", intakeOwner()));
        assertThat(upload.answer().getAnswerCase()).isEqualTo(UploadDocumentResponse.AnswerCase.ACCEPTED);
        upload.send(UploadDocumentRequest.newBuilder().setData(ByteString.copyFromUtf8(UPLOAD_BODY)).build());
        upload.complete();
        var saved = upload.answer();
        assertThat(saved.getAnswerCase()).isEqualTo(UploadDocumentResponse.AnswerCase.SAVED);
        assertThat(saved.getSaved().getAccession().getRevision()).isEqualTo(1L);
    }

    private static final String UPLOAD_BODY = "uploaded body";

    private static SaveOwner intakeOwner() {
        return SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId(DATASOURCE)).build();
    }

    private static UploadDocumentRequest header(String account, String doc, SaveOwner owner) {
        var save = save(account, doc, rule("public", "public")).toBuilder()
                .setOperationId("op-upload-" + doc)
                .setOwner(owner)
                .addWrites(PartWrite.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS))
                        .setAction(WriteAction.WRITE_ACTION_PUT));
        save.getDocumentBuilder().setBlobBag(ai.protomolt.proto.repo.v1.BlobBag.newBuilder()
                .setBlob(ai.protomolt.proto.repo.v1.Blob.newBuilder().setBlobId("b1")
                        .setMimeType("text/plain").setFilename("b1.txt")));
        return UploadDocumentRequest.newBuilder().setHeader(UploadHeader.newBuilder()
                .setStreamed(StreamedBody.getDefaultInstance())
                .setSave(save)
                .setBlobId("b1")
                .setDeclaredLength(UPLOAD_BODY.length())).build();
    }

    /** One UploadDocument call driven a frame at a time, its answers read in order. */
    private static final class Upload {
        final java.util.concurrent.LinkedBlockingQueue<UploadDocumentResponse> answers =
                new java.util.concurrent.LinkedBlockingQueue<>();
        private final java.util.concurrent.CompletableFuture<Void> end = new java.util.concurrent.CompletableFuture<>();
        private final StreamObserver<UploadDocumentRequest> requests;

        Upload(DocumentServiceGrpc.DocumentServiceStub stub) {
            requests = stub.uploadDocument(new StreamObserver<>() {
                @Override public void onNext(UploadDocumentResponse value) { answers.add(value); }
                @Override public void onError(Throwable t) { end.completeExceptionally(t); }
                @Override public void onCompleted() { end.complete(null); }
            });
        }

        void send(UploadDocumentRequest frame) { requests.onNext(frame); }

        void complete() { requests.onCompleted(); }

        UploadDocumentResponse answer() throws Exception {
            var answer = answers.poll(30, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(answer).as("an UploadDocument answer").isNotNull();
            return answer;
        }

        StatusRuntimeException failure() {
            var thrown = org.assertj.core.api.Assertions.catchThrowable(
                    () -> end.get(30, java.util.concurrent.TimeUnit.SECONDS));
            assertThat(thrown).isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(thrown.getCause()).isInstanceOf(StatusRuntimeException.class);
            return (StatusRuntimeException) thrown.getCause();
        }
    }

    private static <S extends AbstractStub<S>> S as(String credential, S stub) {
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), credential);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static ListDrivesRequest drives(String account) {
        return ListDrivesRequest.newBuilder().setAccountId(account).build();
    }

    private static ListDocumentsRequest list(String account) {
        return ListDocumentsRequest.newBuilder().setAccountId(account).setPageSize(100).build();
    }

    private static NodeAddress address(String account, String doc) {
        return NodeAddress.newBuilder().setAccountId(account).setDocId(doc)
                .setGraphId("intake:" + account).setGraphAddressId(DATASOURCE).build();
    }

    private static GetDocumentRequest get(String account, String doc) {
        return GetDocumentRequest.newBuilder().setAddress(address(account, doc)).build();
    }

    @Test
    void archiveReadsPreserveDocumentAcl() {
        var target = entry(ACCOUNT_A).toBuilder().setEntryId("codex-private-archive").build();
        var operatorArchives = as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(channel));
        operatorArchives.putEntry(PutEntryRequest.newBuilder().setAddress(target)
                .addRenditions(rendition("original", "text/plain", "private archive bytes")).build());
        setEntryReader(target, "dave@example.com");
        var node = ai.protomolt.proto.repo.archive.ArchiveCoordinates.address(target);
        assertCode(() -> as(ALICE, DocumentServiceGrpc.newBlockingStub(channel)).getDocument(
                GetDocumentRequest.newBuilder().setAddress(node).build()), Status.Code.PERMISSION_DENIED);
        assertCode(() -> as(ALICE, ArchiveServiceGrpc.newBlockingStub(channel)).getEntry(
                GetEntryRequest.newBuilder().setAddress(target).build()), Status.Code.PERMISSION_DENIED);
        var alice = as(ALICE, ArchiveServiceGrpc.newBlockingStub(channel));
        assertCode(() -> alice.getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(target).build()),
                Status.Code.PERMISSION_DENIED);
        assertCode(() -> alice.listVersions(ListVersionsRequest.newBuilder().setAddress(target).build()),
                Status.Code.PERMISSION_DENIED);
        var renditionRead = ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest.newBuilder()
                .setAddress(target).setRendition("original").build();
        assertCode(() -> alice.readRendition(renditionRead).hasNext(), Status.Code.PERMISSION_DENIED);
        assertThat(alice.listEntries(ListEntriesRequest.newBuilder().setAccountId(ACCOUNT_A)
                .setArchive("docs").build()).getEntriesList())
                .noneMatch(info -> info.getAddress().getEntryId().equals(target.getEntryId()));
        var repository = services.archiveRepository();
        var spiAlice = new ai.protomolt.proto.repo.spi.RepositoryCaller("embedded-reader", false,
                java.util.Set.of(ACCOUNT_A), java.util.Set.of(ai.protomolt.proto.repo.v1.Principal.newBuilder()
                        .setIdentityType("user-principal-name").setIdentity(ALICE_UPN).build()));
        assertThatThrownBy(() -> repository.getEntry(spiAlice,
                GetEntryRequest.newBuilder().setAddress(target).build()))
                .isInstanceOf(ai.protomolt.proto.repo.spi.RepositoryException.class)
                .extracting(t -> ((ai.protomolt.proto.repo.spi.RepositoryException) t).code())
                .isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.PERMISSION_DENIED);
        assertThatThrownBy(() -> repository.readRendition(spiAlice, renditionRead))
                .isInstanceOf(ai.protomolt.proto.repo.spi.RepositoryException.class);
        // This caller is supplied directly by the trusted SPI host, with no configured
        // principal binding: its explicit identity must survive the internal RPC hop.
        assertThat(repository.getEntry(spiAlice, GetEntryRequest.newBuilder()
                .setAddress(entry(ACCOUNT_A)).build()).getRenditions(0).getData().toStringUtf8())
                .isEqualTo("body of " + ACCOUNT_A);
    }

    @Test
    void spiUploadPreservesSuppliedFilename() throws Exception {
        var target = entry(ACCOUNT_A).toBuilder().setEntryId("codex-filename").build();
        var operator = new ai.protomolt.proto.repo.spi.RepositoryCaller("operator", true);
        var repository = services.archiveRepository();
        var descriptor = RenditionDescriptor.newBuilder().setName("original").setMediaType("text/plain").build();
        String sha = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        var first = repository.uploadStream(operator, target, descriptor, 3L, sha,
                null, "kept.txt", null, null, new java.io.ByteArrayInputStream(new byte[] {'a', 'b', 'c'}));
        var read = repository.getEntry(operator, GetEntryRequest.newBuilder().setAddress(target).build());
        assertThat(read.getInfo().getFilename()).isEqualTo("kept.txt");
        var replay = repository.uploadStream(operator, target, descriptor, 3L, sha,
                null, "kept.txt", null, null, new java.io.ByteArrayInputStream(new byte[] {'a', 'b', 'c'}));
        assertThat(replay.version()).isEqualTo(first.version());
        assertThat(replay.deduplicated()).isTrue();
        var renamed = repository.uploadStream(operator, target, descriptor, 3L, sha,
                null, "renamed.txt", null, null, new java.io.ByteArrayInputStream(new byte[] {'a', 'b', 'c'}));
        assertThat(renamed.version()).isGreaterThan(first.version());
        assertThat(repository.getEntry(operator, GetEntryRequest.newBuilder().setAddress(target).build())
                .getInfo().getFilename()).isEqualTo("renamed.txt");
        for (String invalidName : List.of("x".repeat(501))) {
            assertThatThrownBy(() -> repository.uploadStream(operator, target, descriptor, 3L, sha,
                    null, invalidName, null, null, new java.io.ByteArrayInputStream(new byte[] {'a', 'b', 'c'})))
                    .isInstanceOf(ai.protomolt.proto.repo.spi.RepositoryException.class)
                    .hasMessageContaining("filename");
        }
        assertThatThrownBy(() -> repository.uploadStream(operator, target,
                descriptor.toBuilder().setName("thumbnail").build(), 3L, sha,
                null, "thumb.png", null, null, new java.io.ByteArrayInputStream(new byte[] {'a', 'b', 'c'})))
                .isInstanceOf(ai.protomolt.proto.repo.spi.RepositoryException.class)
                .hasMessageContaining("original rendition");
    }

    private static void setEntryReader(EntryAddress target, String identity) {
        var operatorDocuments = as(OPERATOR, DocumentServiceGrpc.newBlockingStub(channel));
        var node = ai.protomolt.proto.repo.archive.ArchiveCoordinates.address(target);
        var current = operatorDocuments.getDocument(GetDocumentRequest.newBuilder().setAddress(node)
                .setSelection(ai.protomolt.proto.repo.v2.PartSelection.newBuilder()
                        .addSelectors(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))).build());
        var restricted = current.getDocument().toBuilder();
        restricted.getOwnershipBuilder().setSecurity(DocumentSecurity.newBuilder()
                .addPermissions(rule("user-principal-name", identity)));
        operatorDocuments.saveDocument(SaveDocumentRequest.newBuilder().setOperationId("entry-acl-" + UUID.randomUUID())
                .setTarget(DocumentTarget.newBuilder().setAddress(node))
                .setExpected(ExpectedRevision.newBuilder().setRevision(current.getManifest().getRevision()))
                .setDocument(restricted)
                .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)).setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder()
                        .setDatasourceId(current.getDocument().getOwnership().getDatasourceId())))
                .setDrive("archive-drive").build());
    }

    private static EntryAddress entry(String account) {
        return EntryAddress.newBuilder().setAccountId(account).setArchive("docs").setEntryId("entry-1").build();
    }

    private static RenditionContent rendition(String name, String mediaType, String body) {
        return RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName(name).setMediaType(mediaType))
                .setData(ByteString.copyFromUtf8(body)).build();
    }

    private static AccessRule rule(String identityType, String identity) {
        return AccessRule.newBuilder().setIdentityType(identityType).setIdentity(identity)
                .setAccess(Access.ACCESS_READ).build();
    }

    private static SaveDocumentRequest save(String account, String doc, AccessRule readRule) {
        var document = Document.newBuilder().setDocId(doc)
                .setOwnership(OwnershipContext.newBuilder().setAccountId(account).setDatasourceId(DATASOURCE)
                        .setSecurity(DocumentSecurity.newBuilder().addPermissions(readRule)))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(doc).setBody("body of " + doc))
                .build();
        return SaveDocumentRequest.newBuilder().setOperationId("op-" + doc)
                .setTarget(DocumentTarget.newBuilder().setAddress(address(account, doc)))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(document)
                .addWrites(PartWrite.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId(DATASOURCE)))
                .build();
    }

    private static Status statusOf(Supplier<?> call) {
        try {
            call.get();
        } catch (StatusRuntimeException failure) {
            return failure.getStatus();
        }
        throw new AssertionError("the call was expected to fail");
    }

    private static void assertCode(ThrowingCallable call, Status.Code code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(code));
    }

    private static void assertDenied(ThrowingCallable call, String message) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(failure.getStatus().getDescription()).isEqualTo(message);
        });
    }
}
