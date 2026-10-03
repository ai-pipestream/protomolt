package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.DeleteBlobResponse;
import ai.protomolt.proto.repo.v1.DeleteDocumentByReferenceCommand;
import ai.protomolt.proto.repo.v1.DeleteDocumentOutcome;
import ai.protomolt.proto.repo.v1.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v1.DeleteDocumentResponse;
import ai.protomolt.proto.repo.v1.DeleteLogicalDocumentCommand;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.Drive;
import ai.protomolt.proto.repo.v1.DriveProviderConfig;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.RedisDriveConfig;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.protomolt.proto.repo.v1.GetDocumentManifestRequest;
import ai.protomolt.proto.repo.v1.GetDocumentManifestResponse;
import ai.protomolt.proto.repo.v1.GetDocumentRequest;
import ai.protomolt.proto.repo.v1.GetDocumentResponse;
import ai.protomolt.proto.repo.v1.GetDriveRequest;
import ai.protomolt.proto.repo.v1.ListDocumentsRequest;
import ai.protomolt.proto.repo.v1.ListDocumentsResponse;
import ai.protomolt.proto.repo.v1.ListDrivesRequest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import ai.protomolt.proto.repo.v1.SaveDocumentRequest;
import ai.protomolt.proto.repo.v1.SaveDocumentResponse;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.SemanticChunk;
import ai.protomolt.proto.repo.v1.SemanticProcessingResult;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentStatus;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.stub.MetadataUtils;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration test of repo-service against REAL infrastructure:
 * shared testcontainers PostgreSQL 17 (Flyway-migrated ledger) and LocalStack
 * S3 (part objects), with the full service stack booted through
 * {@link RepoServiceConfig} + {@link RepoServices} over the gRPC in-process
 * transport — proving the same-JVM embedding path, with no mocks anywhere.
 */
@Testcontainers(disabledWithoutDocker = true)
class RepoServiceIT {

    private static final String CONNECTOR = "connector-1";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static DriveServiceGrpc.DriveServiceBlockingStub drives;

    @BeforeAll
    static void boot() throws Exception {
        RepoServiceConfig config = new RepoServiceConfig(
                0, // unused on the in-process transport
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-docs",
                0, // HTTP upload route not exercised by this IT
                null, null, null, null, 0, 0L); // blob store: the default direct-S3 path
        services = RepoServices.build(config);
        services.startInProcess("it");
        channel = InProcessChannelBuilder.forName("it").build();
        documents = DocumentServiceGrpc.newBlockingStub(channel);
        drives = DriveServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test
    void libraryAndGrpcShareDriveProvisioningAndPagination() {
        var local = services.driveRepository();
        var caller = new ai.protomolt.proto.repo.spi.RepositoryCaller("drive-test", true);
        var create = CreateDriveRequest.newBuilder().setAccountId("acct-drive-shared")
                .setName("first").setDriveType(DriveType.DRIVE_TYPE_CUSTOM)
                .putMetadata("purpose", "shared-library").build();
        var first = local.createDrive(caller, create);
        assertThat(drives.createDrive(create)).isEqualTo(first);
        var second = create.toBuilder().setName("second").build();
        assertThat(local.createDrive(caller, second)).isEqualTo(drives.createDrive(second));
        var byId = GetDriveRequest.newBuilder().setDriveId(first.getDrive().getDriveId()).build();
        assertThat(local.getDrive(caller, byId)).isEqualTo(drives.getDrive(byId));
        var byName = GetDriveRequest.newBuilder().setAccountId("acct-drive-shared").setName("first").build();
        assertThat(local.getDrive(caller, byName)).isEqualTo(drives.getDrive(byName));
        var list = ListDrivesRequest.newBuilder().setAccountId("acct-drive-shared").setLimit(1).build();
        var page = local.listDrives(caller, list);
        assertThat(page).isEqualTo(drives.listDrives(list));
        assertThat(page.getDrivesList()).extracting(Drive::getName).containsExactly("first");
        var next = list.toBuilder().setContinuationToken(page.getNextContinuationToken()).build();
        assertThat(local.listDrives(caller, next)).isEqualTo(drives.listDrives(next));
        assertThat(local.listDrives(caller, next).getDrivesList())
                .extracting(Drive::getName).containsExactly("second");
    }

    @Test
    void scopedListingsCountAndPageOnlyVisibleDocuments() throws Exception {
        String account = "acct-list-policy";
        createDrive("list-policy", account);
        var reader = new ai.protomolt.proto.repo.spi.RepositoryCaller("login", false, Set.of(account),
                Set.of(ai.protomolt.proto.repo.v1.Principal.newBuilder()
                        .setIdentityType("user-principal-name").setIdentity("alice").build()));
        for (int i = 0; i < 5; i++) {
            var doc = fixture("list-policy-" + i, account, "source").toBuilder();
            var policy = doc.getOwnershipBuilder().getSecurityBuilder().setInheritanceEnabled(false);
            if (i % 2 == 0) policy.addPermissions(AccessRule.newBuilder()
                    .setIdentityType("user-principal-name").setIdentity("alice").setAccess(Access.ACCESS_DENY));
            documents.saveDocument(intakeSave(doc.build(), "list-policy", account).build());
        }
        String endpoint = "list-policy-" + UUID.randomUUID();
        var server = policyServer(endpoint, authenticated -> reader);
        var connection = InProcessChannelBuilder.forName(endpoint).build();
        try {
            var remote = DocumentServiceGrpc.newBlockingStub(connection);
            var request = ListDocumentsRequest.newBuilder().setLimit(1).build();
            var first = services.repository().listDocuments(reader, request);
            assertThat(remote.listDocuments(request)).isEqualTo(first);
            assertThat(first.getTotalCount()).isEqualTo(2);
            assertThat(first.getDocumentsList()).extracting(ai.protomolt.proto.repo.v1.DocumentMetadata::getDocId)
                    .containsExactly("list-policy-1");
            assertThat(first.getNextContinuationToken()).isEqualTo("1");
            var second = services.repository().listDocuments(reader,
                    request.toBuilder().setContinuationToken(first.getNextContinuationToken()).build());
            assertThat(second.getTotalCount()).isEqualTo(2);
            assertThat(remote.listDocuments(request.toBuilder().setContinuationToken("1").build())).isEqualTo(second);
            assertThat(second.getDocumentsList()).extracting(ai.protomolt.proto.repo.v1.DocumentMetadata::getDocId)
                    .containsExactly("list-policy-3");
            assertThat(second.getNextContinuationToken()).isEmpty();
            var other = services.repository().listDocuments(reader, request.toBuilder().setAccountId("unbound").build());
            assertThat(other.getDocumentsList()).isEmpty();
            assertThat(other.getTotalCount()).isZero();
            assertThat(remote.listDocuments(request.toBuilder().setAccountId("unbound").build())).isEqualTo(other);
            var pastEnd = request.toBuilder().setContinuationToken("999").build();
            assertThat(remote.listDocuments(pastEnd).getDocumentsList()).isEmpty();
            assertThat(remote.listDocuments(pastEnd).getTotalCount()).isEqualTo(2);
            var row = services.documentLedger().findByNodeId(UUID.fromString(first.getDocuments(0).getNodeId())).orElseThrow();
            row.writeSecurity(DocumentSecurity.getDefaultInstance());
            services.documentLedger().save(row);
            var revoked = remote.listDocuments(request);
            assertThat(revoked).isEqualTo(services.repository().listDocuments(reader, request));
            assertThat(revoked.getTotalCount()).isEqualTo(1);
            assertThat(revoked.getDocumentsList()).extracting(ai.protomolt.proto.repo.v1.DocumentMetadata::getDocId)
                    .containsExactly("list-policy-3");
            assertThat(revoked.getNextContinuationToken()).isEmpty();
            // A malformed policy cannot be skipped just because it falls before the requested visible offset.
            row.security = "{\"unknownPolicy\":true}";
            services.documentLedger().save(row);
            assertThatThrownBy(() -> remote.listDocuments(pastEnd)).satisfies(error ->
                    assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        } finally {
            connection.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Test
    void rewritingADocumentDoesNotOverwriteObjectsInItsPreviousManifest() {
        String account = "acct-write-isolation";
        var drive = createDrive("write-isolation", account);
        var original = fixture("write-isolation-doc", account, "source");
        var saved = documents.saveDocument(intakeSave(original, "write-isolation", account).build());
        var oldManifest = documents.getDocumentManifest(GetDocumentManifestRequest.newBuilder()
                .setNodeId(saved.getNodeId()).build()).getManifest();
        var oldBytes = new java.util.HashMap<String, byte[]>();
        for (var part : oldManifest.getPartsList()) {
            if (part.getState() == PartState.PART_STATE_PRESENT)
                oldBytes.put(part.getObjectKey(), services.blobStore().get(drive.getBucket(), part.getObjectKey()).data());
        }
        var replacement = original.toBuilder();
        replacement.getSearchMetadataBuilder().setTitle("replacement");
        documents.saveDocument(intakeSave(replacement.build(), "write-isolation", account).build());
        for (var part : oldBytes.entrySet()) {
            assertThat(services.blobStore().get(drive.getBucket(), part.getKey()).data()).isEqualTo(part.getValue());
        }
        assertThat(documents.getDocument(GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build())
                .getDocument()).isEqualTo(replacement.build());
        var beforePartial = documents.getDocumentManifest(GetDocumentManifestRequest.newBuilder()
                .setNodeId(saved.getNodeId()).build()).getManifest();
        var beforePartialBytes = new java.util.HashMap<String, byte[]>();
        for (var part : beforePartial.getPartsList()) {
            if (part.getState() == PartState.PART_STATE_PRESENT)
                beforePartialBytes.put(part.getObjectKey(), services.blobStore().get(drive.getBucket(), part.getObjectKey()).data());
        }
        replacement.getBlobBagBuilder().getBlobBuilder().setData(ByteString.copyFromUtf8("new partial bytes"));
        documents.saveDocument(intakeSave(replacement.build(), "write-isolation", account)
                .addPartsWritten(DocumentPart.DOCUMENT_PART_BLOBS)
                .setCopyUnwrittenPartsFrom(saved.getAddress()).build());
        for (var part : beforePartialBytes.entrySet())
            assertThat(services.blobStore().get(drive.getBucket(), part.getKey()).data()).isEqualTo(part.getValue());
        var afterPartial = documents.getDocument(GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build());
        assertThat(afterPartial.getDocument()).isEqualTo(replacement.build());
        var core = afterPartial.getManifest().getPartsList().stream()
                .filter(part -> part.getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
        var storedCore = services.blobStore().get(drive.getBucket(), core.getObjectKey());
        var row = services.documentLedger().findByNodeId(UUID.fromString(saved.getNodeId())).orElseThrow();
        assertThat(row.etag).isEqualTo(storedCore.eTag());
        assertThat(row.versionId).isEqualTo(storedCore.versionId());
    }

    @Test
    void policyEditDuringObjectWritesRejectsCandidateWithoutChangingVisibleBody() throws Exception {
        String account = "acct-commit-policy";
        createDrive("commit-policy", account);
        var original = fixture("commit-policy-doc", account, "source");
        var saved = documents.saveDocument(intakeSave(original, "commit-policy", account).build());
        var before = services.documentLedger().findByNodeId(UUID.fromString(saved.getNodeId())).orElseThrow();
        var injected = new java.util.concurrent.atomic.AtomicBoolean();
        var real = services.blobStore();
        // Every operation reaches the real S3 adapter. Inject one real SQL policy
        // change after a PUT succeeds, while the candidate is still being prepared.
        var store = (ai.protomolt.proto.repo.blob.spi.BlobStore) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {ai.protomolt.proto.repo.blob.spi.BlobStore.class},
                (proxy, method, args) -> {
                    Object result;
                    try { result = method.invoke(real, args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    if (method.getName().equals("put") && injected.compareAndSet(false, true)) {
                        var row = services.documentLedger().findByNodeId(before.nodeId).orElseThrow();
                        row.writeSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                                .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_DENY)).build());
                        services.documentLedger().save(row);
                    }
                    return result;
                });
        var engine = new ai.protomolt.proto.repo.engine.DocumentOperations(services.documentLedger(), services.driveLedger(),
                null, store, new ai.protomolt.proto.repo.container.blob.PartStorage(), null);
        String endpoint = "commit-policy-" + UUID.randomUUID();
        var server = io.grpc.inprocess.InProcessServerBuilder.forName(endpoint).addService(new DocumentGrpcService(engine,
                new ai.protomolt.proto.repo.engine.BlobOperations(store, services.driveLedger()))).build().start();
        var connection = InProcessChannelBuilder.forName(endpoint).build();
        try {
            var candidate = original.toBuilder();
            candidate.getSearchMetadataBuilder().setTitle("must not publish");
            assertThatThrownBy(() -> DocumentServiceGrpc.newBlockingStub(connection)
                    .saveDocument(intakeSave(candidate.build(), "commit-policy", account).build()))
                    .satisfies(error -> assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.ABORTED));
            assertThat(injected).isTrue();
            var after = services.documentLedger().findByNodeId(before.nodeId).orElseThrow();
            assertThat(after.checksum).isEqualTo(before.checksum);
            assertThat(after.partManifest).isEqualTo(before.partManifest);
            assertThat(after.readSecurity().getPermissions(0).getAccess()).isEqualTo(Access.ACCESS_DENY);
            assertThat(documents.getDocument(GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build())
                    .getDocument()).isEqualTo(original);
        } finally {
            connection.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Test
    void boundDocumentReaderUsesCurrentAclAndCannotCrossAccounts() throws Exception {
        String account = "acct-document-policy";
        createDrive("policy", account);
        var doc = fixture("policy-doc", account, "source").toBuilder();
        doc.getOwnershipBuilder().getSecurityBuilder().setInheritanceEnabled(false);
        var saved = documents.saveDocument(intakeSave(doc.build(), "policy", account).build());
        var reader = new ai.protomolt.proto.repo.spi.RepositoryCaller("login", false, Set.of(account),
                Set.of(ai.protomolt.proto.repo.v1.Principal.newBuilder()
                        .setIdentityType("user-principal-name").setIdentity("ALICE").build()));
        var binding = new java.util.concurrent.atomic.AtomicReference<>(reader);
        var row = services.documentLedger().findByNodeId(UUID.fromString(saved.getNodeId())).orElseThrow();
        var originalPolicy = row.readSecurity();
        var request = GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build();
        var reference = GetDocumentByReferenceRequest.newBuilder().setAddress(address(
                row.docId, row.graphAddressId, row.accountId, row.graphId)).build();
        var manifest = GetDocumentManifestRequest.newBuilder().setNodeId(saved.getNodeId()).build();
        String endpoint = "policy-" + UUID.randomUUID();
        var server = policyServer(endpoint, authenticated -> binding.get());
        var connection = InProcessChannelBuilder.forName(endpoint).build();
        try {
            var remote = DocumentServiceGrpc.newBlockingStub(connection).withDeadlineAfter(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(services.repository().getDocument(reader, request).getDocument()).isEqualTo(doc.build());
            assertThat(remote.getDocument(request).getDocument()).isEqualTo(doc.build());
            assertThat(remote.getDocumentByReference(reference).getDocument()).isEqualTo(doc.build());
            assertThat(services.repository().getDocumentByReference(reader, reference).getDocument()).isEqualTo(doc.build());
        assertThat(services.repository().getDocumentManifest(reader, manifest)).isEqualTo(remote.getDocumentManifest(manifest));
        for (var invalidBinding : List.of(new ai.protomolt.proto.repo.spi.RepositoryCaller("other-name", false),
                new ai.protomolt.proto.repo.spi.RepositoryCaller("login", true))) {
            binding.set(invalidBinding);
            assertThatThrownBy(() -> remote.getDocument(request)).satisfies(error ->
                    assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED));
        }
        binding.set(null);
        assertThatThrownBy(() -> remote.getDocument(request)).satisfies(error ->
                assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED));
        binding.set(reader);
            // Membership cannot be substituted with a matching ACL identity.
            binding.set(new ai.protomolt.proto.repo.spi.RepositoryCaller("login", false,
                    Set.of("different-account"), reader.identities()));
            assertThatThrownBy(() -> remote.getDocument(request)).satisfies(error ->
                    assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.NOT_FOUND));
            assertThatThrownBy(() -> services.repository().getDocument(binding.get(), request))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            error -> assertThat(error.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.NOT_FOUND));
        binding.set(reader);
        for (var policy : List.of(DocumentSecurity.getDefaultInstance(),
                originalPolicy.toBuilder().setPermissions(0, originalPolicy.getPermissions(0).toBuilder()
                        .setAccess(Access.ACCESS_WRITE)).build())) {
            row.writeSecurity(policy);
            services.documentLedger().save(row);
            assertThatThrownBy(() -> remote.getDocument(request)).satisfies(error ->
                    assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.NOT_FOUND));
        }
        row.writeSecurity(null);
        services.documentLedger().save(row);
        assertThatThrownBy(() -> remote.getDocument(request)).satisfies(error ->
                assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.NOT_FOUND));
        // Scoped callers cannot treat unresolved inheritance as an empty parent policy.
        row.writeSecurity(originalPolicy.toBuilder().setInheritanceEnabled(true).build());
        services.documentLedger().save(row);
        assertThatThrownBy(() -> remote.getDocument(request)).satisfies(error ->
                assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        row.security = "{\"denyAll\":true}";
        services.documentLedger().save(row);
        assertThatThrownBy(() -> remote.getDocument(request)).satisfies(error ->
                assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        assertThatThrownBy(() -> services.repository().getDocument(reader, request))
                .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION));
        row.writeSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ)).build());
        services.documentLedger().save(row);
        // Public means any bound member of this account, including one without group identities.
        binding.set(new ai.protomolt.proto.repo.spi.RepositoryCaller("login", false, Set.of(account), Set.of()));
        assertThat(remote.getDocument(request).getDocument()).isEqualTo(doc.build());
        binding.set(new ai.protomolt.proto.repo.spi.RepositoryCaller("login", false, Set.of("another"), Set.of()));
        assertThatThrownBy(() -> remote.getDocument(request)).satisfies(error ->
                assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.NOT_FOUND));
        binding.set(reader);
        // The body still contains the old grant. The current ledger policy decides access.
            row.writeSecurity(originalPolicy.toBuilder().addPermissions(AccessRule.newBuilder()
                    .setIdentityType("user-principal-name").setIdentity("alice").setAccess(Access.ACCESS_DENY)).build());
            services.documentLedger().save(row);
            for (Runnable denied : List.<Runnable>of(() -> remote.getDocument(request),
                    () -> remote.getDocumentByReference(reference), () -> remote.getDocumentManifest(manifest))) {
                assertThatThrownBy(denied::run).satisfies(error ->
                        assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.NOT_FOUND));
            }
            for (Runnable denied : List.<Runnable>of(() -> services.repository().getDocument(reader, request),
                    () -> services.repository().getDocumentByReference(reader, reference),
                    () -> services.repository().getDocumentManifest(reader, manifest))) {
                assertThatThrownBy(denied::run).isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.NOT_FOUND));
            }
        } finally {
            connection.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Test
    void libraryAndGrpcShareDocumentSaveReadManifestAndListBehavior() {
        var local = services.repository();
        var caller = new ai.protomolt.proto.repo.spi.RepositoryCaller("integration-test", true);
        for (boolean grpc : new boolean[] {false, true}) {
            String account = "parity-account-" + grpc;
            String drive = "parity-drive-" + grpc;
            createDrive(drive, account);
            var doc = fixture("parity-doc-" + grpc, account, "source");
            var save = intakeSave(doc, drive, account).build();
            var saved = grpc ? documents.saveDocument(save) : local.saveDocument(caller, save);
            var get = GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build();
            var read = grpc ? documents.getDocument(get) : local.getDocument(caller, get);
            assertThat(read.getDocument()).isEqualTo(doc);
            var manifestRequest = GetDocumentManifestRequest.newBuilder().setNodeId(saved.getNodeId()).build();
            var manifest = grpc ? documents.getDocumentManifest(manifestRequest) : local.getDocumentManifest(caller, manifestRequest);
            assertThat(manifest.getManifest()).isEqualTo(read.getManifest());
            var listRequest = ListDocumentsRequest.newBuilder().setAccountId(account).build();
            var listed = grpc ? documents.listDocuments(listRequest) : local.listDocuments(caller, listRequest);
            assertThat(listed.getTotalCount()).isEqualTo(1);
            var again = grpc ? documents.saveDocument(save) : local.saveDocument(caller, save);
            assertThat(again.getNodeId()).isEqualTo(saved.getNodeId());
            assertThat(again.getDeduplicated()).isTrue();
            var byReference = GetDocumentByReferenceRequest.newBuilder().setAddress(saved.getAddress()).build();
            assertThat((grpc ? documents.getDocumentByReference(byReference) : local.getDocumentByReference(caller, byReference))
                    .getDocument()).isEqualTo(doc);
            var delete = DeleteDocumentRequest.newBuilder().setByReference(
                    DeleteDocumentByReferenceCommand.newBuilder().setAddress(saved.getAddress())).build();
            var deleted = grpc ? documents.deleteDocument(delete) : local.deleteDocument(caller, delete);
            assertThat(deleted.getOutcome()).isNotEqualTo(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_UNSPECIFIED);

        }
        assertThatThrownBy(() -> local.listDocuments(
                new ai.protomolt.proto.repo.spi.RepositoryCaller("scoped", false), ListDocumentsRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.PERMISSION_DENIED));
    }

    @Test
    void remoteOutageHasDomainFailureLocallyAndSameGrpcStatus() throws Exception {
        String account = "remote-outage-account";
        String drive = "remote-outage-drive";
        createDrive(drive, account);
        var saved = documents.saveDocument(intakeSave(fixture("outage-doc", account, "source"), drive, account).build());
        var row = services.driveLedger().findByName(account, drive).orElseThrow();
        var absentChannel = InProcessChannelBuilder.forName(java.util.UUID.randomUUID().toString()).build();
        var unavailable = new ai.protomolt.proto.repo.blob.grpc.RemoteBlobStore(
                DocumentServiceGrpc.newBlockingStub(absentChannel), java.util.Map.of(row.bucket, drive), java.time.Duration.ofSeconds(1));
        var local = new ai.protomolt.proto.repo.engine.DocumentOperations(services.documentLedger(), services.driveLedger(),
                null, unavailable, new ai.protomolt.proto.repo.container.blob.PartStorage(), null);
        var request = GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build();
        String serverName = io.grpc.inprocess.InProcessServerBuilder.generateName();
        var server = io.grpc.inprocess.InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(new DocumentGrpcService(local,
                        new ai.protomolt.proto.repo.engine.BlobOperations(unavailable, services.driveLedger())))
                .build().start();
        var testChannel = InProcessChannelBuilder.forName(serverName).build();
        try {
            assertThatThrownBy(() -> local.getDocument(new ai.protomolt.proto.repo.spi.RepositoryCaller("test", true), request))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.UNAVAILABLE))
                    .hasCauseInstanceOf(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class);
            assertThatThrownBy(() -> DocumentServiceGrpc.newBlockingStub(testChannel).getDocument(request))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
        } finally {
            testChannel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            absentChannel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    // --------------------------------------------------------- authentication

    /**
     * The TCP listener holds every account's documents and every claim-check blob, so with
     * a credential configured it must refuse a call that does not present one. The
     * in-process transport the rest of this class uses stays open deliberately: it has no
     * socket, and a caller already inside the JVM is past every boundary a credential
     * could draw.
     */
    @Test
    void theGuardedTcpListenerRefusesACallWithoutTheCredential() throws Exception {
        String token = "repo-it-token";
        Server guarded = services.startNetty(0, token, null);
        ManagedChannel plain = ManagedChannelBuilder
                .forAddress("127.0.0.1", guarded.getPort()).usePlaintext().build();
        try {
            DriveServiceGrpc.DriveServiceBlockingStub anonymous =
                    DriveServiceGrpc.newBlockingStub(plain);

            assertThatThrownBy(() -> anonymous.listDrives(
                            ListDrivesRequest.newBuilder().setAccountId("acct-auth").build()))
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(thrown -> assertThat(
                            ((StatusRuntimeException) thrown).getStatus().getCode())
                            .isEqualTo(Status.Code.UNAUTHENTICATED));

            Metadata credential = new Metadata();
            credential.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    token);
            DriveServiceGrpc.DriveServiceBlockingStub authenticated =
                    DriveServiceGrpc.newBlockingStub(plain).withInterceptors(
                            MetadataUtils.newAttachHeadersInterceptor(credential));

            assertThat(authenticated.listDrives(
                    ListDrivesRequest.newBuilder().setAccountId("acct-auth").build()))
                    .isNotNull();
        } finally {
            plain.shutdownNow();
            guarded.shutdownNow();
        }
    }

    /** An access policy resolves principals from credentials, so it cannot run without one. */
    @Test
    void anAccessPolicyResolverWithoutACredentialIsRefusedAtStartup() {
        assertThatThrownBy(() -> services.startNetty(0, null, caller -> java.util.Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api token");
    }

    private static io.grpc.Server policyServer(String endpoint,
            java.util.function.Function<ai.protomolt.proto.actions.Caller, ai.protomolt.proto.repo.spi.RepositoryCaller> bindings) throws Exception {
        var implementation = new DocumentGrpcService(services.repository(),
                new ai.protomolt.proto.repo.engine.BlobOperations(services.blobStore(), services.driveLedger()),
                bindings);
        return io.grpc.inprocess.InProcessServerBuilder.forName(endpoint)
                .addService(io.grpc.ServerInterceptors.intercept(implementation, new io.grpc.ServerInterceptor() {
                    @Override public <Q,R> io.grpc.ServerCall.Listener<Q> interceptCall(
                            io.grpc.ServerCall<Q,R> call, io.grpc.Metadata headers, io.grpc.ServerCallHandler<Q,R> next) {
                        var context = io.grpc.Context.current().withValue(
                                ai.protomolt.proto.authz.grpc.CallerContexts.CALLER,
                                ai.protomolt.proto.actions.Caller.scoped("login", Set.of()));
                        return io.grpc.Contexts.interceptCall(context, call, headers, next);
                    }
                })).build().start();
    }

    // ------------------------------------------------------------- fixtures

    /** A fully-populated document: blob bag, parsed metadata, two chunk sets. */
    private static Document fixture(String docId, String accountId, String datasourceId) {
        return Document.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(datasourceId)
                        .setConnectorId(CONNECTOR)
                        .setSecurity(DocumentSecurity.newBuilder()
                                .setInheritanceEnabled(true)
                                .addPermissions(AccessRule.newBuilder()
                                        .setIdentity("alice")
                                        .setIdentityType("user-principal-name")
                                        .setDisplayName("Alice")
                                        .setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Quarterly Report")
                        .setBody("the full text body of the document")
                        .setDocumentType("PDF")
                        .setSourceUri("s3://source/" + docId)
                        .addSemanticResults(chunkSet("chunks-a", 2))
                        .addSemanticResults(chunkSet("chunks-b", 1)))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-1")
                        .setData(ByteString.copyFromUtf8("raw-pdf-bytes-of-" + docId))
                        .setMimeType("application/pdf")
                        .setFilename("report.pdf")))
                .putParserResults("tika", ParserResult.newBuilder()
                        .setParserName("tika")
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .setDocument(ParserDocument.newBuilder()
                                .setShape(Any.pack(StringValue.of("tika-exhaust"))))
                        .build())
                .build();
    }

    private static SemanticProcessingResult chunkSet(String resultId, int chunkCount) {
        SemanticProcessingResult.Builder set = SemanticProcessingResult.newBuilder()
                .setResultId(resultId)
                .setChunkerConfigId("chunker-v1");
        for (int i = 0; i < chunkCount; i++) {
            set.addChunks(SemanticChunk.newBuilder()
                    .setChunkId(resultId + "-" + i)
                    .setChunkNumber(i)
                    .setText("chunk text " + i + " of " + resultId));
        }
        return set.build();
    }

    private static SaveDocumentRequest.Builder intakeSave(Document doc, String drive, String accountId) {
        return SaveDocumentRequest.newBuilder()
                .setDocument(doc)
                .setDrive(drive)
                .setConnectorId(CONNECTOR)
                .setUseDatasourceId(true)
                .setGraphId("intake:" + accountId);
    }

    private static NodeAddress address(String docId, String graphAddressId,
            String accountId, String graphId) {
        return NodeAddress.newBuilder()
                .setDocId(docId)
                .setGraphAddressId(graphAddressId)
                .setAccountId(accountId)
                .setGraphId(graphId)
                .build();
    }

    private static Drive createDrive(String name, String accountId) {
        return drives.createDrive(CreateDriveRequest.newBuilder()
                        .setName(name)
                        .setAccountId(accountId)
                        .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                        .build())
                .getDrive();
    }

    // ---------------------------------------------------------------- tests

    @Test
    void createGetListDriveRoundTrip() {
        Drive created = createDrive("intake", "acct-drive");
        assertThat(created.getDriveId()).isNotBlank();
        assertThat(created.getBucket()).isEqualTo("it-docs-acct-drive-intake");
        assertThat(created.getPrefix()).isEqualTo("intake");
        assertThat(created.getDriveType()).isEqualTo(DriveType.DRIVE_TYPE_INTAKE);
        assertThat(created.getCreatedAt().getSeconds()).isPositive();

        // The bucket actually exists in LocalStack.
        services.blobStore().headBucket(created.getBucket());

        // timestamptz stores micros, so the create response (in-memory
        // Instant, full nanos) and re-fetched rows can differ below a micro:
        // compare everything except created_at, which is asserted separately.
        Drive expected = created.toBuilder().clearCreatedAt().build();
        Drive byId = drives.getDrive(GetDriveRequest.newBuilder()
                .setDriveId(created.getDriveId()).build()).getDrive();
        assertThat(byId.toBuilder().clearCreatedAt().build()).isEqualTo(expected);
        assertThat(byId.getCreatedAt().getSeconds()).isEqualTo(created.getCreatedAt().getSeconds());
        Drive byName = drives.getDrive(GetDriveRequest.newBuilder()
                .setName("intake").setAccountId("acct-drive").build()).getDrive();
        assertThat(byName.toBuilder().clearCreatedAt().build()).isEqualTo(expected);

        assertThat(drives.listDrives(ListDrivesRequest.newBuilder()
                .setAccountId("acct-drive").build()).getDrivesList())
                .containsExactly(byId);

        // Deterministic id ⇒ re-create is idempotent.
        Drive recreated = createDrive("intake", "acct-drive");
        assertThat(recreated.getDriveId()).isEqualTo(created.getDriveId());
    }

    @Test
    void createDrivePersistsAndEchoesProviderConfig() {
        DriveProviderConfig providerConfig = DriveProviderConfig.newBuilder()
                .setS3(ai.protomolt.proto.repo.v1.S3DriveConfig.newBuilder()
                        .setEndpointOverride(LOCALSTACK.getEndpoint().toString())
                        .setForcePathStyle(true)).build();
        Drive created = drives.createDrive(CreateDriveRequest.newBuilder()
                        .setName("redis-backed")
                        .setAccountId("acct-pcfg")
                        .setDriveType(DriveType.DRIVE_TYPE_CUSTOM)
                        .setProviderConfig(providerConfig)
                        .build())
                .getDrive();
        assertThat(created.getProviderConfig()).isEqualTo(providerConfig);

        // The jsonb column round-trips: every read path echoes the config
        // verbatim (the boot-time hbm2ddl validate already proved the
        // provider_config mapping against the V2-migrated schema).
        Drive byId = drives.getDrive(GetDriveRequest.newBuilder()
                .setDriveId(created.getDriveId()).build()).getDrive();
        assertThat(byId.getProviderConfig()).isEqualTo(providerConfig);
        Drive byName = drives.getDrive(GetDriveRequest.newBuilder()
                .setName("redis-backed").setAccountId("acct-pcfg").build()).getDrive();
        assertThat(byName.getProviderConfig()).isEqualTo(providerConfig);
        assertThat(drives.listDrives(ListDrivesRequest.newBuilder()
                .setAccountId("acct-pcfg").build()).getDrivesList())
                .singleElement()
                .satisfies(d -> assertThat(d.getProviderConfig()).isEqualTo(providerConfig));

        // Drives created without one carry no provider config.
        assertThat(createDrive("plain", "acct-pcfg").hasProviderConfig()).isFalse();
    }

    @Test
    void fullIntakeSaveRoundTripsByteExact() {
        String account = "acct-full";
        // Unique drive name: GetBlob's storage_ref lookup is by bare name
        // (v1 trusts the caller), so this test's drive must be unambiguous
        // across the whole IT suite.
        createDrive("full-docs", account);
        Document doc = fixture("doc-full-1", account, "ds-1");

        SaveDocumentResponse saved = documents.saveDocument(
                intakeSave(doc, "full-docs", account).build());
        assertThat(saved.getNodeId()).isNotBlank();
        assertThat(saved.getDeduplicated()).isFalse();
        assertThat(saved.getChecksum()).hasSize(64);
        assertThat(saved.getDrive()).isEqualTo("full-docs");
        assertThat(saved.getSizeBytes()).isPositive();
        assertThat(saved.getStoragePrefix())
                .startsWith("full-docs/documents/" + account + "/")
                .endsWith(saved.getNodeId());
        // The response echoes the canonical address the node_id derives from.
        assertThat(saved.getAddress())
                .isEqualTo(address("doc-full-1", "ds-1", account, "intake:" + account));

        GetDocumentResponse got = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build());
        assertThat(got.getDocument().toByteArray()).isEqualTo(doc.toByteArray());
        assertThat(got.getNodeId()).isEqualTo(saved.getNodeId());
        assertThat(got.getSizeBytes()).isEqualTo(got.getDocument().getSerializedSize());

        GetDocumentResponse byRef = documents.getDocumentByReference(
                GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(address("doc-full-1", "ds-1", account, "intake:" + account))
                        .build());
        assertThat(byRef.getDocument().toByteArray()).isEqualTo(doc.toByteArray());
        assertThat(byRef.getNodeId()).isEqualTo(saved.getNodeId());

        // Manifest: CORE, BLOBS, two CHUNKS sub-entries, PARSED — all PRESENT
        // with sha256s, version 1.
        GetDocumentManifestResponse manifest = documents.getDocumentManifest(
                GetDocumentManifestRequest.newBuilder().setNodeId(saved.getNodeId()).build());
        assertThat(manifest.getDrive()).isEqualTo("full-docs");
        DocumentManifest m = manifest.getManifest();
        assertThat(m.getAddress())
                .isEqualTo(address("doc-full-1", "ds-1", account, "intake:" + account));
        assertThat(m.getDocVersion()).isEqualTo(1);
        assertThat(m.getPartsList()).allSatisfy(e -> {
            assertThat(e.getState()).isEqualTo(PartState.PART_STATE_PRESENT);
            assertThat(e.getSha256()).hasSize(64);
            assertThat(e.getSizeBytes()).isPositive();
        });
        assertThat(m.getPartsList().stream().map(PartManifestEntry::getPart))
                .containsExactly(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS,
                        DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_CHUNKS,
                        DocumentPart.DOCUMENT_PART_PARSED);
        assertThat(m.getPartsList().stream()
                .filter(e -> e.getPart() == DocumentPart.DOCUMENT_PART_CHUNKS)
                .map(PartManifestEntry::getSubKey))
                .containsExactly("chunks-a", "chunks-b");

        // GetBlob: raw object fetch by storage_ref (here: the CORE part object).
        String coreKey = m.getPartsList().stream()
                .filter(e -> e.getPart() == DocumentPart.DOCUMENT_PART_CORE)
                .findFirst().orElseThrow().getObjectKey();
        var blob = documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(FileStorageReference.newBuilder()
                        .setDriveName("full-docs").setObjectKey(coreKey))
                .build());
        assertThat(DocumentPartCodec.sha256Hex(blob.getData().toByteArray()))
                .isEqualTo(m.getPartsList().get(0).getSha256());
        assertThat(blob.getSizeBytes()).isEqualTo(blob.getData().size());
    }

    @Test
    void dedupeSkipsSecondIdenticalIntakeSaveAndForceSaveBypasses() {
        String account = "acct-dedupe";
        createDrive("docs", account);
        Document doc = fixture("doc-dedupe-1", account, "ds-1");

        SaveDocumentResponse first = documents.saveDocument(intakeSave(doc, "docs", account).build());
        SaveDocumentResponse second = documents.saveDocument(intakeSave(doc, "docs", account).build());

        assertThat(second.getDeduplicated()).isTrue();
        assertThat(second.getNodeId()).isEqualTo(first.getNodeId());
        assertThat(second.getChecksum()).isEqualTo(first.getChecksum());

        DocumentRecord row = services.documentLedger()
                .findByNodeId(UUID.fromString(first.getNodeId())).orElseThrow();
        assertThat(row.reprocessCount).isEqualTo(1);
        assertThat(row.lastReprocessedAt).isNotNull();

        // force_save bypasses dedupe: the body is rewritten, no reprocess mark.
        SaveDocumentResponse forced = documents.saveDocument(
                intakeSave(doc, "docs", account).setForceSave(true).build());
        assertThat(forced.getDeduplicated()).isFalse();
        assertThat(forced.getNodeId()).isEqualTo(first.getNodeId());
        row = services.documentLedger().findByNodeId(UUID.fromString(first.getNodeId())).orElseThrow();
        assertThat(row.reprocessCount).isEqualTo(1);
        assertThat(row.readManifest().getDocVersion()).isEqualTo(2);
    }

    @Test
    void partialReadCoreOnlyAndChunkSetFilter() {
        String account = "acct-pread";
        createDrive("docs", account);
        Document doc = fixture("doc-pread-1", account, "ds-1");
        SaveDocumentResponse saved = documents.saveDocument(intakeSave(doc, "docs", account).build());

        // CORE only: everything but blob_bag / parser_results / semantic_results.
        Document coreOnly = doc.toBuilder()
                .clearBlobBag()
                .clearParserResults()
                .setSearchMetadata(doc.getSearchMetadata().toBuilder().clearSemanticResults())
                .build();
        GetDocumentResponse core = documents.getDocument(GetDocumentRequest.newBuilder()
                .setNodeId(saved.getNodeId())
                .addParts(DocumentPart.DOCUMENT_PART_CORE)
                .build());
        assertThat(core.getDocument().toByteArray()).isEqualTo(coreOnly.toByteArray());
        // The response still carries the FULL manifest (all parts, all states).
        assertThat(core.getManifest().getPartsCount()).isEqualTo(5);

        // CHUNKS narrowed to one chunk set: only doc_id + that set survives.
        Document chunksAOnly = Document.newBuilder()
                .setDocId(doc.getDocId())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(chunkSet("chunks-a", 2)))
                .build();
        GetDocumentResponse chunks = documents.getDocument(GetDocumentRequest.newBuilder()
                .setNodeId(saved.getNodeId())
                .addParts(DocumentPart.DOCUMENT_PART_CHUNKS)
                .addChunkSets("chunks-a")
                .build());
        assertThat(chunks.getDocument().toByteArray()).isEqualTo(chunksAOnly.toByteArray());
    }

    @Test
    void partialSaveCarriesUnwrittenPartsForwardWithOriginalProvenance() {
        String account = "acct-psave";
        createDrive("pipe", account);
        Document doc = fixture("doc-psave-1", account, "ds-2");
        WriteProvenance hop0Writer = WriteProvenance.newBuilder()
                .setModuleId("parser").setNodeId("hop-0").setGraphId("graph-a")
                .setGraphVersion(7).build();

        // hop-1: full pipeline save of the parsed document.
        SaveDocumentResponse hop1 = documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(doc)
                .setDrive("pipe")
                .setConnectorId(CONNECTOR)
                .setGraphLocationId("hop-1")
                .setGraphId("graph-a")
                .setWrittenBy(hop0Writer)
                .build());
        NodeAddress hop1Ref = address("doc-psave-1", "hop-1", account, "graph-a");
        DocumentManifest hop1Manifest = documents.getDocumentManifest(
                GetDocumentManifestRequest.newBuilder().setAddress(hop1Ref).build())
                .getManifest();

        // hop-2: the chunker re-stages with ONLY new chunks; the rest copies.
        Document doc2 = doc.toBuilder()
                .setSearchMetadata(doc.getSearchMetadata().toBuilder()
                        .clearSemanticResults()
                        .addSemanticResults(chunkSet("chunks-c", 2)))
                .build();
        WriteProvenance hop1Writer = WriteProvenance.newBuilder()
                .setModuleId("chunker").setNodeId("hop-1").setGraphId("graph-a")
                .setGraphVersion(7).build();
        SaveDocumentResponse hop2 = documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(doc2)
                .setDrive("pipe")
                .setConnectorId(CONNECTOR)
                .setGraphLocationId("hop-2")
                .setGraphId("graph-a")
                .setWrittenBy(hop1Writer)
                .addPartsWritten(DocumentPart.DOCUMENT_PART_CHUNKS)
                .setCopyUnwrittenPartsFrom(hop1Ref)
                .build());
        assertThat(hop2.getNodeId()).isNotEqualTo(hop1.getNodeId());

        DocumentManifest hop2Manifest = documents.getDocumentManifest(
                GetDocumentManifestRequest.newBuilder().setNodeId(hop2.getNodeId()).build())
                .getManifest();
        Map<DocumentPart, PartManifestEntry> byPart = new java.util.EnumMap<>(DocumentPart.class);
        hop2Manifest.getPartsList().forEach(e -> byPart.putIfAbsent(e.getPart(), e));

        // Carried entries keep the ORIGINAL sha256/size/updated_at/written_by
        // stamps; only the object key moved to the hop-2 address.
        for (DocumentPart part : List.of(DocumentPart.DOCUMENT_PART_CORE,
                DocumentPart.DOCUMENT_PART_BLOBS, DocumentPart.DOCUMENT_PART_PARSED)) {
            PartManifestEntry carriedEntry = byPart.get(part);
            PartManifestEntry sourceEntry = hop1Manifest.getPartsList().stream()
                    .filter(e -> e.getPart() == part).findFirst().orElseThrow();
            assertThat(carriedEntry.getState()).isEqualTo(PartState.PART_STATE_PRESENT);
            assertThat(carriedEntry.getWrittenBy()).isEqualTo(hop0Writer);
            assertThat(carriedEntry.getSha256()).isEqualTo(sourceEntry.getSha256());
            assertThat(carriedEntry.getSizeBytes()).isEqualTo(sourceEntry.getSizeBytes());
            assertThat(carriedEntry.getUpdatedAt()).isEqualTo(sourceEntry.getUpdatedAt());
            assertThat(carriedEntry.getObjectKey())
                    .startsWith(hop2.getStoragePrefix() + "/")
                    .isNotEqualTo(sourceEntry.getObjectKey());
        }
        // The chunk set this save wrote carries THIS save's provenance.
        PartManifestEntry chunks = byPart.get(DocumentPart.DOCUMENT_PART_CHUNKS);
        assertThat(chunks.getSubKey()).isEqualTo("chunks-c");
        assertThat(chunks.getWrittenBy()).isEqualTo(hop1Writer);

        // The hop-2 state reassembles byte-exact (carried bytes + new chunks).
        GetDocumentResponse reassembled = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(hop2.getNodeId()).build());
        assertThat(reassembled.getDocument().toByteArray()).isEqualTo(doc2.toByteArray());

        // A copy source that is gone fails FAILED_PRECONDITION.
        assertThatThrownBy(() -> documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(doc2)
                .setDrive("pipe")
                .setConnectorId(CONNECTOR)
                .setGraphLocationId("hop-3")
                .setGraphId("graph-a")
                .addPartsWritten(DocumentPart.DOCUMENT_PART_CHUNKS)
                .setCopyUnwrittenPartsFrom(address("doc-psave-1", "nowhere", account, "graph-a"))
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.FAILED_PRECONDITION));
    }

    @Test
    void deleteTombstonesThenPurgesRowsAndObjects() {
        String account = "acct-del";
        Drive drive = createDrive("docs", account);
        Document doc = fixture("doc-del-1", account, "ds-3");
        SaveDocumentResponse saved = documents.saveDocument(intakeSave(doc, "docs", account).build());
        UUID nodeId = UUID.fromString(saved.getNodeId());
        NodeAddress ref = address("doc-del-1", "ds-3", account, "intake:" + account);

        DocumentRecord before = services.documentLedger().findByNodeId(nodeId).orElseThrow();
        List<String> partKeys = before.readManifest().getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .map(PartManifestEntry::getObjectKey)
                .toList();
        assertThat(partKeys).hasSize(5);

        // Metadata-only delete: tombstone to PENDING_PURGE; updated_at must
        // NOT move (the staleness guard only trusts body rewrites).
        DeleteDocumentResponse tombstoned = documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setByReference(DeleteDocumentByReferenceCommand.newBuilder().setAddress(ref))
                .build());
        assertThat(tombstoned.getOutcome())
                .isEqualTo(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_REMOVED);
        assertThat(tombstoned.getDocumentsRemoved()).isEqualTo(1);
        assertThat(tombstoned.getRemovedNodesList().stream().map(n -> n.getNodeId()))
                .containsExactly(saved.getNodeId());
        DocumentRecord after = services.documentLedger().findByNodeId(nodeId).orElseThrow();
        assertThat(after.status).isEqualTo(DocumentStatus.PENDING_PURGE);
        assertThat(after.updatedAt).isEqualTo(before.updatedAt);
        // Objects survive the tombstone.
        for (String key : partKeys) {
            services.blobStore().headObject(drive.getBucket(), key);
        }

        // purge_storage=true: rows AND their part objects go away.
        DeleteDocumentResponse purged = documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setLogicalDocument(DeleteLogicalDocumentCommand.newBuilder()
                        .setDocId("doc-del-1").setAccountId(account).setDatasourceId("ds-3"))
                .setPurgeStorage(true)
                .build());
        assertThat(purged.getOutcome())
                .isEqualTo(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_REMOVED);
        assertThat(services.documentLedger().findByNodeId(nodeId)).isEmpty();
        for (String key : partKeys) {
            assertThatThrownBy(() -> services.blobStore().headObject(drive.getBucket(), key))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }

        // Idempotent: deleting again matches nothing.
        DeleteDocumentResponse again = documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setLogicalDocument(DeleteLogicalDocumentCommand.newBuilder()
                        .setDocId("doc-del-1").setAccountId(account).setDatasourceId("ds-3"))
                .setPurgeStorage(true)
                .build());
        assertThat(again.getOutcome())
                .isEqualTo(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_NOTHING_TO_REMOVE);
        assertThat(again.getDocumentsRemoved()).isZero();
    }

    @Test
    void tombstoneDeleteEnqueuesPurgeRecordAndDrainFinalizes() {
        String account = "acct-lifecycle";
        Drive drive = createDrive("docs", account);
        Document doc = fixture("doc-lifecycle", account, "ds-lc");
        SaveDocumentResponse saved = documents.saveDocument(intakeSave(doc, "docs", account).build());
        UUID nodeId = UUID.fromString(saved.getNodeId());
        NodeAddress ref = address("doc-lifecycle", "ds-lc", account, "intake:" + account);

        List<String> partKeys = services.documentLedger().findByNodeId(nodeId).orElseThrow()
                .readManifest().getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .map(PartManifestEntry::getObjectKey)
                .toList();
        assertThat(partKeys).isNotEmpty();

        // Phase A: the tombstone AND the purge record land in one commit.
        DeleteDocumentResponse response = documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setByReference(DeleteDocumentByReferenceCommand.newBuilder().setAddress(ref))
                .build());
        assertThat(response.getOutcome())
                .isEqualTo(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_REMOVED);
        assertThat(response.getMessage()).isEqualTo("rows tombstoned to PENDING_PURGE");
        assertThat(services.documentLedger().findByNodeId(nodeId).orElseThrow().status)
                .isEqualTo(DocumentStatus.PENDING_PURGE);

        // The purge record is claimable immediately — same transaction as the
        // tombstone — and snapshots the part keys plus the intake raw blob.
        DocumentPurgeRecord record = services.purgeQueue().claimBatch(100).stream()
                .filter(r -> r.nodeId.equals(nodeId))
                .findFirst()
                .orElseThrow();
        assertThat(record.driveName).isEqualTo("docs");
        assertThat(record.readObjectKeys())
                .contains(partKeys.toArray(new String[0]))
                .contains(ai.protomolt.proto.repo.container.lifecycle.PurgeSnapshots
                        .rawBlobKey(drive.getPrefix(), account, "doc-lifecycle", "ds-lc"));

        // Phase B by hand (the background loop is not started in this IT):
        // objects and row go away, and the record leaves the PENDING set.
        int purged = services.s3Purger().drainOnce(services.blobStore(), 100);
        assertThat(purged).isGreaterThanOrEqualTo(1);
        assertThat(services.documentLedger().findByNodeId(nodeId)).isEmpty();
        for (String key : partKeys) {
            assertThatThrownBy(() -> services.blobStore().headObject(drive.getBucket(), key))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }
        assertThat(services.purgeQueue().claimBatch(100).stream()
                .filter(r -> r.nodeId.equals(nodeId)))
                .isEmpty();
    }

    @Test
    void putBlobExplicitKeyRoundTripsAndDeleteIsIdempotent() {
        String account = "acct-blob";
        createDrive("blobs", account);
        byte[] data = ("explicit-key-payload-".repeat(500)).getBytes(
                java.nio.charset.StandardCharsets.UTF_8);

        PutBlobResponse put = documents.putBlob(PutBlobRequest.newBuilder()
                .setDriveName("blobs")
                .setObjectKey("custom/report.pdf")
                .setData(ByteString.copyFrom(data))
                .setMimeType("application/pdf")
                .build());
        assertThat(put.getStorageRef().getDriveName()).isEqualTo("blobs");
        assertThat(put.getStorageRef().getObjectKey()).isEqualTo("custom/report.pdf");
        assertThat(put.getSizeBytes()).isEqualTo(data.length);
        assertThat(put.getSha256()).isEqualTo(DocumentPartCodec.sha256Hex(data));

        // Round trip via GetBlob: the exact bytes back.
        var got = documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(put.getStorageRef())
                .build());
        assertThat(got.getData().toByteArray()).isEqualTo(data);
        assertThat(got.getMimeType()).isEqualTo("application/pdf");

        // Delete is idempotent: first delete reports true, the re-delete
        // reports false (absence is not an error), and the object is gone.
        DeleteBlobResponse deleted = documents.deleteBlob(DeleteBlobRequest.newBuilder()
                .setStorageRef(put.getStorageRef())
                .build());
        assertThat(deleted.getDeleted()).isTrue();
        assertThatThrownBy(() -> documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(put.getStorageRef())
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
        DeleteBlobResponse again = documents.deleteBlob(DeleteBlobRequest.newBuilder()
                .setStorageRef(put.getStorageRef())
                .build());
        assertThat(again.getDeleted()).isFalse();
    }

    @Test
    void conditionalBlobRpcFailsClosedOnUnqualifiedLocalStack() {
        createDrive("conditional", "acct-conditional");
        var key = ConditionalBlobKey.newBuilder().setDriveName("conditional")
                .setObjectKey("state/current").build();
        var read = GetBlobForUpdateRequest.newBuilder().setKey(key).build();
        assertThatThrownBy(() -> documents.getBlobForUpdate(read))
                .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
        assertThatThrownBy(() -> documents.compareAndPutBlob(CompareAndPutBlobRequest.newBuilder()
                .setKey(key).setIfAbsent(true).setData(ByteString.copyFromUtf8("first")).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
        assertThatThrownBy(() -> documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(FileStorageReference.newBuilder().setDriveName("conditional")
                        .setObjectKey("state/current")).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));

        for (var invalid : List.of(
                CompareAndPutBlobRequest.newBuilder().setKey(key).setIfAbsent(false).build(),
                CompareAndPutBlobRequest.newBuilder().setKey(key).setExpectedEtag("*").build(),
                CompareAndPutBlobRequest.newBuilder().setKey(key).build())) {
            assertThatThrownBy(() -> documents.compareAndPutBlob(invalid))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                            assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        }
        var unknownKey = key.toBuilder().setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                .addField(99, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                .build()).build();
        assertThatThrownBy(() -> documents.getBlobForUpdate(
                GetBlobForUpdateRequest.newBuilder().setKey(unknownKey).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void putBlobGeneratedKeyIsContentAddressedAndIdempotent() {
        String account = "acct-blobgen";
        createDrive("gen", account);
        byte[] data = ("generated-key-payload-".repeat(300)).getBytes(
                java.nio.charset.StandardCharsets.UTF_8);

        PutBlobResponse first = documents.putBlob(PutBlobRequest.newBuilder()
                .setDriveName("gen")
                .setData(ByteString.copyFrom(data))
                .build());
        assertThat(first.getStorageRef().getObjectKey()).startsWith("gen/blobs/");

        // Same content, blank key again: the SAME object key — identical puts
        // land idempotently instead of orphaning a second copy.
        PutBlobResponse second = documents.putBlob(PutBlobRequest.newBuilder()
                .setDriveName("gen")
                .setData(ByteString.copyFrom(data))
                .build());
        assertThat(second.getStorageRef().getObjectKey())
                .isEqualTo(first.getStorageRef().getObjectKey());
        assertThat(second.getSha256()).isEqualTo(first.getSha256());

        // Different content addresses a different object.
        PutBlobResponse other = documents.putBlob(PutBlobRequest.newBuilder()
                .setDriveName("gen")
                .setData(ByteString.copyFromUtf8("different"))
                .build());
        assertThat(other.getStorageRef().getObjectKey())
                .isNotEqualTo(first.getStorageRef().getObjectKey());

        // Unknown drive is NOT_FOUND.
        assertThatThrownBy(() -> documents.putBlob(PutBlobRequest.newBuilder()
                .setDriveName("no-such-drive")
                .setData(ByteString.copyFromUtf8("x"))
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void listDocumentsFiltersAndPaginates() {
        String account = "acct-list";
        createDrive("docs", account);
        for (int i = 0; i < 3; i++) {
            documents.saveDocument(intakeSave(fixture("doc-list-" + i, account, "ds-4"), "docs", account)
                    .setCrawlId("crawl-1")
                    .build());
        }
        documents.saveDocument(intakeSave(fixture("doc-list-other", account, "ds-4"), "docs", account)
                .setConnectorId("other-connector")
                .build());

        ListDocumentsResponse page1 = documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).setConnectorId(CONNECTOR).setLimit(2).build());
        assertThat(page1.getDocumentsCount()).isEqualTo(2);
        assertThat(page1.getTotalCount()).isEqualTo(3);
        assertThat(page1.getNextContinuationToken()).isNotBlank();

        ListDocumentsResponse page2 = documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).setConnectorId(CONNECTOR).setLimit(2)
                .setContinuationToken(page1.getNextContinuationToken()).build());
        assertThat(page2.getDocumentsCount()).isEqualTo(1);
        assertThat(page2.getNextContinuationToken()).isEmpty();

        assertThat(page1.getDocumentsList().stream().map(d -> d.getNodeId()).toList())
                .doesNotContainAnyElementsOf(
                        page2.getDocumentsList().stream().map(d -> d.getNodeId()).toList());
        assertThat(page1.getDocumentsList()).allSatisfy(d -> {
            assertThat(d.getConnectorId()).isEqualTo(CONNECTOR);
            assertThat(d.getCrawlId()).isEqualTo("crawl-1");
            assertThat(d.getTitle()).isEqualTo("Quarterly Report");
            assertThat(d.getAddress().getAccountId()).isEqualTo(account);
            assertThat(d.getAddress().getGraphId()).isEqualTo("intake:" + account);
        });

        assertThat(documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).setConnectorId("other-connector").build())
                .getDocumentsCount()).isEqualTo(1);
        assertThat(documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).build()).getTotalCount()).isEqualTo(4);
        assertThat(documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).setCrawlId("crawl-1").build()).getTotalCount()).isEqualTo(3);
    }

    @Test
    void errorMappingSpotChecks() {
        String account = "acct-err";
        createDrive("docs", account);
        Document doc = fixture("doc-err-1", account, "ds-5");

        // Save without graph_id → INVALID_ARGUMENT naming the field.
        assertThatThrownBy(() -> documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(doc).setDrive("docs").setConnectorId(CONNECTOR)
                .setUseDatasourceId(true).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("graph_id");
                });

        // Intake save with a contradicting graph_id → INVALID_ARGUMENT.
        assertThatThrownBy(() -> documents.saveDocument(intakeSave(doc, "docs", account)
                .setGraphId("some-other-graph").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        // Intake save carrying cluster_id → INVALID_ARGUMENT.
        assertThatThrownBy(() -> documents.saveDocument(intakeSave(doc, "docs", account)
                .setClusterId("cluster-1").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        // Save to an unknown drive → NOT_FOUND.
        assertThatThrownBy(() -> documents.saveDocument(intakeSave(doc, "no-such-drive", account)
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));

        // Get unknown node_id → NOT_FOUND; malformed node_id → INVALID_ARGUMENT.
        assertThatThrownBy(() -> documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(UUID.randomUUID().toString()).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
        assertThatThrownBy(() -> documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId("not-a-uuid").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        // Manifest without a coordinate → INVALID_ARGUMENT.
        assertThatThrownBy(() -> documents.getDocumentManifest(
                GetDocumentManifestRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }
}
