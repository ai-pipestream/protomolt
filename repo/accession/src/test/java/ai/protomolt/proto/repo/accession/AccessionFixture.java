package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import com.google.protobuf.Any;
import com.google.protobuf.Int64Value;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.DriverManager;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * Real PostgreSQL and LocalStack S3, shared by the slice tests. Each fixture gets
 * its own catalog database and bucket prefix, so tests never see each other's rows.
 */
final class AccessionFixture implements AutoCloseable {
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
            .withServices("s3");
    static final String BUCKET = "repo-v2-slice";
    static final String ISSUER = "test-issuer";
    private static final AtomicInteger DATABASES = new AtomicInteger();
    private static S3Client client;

    static synchronized void start() {
        if (client != null) return;
        POSTGRES.start();
        S3.start();
        client = S3Client.builder().endpointOverride(URI.create(S3.getEndpoint().toString()))
                .region(Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .httpClient(UrlConnectionHttpClient.create()).build();
        client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }

    final String url;
    final PostgresCatalog catalog;
    final CountingStack stack;
    final DocumentSaves.Drives drives;
    final DocumentSaves saves;
    final DocumentReads reads;
    final String prefix;

    AccessionFixture() throws Exception {
        start();
        url = database("catalog_" + DATABASES.incrementAndGet() + "_" + System.nanoTime());
        catalog = new PostgresCatalog(new CatalogConfig(url, POSTGRES.getUsername(), POSTGRES.getPassword(), 8));
        prefix = "slice/" + UUID.randomUUID();
        var stackId = catalog.registerStack(StackRecord.newBuilder().setStackId("stack-" + UUID.randomUUID())
                .setProvider("s3").setEndpoint(S3.getEndpoint().toString()).setNamespace(BUCKET + "/" + prefix).build());
        stack = new CountingStack(new BlobStoreStack(stackId, new S3BlobStore(client), BUCKET, prefix));
        drives = new DocumentSaves.Drives("default", Map.of("default", stack));
        saves = new DocumentSaves(catalog, drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC());
        reads = new DocumentReads(catalog, drives, saves.containerSchema());
    }

    /** A second host over the same catalog and stack, as after a restart. */
    AccessionFixture restarted() throws Exception { return new AccessionFixture(this); }

    private AccessionFixture(AccessionFixture original) {
        url = original.url;
        catalog = new PostgresCatalog(new CatalogConfig(url, POSTGRES.getUsername(), POSTGRES.getPassword(), 8));
        prefix = original.prefix;
        stack = new CountingStack(new BlobStoreStack(original.stack.stackId(), new S3BlobStore(client), BUCKET, prefix));
        drives = new DocumentSaves.Drives("default", Map.of("default", stack));
        saves = new DocumentSaves(catalog, drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC());
        reads = new DocumentReads(catalog, drives, saves.containerSchema());
    }

    static S3Client s3() { return client; }

    @Override public void close() { catalog.close(); }

    // ------------------------------------------------------------------ schemas

    static final String KEY_URL = "type.googleapis.com/" + EmbeddingSetKey.getDescriptor().getFullName();
    static final String INT64_URL = "type.googleapis.com/" + Int64Value.getDescriptor().getFullName();

    /** Registered: EmbeddingSetKey (with house rules) and Int64Value. Everything else is missing. */
    static DocumentSchemaAdmission.Resolver resolver() {
        var key = BuiltinDocumentSchema.definition(EmbeddingSetKey.getDescriptor());
        var int64 = int64Definition();
        return selection -> KEY_URL.equals(selection.typeUrl()) ? key
                : INT64_URL.equals(selection.typeUrl()) ? int64 : null;
    }

    private static DocumentSchemaAdmission.Definition int64Definition() {
        var closure = ai.protomolt.proto.descriptors.DescriptorFingerprints.closure(Int64Value.getDescriptor());
        var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl(INT64_URL)
                .setArtifactSha256(DocumentSaves.sha256(bytes))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(Int64Value.getDescriptor().getFullName())
                        .setDescriptorFingerprint(ai.protomolt.proto.descriptors.DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("well-known type compiled into protobuf-java")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("protobuf-java").setVersion("test")))
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    static DocumentSchemaAdmission.Limits limits() {
        return new DocumentSchemaAdmission.Limits(16, 64L * 1024 * 1024, 64, 4L * 1024 * 1024, 32,
                32L * 1024 * 1024, 64 * 1024 * 1024);
    }

    // ------------------------------------------------------------------ identities

    static RepositoryCaller processAuthority() { return new RepositoryCaller("intake-host", true); }

    /** A credential-bound caller in one account with READ and WRITE ACL identity. */
    static RepositoryCaller writer(String account, String name, UUID credential, long generation) {
        return new RepositoryCaller(name, false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity(name).build()),
                Optional.of(new RepositoryCredentialBinding(ISSUER, credential, generation)));
    }

    static DocumentSecurity security(String... writers) {
        var security = DocumentSecurity.newBuilder();
        for (var writer : writers) {
            security.addPermissions(AccessRule.newBuilder().setIdentityType("service").setIdentity(writer)
                    .setAccess(Access.ACCESS_READ));
            security.addPermissions(AccessRule.newBuilder().setIdentityType("service").setIdentity(writer)
                    .setAccess(Access.ACCESS_WRITE));
        }
        return security.build();
    }

    WriterGrant stepGrant(String account, UUID credential, String module, boolean create) {
        var grant = WriterGrant.newBuilder().setGrantId("grant-" + UUID.randomUUID()).setAccountId(account)
                .setCredential(WriterCredential.newBuilder().setIssuer(ISSUER).setCredentialId(credential.toString()))
                .setStep(StepWriterScope.newBuilder().setModuleId(module)).setAllowCreate(create).build();
        catalog.installGrant(grant);
        return grant;
    }

    WriterGrant intakeGrant(String account, UUID credential, String datasource, boolean create) {
        var grant = WriterGrant.newBuilder().setGrantId("grant-" + UUID.randomUUID()).setAccountId(account)
                .setCredential(WriterCredential.newBuilder().setIssuer(ISSUER).setCredentialId(credential.toString()))
                .setIntake(IntakeOrigin.newBuilder().setDatasourceId(datasource)).setAllowCreate(create).build();
        catalog.installGrant(grant);
        return grant;
    }

    // ------------------------------------------------------------------ documents

    static NodeAddress address(String account, String doc) {
        return NodeAddress.newBuilder().setDocId(doc).setGraphAddressId("ds-1").setAccountId(account)
                .setGraphId("intake:" + account).build();
    }

    static OwnershipContext ownership(String account, String... writers) {
        return OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("ds-1")
                .setSecurity(security(writers)).build();
    }

    static SaveOwner intakeOwner() {
        return SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-1")).build();
    }

    static SaveOwner stepOwner(String module) {
        return SaveOwner.newBuilder().setStep(WriteProvenance.newBuilder().setModuleId(module).setNodeId("node-1")
                .setGraphId("graph-1").setGraphVersion(3)).build();
    }

    /** A document with CORE, BLOBS and PARSED content and a typed structured_data payload. */
    static Document document(String account, String doc, String title, String... writers) {
        return Document.newBuilder().setDocId(doc).setOwnership(ownership(account, writers))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title).setBody("body of " + title))
                .setStructuredData(Any.pack(Int64Value.of(Long.MAX_VALUE)))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId("b1").setSizeBytes(Long.MAX_VALUE - 7).setData(
                        com.google.protobuf.ByteString.copyFromUtf8("raw bytes of " + title))))
                .putParserResults("tika", ParserResult.newBuilder().setParserName("tika").setParserVersion("1")
                        .setStatus(ParseStatus.PARSE_STATUS_OK).build())
                .build();
    }

    /**
     * What a read returns for a saved document: each raw blob's inline data is
     * replaced by the claim check the server bound it to (decision 45).
     */
    static Document claimChecked(Document sent, StoredManifest stored) {
        var refs = new java.util.HashMap<String, StackRef>();
        stored.getPartsList().forEach(part -> part.getBlobRefsList().forEach(ref -> refs.put(ref.getBlobId(), ref.getRef())));
        java.util.function.UnaryOperator<Blob> swap = blob -> blob.toBuilder().clearData()
                .setStorageRef(ai.protomolt.proto.repo.v1.FileStorageReference.newBuilder()
                        .setDriveName(refs.get(blob.getBlobId()).getStackId())
                        .setObjectKey(refs.get(blob.getBlobId()).getKey())).build();
        if (!sent.hasBlobBag()) return sent;
        var bag = sent.getBlobBag().toBuilder();
        if (bag.hasBlob()) bag.setBlob(swap.apply(bag.getBlob()));
        if (bag.hasBlobs()) {
            var blobs = ai.protomolt.proto.repo.v1.Blobs.newBuilder();
            bag.getBlobs().getBlobList().forEach(blob -> blobs.addBlob(swap.apply(blob)));
            bag.setBlobs(blobs);
        }
        return sent.toBuilder().setBlobBag(bag).build();
    }

    /** The raw bytes of a stored blob, read and verified through the stack. */
    byte[] blobBytes(StoredManifest stored, String blobId) throws IOException {
        var ref = stored.getPartsList().stream().flatMap(part -> part.getBlobRefsList().stream())
                .filter(blob -> blob.getBlobId().equals(blobId)).findFirst().orElseThrow().getRef();
        try (var stream = stack.open(ref)) { return stream.readAllBytes(); }
    }

    static PartWrite put(DocumentPart part) {
        return PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(part))
                .setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    static List<PartWrite> putAll() {
        return List.of(put(DocumentPart.DOCUMENT_PART_CORE), put(DocumentPart.DOCUMENT_PART_BLOBS),
                put(DocumentPart.DOCUMENT_PART_PARSED));
    }

    static SaveDocumentRequest.Builder create(String operation, NodeAddress address, Document document) {
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true)).setDocument(document)
                .addAllWrites(putAll()).setOwner(intakeOwner());
    }

    static ExpectedRevision at(long revision) { return ExpectedRevision.newBuilder().setRevision(revision).build(); }

    private static String database(String name) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.createStatement().execute("CREATE DATABASE " + name);
        }
        return POSTGRES.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + name + "$1");
    }

    /** Counts stack calls and lets a test act between an upload and its commit. */
    static final class CountingStack implements Stack {
        private final Stack delegate;
        final AtomicInteger puts = new AtomicInteger();
        final AtomicInteger opens = new AtomicInteger();
        final Map<String, Integer> opened = new ConcurrentHashMap<>();
        volatile ThrowingRunnable afterPut = () -> {};

        CountingStack(Stack delegate) { this.delegate = delegate; }

        @Override public String stackId() { return delegate.stackId(); }

        @Override public StackRef put(InputStream body, long length) throws IOException {
            var ref = delegate.put(body, length);
            puts.incrementAndGet();
            afterPut.run();
            return ref;
        }

        @Override public StackRef put(byte[] body) throws IOException {
            var ref = delegate.put(body);
            puts.incrementAndGet();
            afterPut.run();
            return ref;
        }

        @Override public InputStream open(StackRef ref) throws IOException {
            opens.incrementAndGet();
            opened.merge(ref.getKey(), 1, Integer::sum);
            return delegate.open(ref);
        }

        @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
            opens.incrementAndGet();
            opened.merge(ref.getKey(), 1, Integer::sum);
            return delegate.openRange(ref, offset, length);
        }

        @Override public long head(StackRef ref) throws IOException { return delegate.head(ref); }

        @Override public void delete(StackRef ref) throws IOException { delegate.delete(ref); }

        void reset() { puts.set(0); opens.set(0); opened.clear(); afterPut = () -> {}; }
    }

    @FunctionalInterface interface ThrowingRunnable { void run() throws IOException; }
}
