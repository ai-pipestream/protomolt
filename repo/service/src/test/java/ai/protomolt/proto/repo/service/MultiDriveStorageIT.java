package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.accession.*;
import ai.protomolt.proto.repo.blob.redis.RedisTestServers;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.inprocess.*;
import java.time.Clock;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import static org.assertj.core.api.Assertions.*;

/** Public parts saves/reads and administrator purge over real, separately selected providers. */
@Testcontainers
class MultiDriveStorageIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3_A = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    @Container static final LocalStackContainer S3_B = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    @Container static final GenericContainer<?> VALKEY = RedisTestServers.container()
            .withCommand(RedisTestServers.server(), "--appendonly", "yes");
    private static final ByteString RAW = ByteString.copyFromUtf8("retained source bytes across drives");

    @Test void twoS3EndpointsCarryForwardSurviveRestartAndReclaimAllVersions() throws Exception {
        exercise("s3", DriveProviderConfig.newBuilder().setS3(S3DriveConfig.newBuilder()
                .setEndpointOverride(S3_B.getEndpoint().toString()).setForcePathStyle(true)).build());
    }

    @Test void nonExpiringValkeyCarriesS3BlobsAndReclaimsItsOwnObjects() throws Exception {
        exercise("valkey", DriveProviderConfig.newBuilder().setRedis(RedisDriveConfig.newBuilder()
                .setUri("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379) + "/0")
                .setKeyPrefix("mesh:" + UUID.randomUUID()).setTtlSeconds(0).setMaxObjectBytes(1024 * 1024)).build());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"s3", "valkey"})
    void existingHostDefaultStackKeepsItsRetainedIdentityAcrossRestart(String provider) throws Exception {
        String account = "legacy-" + UUID.randomUUID();
        String bucket = "legacy-" + UUID.randomUUID();
        var address = address(account, "legacy");
        var host = config(provider, "redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379) + "/0");
        StackRecord retained;
        try (var services = RepoServices.build(host)) {
            String name = "legacy-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var drive = DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                        .setAccountId(account).setName("intake").setBucket(bucket).setPrefix("intake").build()).getDrive();
                retained = StackRecord.newBuilder().setStackId("stack-" + drive.getDriveId()).setProvider(provider.equals("s3") ? "s3" : "redis")
                        .setEndpoint(host.s3Endpoint()).setNamespace(bucket + "/intake/documents").build();
                services.v2Composition().catalog().registerStack(retained);
                DocumentServiceGrpc.newBlockingStub(channel).saveDocument(save(address, "legacy", "intake", 0,
                        core(account, "legacy").toBuilder()).build());
                assertThat(part(services.v2Composition().catalog().manifest(address, 1).orElseThrow(),
                        DocumentPart.DOCUMENT_PART_CORE).getRef().getStackId()).isEqualTo(retained.getStackId());
            } finally { channel.shutdownNow(); }
        }
        try (var services = RepoServices.build(host)) {
            String name = "legacy-restart-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                assertThat(DocumentServiceGrpc.newBlockingStub(channel).getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(address).build()).getDocument().getSearchMetadata().getTitle()).isEqualTo("legacy");
                assertThat(services.v2Composition().catalog().stack(retained.getStackId())).contains(retained);
                // Even if the desired namespace already exists under another ID,
                // an existing stack ID can never be rebound to it.
                var other = retained.toBuilder().setStackId("other-" + UUID.randomUUID()).setEndpoint("redis://elsewhere:6379").build();
                services.v2Composition().catalog().registerStack(other);
                assertThatThrownBy(() -> services.v2Composition().catalog().registerStack(
                        other.toBuilder().setStackId(retained.getStackId()).build()))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("another namespace");
            } finally { channel.shutdownNow(); }
        }
    }

    @Test void equivalentRedisValkeyUrisShareOneStackAndProtectSharedRawBytes() throws Exception {
        String account = "alias-" + UUID.randomUUID();
        String bucket = "alias-" + UUID.randomUUID();
        String uri = "redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379);
        var configA = DriveProviderConfig.newBuilder().setRedis(RedisDriveConfig.newBuilder().setUri(uri)
                .setKeyPrefix("aliases")).build();
        var configB = configA.toBuilder().setRedis(configA.getRedis().toBuilder().setUri(uri + "/00")).build();
        var parent = address(account, "parent"); var child = address(account, "child");
        try (var services = RepoServices.build(config()); var probe = probe("valkey", configA)) {
            String name = "aliases-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var drives = DriveServiceGrpc.newBlockingStub(channel);
                for (var entry : Map.of("valkey", configA, "redis", configB).entrySet())
                    drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName(entry.getKey())
                            .setProvider(entry.getKey()).setBucket(bucket).setPrefix("same").setProviderConfig(entry.getValue()).build());
                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                docs.saveDocument(save(parent, "alias-parent", "valkey", 0, core(account, "parent").toBuilder()
                        .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId("source").setData(RAW))))
                        .addWrites(write(DocumentPart.DOCUMENT_PART_BLOBS)).build());
                docs.saveDocument(save(child, "alias-child", "redis", 0, core(account, "child").toBuilder())
                        .addSources(PartSource.newBuilder().setSelector(selector(DocumentPart.DOCUMENT_PART_BLOBS))
                                .setFromAddress(parent).setFromRevision(1)).build());
                var a = services.v2Composition().catalog().manifest(parent, 1).orElseThrow();
                var b = services.v2Composition().catalog().manifest(child, 1).orElseThrow();
                assertThat(part(a, DocumentPart.DOCUMENT_PART_CORE).getRef().getStackId())
                        .isEqualTo(part(b, DocumentPart.DOCUMENT_PART_CORE).getRef().getStackId());
                var source = part(a, DocumentPart.DOCUMENT_PART_BLOBS);
                var carried = part(b, DocumentPart.DOCUMENT_PART_BLOBS);
                assertThat(carried.getRef()).isEqualTo(source.getRef());
                assertThat(carried.getBlobRefsList().stream().map(ref -> ref.toBuilder().clearWritten().build()).toList())
                        .isEqualTo(source.getBlobRefsList().stream().map(ref -> ref.toBuilder().clearWritten().build()).toList());
                assertRead(docs, child, "child");
                try (var purge = new PurgeEndpoint(services, account)) {
                    assertThat(purge.stub.purgeDocument(PurgeDocumentRequest.newBuilder().setAddress(parent)
                            .setPurgeId("alias-parent-" + account).build()).getObjectsRetainedShared()).isGreaterThan(0);
                    assertRead(docs, child, "child");
                    purge.stub.purgeDocument(PurgeDocumentRequest.newBuilder().setAddress(child)
                            .setPurgeId("alias-child-" + account).build());
                }
                assertThat(probe.store().list(bucket, "same/documents/")).isEmpty();
                // Cross-host concurrent first registration also chooses one alias row.
                var base = StackRecord.newBuilder().setEndpoint(uri + "/0").setNamespace("race-" + UUID.randomUUID());
                try (var peer = RepoServices.build(config()); var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
                    var start = new java.util.concurrent.CountDownLatch(1);
                    var one = executor.submit(() -> { start.await(); return services.v2Composition().catalog().registerStack(
                            base.clone().setStackId("race-a-" + UUID.randomUUID()).setProvider("valkey").build()); });
                    var two = executor.submit(() -> { start.await(); return peer.v2Composition().catalog().registerStack(
                            base.clone().setStackId("race-b-" + UUID.randomUUID()).setProvider("redis").build()); });
                    start.countDown();
                    assertThat(one.get(10, java.util.concurrent.TimeUnit.SECONDS))
                            .isEqualTo(two.get(10, java.util.concurrent.TimeUnit.SECONDS));
                }
            } finally { channel.shutdownNow(); }
        }
    }

    @Test void oversizedNewStorageIdentityIsRefusedBeforeCatalogRegistrationOrBytes() throws Exception {
        String account = "identity-" + UUID.randomUUID();
        byte[] random = new byte[1800]; new java.util.Random(775).nextBytes(random);
        String prefix = Base64.getUrlEncoder().withoutPadding().encodeToString(random);
        var provider = DriveProviderConfig.newBuilder().setRedis(RedisDriveConfig.newBuilder()
                .setUri("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379)).setKeyPrefix(prefix)).build();
        try (var services = RepoServices.build(config())) {
            String name = "identity-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var request = CreateDriveRequest.newBuilder().setAccountId(account).setName("oversized")
                        .setProvider("valkey").setBucket("identity-bucket").setPrefix("documents").setProviderConfig(provider).build();
                assertThatThrownBy(() -> DriveServiceGrpc.newBlockingStub(channel).createDrive(request))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                error -> assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
                assertThat(services.driveLedger().findByName(account, "oversized")).isEmpty();
                // A previously imported invalid drive is rejected through the canonical save admission too.
                var drive = new ai.protomolt.proto.repo.container.ledger.DriveRecord();
                drive.driveId = UUID.randomUUID(); drive.accountId = account; drive.name = "imported";
                drive.provider = "valkey"; drive.bucket = "identity-bucket"; drive.prefix = "documents";
                drive.driveType = "CUSTOM"; drive.status = "ACTIVE"; drive.writeProviderConfig(provider);
                services.driveLedger().insert(drive);
                var save = save(address(account, "rejected"), "oversized", "imported", 0, core(account, "rejected").toBuilder()).build();
                assertThatThrownBy(() -> DocumentServiceGrpc.newBlockingStub(channel).saveDocument(save))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                            var status = io.grpc.protobuf.StatusProto.fromThrowable(failure);
                            assertThat(status.getDetailsList()).anySatisfy(detail -> {
                                try { assertThat(detail.unpack(SaveRejection.class).getReason())
                                        .isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED); }
                                catch (com.google.protobuf.InvalidProtocolBufferException invalid) { throw new AssertionError(invalid); }
                            });
                        });
                assertThat(services.v2Composition().catalog().stack("stack-" + drive.driveId)).isEmpty();
                assertThat(services.v2Composition().catalog().current(address(account, "rejected"))).isEmpty();
                try (var physical = probe("valkey", provider)) { assertThat(physical.store().list(drive.bucket, "documents/")).isEmpty(); }
            } finally { channel.shutdownNow(); }
        }
    }

    private void exercise(String provider, DriveProviderConfig secondary) throws Exception {
        String account = "mesh-" + UUID.randomUUID();
        String bucket = "mesh-" + UUID.randomUUID();
        var parent = address(account, "parent");
        var child = address(account, "child");
        List<StackRef> physical;
        String firstStack;
        String secondStack;
        try (var clientA = s3(S3_A); var clientB = s3(S3_B);
                var primaryProbe = ai.protomolt.proto.repo.blob.spi.BlobStores.discover().open("s3", RepoStorageWiring.s3Options(config()));
                var secondaryProbe = probe(provider, secondary)) {
            try (var services = RepoServices.build(config())) {
                String name = "mesh-" + UUID.randomUUID(); services.startInProcess(name);
                var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
                try {
                    var drives = DriveServiceGrpc.newBlockingStub(channel);
                    drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("intake")
                            .setBucket(bucket).setPrefix("intake").build());
                    drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("other")
                            .setBucket(bucket).setPrefix("other").setProvider(provider).setProviderConfig(secondary)
                            .setCredentialsRef(provider.equals("s3") ? "host:default" : "").build());
                    clientA.putBucketVersioning(r -> r.bucket(bucket)
                            .versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
                    if (provider.equals("s3")) clientB.putBucketVersioning(r -> r.bucket(bucket)
                            .versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
                    var docs = DocumentServiceGrpc.newBlockingStub(channel);
                    var initial = save(parent, "first", "intake", 0, core(account, "first").toBuilder()
                            .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId("source")
                                    .setFilename("source.txt").setMimeType("text/plain").setData(RAW))))
                            .addWrites(write(DocumentPart.DOCUMENT_PART_BLOBS)).build();
                    docs.saveDocument(initial);
                    var first = services.v2Composition().catalog().manifest(parent, 1).orElseThrow();
                    docs.saveDocument(save(parent, "second", "other", 1, core(account, "second").toBuilder()).build());
                    var second = services.v2Composition().catalog().manifest(parent, 2).orElseThrow();
                    var firstCore = part(first, DocumentPart.DOCUMENT_PART_CORE);
                    var secondCore = part(second, DocumentPart.DOCUMENT_PART_CORE);
                    firstStack = firstCore.getRef().getStackId(); secondStack = secondCore.getRef().getStackId();
                    assertThat(secondStack).isNotEqualTo(firstStack);
                    assertThat(part(second, DocumentPart.DOCUMENT_PART_BLOBS))
                            .isEqualTo(part(first, DocumentPart.DOCUMENT_PART_BLOBS));
                    docs.saveDocument(save(child, "child", "other", 0, core(account, "child").toBuilder())
                            .addSources(PartSource.newBuilder().setSelector(selector(DocumentPart.DOCUMENT_PART_BLOBS))
                                    .setFromAddress(parent).setFromRevision(2)).build());
                    assertRead(docs, parent, "second");
                    assertRead(docs, child, "child");
                    physical = new ArrayList<>();
                    for (var manifest : List.of(first, second, services.v2Composition().catalog().manifest(child, 1).orElseThrow()))
                        for (var part : manifest.getPartsList()) {
                            if (part.hasRef()) physical.add(part.getRef());
                            for (var blob : part.getBlobRefsList()) physical.add(blob.getRef());
                        }
                    physical = physical.stream().distinct().toList();
                    // Duplicate an exact key to prove physical purge removes old versions,
                    // not only a current object or an S3 delete marker.
                    clientA.putObject(r -> r.bucket(bucket).key(firstCore.getRef().getKey()),
                            software.amazon.awssdk.core.sync.RequestBody.fromBytes("old-key-version".getBytes()));
                    // Restore the retained bytes as another version so historical reads remain valid.
                    var versions = clientA.listObjectVersions(r -> r.bucket(bucket).prefix(firstCore.getRef().getKey()));
                    var original = versions.versions().stream().filter(v -> !v.isLatest()).findFirst().orElseThrow();
                    var originalBytes = clientA.getObjectAsBytes(r -> r.bucket(bucket).key(original.key()).versionId(original.versionId()));
                    clientA.putObject(r -> r.bucket(bucket).key(firstCore.getRef().getKey()),
                            software.amazon.awssdk.core.sync.RequestBody.fromBytes(originalBytes.asByteArray()));
                } finally { channel.shutdownNow(); }
            }
            // Fresh host resolves retained stack IDs from the drive ledger, with an empty pool/cache.
            try (var services = RepoServices.build(config())) {
                String name = "mesh-restart-" + UUID.randomUUID(); services.startInProcess(name);
                var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
                try {
                    var docs = DocumentServiceGrpc.newBlockingStub(channel);
                    assertRead(docs, parent, "second");
                    assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(parent)
                            .setRevision(RevisionSelector.newBuilder().setRevision(1)).build())
                            .getDocument().getSearchMetadata().getTitle()).isEqualTo("first");
                    assertStackIdentity(firstStack, "s3", S3_A.getEndpoint().toString());
                    assertStackIdentity(secondStack, provider, provider.equals("s3")
                            ? S3_B.getEndpoint().toString()
                            : "redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379) + "/0");
                    docs.deleteDocument(DeleteDocumentRequest.newBuilder().setAddress(parent).setOperationId("delete-" + account)
                            .setExpected(ExpectedRevision.newBuilder().setRevision(2)).build());
                    // Tombstoning retains history and every object.
                    for (var ref : physical) assertThat((long) (ref.getStackId().equals(firstStack) ? primaryProbe : secondaryProbe).store().get(bucket, ref.getKey(), null).data().length)
                            .isEqualTo(ref.getLength());
                    try (var purge = new PurgeEndpoint(services, account)) {
                        var result = purge.stub.purgeDocument(PurgeDocumentRequest.newBuilder().setAddress(parent)
                                .setPurgeId("purge-parent-" + account).build());
                        assertThat(result.getObjectsRetainedShared()).isGreaterThan(0);
                        assertRead(docs, child, "child");
                        purge.stub.purgeDocument(PurgeDocumentRequest.newBuilder().setAddress(child)
                                .setPurgeId("purge-child-" + account).build());
                    }
                    for (var ref : physical) assertThatThrownBy(() ->
                            (ref.getStackId().equals(firstStack) ? primaryProbe : secondaryProbe).store().get(bucket, ref.getKey(), null))
                            .isInstanceOf(ai.protomolt.proto.repo.blob.spi.BlobStore.BlobNotFoundException.class);
                    assertThat(clientA.listObjectVersions(r -> r.bucket(bucket)).versions()).isEmpty();
                    assertThat(clientA.listObjectVersions(r -> r.bucket(bucket)).deleteMarkers()).isEmpty();
                    if (provider.equals("s3")) {
                        assertThat(clientB.listObjectVersions(r -> r.bucket(bucket)).versions()).isEmpty();
                        assertThat(clientB.listObjectVersions(r -> r.bucket(bucket)).deleteMarkers()).isEmpty();
                    }
                } finally { channel.shutdownNow(); }
            }
        }
    }

    private static ai.protomolt.proto.repo.blob.spi.OpenedBlobStore probe(String provider, DriveProviderConfig config) {
        Map<String, String> options;
        if (provider.equals("s3")) {
            options = new HashMap<>(RepoStorageWiring.s3Options(config()));
            options.put("endpoint", config.getS3().getEndpointOverride());
        } else {
            var redis = config.getRedis();
            options = Map.of("uri", redis.getUri(), "ttl-seconds", "0", "key-prefix", redis.getKeyPrefix(),
                    "max-object-bytes", Long.toString(redis.getMaxObjectBytes()), "write-policy", "replace");
        }
        return ai.protomolt.proto.repo.blob.spi.BlobStores.discover().open(provider, options);
    }

    private static void assertRead(DocumentServiceGrpc.DocumentServiceBlockingStub docs, NodeAddress address, String title) {
        var document = docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument();
        assertThat(document.getSearchMetadata().getTitle()).isEqualTo(title);
        var bytes = new java.io.ByteArrayOutputStream();
        docs.readBlob(ReadBlobRequest.newBuilder().setAddress(address).setBlobId("source").build())
                .forEachRemaining(frame -> { if (frame.hasData()) bytes.writeBytes(frame.getData().toByteArray()); });
        assertThat(bytes.toByteArray()).isEqualTo(RAW.toByteArray());
    }
    private static void assertStackIdentity(String id, String provider, String endpoint) throws Exception {
        try (var db = java.sql.DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = db.prepareStatement("SELECT provider, endpoint FROM repo_catalog.catalog_stacks WHERE stack_id = ?")) {
            query.setString(1, id);
            try (var rows = query.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo(provider); assertThat(rows.getString(2)).isEqualTo(endpoint);
            }
        }
    }
    private static StoredPartEntry part(StoredManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream().filter(p -> p.getEntry().getSelector().getPart() == part).findFirst().orElseThrow();
    }
    private static PartSelector selector(DocumentPart part) { return PartSelector.newBuilder().setPart(part).build(); }
    private static PartWrite write(DocumentPart part) {
        return PartWrite.newBuilder().setSelector(selector(part)).setAction(WriteAction.WRITE_ACTION_PUT).build();
    }
    private static NodeAddress address(String account, String graphAddress) {
        return NodeAddress.newBuilder().setAccountId(account).setGraphId("intake:" + account)
                .setGraphAddressId(graphAddress).setDocId("document").build();
    }
    private static Document core(String account, String title) {
        return Document.newBuilder().setDocId("document").setOwnership(OwnershipContext.newBuilder()
                .setAccountId(account).setDatasourceId("source").setSecurity(DocumentSecurity.newBuilder()
                        .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title)).build();
    }
    private static SaveDocumentRequest.Builder save(NodeAddress address, String operation, String drive, long revision,
            Document.Builder document) {
        return SaveDocumentRequest.newBuilder().setOperationId(operation + "-" + address.getAccountId())
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(revision == 0 ? ExpectedRevision.newBuilder().setNone(true) : ExpectedRevision.newBuilder().setRevision(revision))
                .setDocument(document).setDrive(drive).addWrites(write(DocumentPart.DOCUMENT_PART_CORE))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("source")));
    }
    private static RepoServiceConfig config() { return config(null, null); }
    private static RepoServiceConfig config(String provider, String redisUri) {
        return new RepoServiceConfig(0, new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3_A.getEndpoint().toString(), S3_A.getRegion(), S3_A.getAccessKey(), S3_A.getSecretKey(), "mesh", 0,
                provider, redisUri, 0, 0, false, 0, 0, false, true, 0, null, null, null, null, null, null,
                false, ManagedStoragePolicy.disabled(), 0);
    }
    private static S3Client s3(LocalStackContainer container) {
        return S3Client.builder().endpointOverride(container.getEndpoint()).region(Region.of(container.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey())))
                .forcePathStyle(true).build();
    }
    /** Same public gRPC implementation with an explicit test administrator authority. Production defaults deny purge. */
    private static final class PurgeEndpoint implements AutoCloseable {
        final Server server; final ManagedChannel channel; final DocumentServiceGrpc.DocumentServiceBlockingStub stub;
        PurgeEndpoint(RepoServices services, String account) throws Exception {
            var v2 = services.v2Composition(); var clock = Clock.systemUTC();
            var saves = new DocumentSaves(v2.catalog(), v2.drives(), v2.resolver(), v2.limits(), ReadByteBudget.forSaves(64 * 1024 * 1024), clock);
            var service = new AccessionGrpcService(saves, new DocumentUploads(saves),
                    new DocumentReads(v2.catalog(), v2.drives(), v2.limits(), new ReadByteBudget(64 * 1024 * 1024), clock), null, null,
                    new DocumentPurges(v2.catalog(), v2.drives(), (caller, tenant) -> new PurgeAuthorization.Decision(true, true), clock),
                    auth -> new RepositoryCaller("mesh-admin", false, Set.of(account), Set.of()));
            var interceptor = new ServerInterceptor() {
                public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
                    return Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER,
                            new Caller("mesh-admin", Set.of(), false)), call, headers, next);
                }
            };
            String name = "purge-" + UUID.randomUUID();
            server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(ServerInterceptors.intercept(service, interceptor)).build().start();
            channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            stub = DocumentServiceGrpc.newBlockingStub(channel);
        }
        public void close() { channel.shutdownNow(); server.shutdownNow(); }
    }
}
