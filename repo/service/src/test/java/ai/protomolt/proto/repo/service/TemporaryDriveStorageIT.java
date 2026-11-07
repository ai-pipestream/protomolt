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
import ai.protomolt.proto.repo.v2.DocumentEvent;
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

/**
 * Explicit temporary Redis/Valkey drives over real providers: positive-TTL drives save,
 * carry mixed references, promote to durable S3 and purge on the public gRPC surface;
 * differing-TTL and durable configurations cannot alias physical keys; expired bytes
 * fail exact reads as DATA_LOSS; retention-qualified hosts refuse temporary drives.
 */
@Testcontainers
class TemporaryDriveStorageIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    @Container static final GenericContainer<?> VALKEY = new GenericContainer<>(RedisTestServers.image())
            .withExposedPorts(6379).withCommand(RedisTestServers.server(), "--appendonly", "yes");
    private static final ByteString RAW = ByteString.copyFromUtf8("temporary drive source bytes");

    @Test
    void temporaryDriveSavesMixedCarryRefusesDurableInheritanceAndPromotesToS3() throws Exception {
        String account = "temp-" + UUID.randomUUID();
        String bucket = "temp-" + UUID.randomUUID();
        var address = address(account, "doc");
        var cache = redis(account, 300);
        try (var services = RepoServices.build(config())) {
            String name = "temp-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var drives = DriveServiceGrpc.newBlockingStub(channel);
                drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("base")
                        .setBucket(bucket).setPrefix("base").build());
                drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("cache")
                        .setProvider("valkey").setBucket(bucket).setPrefix("cache").setProviderConfig(cache).build());
                drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("durable")
                        .setProvider("s3").setBucket(bucket).setPrefix("durable").build());
                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                var s3 = s3();
                // Immediate save and hydrate on the temporary drive.
                docs.saveDocument(save(address, "one", "base", 0, core(account, "one").toBuilder()
                        .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId("source").setData(RAW))))
                        .addWrites(write(DocumentPart.DOCUMENT_PART_BLOBS)).build());
                docs.saveDocument(save(address, "two", "cache", 1, core(account, "two").toBuilder()).build());
                assertRead(docs, address, "two");
                var second = services.v2Composition().catalog().manifest(address, 2).orElseThrow();
                var secondCore = part(second, DocumentPart.DOCUMENT_PART_CORE);
                var carriedBlobs = part(second, DocumentPart.DOCUMENT_PART_BLOBS);
                assertThat(secondCore.getRef().getStackId()).isNotEqualTo(carriedBlobs.getRef().getStackId());
                // An ordinary durable save cannot inherit the temporary CORE reference.
                var refused = SaveDocumentRequest.newBuilder().setOperationId("three-" + account)
                        .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(2))
                        .setDocument(Document.newBuilder().setDocId("document").setBlobBag(BlobBag.newBuilder()
                                .setBlob(Blob.newBuilder().setBlobId("source").setData(RAW))))
                        .addWrites(write(DocumentPart.DOCUMENT_PART_BLOBS)).setDrive("durable")
                        .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("source")))
                        .build();
                assertThatThrownBy(() -> docs.saveDocument(refused)).isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    var status = io.grpc.protobuf.StatusProto.fromThrowable(failure);
                    assertThat(status.getDetailsList()).anySatisfy(detail -> {
                        try {
                            var rejection = detail.unpack(SaveRejection.class);
                            assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
                            assertThat(rejection.getMessage()).contains("STORAGE_PLACEMENT_SELECTED_DRIVE");
                        } catch (com.google.protobuf.InvalidProtocolBufferException invalid) { throw new AssertionError(invalid); }
                    });
                });
                assertThat(services.v2Composition().catalog().current(address).orElseThrow().revision()).isEqualTo(2);
                assertThat(s3.listObjectsV2(r -> r.bucket(bucket).prefix("durable/")).contents()).isEmpty();
                // Explicit placement copies every temporary object to S3 and keeps history.
                var promotion = SaveDocumentRequest.newBuilder().setOperationId("promotion-" + account)
                        .setTarget(DocumentTarget.newBuilder().setAddress(address))
                        .setExpected(ExpectedRevision.newBuilder().setRevision(2)).setDrive("durable")
                        .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("source")))
                        .setStoragePlacement(StoragePlacement.STORAGE_PLACEMENT_SELECTED_DRIVE).build();
                var saved = docs.saveDocument(promotion);
                assertThat(saved.getAccession().getRevision()).isEqualTo(3);
                var promoted = services.v2Composition().catalog().manifest(address, 3).orElseThrow();
                assertThat(services.v2Composition().catalog().manifest(address, 2)).contains(second);
                assertThat(promoted.getValidation().getPartSchemasList())
                        .isEqualTo(second.getValidation().getPartSchemasList());
                var durableStack = part(promoted, DocumentPart.DOCUMENT_PART_CORE).getRef().getStackId();
                assertThat(durableStack).isNotEqualTo(secondCore.getRef().getStackId());
                for (var storedPart : promoted.getPartsList()) {
                    assertThat(storedPart.getRef().getStackId()).isEqualTo(durableStack);
                    for (var blob : storedPart.getBlobRefsList())
                        assertThat(blob.getRef().getStackId()).isEqualTo(durableStack);
                }
                assertThat(s3.listObjectsV2(r -> r.bucket(bucket).prefix("durable/")).contents()).hasSize(3);
                assertRead(docs, address, "two");
                // Placement to a temporary destination is refused before copying.
                assertThatThrownBy(() -> docs.saveDocument(promotion.toBuilder()
                        .setOperationId("promotion-down-" + account).setExpected(at(3)).setDrive("cache").build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                            assertThat(failure.getMessage()).contains("temporary drive");
                        });
                assertThat(services.v2Composition().catalog().current(address).orElseThrow().revision()).isEqualTo(3);
            } finally { channel.shutdownNow(); }
        }
    }

    @Test
    void temporaryDriveSurvivesRestartAndPurgesBeforeExpiry() throws Exception {
        String account = "restart-" + UUID.randomUUID();
        String bucket = "restart-" + UUID.randomUUID();
        var address = address(account, "doc");
        try (var services = RepoServices.build(config())) {
            String name = "restart-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var drives = DriveServiceGrpc.newBlockingStub(channel);
                drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("cache")
                        .setProvider("valkey").setBucket(bucket).setPrefix("cache")
                        .setProviderConfig(redis(account, 600)).build());
                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                docs.saveDocument(save(address, "one", "cache", 0, core(account, "one").toBuilder()).build());
                docs.saveDocument(save(address, "two", "cache", 1, core(account, "two").toBuilder()).build());
            } finally { channel.shutdownNow(); }
        }
        try (var probe = probe(redis(account, 600).getRedis(), 600);
                var services = RepoServices.build(config());
                var purge = new PurgeEndpoint(services, account)) {
            String name = "restart-again-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument()
                        .getSearchMetadata().getTitle()).isEqualTo("two");
                assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address)
                        .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()).getDocument()
                        .getSearchMetadata().getTitle()).isEqualTo("one");
                var manifest = services.v2Composition().catalog().manifest(address, 2).orElseThrow();
                for (var storedPart : manifest.getPartsList())
                    assertThat(probe.store().get(bucket, storedPart.getRef().getKey(), null).data().length)
                            .isEqualTo(storedPart.getRef().getLength());
                // Tombstone and purge before expiry: reference-aware deletion over the real reclaimer.
                docs.deleteDocument(DeleteDocumentRequest.newBuilder().setAddress(address).setOperationId("delete")
                        .setExpected(at(2)).build());
                var request = PurgeDocumentRequest.newBuilder().setAddress(address).setPurgeId("purge-" + account).build();
                var purged = purge.stub.purgeDocument(request);
                assertThat(purge.stub.purgeDocument(request)).isEqualTo(purged);
                assertThat(services.v2Composition().catalog().events(address).stream()
                        .filter(DocumentEvent::hasPurgeCompleted)).hasSize(1);
                assertThat(probe.store().list(bucket, "cache/documents/")).isEmpty();
            } finally { channel.shutdownNow(); }
        }
    }

    @Test
    void samePrefixDifferingTtlConfigurationsCannotAliasPhysicalKeys() throws Exception {
        String account = "alias-" + UUID.randomUUID();
        String bucket = "alias-" + UUID.randomUUID();
        String keyPrefix = "shared:" + UUID.randomUUID();
        var snap = RedisDriveConfig.newBuilder().setUri(valkeyUri()).setTtlSeconds(2)
                .setMaxObjectBytes(1024 * 1024).setKeyPrefix(keyPrefix).build();
        var keep = snap.toBuilder().setTtlSeconds(0).build();
        var snapAddress = address(account, "snap");
        var keepAddress = address(account, "keep");
        try (var snapProbe = probe(snap, 2); var keepProbe = probe(keep, 0);
                var services = RepoServices.build(config())) {
            String name = "alias-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var drives = DriveServiceGrpc.newBlockingStub(channel);
                drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("snap")
                        .setProvider("valkey").setBucket(bucket).setPrefix("same")
                        .setProviderConfig(DriveProviderConfig.newBuilder().setRedis(snap)).build());
                drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("keep")
                        .setProvider("valkey").setBucket(bucket).setPrefix("same")
                        .setProviderConfig(DriveProviderConfig.newBuilder().setRedis(keep)).build());
                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                docs.saveDocument(save(snapAddress, "snap", "snap", 0, core(account, "snap").toBuilder()).build());
                docs.saveDocument(save(keepAddress, "keep", "keep", 0, core(account, "keep").toBuilder()).build());
                var snapManifest = services.v2Composition().catalog().manifest(snapAddress, 1).orElseThrow();
                var keepManifest = services.v2Composition().catalog().manifest(keepAddress, 1).orElseThrow();
                var snapStack = part(snapManifest, DocumentPart.DOCUMENT_PART_CORE).getRef().getStackId();
                var keepStack = part(keepManifest, DocumentPart.DOCUMENT_PART_CORE).getRef().getStackId();
                assertThat(snapStack).isNotEqualTo(keepStack);
                var snapRecord = services.v2Composition().catalog().stack(snapStack).orElseThrow();
                var keepRecord = services.v2Composition().catalog().stack(keepStack).orElseThrow();
                assertThat(snapRecord.getNamespace()).isNotEqualTo(keepRecord.getNamespace());
                // Same bucket, same stack prefix, same configured key prefix: the physical
                // key sets are still disjoint, and neither handle can see the other's bytes.
                var snapKeys = snapProbe.store().list(bucket, "same/documents/").stream().map(o -> o.key()).toList();
                var keepKeys = keepProbe.store().list(bucket, "same/documents/").stream().map(o -> o.key()).toList();
                assertThat(snapKeys).isNotEmpty();
                assertThat(keepKeys).isNotEmpty();
                assertThat(snapKeys).doesNotContainAnyElementsOf(keepKeys);
                assertThatThrownBy(() -> keepProbe.store().get(bucket, snapKeys.getFirst(), null))
                        .isInstanceOf(ai.protomolt.proto.repo.blob.spi.BlobStore.BlobNotFoundException.class);
                // The durable bytes survive the temporary drive's expiry.
                Thread.sleep(3500);
                assertThat(snapProbe.store().list(bucket, "same/documents/")).isEmpty();
                assertThat(keepProbe.store().list(bucket, "same/documents/")).hasSize(keepKeys.size());
                assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(keepAddress).build()).getDocument()
                        .getSearchMetadata().getTitle()).isEqualTo("keep");
                assertThatThrownBy(() -> docs.getDocument(GetDocumentRequest.newBuilder().setAddress(snapAddress).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
            } finally { channel.shutdownNow(); }
        }
    }

    @Test
    void expiredTemporaryBytesFailExactReadsAsDataLossAndPurgeAfterExpiry() throws Exception {
        String account = "expiry-" + UUID.randomUUID();
        String bucket = "expiry-" + UUID.randomUUID();
        var address = address(account, "doc");
        var shortLived = redis(account, 2);
        try (var services = RepoServices.build(config())) {
            String name = "expiry-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var drives = DriveServiceGrpc.newBlockingStub(channel);
                drives.createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("short")
                        .setProvider("valkey").setBucket(bucket).setPrefix("short")
                        .setProviderConfig(shortLived).build());
                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                docs.saveDocument(save(address, "one", "short", 0, core(account, "one").toBuilder()
                        .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId("source").setData(RAW))))
                        .addWrites(write(DocumentPart.DOCUMENT_PART_BLOBS)).build());
                docs.saveDocument(save(address, "two", "short", 1, core(account, "two").toBuilder()).build());
                assertRead(docs, address, "two");
            } finally { channel.shutdownNow(); }
        }
        try (var probe = probe(shortLived.getRedis(), 2);
                var services = RepoServices.build(config());
                var purge = new PurgeEndpoint(services, account)) {
            String name = "expiry-after-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                Thread.sleep(3500); // Both revisions' objects carried a 2-second TTL.
                assertThat(probe.store().list(bucket, "short/documents/")).isEmpty();
                // Expired exact and current reads fail DATA_LOSS: no empty part, no substitute.
                assertThatThrownBy(() -> docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
                assertThatThrownBy(() -> docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address)
                        .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
                // The manifests remain; their bytes are unreadable. Delete and purge after
                // expiry succeed: the reclaimer treats expired objects as physically absent.
                docs.deleteDocument(DeleteDocumentRequest.newBuilder().setAddress(address).setOperationId("delete")
                        .setExpected(at(2)).build());
                var request = PurgeDocumentRequest.newBuilder().setAddress(address).setPurgeId("purge-" + account).build();
                var purged = purge.stub.purgeDocument(request);
                assertThat(purged.getObjectsToDelete() + purged.getObjectsRetainedShared()).isGreaterThan(0);
                assertThat(purge.stub.purgeDocument(request)).isEqualTo(purged);
                assertThat(services.v2Composition().catalog().events(address).stream()
                        .filter(DocumentEvent::hasPurgeCompleted)).hasSize(1);
            } finally { channel.shutdownNow(); }
        }
    }

    @Test
    void retentionQualifiedHostRefusesTemporaryDriveProvisioningAndSaves() throws Exception {
        String account = "qualified-" + UUID.randomUUID();
        var qualified = config(new ManagedStoragePolicy("generation-1", "realm-1", true), true);
        try (var services = RepoServices.build(qualified)) {
            String name = "qualified-" + UUID.randomUUID(); services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var drives = DriveServiceGrpc.newBlockingStub(channel);
                var request = CreateDriveRequest.newBuilder().setAccountId(account).setName("cache")
                        .setProvider("valkey").setBucket("qualified-" + UUID.randomUUID()).setPrefix("cache")
                        .setProviderConfig(redis(account, 60)).build();
                assertThatThrownBy(() -> drives.createDrive(request))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                                assertThat(failure.getMessage()).contains("expiring drives"));
                // A drive row inserted beside the gate is refused by the save path too.
                var drive = new ai.protomolt.proto.repo.container.ledger.DriveRecord();
                drive.driveId = UUID.randomUUID(); drive.accountId = account; drive.name = "imported";
                drive.provider = "valkey"; drive.bucket = "qualified-" + UUID.randomUUID(); drive.prefix = "cache";
                drive.driveType = "CUSTOM"; drive.status = "ACTIVE";
                drive.writeProviderConfig(DriveProviderConfig.newBuilder().setRedis(redis(account, 60).getRedis()).build());
                services.driveLedger().insert(drive);
                var save = save(address(account, "rejected"), "rejected", "imported", 0,
                        core(account, "rejected").toBuilder()).build();
                assertThatThrownBy(() -> DocumentServiceGrpc.newBlockingStub(channel).saveDocument(save))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                                assertThat(failure.getMessage()).contains("expiring drives"));
                assertThat(services.v2Composition().catalog().current(address(account, "rejected"))).isEmpty();
            } finally { channel.shutdownNow(); }
        }
    }

    private static ai.protomolt.proto.repo.blob.spi.OpenedBlobStore probe(RedisDriveConfig redis, int ttlSeconds) {
        String keyPrefix = redis.getKeyPrefix();
        if (ttlSeconds > 0) keyPrefix = DriveBackingPool.temporaryKeyPrefix(keyPrefix, ttlSeconds);
        var options = Map.of("uri", redis.getUri(), "ttl-seconds", Integer.toString(ttlSeconds),
                "key-prefix", keyPrefix, "max-object-bytes", Long.toString(redis.getMaxObjectBytes()),
                "write-policy", "replace");
        return ai.protomolt.proto.repo.blob.spi.BlobStores.discover().open("valkey", options);
    }

    private static DriveProviderConfig redis(String account, int ttlSeconds) {
        return DriveProviderConfig.newBuilder().setRedis(RedisDriveConfig.newBuilder().setUri(valkeyUri())
                .setTtlSeconds(ttlSeconds).setMaxObjectBytes(1024 * 1024).setKeyPrefix("kt:" + account)).build();
    }

    private static String valkeyUri() {
        return "redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379) + "/0";
    }

    private static void assertRead(DocumentServiceGrpc.DocumentServiceBlockingStub docs, NodeAddress address, String title) throws Exception {
        var document = docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument();
        assertThat(document.getSearchMetadata().getTitle()).isEqualTo(title);
        var bytes = new java.io.ByteArrayOutputStream();
        docs.readBlob(ReadBlobRequest.newBuilder().setAddress(address).setBlobId("source").build())
                .forEachRemaining(frame -> { if (frame.hasData()) bytes.writeBytes(frame.getData().toByteArray()); });
        assertThat(bytes.toByteArray()).isEqualTo(RAW.toByteArray());
    }

    private static StoredPartEntry part(StoredManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream().filter(p -> p.getEntry().getSelector().getPart() == part).findFirst().orElseThrow();
    }

    private static PartWrite write(DocumentPart part) {
        return PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(part))
                .setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    private static ExpectedRevision at(long revision) {
        return ExpectedRevision.newBuilder().setRevision(revision).build();
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

    private static RepoServiceConfig config() { return config(ManagedStoragePolicy.disabled(), false); }

    private static RepoServiceConfig config(ManagedStoragePolicy managedStorage) { return config(managedStorage, false); }

    private static RepoServiceConfig config(ManagedStoragePolicy managedStorage, boolean lifecycleEnabled) {
        return new RepoServiceConfig(0, new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(), "mesh", 0,
                null, null, 0, 0, lifecycleEnabled, 0, 0, false, true, 0, null, null, null, null, null, null,
                false, managedStorage, 0, null);
    }

    private static S3Client s3() {
        return S3Client.builder().endpointOverride(S3.getEndpoint()).region(Region.of(S3.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
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
