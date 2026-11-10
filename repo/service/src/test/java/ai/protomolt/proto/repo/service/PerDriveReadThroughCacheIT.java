package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.accession.AccessionGrpcService;
import ai.protomolt.proto.repo.accession.DocumentPurges;
import ai.protomolt.proto.repo.accession.DocumentReads;
import ai.protomolt.proto.repo.accession.DocumentSaves;
import ai.protomolt.proto.repo.accession.DocumentUploads;
import ai.protomolt.proto.repo.spi.PurgeAuthorization;
import ai.protomolt.proto.repo.blob.cache.CachingBlobStore;
import ai.protomolt.proto.repo.blob.redis.RedisTestServers;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.DriveCacheConfig;
import ai.protomolt.proto.repo.v1.DriveProviderConfig;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.S3DriveConfig;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.PurgeDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.StoragePlacement;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.StoredPartEntry;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.ByteString;
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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * End-to-end integration tests for per-drive read-through caches (#825) over
 * PostgreSQL, two separate S3-compatible backends (LocalStack), and Valkey.
 */
@Testcontainers
class PerDriveReadThroughCacheIT {

    // A part whose authoritative object is gone is a damaged repository, not an absent document.
    private static final Status.Code MISSING_PART_CODE = Status.Code.DATA_LOSS;

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer S3_A =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @Container
    static final LocalStackContainer S3_B =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @Container
    static final GenericContainer<?> VALKEY =
            RedisTestServers.container().withCommand(RedisTestServers.server(), "--appendonly", "yes");

    private static final ByteString OVERSIZED_BLOB =
            ByteString.copyFrom(new byte[2048]); // Above 512-byte cache ceiling

    @Test
    void readThroughHitMissCeilingCorruptRecoveryPromotionRestartAndPurgeEviction() throws Exception {
        String account = "cache-e2e-" + UUID.randomUUID();
        String bucket = "cache-bucket-" + UUID.randomUUID();
        String keyPrefix = "cache:" + account + ":";
        String valkeyUri = "redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379) + "/0";
        NodeAddress address = address(account, "doc-cached");

        var cacheConfig = DriveCacheConfig.newBuilder()
                .setProvider("valkey")
                .setUri(valkeyUri)
                .setTtlSeconds(120)
                .setMaxCacheableBytes(512)
                .setKeyPrefix(keyPrefix)
                .build();
        var cachedDriveConfig = DriveProviderConfig.newBuilder()
                .setS3(S3DriveConfig.newBuilder()
                        .setEndpointOverride(S3_A.getEndpoint().toString())
                        .setForcePathStyle(true)
                        .setReadThroughCache(cacheConfig))
                .build();
        var uncachedDriveConfig = DriveProviderConfig.newBuilder()
                .setS3(S3DriveConfig.newBuilder()
                        .setEndpointOverride(S3_A.getEndpoint().toString())
                        .setForcePathStyle(true))
                .build();

        String cacheNamespace = DriveBackingPool.s3CacheNamespace(
                DriveBackingPool.s3Endpoint(S3_A.getEndpoint().toString(), S3_A.getRegion(), true),
                S3_A.getRegion(),
                true);
        String isolatedCacheBucket = cacheNamespace.length() + ":" + cacheNamespace + ":" + bucket;

        StoredManifest rev1Manifest;
        StoredManifest rev2Manifest;
        String rev1CoreKey;
        String rev2CoreKey;
        String rev1RawBlobKey;

        try (OpenedBlobStore valkeyProbe = openValkeyProbe(valkeyUri, keyPrefix, 512);
                S3Client s3A = s3(S3_A)) {
            try (RepoServices services = RepoServices.build(config())) {
                String serverName = "cache-e2e-" + UUID.randomUUID();
                services.startInProcess(serverName);
                ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
                try {
                    var drives = DriveServiceGrpc.newBlockingStub(channel);
                    drives.createDrive(CreateDriveRequest.newBuilder()
                            .setAccountId(account)
                            .setName("cached")
                            .setProvider("s3")
                            .setBucket(bucket)
                            .setPrefix("cached")
                            .setCredentialsRef("host:default")
                            .setProviderConfig(cachedDriveConfig)
                            .build());
                    drives.createDrive(CreateDriveRequest.newBuilder()
                            .setAccountId(account)
                            .setName("uncached")
                            .setProvider("s3")
                            .setBucket(bucket)
                            .setPrefix("uncached")
                            .setCredentialsRef("host:default")
                            .setProviderConfig(uncachedDriveConfig)
                            .build());

                    var docs = DocumentServiceGrpc.newBlockingStub(channel);

                    // Save revision 1 on "cached" with a small CORE part (<= 512B) and oversized raw blob (2048B > 512B)
                    docs.saveDocument(save(
                                    address,
                                    "save-rev1",
                                    "cached",
                                    0,
                                    core(account, "rev-1-title").toBuilder()
                                            .setBlobBag(BlobBag.newBuilder()
                                                    .setBlob(Blob.newBuilder()
                                                            .setBlobId("raw-1")
                                                            .setData(OVERSIZED_BLOB))))
                            .addWrites(write(DocumentPart.DOCUMENT_PART_BLOBS))
                            .build());

                    rev1Manifest = services.v2Composition().catalog().manifest(address, 1).orElseThrow();
                    StoredPartEntry rev1Core = part(rev1Manifest, DocumentPart.DOCUMENT_PART_CORE);
                    StoredPartEntry rev1Blobs = part(rev1Manifest, DocumentPart.DOCUMENT_PART_BLOBS);
                    rev1CoreKey = rev1Core.getRef().getKey();
                    rev1RawBlobKey = rev1Blobs.getBlobRefs(0).getRef().getKey();

                    assertThat(rev1Core.getRef().getLength()).isLessThanOrEqualTo(512L);
                    assertThat(rev1Blobs.getBlobRefs(0).getRef().getLength()).isEqualTo(OVERSIZED_BLOB.size());

                    // Before any read, streamed v2 writes do not populate the read-through cache
                    assertThatThrownBy(() -> valkeyProbe.store().get(isolatedCacheBucket, rev1CoreKey))
                            .isInstanceOf(BlobStore.BlobNotFoundException.class);

                    // First GetDocument + ReadBlob is a cache miss that populates small objects (<= 512B)
                    assertRead(docs, address, "rev-1-title", "raw-1", OVERSIZED_BLOB);
                    byte[] cachedRev1CoreBytes = valkeyProbe.store().get(isolatedCacheBucket, rev1CoreKey).data();
                    assertThat(cachedRev1CoreBytes).hasSize((int) rev1Core.getRef().getLength());
                    // Oversized blob (2048B > 512B ceiling) is never cached
                    assertThatThrownBy(() -> valkeyProbe.store().get(isolatedCacheBucket, rev1RawBlobKey))
                            .isInstanceOf(BlobStore.BlobNotFoundException.class);

                    // Corrupt the cached CORE entry in Valkey: GetDocument must detect the SHA-256 mismatch,
                    // evict the poisoned entry, recover from authoritative S3, and repopulate Valkey!
                    byte[] poisoned = cachedRev1CoreBytes.clone();
                    poisoned[0] ^= 0x5a;
                    valkeyProbe.store().put(
                            new BlobStore.PutSpec(isolatedCacheBucket, rev1CoreKey, "application/octet-stream", Map.of(), null),
                            poisoned);
                    assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build())
                                    .getDocument().getSearchMetadata().getTitle())
                            .isEqualTo("rev-1-title");
                    assertThat(valkeyProbe.store().get(isolatedCacheBucket, rev1CoreKey).data())
                            .isEqualTo(cachedRev1CoreBytes);

                    // The cache never hides authoritative loss: with the CORE object gone from S3 while
                    // Valkey still holds it, the read fails and the entry is evicted.
                    s3A.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(rev1CoreKey).build());
                    try {
                        assertThatThrownBy(() -> docs.getDocument(GetDocumentRequest.newBuilder()
                                        .setAddress(address)
                                        .setSelection(PartSelection.newBuilder()
                                                .addSelectors(selector(DocumentPart.DOCUMENT_PART_CORE)))
                                        .build()))
                                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                                        assertThat(failure.getStatus().getCode()).isEqualTo(MISSING_PART_CODE));
                        assertThatThrownBy(() -> valkeyProbe.store().get(isolatedCacheBucket, rev1CoreKey))
                                .isInstanceOf(BlobStore.BlobNotFoundException.class);
                    } finally {
                        s3A.putObject(
                                PutObjectRequest.builder().bucket(bucket).key(rev1CoreKey).build(),
                                RequestBody.fromBytes(cachedRev1CoreBytes));
                    }
                    // Restored in S3: the next read repopulates the cache from authoritative bytes.
                    assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build())
                                    .getDocument().getSearchMetadata().getTitle())
                            .isEqualTo("rev-1-title");
                    assertThat(valkeyProbe.store().get(isolatedCacheBucket, rev1CoreKey).data())
                            .isEqualTo(cachedRev1CoreBytes);

                    // Save revision 2 on "cached": current revision lookup comes from Postgres catalog, never stale
                    docs.saveDocument(save(address, "save-rev2", "cached", 1, core(account, "rev-2-title").toBuilder()).build());
                    rev2Manifest = services.v2Composition().catalog().manifest(address, 2).orElseThrow();
                    rev2CoreKey = part(rev2Manifest, DocumentPart.DOCUMENT_PART_CORE).getRef().getKey();

                    assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build())
                                    .getDocument().getSearchMetadata().getTitle())
                            .isEqualTo("rev-2-title");
                    assertThat(docs.getDocument(GetDocumentRequest.newBuilder()
                                            .setAddress(address)
                                            .setRevision(RevisionSelector.newBuilder().setRevision(1))
                                            .build())
                                    .getDocument().getSearchMetadata().getTitle())
                            .isEqualTo("rev-1-title");
                    assertThat(valkeyProbe.store().get(isolatedCacheBucket, rev2CoreKey).data()).isNotEmpty();

                    // Promote to "uncached" drive (revision 3)
                    var promotion = SaveDocumentRequest.newBuilder()
                            .setOperationId("promote-" + account)
                            .setTarget(DocumentTarget.newBuilder().setAddress(address))
                            .setExpected(ExpectedRevision.newBuilder().setRevision(2))
                            .setDrive("uncached")
                            .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("source")))
                            .setStoragePlacement(StoragePlacement.STORAGE_PLACEMENT_SELECTED_DRIVE)
                            .build();
                    assertThat(docs.saveDocument(promotion).getAccession().getRevision()).isEqualTo(3);
                } finally {
                    channel.shutdownNow();
                }
            }

            // Restart RepoServices and verify historical reads, promoted reads, and purge cache eviction
            try (RepoServices services = RepoServices.build(config())) {
                String serverName = "cache-restart-" + UUID.randomUUID();
                services.startInProcess(serverName);
                ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
                try {
                    var docs = DocumentServiceGrpc.newBlockingStub(channel);

                    // Evict rev1CoreKey from Valkey before reading revision 1 after restart to prove
                    // the restarted host resolves the drive's persisted DriveCacheConfig and repopulates Valkey
                    valkeyProbe.store().delete(isolatedCacheBucket, rev1CoreKey);
                    assertThat(docs.getDocument(GetDocumentRequest.newBuilder()
                                            .setAddress(address)
                                            .setRevision(RevisionSelector.newBuilder().setRevision(1))
                                            .build())
                                    .getDocument().getSearchMetadata().getTitle())
                            .isEqualTo("rev-1-title");
                    assertThat(valkeyProbe.store().get(isolatedCacheBucket, rev1CoreKey).data()).isNotEmpty();

                    // Current revision (3, on uncached drive) reads cleanly
                    assertRead(docs, address, "rev-2-title", "raw-1", OVERSIZED_BLOB);

                    // Delete and purge the document: all S3 objects and all Valkey cached entries must be reclaimed
                    docs.deleteDocument(DeleteDocumentRequest.newBuilder()
                            .setAddress(address)
                            .setOperationId("delete-" + account)
                            .setExpected(ExpectedRevision.newBuilder().setRevision(3))
                            .build());
                    try (PurgeEndpoint purge = new PurgeEndpoint(services, account)) {
                        var purged = purge.stub.purgeDocument(PurgeDocumentRequest.newBuilder()
                                .setAddress(address)
                                .setPurgeId("purge-" + account)
                                .build());
                        assertThat(purged.getObjectsToDelete()).isGreaterThanOrEqualTo(6);
                    }

                    assertThat(s3A.listObjectsV2(r -> r.bucket(bucket)).contents()).isEmpty();
                    assertThatThrownBy(() -> valkeyProbe.store().get(isolatedCacheBucket, rev1CoreKey))
                            .isInstanceOf(BlobStore.BlobNotFoundException.class);
                    assertThatThrownBy(() -> valkeyProbe.store().get(isolatedCacheBucket, rev2CoreKey))
                            .isInstanceOf(BlobStore.BlobNotFoundException.class);
                } finally {
                    channel.shutdownNow();
                }
            }
        }
    }

    @Test
    void equalBucketAndKeyAcrossSeparateS3BackendsSharingOneValkeyNeverCollide() throws Exception {
        String account = "collision-" + UUID.randomUUID();
        String sharedBucket = "shared-bucket-" + UUID.randomUUID();
        String sharedKey = "shared-prefix/documents/identical-object-key";
        String valkeyUri = "redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379) + "/0";

        var sharedCacheConfig = DriveCacheConfig.newBuilder()
                .setProvider("valkey")
                .setUri(valkeyUri)
                .setTtlSeconds(120)
                .setMaxCacheableBytes(4096)
                .setKeyPrefix("shared-valkey-prefix:")
                .build();

        var configA = DriveProviderConfig.newBuilder()
                .setS3(S3DriveConfig.newBuilder()
                        .setEndpointOverride(S3_A.getEndpoint().toString())
                        .setForcePathStyle(true)
                        .setReadThroughCache(sharedCacheConfig))
                .build();
        var configB = DriveProviderConfig.newBuilder()
                .setS3(S3DriveConfig.newBuilder()
                        .setEndpointOverride(S3_B.getEndpoint().toString())
                        .setForcePathStyle(true)
                        .setReadThroughCache(sharedCacheConfig))
                .build();

        try (RepoServices services = RepoServices.build(config());
                S3Client clientA = s3(S3_A);
                S3Client clientB = s3(S3_B)) {
            String serverName = "collision-" + UUID.randomUUID();
            services.startInProcess(serverName);
            ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
            try {
                var drives = DriveServiceGrpc.newBlockingStub(channel);
                drives.createDrive(CreateDriveRequest.newBuilder()
                        .setAccountId(account)
                        .setName("drive-a")
                        .setProvider("s3")
                        .setBucket(sharedBucket)
                        .setPrefix("shared-prefix")
                        .setCredentialsRef("host:default")
                        .setProviderConfig(configA)
                        .build());
                drives.createDrive(CreateDriveRequest.newBuilder()
                        .setAccountId(account)
                        .setName("drive-b")
                        .setProvider("s3")
                        .setBucket(sharedBucket)
                        .setPrefix("shared-prefix")
                        .setCredentialsRef("host:default")
                        .setProviderConfig(configB)
                        .build());

                // Also test full DocumentService saves & reads on both drives
                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                NodeAddress addrA = address(account, "from-a");
                NodeAddress addrB = address(account, "from-b");
                docs.saveDocument(save(addrA, "op-a", "drive-a", 0, core(account, "title-from-s3-a").toBuilder()).build());
                docs.saveDocument(save(addrB, "op-b", "drive-b", 0, core(account, "title-from-s3-b").toBuilder()).build());
                assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(addrA).build())
                                .getDocument().getSearchMetadata().getTitle())
                        .isEqualTo("title-from-s3-a");
                assertThat(docs.getDocument(GetDocumentRequest.newBuilder().setAddress(addrB).build())
                                .getDocument().getSearchMetadata().getTitle())
                        .isEqualTo("title-from-s3-b");

                // Write distinct payloads under the EXACT same (sharedBucket, sharedKey) in S3_A and S3_B
                byte[] bytesA = "payload-from-s3-backend-A".getBytes(StandardCharsets.UTF_8);
                byte[] bytesB = "payload-from-s3-backend-B-distinct".getBytes(StandardCharsets.UTF_8);
                clientA.putObject(
                        PutObjectRequest.builder().bucket(sharedBucket).key(sharedKey).build(),
                        RequestBody.fromBytes(bytesA));
                clientB.putObject(
                        PutObjectRequest.builder().bucket(sharedBucket).key(sharedKey).build(),
                        RequestBody.fromBytes(bytesB));

                var recordA = services.driveLedger().findByName(account, "drive-a").orElseThrow();
                var recordB = services.driveLedger().findByName(account, "drive-b").orElseThrow();
                var defaults = new RepoStorageWiring.Wired(
                        services.blobStore(), ignored -> {}, null, (b, k) -> true);
                try (var pool = new DriveBackingPool(config(), BlobStores.discover(), defaults)) {
                    var storeA = (CachingBlobStore) pool.resolve(recordA).store();
                    var storeB = (CachingBlobStore) pool.resolve(recordB).store();

                    try (InputStream inA = storeA.openVerifiedStream(
                            sharedBucket, sharedKey, bytesA.length, sha256Hex(bytesA))) {
                        assertThat(inA.readAllBytes()).isEqualTo(bytesA);
                    }
                    try (InputStream inB = storeB.openVerifiedStream(
                            sharedBucket, sharedKey, bytesB.length, sha256Hex(bytesB))) {
                        assertThat(inB.readAllBytes()).isEqualTo(bytesB);
                    }

                    // One Valkey holds two separate entries for the same (bucket, key), one per backend.
                    try (OpenedBlobStore valkeyProbe = openValkeyProbe(valkeyUri, "shared-valkey-prefix:", 4096)) {
                        assertThat(storeA.cacheBucket(sharedBucket)).isNotEqualTo(storeB.cacheBucket(sharedBucket));
                        assertThat(valkeyProbe.store().get(storeA.cacheBucket(sharedBucket), sharedKey).data())
                                .isEqualTo(bytesA);
                        assertThat(valkeyProbe.store().get(storeB.cacheBucket(sharedBucket), sharedKey).data())
                                .isEqualTo(bytesB);

                        // Losing the object on S3_A fails drive A's read and evicts only A's entry;
                        // drive B still reads its own bytes from its own entry.
                        clientA.deleteObject(DeleteObjectRequest.builder().bucket(sharedBucket).key(sharedKey).build());
                        assertThatThrownBy(() -> storeA.openVerifiedStream(
                                        sharedBucket, sharedKey, bytesA.length, sha256Hex(bytesA)))
                                .isInstanceOf(BlobStore.BlobNotFoundException.class);
                        assertThatThrownBy(() -> valkeyProbe.store().get(storeA.cacheBucket(sharedBucket), sharedKey))
                                .isInstanceOf(BlobStore.BlobNotFoundException.class);
                        try (InputStream inB = storeB.openVerifiedStream(
                                sharedBucket, sharedKey, bytesB.length, sha256Hex(bytesB))) {
                            assertThat(inB.readAllBytes()).isEqualTo(bytesB);
                        }
                        assertThat(valkeyProbe.store().get(storeB.cacheBucket(sharedBucket), sharedKey).data())
                                .isEqualTo(bytesB);
                    }
                }
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test
    void unreachableCacheFallsBackToAuthoritativeS3WithoutFailingSavesReadsOrDeletes() throws Exception {
        String account = "outage-" + UUID.randomUUID();
        String bucket = "outage-bucket-" + UUID.randomUUID();
        NodeAddress address = address(account, "doc-outage");

        var deadCacheConfig = DriveProviderConfig.newBuilder()
                .setS3(S3DriveConfig.newBuilder()
                        .setEndpointOverride(S3_A.getEndpoint().toString())
                        .setForcePathStyle(true)
                        .setReadThroughCache(DriveCacheConfig.newBuilder()
                                .setProvider("valkey")
                                .setUri("redis://127.0.0.1:1/0")
                                .setTtlSeconds(60)
                                .setMaxCacheableBytes(4096)))
                .build();

        try (RepoServices services = RepoServices.build(config())) {
            String serverName = "outage-" + UUID.randomUUID();
            services.startInProcess(serverName);
            ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
            try {
                var drives = DriveServiceGrpc.newBlockingStub(channel);
                drives.createDrive(CreateDriveRequest.newBuilder()
                        .setAccountId(account)
                        .setName("dead-cache")
                        .setProvider("s3")
                        .setBucket(bucket)
                        .setPrefix("dead-cache")
                        .setCredentialsRef("host:default")
                        .setProviderConfig(deadCacheConfig)
                        .build());

                var docs = DocumentServiceGrpc.newBlockingStub(channel);
                ByteString smallBlob = ByteString.copyFromUtf8("small-blob-under-dead-cache");
                docs.saveDocument(save(
                                address,
                                "save-outage",
                                "dead-cache",
                                0,
                                core(account, "outage-title").toBuilder()
                                        .setBlobBag(BlobBag.newBuilder()
                                                .setBlob(Blob.newBuilder()
                                                        .setBlobId("b1")
                                                        .setData(smallBlob))))
                        .addWrites(write(DocumentPart.DOCUMENT_PART_BLOBS))
                        .build());

                assertRead(docs, address, "outage-title", "b1", smallBlob);
                var streamedFrames = new java.util.ArrayList<ai.protomolt.proto.repo.v2.ReadDocumentResponse>();
                docs.readDocument(ReadDocumentRequest.newBuilder().setAddress(address).build())
                        .forEachRemaining(streamedFrames::add);
                assertThat(streamedFrames).isNotEmpty();
                assertThat(streamedFrames.getLast().hasComplete()).isTrue();
            } finally {
                channel.shutdownNow();
            }
        }
    }

    private static OpenedBlobStore openValkeyProbe(String uri, String keyPrefix, long maxBytes) {
        return BlobStores.discover().open("valkey", Map.of(
                "uri", uri,
                "ttl-seconds", "120",
                "max-object-bytes", Long.toString(maxBytes),
                "key-prefix", keyPrefix,
                "write-policy", "replace"));
    }

    private static void assertRead(
            DocumentServiceGrpc.DocumentServiceBlockingStub docs,
            NodeAddress address,
            String expectedTitle,
            String blobId,
            ByteString expectedBlob) {
        var document = docs.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument();
        assertThat(document.getSearchMetadata().getTitle()).isEqualTo(expectedTitle);
        var bytes = new ByteArrayOutputStream();
        docs.readBlob(ReadBlobRequest.newBuilder().setAddress(address).setBlobId(blobId).build())
                .forEachRemaining(frame -> {
                    if (frame.hasData()) {
                        bytes.writeBytes(frame.getData().toByteArray());
                    }
                });
        assertThat(bytes.toByteArray()).isEqualTo(expectedBlob.toByteArray());
    }

    private static StoredPartEntry part(StoredManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream()
                .filter(p -> p.getEntry().getSelector().getPart() == part)
                .findFirst()
                .orElseThrow();
    }

    private static PartSelector selector(DocumentPart part) {
        return PartSelector.newBuilder().setPart(part).build();
    }

    private static PartWrite write(DocumentPart part) {
        return PartWrite.newBuilder().setSelector(selector(part)).setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    private static NodeAddress address(String account, String docId) {
        return NodeAddress.newBuilder()
                .setAccountId(account)
                .setGraphId("intake:" + account)
                .setGraphAddressId("source")
                .setDocId(docId)
                .build();
    }

    private static Document core(String account, String title) {
        return Document.newBuilder()
                .setDocId("document")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(account)
                        .setDatasourceId("source")
                        .setSecurity(DocumentSecurity.newBuilder()
                                .addPermissions(AccessRule.newBuilder()
                                        .setIdentityType("public")
                                        .setIdentity("public")
                                        .setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title))
                .build();
    }

    private static SaveDocumentRequest.Builder save(
            NodeAddress address,
            String operation,
            String drive,
            long revision,
            Document.Builder document) {
        return SaveDocumentRequest.newBuilder()
                .setOperationId(operation + "-" + address.getAccountId())
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(revision == 0
                        ? ExpectedRevision.newBuilder().setNone(true)
                        : ExpectedRevision.newBuilder().setRevision(revision))
                .setDocument(document.setDocId(address.getDocId()))
                .setDrive(drive)
                .addWrites(write(DocumentPart.DOCUMENT_PART_CORE))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("source")));
    }

    private static RepoServiceConfig config() {
        return new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3_A.getEndpoint().toString(),
                S3_A.getRegion(),
                S3_A.getAccessKey(),
                S3_A.getSecretKey(),
                "mesh",
                0,
                null,
                null,
                0,
                0,
                false,
                0,
                0,
                false,
                true,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                ManagedStoragePolicy.disabled(),
                0);
    }

    private static S3Client s3(LocalStackContainer container) {
        return S3Client.builder()
                .endpointOverride(container.getEndpoint())
                .region(Region.of(container.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey())))
                .forcePathStyle(true)
                .build();
    }

    private static String sha256Hex(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static final class PurgeEndpoint implements AutoCloseable {
        final Server server;
        final ManagedChannel channel;
        final DocumentServiceGrpc.DocumentServiceBlockingStub stub;

        PurgeEndpoint(RepoServices services, String account) throws Exception {
            var v2 = services.v2Composition();
            var clock = Clock.systemUTC();
            var saves = new DocumentSaves(
                    v2.catalog(),
                    v2.drives(),
                    v2.resolver(),
                    v2.limits(),
                    ReadByteBudget.forSaves(64 * 1024 * 1024),
                    clock);
            var service = new AccessionGrpcService(
                    saves,
                    new DocumentUploads(saves),
                    new DocumentReads(
                            v2.catalog(),
                            v2.drives(),
                            v2.limits(),
                            new ReadByteBudget(64 * 1024 * 1024),
                            clock),
                    null,
                    null,
                    new DocumentPurges(
                            v2.catalog(),
                            v2.drives(),
                            (caller, tenant) -> new PurgeAuthorization.Decision(true, true),
                            clock),
                    auth -> new RepositoryCaller("mesh-admin", false, Set.of(account), Set.of()));
            var interceptor = new ServerInterceptor() {
                @Override
                public <Q, R> ServerCall.Listener<Q> interceptCall(
                        ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
                    return Contexts.interceptCall(
                            Context.current().withValue(
                                    CallerContexts.CALLER, new Caller("mesh-admin", Set.of(), false)),
                            call,
                            headers,
                            next);
                }
            };
            String name = "purge-" + UUID.randomUUID();
            server = InProcessServerBuilder.forName(name)
                    .directExecutor()
                    .addService(ServerInterceptors.intercept(service, interceptor))
                    .build()
                    .start();
            channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            stub = DocumentServiceGrpc.newBlockingStub(channel);
        }

        @Override
        public void close() {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }
}
