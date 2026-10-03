package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.blob.grpc.RemoteBlobStore;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test of the dogfood {@link RemoteBlobStore}: the
 * {@link BlobStore} port served by a repo-service's own blob RPCs, here over
 * the in-process transport against the fully wired stack (testcontainers
 * PostgreSQL + LocalStack S3). No mocks: every operation crosses the gRPC
 * boundary and lands in real object storage.
 */
@Testcontainers(disabledWithoutDocker = true)
class RemoteBlobStoreIT {

    private static final String DRIVE = "remote";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static RemoteBlobStore store;

    @BeforeAll
    static void boot() {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-remote-docs",
                0, null, null, null, null, 0, 0L);
        services = RepoServices.build(config);
        services.startInProcess("it-remote");
        channel = InProcessChannelBuilder.forName("it-remote").build();
        DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                .setName(DRIVE)
                .setAccountId("acct-remote")
                .build());
        store = new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(channel), DRIVE);
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test
    void putGetHeadDeleteRoundTrip() {
        byte[] data = "remote-payload".getBytes(StandardCharsets.UTF_8);
        // The logical bucket names the configured remote drive: the
        // drive resolves the real bucket server-side.
        store.put(new BlobStore.PutSpec(DRIVE, "rt/one.bin", "text/plain", null, null),
                data);

        BlobStore.GetResult got = store.get(DRIVE, "rt/one.bin");
        assertThat(got.data()).isEqualTo(data);
        assertThat(got.contentType()).isEqualTo("text/plain");

        // headObject is a full fetch whose bytes are discarded (documented
        // gap: the v1 API has no cheap existence probe).
        store.headObject(DRIVE, "rt/one.bin");

        assertThat(store.delete(DRIVE, "rt/one.bin")).isTrue();
        assertThatThrownBy(() -> store.get(DRIVE, "rt/one.bin"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        // Idempotent re-delete.
        assertThat(store.delete(DRIVE, "rt/one.bin")).isFalse();
    }

    @Test
    void conditionalOperationsRejectUnqualifiedLocalStack() {
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        var spec = new BlobStore.PutSpec(DRIVE, "rt/conditional.bin",
                "text/plain", null, null);
        assertThatThrownBy(() -> store.conditionalPut(spec, first, BlobStore.WriteCondition.absent()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getForUpdate(DRIVE, spec.key()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void streamingPutVariantReadsTheStream() {
        byte[] data = "streamed-via-port".getBytes(StandardCharsets.UTF_8);
        store.put(new BlobStore.PutSpec(DRIVE, "rt/streamed.bin",
                        "application/octet-stream", null, null),
                new ByteArrayInputStream(data), data.length);
        assertThat(store.get(DRIVE, "rt/streamed.bin").data()).isEqualTo(data);
        store.delete(DRIVE, "rt/streamed.bin");
    }

    @Test
    void exactUnaryLimitAndEmptyStreamRoundTrip() {
        var largeStore = new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(channel)
                .withMaxInboundMessageSize(10 * 1024 * 1024), DRIVE);
        for (int size : new int[] {0, RemoteBlobStore.MAX_UNARY_BYTES}) {
            byte[] data = new byte[size];
            String key = "rt/bound-" + size;
            var spec = new BlobStore.PutSpec(DRIVE, key, null, null,
                    ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(data));
            largeStore.put(spec, new ByteArrayInputStream(data), size);
            assertThat(largeStore.get(DRIVE, key).data()).isEqualTo(data);
            assertThat(largeStore.delete(DRIVE, key)).isTrue();
        }
    }

    @Test
    void explicitBindingsKeepIdenticalKeysInDifferentRemoteDrives() {
        DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                .setName("second-remote").setAccountId("acct-remote").build());
        var mapped = new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(channel),
                java.util.Map.of("bucket-a", DRIVE, "bucket-b", "second-remote"), java.time.Duration.ofSeconds(30));
        mapped.put(new BlobStore.PutSpec("bucket-a", "same-key", null, null, null), new byte[] {1});
        mapped.put(new BlobStore.PutSpec("bucket-b", "same-key", null, null, null), new byte[] {2});
        assertThat(mapped.get("bucket-a", "same-key").data()).containsExactly((byte) 1);
        assertThat(mapped.get("bucket-b", "same-key").data()).containsExactly((byte) 2);
        assertThatThrownBy(() -> mapped.get("unmapped", "same-key")).isInstanceOf(IllegalArgumentException.class);
        mapped.delete("bucket-a", "same-key");
        assertThat(mapped.get("bucket-b", "same-key").data()).containsExactly((byte) 2);
        mapped.delete("bucket-b", "same-key");
    }

    @Test
    void remoteAssemblyRequiresMappingAndReadsExistingMappedDrive() {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                null, null, null, null, "local-base", 0, "repo-inprocess", "it-remote", DRIVE,
                null, 0, 0);
        assertThatThrownBy(() -> RepoServices.build(config)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(RepoServiceConfig.ENV_REPO_BUCKET_BINDINGS);
        try (var downstream = RepoServices.build(config.withRepoBucketBindings(java.util.Map.of("legacy-local-bucket", DRIVE)))) {
            var row = new ai.protomolt.proto.repo.container.ledger.DriveRecord();
            row.driveId = java.util.UUID.randomUUID();
            row.accountId = "downstream-account";
            row.name = "downstream";
            row.provider = "repo-inprocess";
            row.bucket = "legacy-local-bucket";
            row.prefix = "downstream";
            row.driveType = "CUSTOM";
            row.status = "ACTIVE";
            row.writeProviderConfig(ai.protomolt.proto.repo.v1.DriveProviderConfig.newBuilder()
                    .setRemote(ai.protomolt.proto.repo.v1.RemoteDriveConfig.newBuilder()
                            .setTarget("it-remote").setDriveName(DRIVE)).build());
            downstream.driveLedger().insert(row);

            assertThat(downstream.driveLedger().findById(row.driveId)).isPresent();
            downstream.blobStore().put(new BlobStore.PutSpec(row.bucket, "unchanged-key", null, null, null), new byte[] {3});
            assertThat(store.get(DRIVE, "unchanged-key").data()).containsExactly((byte) 3);
            String downstreamName = io.grpc.inprocess.InProcessServerBuilder.generateName();
            downstream.startInProcess(downstreamName);
            var downstreamChannel = InProcessChannelBuilder.forName(downstreamName).build();
            try {
                var response = DocumentServiceGrpc.newBlockingStub(downstreamChannel).getBlob(
                        ai.protomolt.proto.repo.v1.GetBlobRequest.newBuilder().setStorageRef(
                                ai.protomolt.proto.repo.v1.FileStorageReference.newBuilder()
                                        .setDriveName(row.name).setObjectKey("unchanged-key")).build());
                assertThat(response.getData().toByteArray()).containsExactly((byte) 3);
            } finally { downstreamChannel.shutdownNow(); }
            downstream.blobStore().delete(row.bucket, "unchanged-key");

            try (var rebound = RepoServices.build(config.withRepoBucketBindings(java.util.Map.of(row.bucket, "different-drive")))) {
                assertThatThrownBy(() -> rebound.driveLedger().findById(row.driveId))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class).hasMessageContaining("FAILED_PRECONDITION");
            }
            var targetChanged = new RepoServiceConfig(0, config.ledger(), null, null, null, null,
                    "local-base", 0, "repo-inprocess", "different-target", DRIVE, null, 0, 0)
                    .withRepoBucketBindings(java.util.Map.of(row.bucket, DRIVE));
            try (var rebound = RepoServices.build(targetChanged)) {
                assertThatThrownBy(() -> rebound.driveLedger().findById(row.driveId))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class).hasMessageContaining("FAILED_PRECONDITION");
            }
            var missing = new ai.protomolt.proto.repo.container.ledger.DriveRecord();

            missing.driveId = java.util.UUID.randomUUID();
            missing.accountId = "downstream-account";
            missing.name = "unbound-legacy";
            missing.provider = "repo-inprocess";
            missing.bucket = row.bucket;
            missing.prefix = "missing";
            missing.driveType = "CUSTOM";
            missing.status = "ACTIVE";
            downstream.driveLedger().insert(missing);
            assertThatThrownBy(() -> downstream.driveLedger().findById(missing.driveId))
                    .isInstanceOf(io.grpc.StatusRuntimeException.class).hasMessageContaining("explicit migration");

            assertThatThrownBy(() -> downstream.driveLedger().findByName("acct-remote", DRIVE))
                    .isInstanceOf(io.grpc.StatusRuntimeException.class).hasMessageContaining("FAILED_PRECONDITION");
        }
    }

    @Test
    void copyIsAClientSideGetPlusPut() {
        byte[] data = "copy-source".getBytes(StandardCharsets.UTF_8);
        store.put(new BlobStore.PutSpec(DRIVE, "cp/src.bin", "text/plain", null, null), data);
        store.copy(DRIVE, "cp/src.bin", DRIVE, "cp/dst.bin");
        assertThat(store.get(DRIVE, "cp/dst.bin").data()).isEqualTo(data);
        store.delete(DRIVE, "cp/src.bin");
        store.delete(DRIVE, "cp/dst.bin");
    }

    @Test
    void absentGetAndHeadMapToBlobNotFound() {
        assertThatThrownBy(() -> store.get(DRIVE, "never/existed.bin"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThatThrownBy(() -> store.headObject(DRIVE, "never/existed.bin"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test
    void enumerateAndAdminOpsAreUnsupported() {
        assertThatThrownBy(() -> store.list("bucket", "prefix"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("repo-backed store");
        assertThatThrownBy(() -> store.deleteAll("bucket", List.of("a", DRIVE)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("repo-backed store");
        assertThatThrownBy(() -> store.headBucket("bucket"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("repo-backed store");
    }
}
