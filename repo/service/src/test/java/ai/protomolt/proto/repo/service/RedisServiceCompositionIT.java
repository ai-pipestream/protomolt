package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class RedisServiceCompositionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Test void redisStartsAndProvisionsWithoutAnS3Client() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                "http://127.0.0.1:1", "us-east-1", "unused", "unused", "redis-test", 0,
                "redis", null, null, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), 0, 1024);
        var unopenedS3 = new ai.protomolt.proto.repo.blob.spi.BlobStoreProvider() {
            public String id() { return "s3"; }
            public ai.protomolt.proto.repo.blob.spi.OpenedBlobStore open(java.util.Map<String, String> options) {
                throw new AssertionError("Redis composition opened S3");
            }
        };
        var providers = ai.protomolt.proto.repo.blob.spi.BlobStores.of(java.util.List.of(
                unopenedS3, new ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider()));
        try (var services = new RepoServices(config, ai.protomolt.proto.asset.bridge.BridgeEngine.standard(), providers)) {
            assertThat(services.blobStore()).isInstanceOf(ai.protomolt.proto.repo.blob.redis.RedisBlobStore.class);
            String name = "redis-composition";
            services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).build();
            try {
                var drive = DriveServiceGrpc.newBlockingStub(channel).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId("redis-account").setName("input").build()).getDrive();
                assertThat(drive.getProvider()).isEqualTo("redis");
                services.blobStore().headBucket(drive.getBucket());
                for (String requestedName : java.util.List.of("input", "wrong-endpoint")) {
                assertThatThrownBy(() -> DriveServiceGrpc.newBlockingStub(channel).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId("redis-account").setName(requestedName)
                                .setProviderConfig(ai.protomolt.proto.repo.v1.DriveProviderConfig.newBuilder()
                                        .setRedis(ai.protomolt.proto.repo.v1.RedisDriveConfig.newBuilder()
                                                .setUri("redis://127.0.0.1:1"))).build()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class).hasMessageContaining("FAILED_PRECONDITION");
                }
                assertThat(services.driveLedger().findByName("redis-account", "wrong-endpoint")).isEmpty();
                assertThatThrownBy(() -> DriveServiceGrpc.newBlockingStub(channel).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId("redis-account").setName("wrong")
                                .setProvider("s3").build()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class)
                        .hasMessageContaining("FAILED_PRECONDITION");
                var old = new ai.protomolt.proto.repo.container.ledger.DriveRecord();
                old.driveId = java.util.UUID.nameUUIDFromBytes("drive|redis-account|legacy".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                old.accountId = "redis-account";
                old.name = "legacy";
                old.provider = "s3";
                old.bucket = "legacy";
                old.prefix = "legacy";
                old.driveType = "CUSTOM";
                old.status = "ACTIVE";
                services.driveLedger().insert(old);
                assertThatThrownBy(() -> ai.protomolt.proto.repo.v1.DocumentServiceGrpc.newBlockingStub(channel)
                        .getBlob(ai.protomolt.proto.repo.v1.GetBlobRequest.newBuilder()
                                .setStorageRef(ai.protomolt.proto.repo.v1.FileStorageReference.newBuilder()
                                        .setDriveName("legacy").setObjectKey("anything")).build()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class).hasMessageContaining("FAILED_PRECONDITION");
                var local = new ai.protomolt.proto.repo.engine.BlobOperations(services.blobStore(), services.driveLedger());
                assertThatThrownBy(() -> local.get(new ai.protomolt.proto.repo.spi.RepositoryCaller("test", true),
                        ai.protomolt.proto.repo.v1.GetBlobRequest.newBuilder().setStorageRef(
                                ai.protomolt.proto.repo.v1.FileStorageReference.newBuilder()
                                        .setDriveName("legacy").setObjectKey("anything")).build()))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION));
                assertThatThrownBy(() -> services.driveLedger().findById(old.driveId))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION));
                assertThatThrownBy(() -> services.driveLedger().findByName(old.accountId, old.name))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION));
                assertThatThrownBy(() -> services.driveLedger().listByAccount(old.accountId, 100, null))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION));
                assertThatThrownBy(() -> services.driveLedger().listAll(100))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION));
                assertThatThrownBy(() -> DriveServiceGrpc.newBlockingStub(channel).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId("redis-account").setName("legacy").build()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class)
                        .hasMessageContaining("FAILED_PRECONDITION");
            } finally { channel.shutdownNow(); }
        }
    }

    @Test void failedCacheAcquisitionClosesAlreadyOpenedBackingStore() {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                "http://127.0.0.1:1", "us-east-1", "unused", "unused", "startup-test", 0,
                "s3-redis-cache", null, null, "redis://127.0.0.1:1", 0, 1024);
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        var backing = new ai.protomolt.proto.repo.blob.spi.BlobStoreProvider() {
            public String id() { return "s3"; }
            public ai.protomolt.proto.repo.blob.spi.OpenedBlobStore open(java.util.Map<String, String> options) {
                var real = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(options);
                return new ai.protomolt.proto.repo.blob.spi.OpenedBlobStore(real.store(), () -> {
                    closes.incrementAndGet();
                    real.close();
                }, real.capabilities(), real::ensureNamespace);
            }
        };
        var failure = new IllegalStateException("Injected cache acquisition failure");
        var cache = new ai.protomolt.proto.repo.blob.spi.BlobStoreProvider() {
            public String id() { return "redis"; }
            public ai.protomolt.proto.repo.blob.spi.OpenedBlobStore open(java.util.Map<String, String> options) {
                throw failure;
            }
        };
        assertThatThrownBy(() -> new RepoServices(config, ai.protomolt.proto.asset.bridge.BridgeEngine.standard(),
                ai.protomolt.proto.repo.blob.spi.BlobStores.of(java.util.List.of(backing, cache))))
                .isSameAs(failure);
        assertThat(closes.get()).isEqualTo(1);
    }
}
