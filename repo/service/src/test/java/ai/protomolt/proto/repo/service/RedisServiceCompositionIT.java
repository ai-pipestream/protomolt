package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.redis.RedisTestServers;
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

@Testcontainers
class RedisServiceCompositionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>(RedisTestServers.image()).withExposedPorts(6379);

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"valkey", "redis"})
    void authoritativeHostDefaultCannotExpireRetainedDocumentBytes(String mode) {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                null, null, null, null, "expiry-refusal", 0,
                mode, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), 60, 1024);
        assertThatThrownBy(() -> RepoServices.build(config))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retain bytes until explicit purge");
    }

    @Container static final org.testcontainers.localstack.LocalStackContainer S3 =
            new org.testcontainers.localstack.LocalStackContainer(
                    org.testcontainers.utility.DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"s3-valkey-cache", "s3-redis-cache"})
    void cacheSelectionRetainsS3DrivesAndStreamedBackingReads(String mode) throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(), "cache-test", 0,
                mode, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), 60, 1024);
        try (var services = RepoServices.build(config)) {
            assertThat(services.blobStore()).isInstanceOf(ai.protomolt.proto.repo.blob.cache.CachingBlobStore.class);
            String name = mode + "-composition";
            services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).build();
            try {
                var drive = DriveServiceGrpc.newBlockingStub(channel).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId(mode).setName("input").build()).getDrive();
                assertThat(drive.getProvider()).isEqualTo("s3");
                byte[] body = {0, (byte) 255, 8, 42};
                services.blobStore().put(new ai.protomolt.proto.repo.blob.spi.BlobStore.PutSpec(
                        drive.getBucket(), "binary", "application/x-protobuf", java.util.Map.of(), null), body);
                assertThat(services.blobStore().get(drive.getBucket(), "binary").data()).isEqualTo(body);
                try (var streamed = services.blobStore().openStream(drive.getBucket(), "binary", 0, -1)) {
                    assertThat(streamed.readAllBytes()).isEqualTo(body);
                }
            } finally { channel.shutdownNow(); }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"valkey", "redis"})
    void redisStartsAndProvisionsWithoutAnS3Client(String mode) throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                "http://127.0.0.1:1", "us-east-1", "unused", "unused", "redis-test", 0,
                mode, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), 0, 1024);
        var unopenedS3 = new ai.protomolt.proto.repo.blob.spi.BlobStoreProvider() {
            public String id() { return "s3"; }
            public ai.protomolt.proto.repo.blob.spi.OpenedBlobStore open(java.util.Map<String, String> options) {
                throw new AssertionError("Redis composition opened S3");
            }
            public ai.protomolt.proto.repo.blob.spi.BackendIdentity managedIdentity(java.util.Map<String, String> options) {
                throw new AssertionError("Redis composition requested S3 identity");
            }
        };
        var providers = ai.protomolt.proto.repo.blob.spi.BlobStores.of(java.util.List.of(
                unopenedS3, mode.equals("valkey")
                        ? new ai.protomolt.proto.repo.blob.redis.ValkeyBlobStoreProvider()
                        : new ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider()));
        try (var services = new RepoServices(config, ai.protomolt.proto.asset.bridge.BridgeEngine.standard(), providers)) {
            assertThat(services.blobStore()).isInstanceOf(ai.protomolt.proto.repo.blob.redis.RedisBlobStore.class);
            String name = mode + "-composition";
            services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).build();
            try {
                var drive = DriveServiceGrpc.newBlockingStub(channel).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId(mode + "-account").setName("input").build()).getDrive();
                assertThat(drive.getProvider()).isEqualTo(mode);
                services.blobStore().headBucket(drive.getBucket());
                for (String requestedName : java.util.List.of("input", "wrong-endpoint")) {
                assertThatThrownBy(() -> DriveServiceGrpc.newBlockingStub(channel).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId(mode + "-account").setName(requestedName)
                                .setProviderConfig(ai.protomolt.proto.repo.v1.DriveProviderConfig.newBuilder()
                                        .setRedis(ai.protomolt.proto.repo.v1.RedisDriveConfig.newBuilder()
                                                .setUri("redis://127.0.0.1:1").setTtlSeconds(1))).build()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class).hasMessageContaining("FAILED_PRECONDITION");
                }
                assertThat(services.driveLedger().findByName(mode + "-account", "wrong-endpoint")).isEmpty();
                assertThatThrownBy(() -> DriveServiceGrpc.newBlockingStub(channel).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId(mode + "-account").setName("wrong")
                                .setProvider("s3").build()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class)
                        .hasMessageContaining("FAILED_PRECONDITION");
                var old = new ai.protomolt.proto.repo.container.ledger.DriveRecord();
                old.driveId = java.util.UUID.nameUUIDFromBytes(("drive|" + mode + "-account|" + mode + "-legacy").getBytes(java.nio.charset.StandardCharsets.UTF_8));
                old.accountId = mode + "-account";
                old.name = mode + "-legacy";
                old.provider = "s3";
                old.bucket = "legacy";
                old.prefix = "legacy";
                old.driveType = "CUSTOM";
                old.status = "ACTIVE";
                services.driveLedger().insert(old);
                assertThatThrownBy(() -> services.driveLedger().findUniqueByName(mode + "-legacy"))
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
                        CreateDriveRequest.newBuilder().setAccountId(mode + "-account").setName(mode + "-legacy").build()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class)
                        .hasMessageContaining("FAILED_PRECONDITION");
            } finally { channel.shutdownNow(); }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"valkey", "redis"})
    void failedCacheAcquisitionClosesAlreadyOpenedBackingStore(String provider) {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                "http://127.0.0.1:1", "us-east-1", "unused", "unused", "startup-test", 0,
                "s3-" + provider + "-cache", "redis://127.0.0.1:1", 0, 1024);
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        var backing = new ai.protomolt.proto.repo.blob.spi.BlobStoreProvider() {
            public String id() { return "s3"; }
            public ai.protomolt.proto.repo.blob.spi.OpenedBlobStore open(java.util.Map<String, String> options) {
                var real = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(options);
                return new ai.protomolt.proto.repo.blob.spi.OpenedBlobStore(real.store(), () -> {
                    closes.incrementAndGet();
                    real.close();
                        }, real.capabilities(), real::ensureNamespace, real.reclaimer(), real.exactVersionReclaimer());
            }
        };
        var failure = new IllegalStateException("Injected cache acquisition failure");
        var cache = new ai.protomolt.proto.repo.blob.spi.BlobStoreProvider() {
            public String id() { return provider; }
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
