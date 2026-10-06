package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.redis.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.engine.ArchivePutAdmission;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Delayed real Redis acknowledgment, with no replacement provider or generated success response. */
@Testcontainers
class BoundedArchiveRedisReplyIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Test void lostProviderAcknowledgmentLeavesNoPublishedVersionAndCleanupPreservesRetry() throws Exception {
        String account = "lost-reply-" + UUID.randomUUID();
        var caller = new RepositoryCaller("lost-reply", true);
        try (var gate = new RedisReplyGate(REDIS.getHost(), REDIS.getMappedPort(6379));
                var direct = new RedisBlobStore(new RedisBlobStoreConfig("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                        0, 1024, "", RedisWritePolicy.CREATE_ONLY));
                var sql = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            var env = RepoBoundedArchiveMainTest.environment();
            env.put(LedgerConfig.ENV_JDBC_URL, POSTGRES.getJdbcUrl());
            env.put(LedgerConfig.ENV_USERNAME, POSTGRES.getUsername());
            env.put(LedgerConfig.ENV_PASSWORD, POSTGRES.getPassword());
            env.put(RepoServiceConfig.ENV_REDIS_URI, gate.uri());
            env.put(RepoServiceConfig.ENV_SWEEP_INTERVAL_MS, "250");
            env.put(RepoServiceConfig.ENV_RECONCILE_MIN_AGE_MS, "0");
            env.put(ManagedStoragePolicy.ENV_GENERATION, "lost-reply");
            env.put(ManagedStoragePolicy.ENV_REALM, "lost-reply-realm");
            try (var host = RepoServices.buildBoundedArchive(RepoServiceConfig.fromEnvironment(env),
                    new BoundedArchiveOptions(1024, 2048, 4, 16384, 2))) {
                host.driveRepository().createDrive(caller, CreateDriveRequest.newBuilder().setAccountId(account).setName("storage").build());
                host.archiveRepository().createArchive(caller, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                        .setAccountId(account).setName("records").setDriveName("storage")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                var address = EntryAddress.newBuilder().setAccountId(account).setArchive("records").setEntryId("entry").build();
                var data = ByteString.copyFromUtf8("lost response must not publish");
                var request = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(data)).build();
                var listener = host.startBoundedArchiveNetty(0, "lost-token");
                var channel = NettyChannelBuilder.forAddress("127.0.0.1", listener.getPort()).usePlaintext().build();
                try {
                    var headers = new Metadata();
                    headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), "lost-token");
                    var auth = MetadataUtils.newAttachHeadersInterceptor(headers);
                    var future = ArchiveServiceGrpc.newFutureStub(channel).withInterceptors(auth).withDeadlineAfter(10, TimeUnit.SECONDS);
                    var stub = ArchiveServiceGrpc.newBlockingStub(channel).withInterceptors(auth).withDeadlineAfter(10, TimeUnit.SECONDS);
                    gate.armLostAcknowledgment();
                    var pending = future.putEntry(request);
                    UUID abandoned = null; String namespace = null, key = null;
                    try {
                        assertThat(gate.held.await(5, TimeUnit.SECONDS)).isTrue();
                        try (var query = sql.prepareStatement("SELECT b.object_id,b.bucket,b.object_key,u.state FROM archive_object_bindings b JOIN archive_object_uploads u USING(object_id) WHERE b.account_id=?")) {
                            query.setString(1, account);
                            try (var result = query.executeQuery()) {
                                assertThat(result.next()).isTrue();
                                abandoned = result.getObject(1, UUID.class); namespace = result.getString(2); key = result.getString(3);
                                assertThat(result.getString(4)).isEqualTo("STAGING");
                                assertThat(direct.get(namespace, key, null).data()).isEqualTo(data.toByteArray());
                                assertThat(result.next()).isFalse();
                            }
                        }
                    } finally { gate.release.countDown(); }
                    assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class)
                            .satisfies(failure -> assertThat(Status.fromThrowable(failure.getCause()).getCode()).isEqualTo(Status.Code.INTERNAL));
                    assertThatThrownBy(() -> stub.getEntry(GetEntryRequest.newBuilder().setAddress(address).build()))
                            .isInstanceOfSatisfying(StatusRuntimeException.class,
                                    e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
                    var saved = stub.putEntry(request);
                    assertThat(saved.getVersion()).isEqualTo(1);
                    assertThat(saved.getManifest().getRenditions(0).getStorageObjectId()).isNotEqualTo(abandoned.toString());
                    assertThat(stub.putEntry(request)).isEqualTo(saved.toBuilder().setDeduplicated(true).build());
                    awaitAbandonedCleanup(sql, abandoned);
                    String deletedNamespace = namespace, deletedKey = key;
                    assertThatThrownBy(() -> direct.get(deletedNamespace, deletedKey, null)).isInstanceOf(BlobStore.BlobNotFoundException.class);
                    assertThat(stub.withDeadlineAfter(10, TimeUnit.SECONDS).getEntry(GetEntryRequest.newBuilder().setAddress(address).build())
                            .getRenditions(0).getData()).isEqualTo(data);
                    assertThat(stub.withDeadlineAfter(10, TimeUnit.SECONDS).putEntry(request)).isEqualTo(saved.toBuilder().setDeduplicated(true).build());
                    gate.expectClientClose();
                } finally {
                    gate.release.countDown(); channel.shutdownNow();
                    assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                }
            }
        }
    }

    private static void awaitAbandonedCleanup(java.sql.Connection sql, UUID object) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(6);
        boolean observedActiveLease = false;
        try (var query = sql.prepareStatement("SELECT state,lease_until<=clock_timestamp(),(SELECT count(*) FROM archive_version_object_refs WHERE object_id=?) FROM archive_object_uploads WHERE object_id=?")) {
            query.setObject(1, object); query.setObject(2, object);
            while (System.nanoTime() < deadline) {
                try (var result = query.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getLong(3)).as("unacknowledged object never becomes a version reference").isZero();
                    String state = result.getString(1); boolean expired = result.getBoolean(2);
                    if (!expired) { observedActiveLease = true; assertThat(state).isEqualTo("STAGING"); }
                    if ("DELETED".equals(state)) {
                        assertThat(observedActiveLease).isTrue(); assertThat(expired).isTrue(); return;
                    }
                }
                Thread.sleep(250);
            }
        }
        throw new AssertionError("Abandoned upload was not reclaimed after its real five-minute lease");
    }

    @Test void drainRetainsRpcAndBudgetUntilActualRedisAcknowledgmentArrives() throws Exception {
        var budget = new PayloadBudget(16384);
        var caller = new RepositoryCaller("reply-gate", true);
        String account = "reply-" + UUID.randomUUID();
        try (var gate = new RedisReplyGate(REDIS.getHost(), REDIS.getMappedPort(6379));
                var direct = new RedisBlobStore(new RedisBlobStoreConfig("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                        0, 1024, "", RedisWritePolicy.CREATE_ONLY));
                var sql = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            var config = new RepoServiceConfig(0,
                    new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                    null, "us-east-1", null, null, "reply-gate", 0, "redis", null, null, gate.uri(), 0, 1024)
                    .withManagedStorage(new ManagedStoragePolicy("reply-gate", "reply-realm", true));
            var profile = new BoundedArchiveProfile(new ArchivePutAdmission.Limits(1024, 2048, 4), budget, 1);
            try (var host = new RepoServices(config, BridgeEngine.standard(), BlobStores.discover(), profile)) {
                host.driveRepository().createDrive(caller, CreateDriveRequest.newBuilder().setAccountId(account).setName("storage").build());
                host.archiveRepository().createArchive(caller, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                        .setAccountId(account).setName("records").setDriveName("storage")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                var address = EntryAddress.newBuilder().setAccountId(account).setArchive("records").setEntryId("entry").build();
                var data = ByteString.copyFromUtf8("stored before acknowledgment");
                var request = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(data)).build();
                var listener = host.startBoundedArchiveNetty(0, "reply-token");
                var channel = NettyChannelBuilder.forAddress("127.0.0.1", listener.getPort()).usePlaintext().build();
                try {
                    var headers = new Metadata();
                    headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), "reply-token");
                    var auth = MetadataUtils.newAttachHeadersInterceptor(headers);
                    var future = ArchiveServiceGrpc.newFutureStub(channel).withInterceptors(auth).withDeadlineAfter(10, TimeUnit.SECONDS);
                    gate.arm();
                    var pending = future.putEntry(request);
                    UUID stagedObject = null;
                    try {
                        assertThat(gate.held.await(5, TimeUnit.SECONDS)).as("real conditional-write reply held at TCP boundary").isTrue();
                        // Keep this interval below the production client's two-second socket timeout.
                        long observed = System.nanoTime();
                        try (var query = sql.prepareStatement("""
                                SELECT b.bucket,b.object_key,u.state,b.object_id FROM archive_object_bindings b
                                JOIN archive_object_uploads u USING(object_id) WHERE b.account_id=?
                                """)) {
                            query.setString(1, account);
                            try (var result = query.executeQuery()) {
                                assertThat(result.next()).isTrue();
                                assertThat(result.getString(3)).isEqualTo("STAGING");
                                stagedObject = result.getObject(4, UUID.class);
                                assertThat(direct.get(result.getString(1), result.getString(2), null).data()).isEqualTo(data.toByteArray());
                                assertThat(result.next()).isFalse();
                            }
                        }
                        long reserved = budget.reservedBytes(); assertThat(reserved).isPositive();
                        assertThatThrownBy(() -> host.close(Duration.ofMillis(100)))
                                .isInstanceOfSatisfying(RepositoryDrainTimeoutException.class,
                                        e -> assertThat(e.phase()).isEqualTo(RepositoryDrainTimeoutException.Phase.ARCHIVE_RPC));
                        assertThat(budget.reservedBytes()).isEqualTo(reserved);
                        assertThat(pending.isDone()).isFalse();
                        assertThatThrownBy(() -> ArchiveServiceGrpc.newBlockingStub(channel).withInterceptors(auth)
                                .withDeadlineAfter(1, TimeUnit.SECONDS).getArchive(GetArchiveRequest.getDefaultInstance()))
                                .isInstanceOfSatisfying(StatusRuntimeException.class,
                                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
                        assertThat(Duration.ofNanos(System.nanoTime() - observed)).isLessThan(Duration.ofSeconds(1));
                    } finally { gate.release.countDown(); }
                    var saved = pending.get(5, TimeUnit.SECONDS);
                    assertThat(saved.getVersion()).isEqualTo(1);
                    assertThat(saved.getManifest().getRenditions(0).getStorageObjectId()).isEqualTo(stagedObject.toString());
                    gate.expectClientClose();
                    host.close();
                    assertThat(budget.reservedBytes()).isZero();
                    try (var query = sql.prepareStatement("SELECT count(*) FROM archive_object_uploads u JOIN archive_object_bindings b USING(object_id) WHERE b.account_id=? AND u.state='LIVE'")) {
                        query.setString(1, account);
                        try (var result = query.executeQuery()) { result.next(); assertThat(result.getLong(1)).isEqualTo(1); }
                    }
                } finally {
                    gate.release.countDown(); channel.shutdownNow();
                    assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                }
            }
        }
        assertThat(budget.reservedBytes()).isZero();
    }
}
