package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.engine.ArchivePutAdmission;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Actual bounded local host; transport activation requires separate ingress qualification. */
@Testcontainers
class BoundedArchiveHostIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static final RepositoryCaller CALLER = new RepositoryCaller("bounded-host", true);

    private RepoServiceConfig config() { return config(0); }

    private RepoServiceConfig config(int database) {
        return new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                "http://127.0.0.1:1", "us-east-1", "unused", "unused", "bounded-host", 0,
                "redis", null, null, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379) + "/" + database, 0, 1024)
                .withManagedStorage(new ManagedStoragePolicy("bounded-redis", "bounded-realm", true));
    }

    private BlobStores providers() {
        var unopened = new BlobStoreProvider() {
            public String id() { return "s3"; }
            public OpenedBlobStore open(Map<String, String> options) { throw new AssertionError("Opened S3"); }
            public BackendIdentity managedIdentity(Map<String, String> options) { throw new AssertionError("Requested S3 identity"); }
        };
        return BlobStores.of(List.of(unopened, new RedisBlobStoreProvider()));
    }

    @Test void managedLocalArchiveRetainsVersionsAndRefusesUnqualifiedSurfaces() {
        var budget = new PayloadBudget(8192);
        var profile = new BoundedArchiveProfile(new ArchivePutAdmission.Limits(16, 2048, 4), budget, 2);
        String account = "bounded-" + UUID.randomUUID();
        var address = EntryAddress.newBuilder().setAccountId(account).setArchive("records").setEntryId("entry").build();
        try (var host = new RepoServices(config(), BridgeEngine.standard(), providers(), profile)) {
            host.driveRepository().createDrive(CALLER, CreateDriveRequest.newBuilder()
                    .setAccountId(account).setName("storage").build());
            var archive = host.archiveRepository();
            archive.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setAccountId(account).setName("records").setDriveName("storage")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            var request = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                    .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                    .setData(ByteString.copyFromUtf8("first"))).build();
            var saved = archive.putEntry(CALLER, request);
            assertThat(saved.getManifest().getRenditions(0).getStorageObjectId()).isNotBlank();
            assertThat(archive.putEntry(CALLER, request).getVersion()).isEqualTo(saved.getVersion());
            assertThat(archive.putEntry(CALLER, request.toBuilder().setRenditions(0, request.getRenditions(0).toBuilder()
                    .setData(ByteString.copyFromUtf8("second"))).build()).getVersion()).isEqualTo(2);
            assertThat(archive.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(address).setVersion(1).build())
                    .getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("first"));
            assertThatThrownBy(() -> archive.putEntry(CALLER, request.toBuilder().setRenditions(0,
                    request.getRenditions(0).toBuilder().setData(ByteString.copyFromUtf8("x".repeat(17)))).build()))
                    .isInstanceOf(RepositoryException.class);
            assertThat(archive.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(address).build()).getRenditions(0).getData())
                    .isEqualTo(ByteString.copyFromUtf8("second"));
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(host::services).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(host::repository).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> host.startHttp(0, "synthetic-operator-key")).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(host::historicalRepository).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> archive.bridgeEntry(CALLER, BridgeEntryRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.UNSUPPORTED));
        }
        assertThat(budget.reservedBytes()).isZero();
        try (var restarted = new RepoServices(config(), BridgeEngine.standard(), providers(), profile)) {
            assertThat(restarted.archiveRepository().getEntry(CALLER,
                    GetEntryRequest.newBuilder().setAddress(address).setVersion(1).build())
                    .getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("first"));
        }
    }

    @Test void shutdownRetainsCapacityAndProviderUntilRealWriteReturns() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        var realProvider = new RedisBlobStoreProvider();
        var heldProvider = new BlobStoreProvider() {
            public String id() { return "redis"; }
            public BackendIdentity managedIdentity(Map<String, String> options) { return realProvider.managedIdentity(options); }
            public OpenedBlobStore open(Map<String, String> options) {
                var actual = realProvider.open(options);
                var store = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                        new Class<?>[]{BlobStore.class}, (proxy, method, args) -> {
                            final Object result;
                            try { result = method.invoke(actual.store(), args); }
                            catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                            if (method.getName().equals("put")) {
                                entered.countDown();
                                if (!release.await(20, java.util.concurrent.TimeUnit.SECONDS))
                                    throw new AssertionError("Delayed write was not released");
                            }
                            return result;
                        });
                return new OpenedBlobStore(store, () -> { closes.incrementAndGet(); actual.close(); },
                        actual.capabilities(), actual::ensureNamespace, actual.reclaimer());
            }
        };
        var budget = new PayloadBudget(8192);
        var profile = new BoundedArchiveProfile(new ArchivePutAdmission.Limits(16, 2048, 4), budget, 1);
        var host = new RepoServices(config(), BridgeEngine.standard(), BlobStores.of(List.of(heldProvider)), profile);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            String account = "delayed-" + UUID.randomUUID();
            host.driveRepository().createDrive(CALLER, CreateDriveRequest.newBuilder().setAccountId(account).setName("storage").build());
            var archive = host.archiveRepository();
            archive.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setAccountId(account).setName("records").setDriveName("storage")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            var request = PutEntryRequest.newBuilder().setAddress(EntryAddress.newBuilder().setAccountId(account)
                    .setArchive("records").setEntryId("entry")).addRenditions(RenditionContent.newBuilder()
                    .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                    .setData(ByteString.copyFromUtf8("held"))).build();
            var pending = executor.submit(() -> archive.putEntry(CALLER, request));
            assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(budget.reservedBytes()).isPositive();
            assertThatThrownBy(() -> archive.putEntry(CALLER, request)).isInstanceOfSatisfying(RepositoryException.class,
                    e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
            assertThatThrownBy(() -> host.close(java.time.Duration.ofMillis(100)))
                    .isInstanceOfSatisfying(RepositoryDrainTimeoutException.class,
                            e -> assertThat(e.phase()).isEqualTo(RepositoryDrainTimeoutException.Phase.ARCHIVE_PUT))
                    .hasMessageContaining("Archive puts still active");
            assertThat(closes.get()).isZero();
            assertThat(budget.reservedBytes()).isPositive();
            assertThatThrownBy(() -> archive.putEntry(CALLER, request)).isInstanceOfSatisfying(RepositoryException.class,
                    e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
            release.countDown();
            assertThat(pending.get(10, java.util.concurrent.TimeUnit.SECONDS).getVersion()).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();
            host.close();
            assertThat(closes.get()).isEqualTo(1);
        } finally {
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(20, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            host.close();
        }
    }

    @Test void invalidProfileFailsBeforeOpeningProviderAndDefaultCompositionStaysDisabled() {
        var invalid = new BoundedArchiveProfile(new ArchivePutAdmission.Limits(1025, 2048, 4), new PayloadBudget(8192), 1);
        assertThatThrownBy(() -> new RepoServices(config(), BridgeEngine.standard(), BlobStores.of(List.of()), invalid))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("object limit");
        assertThatThrownBy(() -> new RepoServices(config(), BridgeEngine.standard(), BlobStores.of(List.of())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires an S3");
    }

    @Test void unqualifiedGrpcStartFailsAndClosesComposition() {
        for (boolean network : List.of(false, true)) {
            var profile = new BoundedArchiveProfile(new ArchivePutAdmission.Limits(16, 2048, 4), new PayloadBudget(8192), 1);
            try (var host = new RepoServices(config(), BridgeEngine.standard(), providers(), profile)) {
                assertThatThrownBy(() -> {
                    if (network) host.startNetty(0, "synthetic-operator-key", null);
                    else host.startInProcess("bounded-disabled-" + UUID.randomUUID());
                }).isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("transport admission");
                assertThatThrownBy(host::archiveRepository).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
            }
        }
    }

    @Test void backendIdentityConflictClosesNewProviderWithoutReplacingOriginalBinding() {
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        var actual = new RedisBlobStoreProvider();
        var observed = new BlobStoreProvider() {
            public String id() { return "redis"; }
            public BackendIdentity managedIdentity(Map<String, String> options) { return actual.managedIdentity(options); }
            public OpenedBlobStore open(Map<String, String> options) {
                assertThat(options.get("write-policy")).isEqualTo("create-only");
                var opened = actual.open(options);
                return new OpenedBlobStore(opened.store(), () -> { closes.incrementAndGet(); opened.close(); },
                        opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
            }
        };
        var selected = BlobStores.of(List.of(observed));
        var profile = new BoundedArchiveProfile(new ArchivePutAdmission.Limits(16, 2048, 4), new PayloadBudget(8192), 1);
        try (var original = new RepoServices(config(0), BridgeEngine.standard(), selected, profile)) {
            assertThatThrownBy(() -> new RepoServices(config(1), BridgeEngine.standard(), selected, profile))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("another physical profile");
            assertThat(closes.get()).isEqualTo(1);
            try (var same = new RepoServices(config(0), BridgeEngine.standard(), selected, profile)) {
                assertThat(same.archiveRepository()).isNotNull();
            }
            assertThat(closes.get()).isEqualTo(2);
        }
        assertThat(closes.get()).isEqualTo(3);
    }

    @Test void unsupportedProviderCapabilityClosesAcquiredHandle() {
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        var actual = new RedisBlobStoreProvider();
        var restricted = new BlobStoreProvider() {
            public String id() { return "redis"; }
            public BackendIdentity managedIdentity(Map<String, String> options) { return actual.managedIdentity(options); }
            public OpenedBlobStore open(Map<String, String> options) {
                var opened = actual.open(options);
                var capabilities = new java.util.HashSet<>(opened.capabilities());
                capabilities.remove(BlobCapability.NON_EXPIRING_WRITES);
                return new OpenedBlobStore(opened.store(), () -> { closes.incrementAndGet(); opened.close(); },
                        capabilities, opened::ensureNamespace, opened.reclaimer());
            }
        };
        var profile = new BoundedArchiveProfile(new ArchivePutAdmission.Limits(16, 2048, 4), new PayloadBudget(8192), 1);
        assertThatThrownBy(() -> new RepoServices(config(), BridgeEngine.standard(), BlobStores.of(List.of(restricted)), profile))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot support managed ingestion");
        assertThat(closes.get()).isEqualTo(1);
    }
}
