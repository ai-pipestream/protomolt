package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.archive.*;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Shared library composition only; does not enable the managed host or qualify Redis durability. */
@Testcontainers
class RedisArchiveLifecycleIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static LedgerDatabase database;
    static Tx tx;
    static OpenedBlobStore opened;
    static BackendIdentity identity;
    static ArchiveLedger ledger;
    static DriveLedger drives;
    static final RepositoryCaller CALLER = new RepositoryCaller("redis-archive-test", true);

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new ArchiveLedger(tx); drives = new DriveLedger(tx);
        var options = Map.of("uri", "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                "ttl-seconds", "0", "max-object-bytes", "0", "key-prefix", "archive-test", "write-policy", "create-only");
        var provider = new RedisBlobStoreProvider();
        identity = provider.managedIdentity(options); opened = provider.open(options);
        new ManagedBackendLedger(tx).bind("redis-original", new ManagedBackendLedger.Profile(identity, "redis-realm"));
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = "account"; drive.name = "redis-drive";
        drive.provider = "redis"; drive.bucket = "archive-bytes"; drive.prefix = "archive"; drive.driveType = "CUSTOM";
        drives.insert(drive);
        try (var fixture = new Fixture(opened.store())) {
            fixture.operations.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setAccountId("account").setName("records").setDriveName("redis-drive")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
        }
    }

    @AfterAll static void close() throws Exception {
        try { if (opened != null) opened.close(); } finally { if (database != null) database.close(); }
    }

    static class Fixture implements AutoCloseable {
        final ArchiveObjectReader reader;
        final ArchiveOperations operations;
        Fixture(BlobStore writer) {
            this(writer, null);
        }
        Fixture(BlobStore writer, ArchivePutAdmission admission) {
            this(writer, admission, opened.store(), null);
        }
        Fixture(BlobStore writer, ArchivePutAdmission admission, BlobStore readStore, ArchiveGetAdmission getAdmission) {
            reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> {
                assertThat(generation).isEqualTo("redis-original"); assertThat(realm).isEqualTo("redis-realm");
                return readStore;
            });
            operations = new ArchiveOperations(ledger, drives, opened.store(), BridgeEngine.standard(), reader,
                    writer(writer), admission, getAdmission);
        }
        @Override public void close() {
            reader.close();
            try { assertThat(reader.awaitIdle(Duration.ofSeconds(5))).isTrue(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            reader.attestLocalQuiescence();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void manifestJsonUsesAnAggregateSqlBoundLocallyAndOverGrpc(boolean transport) throws Exception {
        String archive = "manifest-" + UUID.randomUUID();
        var saved = request("one");
        saved = saved.toBuilder().setAddress(saved.getAddress().toBuilder().setArchive(archive)).build();
        PutEntryResponse first;
        try (var fixture = new Fixture(opened.store())) {
            fixture.operations.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setAccountId("account").setName(archive).setDriveName("redis-drive")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            first = fixture.operations.putEntry(CALLER, saved);
            fixture.operations.putEntry(CALLER, saved.toBuilder().setRenditions(0, saved.getRenditions(0).toBuilder()
                    .setData(ByteString.copyFromUtf8("two"))).build());
            fixture.operations.putEntry(CALLER, saved.toBuilder().setAddress(saved.getAddress().toBuilder().setEntryId("second")).build());
        }
        var id = UUID.fromString(first.getEntryUuid());
        int oneManifest = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT max(octet_length(manifest::text)) FROM archive_versions WHERE entry_uuid=:id")
                .setParameter("id", id).getSingleResult()).intValue());
        var exact = ledger.findManifest(id, 1, oneManifest).orElseThrow();
        assertThat(exact.utf8Bytes()).isEqualTo(exact.json().getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        assertThatThrownBy(() -> ledger.findManifest(id, 1, (int) exact.utf8Bytes() - 1))
                .isInstanceOf(ai.protomolt.proto.repo.spi.RepositoryException.class);
        assertThat(ledger.findManifest(id, 999, 0)).isEmpty();
        var budget = new PayloadBudget(16384);
        try (var gate = new ArchiveGetAdmission(new ArchiveGetAdmission.Limits(64, 4096, 4, oneManifest), budget, 1);
                var fixture = new Fixture(opened.store(), null, opened.store(), gate)) {
            String name = "manifest-json-" + UUID.randomUUID();
            var server = io.grpc.inprocess.InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(fixture.operations)).build().start();
            var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
            try {
                var stub = ArchiveServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                var versions = ListVersionsRequest.newBuilder().setAddress(saved.getAddress()).setLimit(2).build();
                Throwable refusal = catchThrowable(() -> {
                    if (transport) stub.listVersions(versions); else fixture.operations.listVersions(CALLER, versions);
                });
                assertManifestLimit(refusal, transport);
                assertThat(budget.reservedBytes()).isZero();
                var entries = ListEntriesRequest.newBuilder().setAccountId("account").setArchive(archive)
                        .setIncludeManifests(true).setLimit(2).build();
                assertManifestLimit(catchThrowable(() -> {
                    if (transport) stub.listEntries(entries); else fixture.operations.listEntries(CALLER, entries);
                }), transport);
                assertThat(budget.reservedBytes()).isZero();
                var one = versions.toBuilder().setLimit(1).build();
                var page = transport ? stub.listVersions(one) : fixture.operations.listVersions(CALLER, one);
                assertThat(page.getVersionsCount()).isEqualTo(1);
                assertThat(page.getVersions(0).getVersion()).isEqualTo(2);
                assertThat(page.getNextContinuationToken()).isEqualTo("1");
                var next = one.toBuilder().setContinuationToken(page.getNextContinuationToken()).build();
                var older = transport ? stub.listVersions(next) : fixture.operations.listVersions(CALLER, next);
                assertThat(older.getVersionsCount()).isEqualTo(1);
                assertThat(older.getVersions(0).getVersion()).isEqualTo(1);
                var historical = GetEntryRequest.newBuilder().setAddress(saved.getAddress()).setVersion(1).build();
                var read = transport ? stub.getEntry(historical) : fixture.operations.getEntry(CALLER, historical);
                assertThat(read.getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("one"));
                assertThat(budget.reservedBytes()).isZero();
            } finally {
                channel.shutdownNow(); server.shutdownNow();
                assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static void assertManifestLimit(Throwable refusal, boolean transport) {
        if (transport) assertThat(io.grpc.Status.fromThrowable(refusal).getCode()).isEqualTo(io.grpc.Status.Code.RESOURCE_EXHAUSTED);
        else assertThat(refusal).isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void aggregateReadRefusesBeforeProviderIoButSelectedHistoricalSubsetFits(boolean transport) throws Exception {
        var saved = request("a".repeat(64)).toBuilder().addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("later"))
                .setData(ByteString.copyFromUtf8("b".repeat(64)))).build();
        var request = GetEntryRequest.newBuilder().setAddress(saved.getAddress()).setVersion(1).build();
        int responseLimit;
        try (var fixture = new Fixture(opened.store())) {
            fixture.operations.putEntry(CALLER, saved);
            responseLimit = fixture.operations.getEntry(CALLER, request).getSerializedSize() - 1;
            fixture.operations.putEntry(CALLER, saved.toBuilder().setTitle("new revision").build());
        }
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        var observed = intercepted((proxy, method, args) -> {
            if (method.getName().equals("getBounded")) reads.incrementAndGet();
            return actual(method, args);
        });
        var budget = new PayloadBudget(8192);
        try (var gate = new ArchiveGetAdmission(new ArchiveGetAdmission.Limits(64, responseLimit, 4, 2048), budget, 2);
                var fixture = new Fixture(opened.store(), null, observed, gate)) {
            String name = "bounded-get-" + UUID.randomUUID();
            var server = io.grpc.inprocess.InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(fixture.operations)).build().start();
            var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
            try {
                var stub = ArchiveServiceGrpc.newBlockingStub(channel);
                java.util.function.Function<GetEntryRequest, GetEntryResponse> get = transport ? stub::getEntry
                        : r -> fixture.operations.getEntry(CALLER, r);
                Throwable refusal = catchThrowable(() -> get.apply(request));
                if (transport) assertThat(io.grpc.Status.fromThrowable(refusal).getCode()).isEqualTo(io.grpc.Status.Code.RESOURCE_EXHAUSTED);
                else assertThat(refusal).isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                        e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(reads.get()).isZero();
                assertThat(budget.reservedBytes()).isZero();
                var subset = get.apply(request.toBuilder().addRenditions("later").build());
                assertThat(subset.getRenditionsCount()).isEqualTo(1);
                assertThat(subset.getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("b".repeat(64)));
                assertThat(subset.getManifest().getVersion()).isEqualTo(1);
                assertThat(reads.get()).isEqualTo(1);
                assertThat(budget.reservedBytes()).isZero();
            } finally {
                channel.shutdownNow(); server.shutdownNow();
                assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void boundedUnaryPutChecksEveryRenditionBeforeWriting(boolean transport) throws Exception {
        var writes = new java.util.concurrent.atomic.AtomicInteger();
        var observed = intercepted((proxy, method, args) -> {
            if (method.getName().equals("put")) writes.incrementAndGet();
            return actual(method, args);
        });
        var budget = new PayloadBudget(2048);
        try (var admission = new ArchivePutAdmission(new ArchivePutAdmission.Limits(4, 1024, 4), budget, 2);
                var fixture = new Fixture(observed, admission)) {
            String name = "bounded-redis-" + UUID.randomUUID();
            var server = io.grpc.inprocess.InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(fixture.operations)).build().start();
            var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
            try {
                var stub = ArchiveServiceGrpc.newBlockingStub(channel);
                java.util.function.Function<PutEntryRequest, PutEntryResponse> put = transport ? stub::putEntry
                        : r -> fixture.operations.putEntry(CALLER, r);
                var oversized = request("ok").toBuilder().addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("later"))
                        .setData(ByteString.copyFromUtf8("large"))).build();
                assertThatThrownBy(() -> put.apply(oversized)).isInstanceOf(transport ? io.grpc.StatusRuntimeException.class
                        : ai.protomolt.proto.repo.spi.RepositoryException.class);
                assertThat(writes.get()).isZero();
                assertThat(ledger.findEntry(ArchiveIds.entryUuid(oversized.getAddress()))).isEmpty();
                var accepted = request("ok");
                var first = put.apply(accepted);
                assertThat(put.apply(accepted).getVersion()).isEqualTo(first.getVersion());
                assertThat(writes.get()).isEqualTo(1);
                assertThat(put.apply(accepted.toBuilder().setRenditions(0, accepted.getRenditions(0).toBuilder()
                        .setData(ByteString.copyFromUtf8("next"))).build()).getVersion()).isEqualTo(2);
                var historical = GetEntryRequest.newBuilder().setAddress(accepted.getAddress()).setVersion(1).build();
                var read = transport ? stub.getEntry(historical) : fixture.operations.getEntry(CALLER, historical);
                assertThat(read.getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("ok"));
                assertThat(budget.reservedBytes()).isZero();
                assertThatThrownBy(() -> fixture.operations.uploadStream(CALLER, null, null, 1, "", null,
                        null, null, null, new java.io.InputStream() {
                            public int read() { throw new AssertionError("Unsupported upload consumed input"); }
                        })).isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.UNSUPPORTED));
                assertThatThrownBy(() -> fixture.operations.bridgeEntry(CALLER, BridgeEntryRequest.getDefaultInstance()))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.UNSUPPORTED));
            } finally { channel.shutdownNow(); server.shutdownNow();
                assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue(); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void boundedAdmissionHoldsBytesAndSlotsUntilDelayedRealPutReturns(boolean slotLimit) throws Exception {
        var request = request("held");
        long reservation = request.getSerializedSize() + 4L * request.getRenditions(0).getData().size();
        var budget = new PayloadBudget(slotLimit ? reservation * 2 : reservation);
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var held = intercepted((proxy, method, args) -> {
            if (method.getName().equals("put")) {
                entered.countDown();
                if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Put gate expired");
            }
            return actual(method, args);
        });
        try (var admission = new ArchivePutAdmission(new ArchivePutAdmission.Limits(16, 1024, 4), budget, slotLimit ? 1 : 2);
                var fixture = new Fixture(held, admission); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> fixture.operations.putEntry(CALLER, request));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(budget.reservedBytes()).isEqualTo(reservation);
                assertThatThrownBy(() -> fixture.operations.putEntry(CALLER, request("next")))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
                admission.close();
                assertThat(admission.awaitIdle(Duration.ZERO)).isFalse();
                assertThat(budget.reservedBytes()).isEqualTo(reservation);
            } finally { release.countDown(); }
            assertThat(first.get(10, TimeUnit.SECONDS).getVersion()).isEqualTo(1);
            assertThat(admission.awaitIdle(Duration.ofSeconds(1))).isTrue();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    static ArchiveObjectWriter writer(BlobStore store) {
        return new ArchiveObjectWriter(new ArchiveUploadLedger(tx), store, "redis-original", opened.capabilities(), Duration.ofMinutes(1));
    }

    @Test void boundedReadRefusesLegacyManifestBeforeUnboundedProviderFallback() {
        var legacy = new ArchiveOperations(ledger, drives, opened.store());
        var saved = request("legacy bytes");
        var put = legacy.putEntry(CALLER, saved);
        assertThat(put.getManifest().getRenditions(0).getStorageObjectId()).isEmpty();
        var request = GetEntryRequest.newBuilder().setAddress(saved.getAddress()).build();
        assertThat(legacy.getEntry(CALLER, request).getRenditions(0).getData()).isEqualTo(saved.getRenditions(0).getData());
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        var observed = intercepted((proxy, method, args) -> {
            if (method.getName().equals("get") || method.getName().equals("getBounded")) reads.incrementAndGet();
            return actual(method, args);
        });
        var budget = new PayloadBudget(8192);
        try (var gate = new ArchiveGetAdmission(new ArchiveGetAdmission.Limits(64, 2048, 4), budget, 1);
                var fixture = new Fixture(opened.store(), null, observed, gate)) {
            // Also observe the raw legacy fallback, which bypasses the managed resolver.
            var bounded = new ArchiveOperations(ledger, drives, observed, BridgeEngine.standard(), fixture.reader,
                    null, null, gate);
            assertThatThrownBy(() -> bounded.getEntry(CALLER, request))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION));
            assertThat(reads.get()).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void readConstructionRetainsBudgetAndPinThroughProviderCompletion(boolean corrupt) throws Exception {
        var saved = request("retained read bytes");
        try (var fixture = new Fixture(opened.store())) { fixture.operations.putEntry(CALLER, saved); }
        var request = GetEntryRequest.newBuilder().setAddress(saved.getAddress()).build();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var held = intercepted((proxy, method, args) -> {
            Object result = actual(method, args);
            if (method.getName().equals("getBounded")) {
                entered.countDown();
                if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Read completion gate expired");
                if (corrupt) {
                    var read = (BlobStore.GetResult) result;
                    read.data()[0] ^= 1;
                }
            }
            return result;
        });
        var budget = new PayloadBudget(8192);
        try (var gate = new ArchiveGetAdmission(new ArchiveGetAdmission.Limits(64, 2048, 4), budget, 1);
                var fixture = new Fixture(opened.store(), null, held, gate);
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var read = executor.submit(() -> fixture.operations.getEntry(CALLER, request));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                long reserved = budget.reservedBytes();
                assertThat(reserved).isPositive();
                assertThatThrownBy(() -> fixture.operations.getEntry(CALLER, request))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
                gate.close();
                assertThat(gate.awaitIdle(Duration.ZERO)).isFalse();
                assertThat(budget.reservedBytes()).isEqualTo(reserved);
                long pins = tx.readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM archive_read_pins WHERE entry_uuid=:entry")
                        .setParameter("entry", ArchiveIds.entryUuid(saved.getAddress())).getSingleResult()).longValue());
                assertThat(pins).isEqualTo(1);
            } finally { release.countDown(); }
            if (corrupt) assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ai.protomolt.proto.repo.spi.RepositoryException.class);
            else assertThat(read.get(10, TimeUnit.SECONDS).getRenditions(0).getData()).isEqualTo(saved.getRenditions(0).getData());
            assertThat(gate.awaitIdle(Duration.ofSeconds(1))).isTrue();
            assertThat(budget.reservedBytes()).isZero();
            long pins = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM archive_read_pins WHERE entry_uuid=:entry")
                    .setParameter("entry", ArchiveIds.entryUuid(saved.getAddress())).getSingleResult()).longValue());
            assertThat(pins).isZero();
        }
    }
    static PutEntryRequest request(String value) {
        return PutEntryRequest.newBuilder().setAddress(EntryAddress.newBuilder().setAccountId("account")
                .setArchive("records").setEntryId(UUID.randomUUID().toString()))
                .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFromUtf8(value))).build();
    }
    static ArchiveObjectRecovery recovery(ObjectReclaimer reclaimer) {
        return new ArchiveObjectRecovery(new ArchiveCleanupLedger(tx), new ManagedBackendLedger(tx), (generation, profile) -> {
            assertThat(generation).isEqualTo("redis-original"); assertThat(profile.identity()).isEqualTo(identity);
            assertThat(profile.storageRealm()).isEqualTo("redis-realm"); return reclaimer;
        });
    }
    static UUID object(UUID entry) {
        return tx.readOnly(em -> (UUID) em.createNativeQuery("SELECT object_id FROM archive_object_bindings WHERE entry_uuid=:entry")
                .setParameter("entry", entry).getSingleResult());
    }
    static String state(UUID object) {
        return tx.readOnly(em -> (String) em.createNativeQuery("SELECT state FROM archive_object_uploads WHERE object_id=:id")
                .setParameter("id", object).getSingleResult());
    }
    static void expire(UUID object) {
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE archive_object_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE object_id=:id")
                .setParameter("id", object).executeUpdate(); });
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void localAndGrpcPublishReuseAndReadHistoricalBytes(boolean transport) throws Exception {
        try (var fixture = new Fixture(opened.store())) {
            String name = "redis-archive-" + UUID.randomUUID();
            var server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                    .addService(new ArchiveGrpcService(fixture.operations)).build().start();
            var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
            try {
                var stub = ArchiveServiceGrpc.newBlockingStub(channel);
                java.util.function.Function<PutEntryRequest, PutEntryResponse> put = transport ? stub::putEntry
                        : r -> fixture.operations.putEntry(CALLER, r);
                var request = request("first");
                var first = put.apply(request); var repeat = put.apply(request);
                assertThat(repeat.getVersion()).isEqualTo(first.getVersion());
                assertThat(repeat.getManifest()).isEqualTo(first.getManifest());
                var second = put.apply(request.toBuilder().setRenditions(0, request.getRenditions(0).toBuilder()
                        .setData(ByteString.copyFromUtf8("second"))).build());
                assertThat(second.getVersion()).isEqualTo(2);
                var historical = GetEntryRequest.newBuilder().setAddress(request.getAddress()).setVersion(1).build();
                var read = transport ? stub.getEntry(historical) : fixture.operations.getEntry(CALLER, historical);
                assertThat(read.getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("first"));
                var id = UUID.fromString(first.getManifest().getRenditions(0).getStorageObjectId());
                assertThat(recovery(opened.reclaimer()).recover(id, Instant.now().plusSeconds(60))).isEqualTo(ArchiveObjectRecovery.Outcome.SKIPPED);
                long pins = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM archive_read_pins").getSingleResult()).longValue());
                assertThat(pins).isZero();
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    static BlobStore intercepted(java.lang.reflect.InvocationHandler handler) {
        return (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class}, handler);
    }
    static Object actual(java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(opened.store(), args); }
        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }

    @Test void lostAcknowledgmentLeavesRealBytesAndUnpublishedReservationForRecovery() {
        var fault = intercepted((proxy, method, args) -> {
            var result = actual(method, args);
            if (method.getName().equals("put")) throw new IllegalStateException("injected caller acknowledgment loss");
            return result;
        });
        var request = request("uncertain bytes"); var entry = ArchiveIds.entryUuid(request.getAddress());
        var budget = new PayloadBudget(4096);
        try (var admission = new ArchivePutAdmission(new ArchivePutAdmission.Limits(32, 1024, 4), budget, 1);
                var fixture = new Fixture(fault, admission)) {
            assertThatThrownBy(() -> fixture.operations.putEntry(CALLER, request)).hasStackTraceContaining("injected caller acknowledgment loss");
            assertThat(budget.reservedBytes()).isZero();
        }
        assertThat(ledger.findEntry(entry)).isEmpty();
        UUID object = object(entry); assertThat(state(object)).isEqualTo("STAGING");
        String key = new ArchiveObjectLedger(tx).find(object).orElseThrow().location().objectKey();
        assertThat(opened.store().get("archive-bytes", key).data()).isEqualTo(request.getRenditions(0).getData().toByteArray());
        expire(object);
        assertThat(recovery(opened.reclaimer()).recover(object, Instant.now().plusSeconds(60))).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThat(state(object)).isEqualTo("DELETED");
        assertThatThrownBy(() -> opened.store().get("archive-bytes", key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test void delayedRealWriteAfterReclamationCannotPublishAndIsReclaimedAgain() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var delayed = intercepted((proxy, method, args) -> {
            if (method.getName().equals("put")) {
                entered.countDown(); if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Delayed write gate expired");
            }
            return actual(method, args);
        });
        byte[] bytes = {1, 2}; UUID entry = UUID.randomUUID(); String key = "late/" + entry;
        var rendition = RenditionManifestEntry.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setState(RenditionState.RENDITION_STATE_PRESENT).setObjectKey(key).setSizeBytes(bytes.length)
                .setSha256(ArchiveManifests.sha256Hex(bytes)).build();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var write = executor.submit(() -> writer(delayed).stage(entry, "account", "records", "archive-bytes", rendition, "application/octet-stream", bytes));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                UUID object = object(entry); expire(object); var cutoff = Instant.now().plusSeconds(60);
                assertThat(recovery(opened.reclaimer()).recover(object, cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
                release.countDown();
                assertThatThrownBy(() -> write.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(ArchiveUploadLedger.FenceException.class);
                assertThat(opened.store().get("archive-bytes", key).data()).containsExactly(bytes);
                assertThat(state(object)).isEqualTo("DELETED");
                assertThat(new ArchiveCleanupLedger(tx).candidates(cutoff, 1000)).contains(object);
                assertThat(recovery(opened.reclaimer()).recover(object, cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
                assertThatThrownBy(() -> opened.store().get("archive-bytes", key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
            } finally { release.countDown(); }
        }
    }

    @Test void heldProviderReadPinsBytesAcrossLogicalDeletionAndCleanupFailureRemainsRetryable() throws Exception {
        var request = request("pinned bytes"); PutEntryResponse saved;
        try (var fixture = new Fixture(opened.store())) { saved = fixture.operations.putEntry(CALLER, request); }
        UUID entry = UUID.fromString(saved.getEntryUuid());
        var rendition = saved.getManifest().getRenditions(0);
        UUID object = UUID.fromString(rendition.getStorageObjectId());
        var entryRecord = ledger.findEntry(entry).orElseThrow();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var held = intercepted((proxy, method, args) -> {
            if (method.getName().equals("getBounded")) {
                entered.countDown(); if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Read gate expired");
            }
            return actual(method, args);
        });
        var reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> {
            assertThat(generation).isEqualTo("redis-original"); assertThat(realm).isEqualTo("redis-realm"); return held;
        });
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var read = executor.submit(() -> reader.read(entryRecord, saved.getVersion(), rendition));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var command = new ArchiveMutationCommand(ArchiveMutationRequest.newBuilder().setOperationId(UUID.randomUUID().toString())
                        .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(request.getAddress())).build());
                new ArchiveMutationLedger(tx).execute(CALLER.principalName(), command, entryRecord.mutationRevision);
                assertThat(ledger.findEntry(entry)).isEmpty();
                assertThat(recovery(opened.reclaimer()).recover(object, Instant.now().plusSeconds(60)))
                        .isEqualTo(ArchiveObjectRecovery.Outcome.SKIPPED);
                release.countDown();
                assertThat(read.get(10, TimeUnit.SECONDS).data()).isEqualTo(request.getRenditions(0).getData().toByteArray());
            } finally { release.countDown(); }
        } finally {
            reader.close(); assertThat(reader.awaitIdle(Duration.ofSeconds(5))).isTrue(); reader.attestLocalQuiescence();
        }
        var lostAck = recovery((namespace, key) -> {
            opened.reclaimer().reclaim(namespace, key);
            throw new IllegalStateException("injected reclamation acknowledgment loss");
        });
        var cutoff = Instant.now().plusSeconds(60);
        assertThatThrownBy(() -> lostAck.recover(object, cutoff)).hasRootCauseMessage("injected reclamation acknowledgment loss");
        assertThat(state(object)).isEqualTo("DELETING");
        assertThatThrownBy(() -> opened.store().get("archive-bytes", rendition.getObjectKey())).isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThat(recovery(opened.reclaimer()).recover(object, cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThat(state(object)).isEqualTo("DELETED");
    }
}
