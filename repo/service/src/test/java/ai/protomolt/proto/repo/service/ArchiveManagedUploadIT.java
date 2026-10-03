package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.asset.v1.BridgeStatus;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.archive.*;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real SQL publication and S3 writes, including uncertain provider outcomes. */
@Testcontainers
class ArchiveManagedUploadIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static LedgerDatabase database;
    static Tx tx;
    static ArchiveLedger ledger;
    static DriveLedger drives;
    static OpenedBlobStore opened;
    static ArchiveOperations managed;
    static final RepositoryCaller CALLER = new RepositoryCaller("archive-managed-test", true);

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new ArchiveLedger(tx);
        drives = new DriveLedger(tx);
        var providers = BlobStores.discover();
        var options = Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(), "path-style", "true", "conditional-writes", "false");
        opened = providers.open("s3", options);
        opened.ensureNamespace("managed-archive");
        new ManagedBackendLedger(tx).bind("original", new ManagedBackendLedger.Profile(providers.managedIdentity("s3", options), "original-realm"));
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID();
        drive.accountId = "account";
        drive.name = "archive-drive";
        drive.bucket = "managed-archive";
        drive.prefix = "archive";
        drive.driveType = "CUSTOM";
        drives.insert(drive);
        managed = operations(opened.store());
        managed.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setAccountId("account").setName("records").setDriveName("archive-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
    }

    @AfterAll static void close() throws Exception {
        try { if (opened != null) opened.close(); }
        finally { if (database != null) database.close(); }
    }

    @AfterEach void completedOperationsLeaveNoReaderPins() {
        long pins = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM archive_read_pins")
                .getSingleResult()).longValue());
        assertThat(pins).isZero();
    }

    static ArchiveOperations operations(BlobStore writerStore) {
        return operations(writerStore, opened.store());
    }

    static ArchiveOperations operations(BlobStore writerStore, BlobStore readerStore) {
        var reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> {
            if (!generation.equals("original") || !realm.equals("original-realm"))
                throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Original backend unavailable");
            return readerStore;
        });
        return new ArchiveOperations(ledger, drives, opened.store(), BridgeEngine.standard(), reader,
                new ArchiveObjectWriter(new ArchiveUploadLedger(tx), writerStore, "original", opened.capabilities(), Duration.ofMinutes(5)));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void managedReadsBoundProviderBytesToThePublishedLength(boolean transport) throws Exception {
        var bounded = new java.util.concurrent.atomic.AtomicInteger();
        var unbounded = new java.util.concurrent.atomic.AtomicInteger();
        var limit = new java.util.concurrent.atomic.AtomicInteger(-1);
        BlobStore observed = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("get")) unbounded.incrementAndGet();
                    if (method.getName().equals("getBounded")) {
                        bounded.incrementAndGet(); limit.set((int) args[3]);
                    }
                    try { return method.invoke(opened.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var repository = operations(opened.store(), observed);
        var request = request("recorded bytes");
        var saved = repository.putEntry(CALLER, request);
        var rendition = saved.getManifest().getRenditions(0);
        String name = "archive-bounded-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(repository)).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            var readRequest = GetEntryRequest.newBuilder().setAddress(request.getAddress()).build();
            java.util.function.Supplier<GetEntryResponse> read = transport
                    ? () -> ArchiveServiceGrpc.newBlockingStub(channel).getEntry(readRequest)
                    : () -> repository.getEntry(CALLER, readRequest);
            assertThat(read.get().getRenditions(0).getData()).isEqualTo(request.getRenditions(0).getData());
            assertThat(bounded.get()).isEqualTo(1);
            assertThat(unbounded.get()).isZero();
            assertThat(limit.get()).isEqualTo(rendition.getSizeBytes());
            // This fixture uses an unversioned namespace. Replace real provider
            // bytes to simulate corruption after publication, not a fake GET.
            byte[] oversized = new byte[(int) rendition.getSizeBytes() + 1];
            opened.store().put(new BlobStore.PutSpec("managed-archive", rendition.getObjectKey(), "text/plain", Map.of(),
                    ArchiveManifests.sha256Hex(oversized)), oversized);
            if (transport) {
                assertThatThrownBy(read::get).isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class,
                        failure -> assertThat(failure.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.DATA_LOSS));
            } else {
                assertThatThrownBy(read::get).isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS))
                        .hasCauseInstanceOf(BlobStore.BlobReadLimitException.class);
            }
            assertThat(bounded.get()).isEqualTo(2);
            assertThat(unbounded.get()).isZero();
        } finally {
            channel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void publicationReuseAndHistoricalReadsShareLibraryAndGrpcBehavior(boolean transport) throws Exception {
        String name = "archive-managed-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(managed)).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            var stub = ArchiveServiceGrpc.newBlockingStub(channel);
            var request = request("first bytes");
            java.util.function.Function<PutEntryRequest, PutEntryResponse> put = transport ? stub::putEntry : r -> managed.putEntry(CALLER, r);
            var first = put.apply(request);
            var same = put.apply(request);
            assertThat(same.getVersion()).isEqualTo(first.getVersion());
            assertThat(same.getManifest().getRenditions(0).getStorageObjectId()).isEqualTo(first.getManifest().getRenditions(0).getStorageObjectId());
            assertThat(uploadCount(UUID.fromString(first.getEntryUuid()))).isEqualTo(1);
            var second = put.apply(request.toBuilder().setRenditions(0, request.getRenditions(0).toBuilder()
                    .setData(ByteString.copyFromUtf8("second bytes"))).build());
            assertThat(second.getVersion()).isEqualTo(2);
            assertThat(uploadCount(UUID.fromString(first.getEntryUuid()))).isEqualTo(2);
            var get = GetEntryRequest.newBuilder().setAddress(request.getAddress()).setVersion(1).build();
            var historical = transport ? stub.getEntry(get) : managed.getEntry(CALLER, get);
            assertThat(historical.getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("first bytes"));
            assertThat(new ArchiveObjectLedger(tx).readable(UUID.fromString(first.getEntryUuid()), 1,
                    UUID.fromString(first.getManifest().getRenditions(0).getStorageObjectId()))).isPresent();
            assertThatThrownBy(() -> new ArchiveOperations(ledger, drives, opened.store()).putEntry(CALLER, request))
                    .isInstanceOf(RepositoryException.class);
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }

    @Test void unsupportedBoundedReadNeverFallsBackToAnUnboundedFetch() {
        var bounded = new java.util.concurrent.atomic.AtomicInteger();
        var unbounded = new java.util.concurrent.atomic.AtomicInteger();
        BlobStore unsupported = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("get")) unbounded.incrementAndGet();
                    if (method.getName().equals("getBounded")) {
                        bounded.incrementAndGet();
                        throw new UnsupportedOperationException("Injected missing bounded-read capability");
                    }
                    try { return method.invoke(opened.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var request = request("real bytes on backend without bounded read capability");
        managed.putEntry(CALLER, request);
        assertThatThrownBy(() -> operations(opened.store(), unsupported).getEntry(CALLER,
                GetEntryRequest.newBuilder().setAddress(request.getAddress()).build()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
        assertThat(bounded.get()).isEqualTo(1);
        assertThat(unbounded.get()).isZero();
    }

    @Test void documentOrphanSweepCannotDeleteArchiveObjectsOrUploadCandidates() {
        var request = request("published archive content");
        var saved = managed.putEntry(CALLER, request);
        var live = saved.getManifest().getRenditions(0);
        String prefix = live.getObjectKey().substring(0, live.getObjectKey().indexOf("/original/"));
        String legacyKey = prefix + "/legacy-unbound";
        byte[] legacy = "historical unbound archive content".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        opened.store().put(new BlobStore.PutSpec("managed-archive", legacyKey, "text/plain", Map.of(), null), legacy);
        byte[] candidate = "unpublished candidate".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var pending = live.toBuilder().clearStorageObjectId().setObjectKey(prefix + "/writes/" + UUID.randomUUID())
                .setSizeBytes(candidate.length).setSha256(ArchiveManifests.sha256Hex(candidate)).build();
        var staged = new ArchiveObjectWriter(new ArchiveUploadLedger(tx), opened.store(), "original",
                opened.capabilities(), Duration.ofMinutes(5)).stage(UUID.fromString(saved.getEntryUuid()),
                        "account", "records", "managed-archive", pending, "text/plain", candidate);
        var reconciler = new ai.protomolt.proto.repo.container.lifecycle.StorageReconciler(new DocumentLedger(tx));
        // An explicitly armed zero-age sweep must still respect the archive's ownership domain.
        var report = reconciler.reconcile(opened.store(), "managed-archive", prefix, Duration.ZERO, false);
        assertThat(report.scanned()).isEqualTo(3);
        assertThat(report.orphans()).isZero();
        assertThat(report.deleted()).isZero();
        assertThat(report.orphanKeys()).isEmpty();
        assertThat(opened.store().get("managed-archive", live.getObjectKey()).data()).isEqualTo(request.getRenditions(0).getData().toByteArray());
        assertThat(opened.store().get("managed-archive", pending.getObjectKey()).data()).isEqualTo(candidate);
        assertThat(opened.store().get("managed-archive", legacyKey).data()).isEqualTo(legacy);
        assertThat(managed.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(request.getAddress()).build())
                .getRenditions(0).getData()).isEqualTo(request.getRenditions(0).getData());
        // Excluding generic sweeps must not disable the owning lifecycle's reclamation.
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE archive_object_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE object_id=:id")
                    .setParameter("id", staged.objectId()).executeUpdate();
        });
        assertThat(recovery().recover(staged.objectId(), java.time.Instant.now().plusSeconds(60)))
                .isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThatThrownBy(() -> opened.store().get("managed-archive", pending.getObjectKey())).isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThat(opened.store().get("managed-archive", legacyKey).data()).isEqualTo(legacy);
        assertThat(new ArchiveObjectLedger(tx).readable(UUID.fromString(saved.getEntryUuid()), 1,
                UUID.fromString(live.getStorageObjectId()))).isPresent();
    }

    @Test void lostWriteAcknowledgementLeavesOneStagedObjectAndNoPublishedVersion() {
        var request = request("landed without acknowledgement");
        BlobStore failing = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(opened.store(), args);
                        if (method.getName().equals("put"))
                            throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Injected lost write acknowledgement");
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        assertThatThrownBy(() -> operations(failing).putEntry(CALLER, request)).isInstanceOf(RepositoryException.class);
        UUID entry = ArchiveIds.entryUuid(request.getAddress());
        assertThat(ledger.findEntry(entry)).isEmpty();
        assertThat(uploadCount(entry)).isEqualTo(1);
        tx.readOnly(em -> {
            var row = (Object[]) em.createNativeQuery("SELECT b.object_key,u.state FROM archive_object_bindings b JOIN archive_object_uploads u USING(object_id) WHERE b.entry_uuid=:entry")
                    .setParameter("entry", entry).getSingleResult();
            assertThat(row[1]).isEqualTo("STAGING");
            assertThat(opened.store().get("managed-archive", (String) row[0]).data()).isEqualTo(request.getRenditions(0).getData().toByteArray());
            return null;
        });
    }

    static long uploadCount(UUID entry) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM archive_object_bindings WHERE entry_uuid=:entry")
                .setParameter("entry", entry).getSingleResult()).longValue());
    }

    @Test void sqlPublicationFailureLeavesVerifiedCandidateWithoutBlindRetry() {
        var request = request("bytes awaiting publication");
        UUID entry = ArchiveIds.entryUuid(request.getAddress());
        String function = "reject_managed_" + UUID.randomUUID().toString().replace("-", "");
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION " + function + "() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN RAISE EXCEPTION 'injected managed publication failure' USING ERRCODE='40001'; END; $$").executeUpdate();
            em.createNativeQuery("CREATE TRIGGER " + function + " BEFORE INSERT ON archive_version_object_refs "
                    + "FOR EACH ROW WHEN (NEW.entry_uuid='" + entry + "'::uuid) EXECUTE FUNCTION " + function + "()")
                    .executeUpdate();
        });
        try {
            assertThatThrownBy(() -> managed.putEntry(CALLER, request)).hasStackTraceContaining("injected managed publication failure");
            assertThat(ledger.findEntry(entry)).isEmpty();
            assertThat(uploadCount(entry)).isEqualTo(1);
            tx.readOnly(em -> {
                assertThat(em.createNativeQuery("SELECT u.state FROM archive_object_uploads u JOIN archive_object_bindings b USING(object_id) WHERE b.entry_uuid=:entry")
                        .setParameter("entry", entry).getSingleResult()).isEqualTo("VERIFIED");
                return null;
            });
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER " + function + " ON archive_version_object_refs").executeUpdate();
                em.createNativeQuery("DROP FUNCTION " + function + "()").executeUpdate();
            });
        }
    }

    @Test void invalidManagedStreamingRequestDoesNotConsumeInput() {
        var request = request("unused");
        assertThatThrownBy(() -> managed.uploadStream(CALLER, request.getAddress(), request.getRenditions(0).getRendition(),
                0, "", null, "", null, null, new java.io.InputStream() {
                    @Override public int read() { throw new AssertionError("Unadmitted input was consumed"); }
                })).isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.INVALID_ARGUMENT));
        assertThat(uploadCount(ArchiveIds.entryUuid(request.getAddress()))).isZero();
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void managedStreamingPublishesAndDeduplicatesThroughLibraryAndGrpc(boolean knownHash) throws Exception {
        byte[] bytes = "streamed original".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var request = request("unused");
        String checksum = knownHash ? ArchiveManifests.sha256Hex(bytes) : "";
        var descriptor = request.getRenditions(0).getRendition();
        var first = managed.uploadStream(CALLER, request.getAddress(), descriptor, bytes.length, checksum,
                null, "stream.txt", null, null, new java.io.ByteArrayInputStream(bytes));
        var manifest = ledger.findVersion(ArchiveIds.entryUuid(request.getAddress()), first.version()).orElseThrow();
        var original = ArchiveManifests.fromJson(manifest.manifest).getRenditions(0);
        assertThat(original.getStorageObjectId()).isNotBlank();
        String name = "managed-stream-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(managed)).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            var result = new java.util.concurrent.CompletableFuture<UploadRenditionResponse>();
            var sink = ArchiveServiceGrpc.newStub(channel).uploadRendition(new io.grpc.stub.StreamObserver<UploadRenditionResponse>() {
                @Override public void onNext(UploadRenditionResponse value) { result.complete(value); }
                @Override public void onError(Throwable failure) { result.completeExceptionally(failure); }
                @Override public void onCompleted() {}
            });
            sink.onNext(UploadRenditionRequest.newBuilder().setHeader(UploadRenditionHeader.newBuilder()
                    .setAddress(request.getAddress()).setRendition(descriptor).setSizeBytes(bytes.length).setExpectedSha256(checksum)).build());
            sink.onNext(UploadRenditionRequest.newBuilder().setChunk(ByteString.copyFrom(bytes)).build());
            sink.onCompleted();
            assertThat(result.get(10, java.util.concurrent.TimeUnit.SECONDS).getVersion()).isEqualTo(first.version());
            assertThat(uploadCount(ArchiveIds.entryUuid(request.getAddress()))).isEqualTo(2);
            assertThat(managed.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(request.getAddress()).build())
                    .getRenditions(0).getData().toByteArray()).isEqualTo(bytes);
            var retained = ArchiveManifests.fromJson(ledger.findVersion(ArchiveIds.entryUuid(request.getAddress()), first.version()).orElseThrow().manifest);
            assertThat(retained.getRenditions(0).getStorageObjectId()).isEqualTo(original.getStorageObjectId());
            tx.readOnly(em -> {
                assertThat(((Number) em.createNativeQuery("SELECT count(*) FROM archive_object_uploads u JOIN archive_object_bindings b USING(object_id) WHERE b.entry_uuid=:entry AND u.state='VERIFIED'")
                        .setParameter("entry", ArchiveIds.entryUuid(request.getAddress())).getSingleResult()).longValue()).isEqualTo(1);
                return null;
            });
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }

    @ParameterizedTest @ValueSource(strings = {"short", "long", "checksum"})
    void rejectedManagedStreamLeavesRecoverableCandidateAndNoEntry(String failure) {
        byte[] bytes = "wrong sized input".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var request = request("unused");
        long length = failure.equals("short") ? bytes.length + 1 : failure.equals("long") ? bytes.length - 1 : bytes.length;
        String hash = failure.equals("checksum") ? "a".repeat(64) : "";
        assertThatThrownBy(() -> managed.uploadStream(CALLER, request.getAddress(), request.getRenditions(0).getRendition(),
                length, hash, null, "", null, null, new java.io.ByteArrayInputStream(bytes))).isInstanceOf(Exception.class);
        UUID entry = ArchiveIds.entryUuid(request.getAddress());
        assertThat(ledger.findEntry(entry)).isEmpty();
        assertThat(uploadCount(entry)).isEqualTo(1);
    }

    @Test void managedBridgePublishesBoundDerivedContentAndReusesTheOriginal() {
        var request = request("id;label\n1;first\n2;second\n").toBuilder().setFilename("rows.csv")
                .setDeclared(ai.protomolt.proto.asset.v1.FormatFact.newBuilder().setDelimited(ai.protomolt.proto.asset.v1.DelimitedTable.newBuilder()
                        .setFilename("rows.csv").setDelimiter(";")
                        .setHeader(ai.protomolt.proto.asset.v1.HeaderPresence.HEADER_PRESENCE_PRESENT))).build();
        var first = managed.putEntry(CALLER, request);
        var response = managed.bridgeEntry(CALLER, BridgeEntryRequest.newBuilder().setAddress(request.getAddress()).build());
        assertThat(response.getOutcomesList()).anySatisfy(outcome ->
                assertThat(outcome.getStatus()).isEqualTo(BridgeStatus.BRIDGE_STATUS_PRODUCED));
        var manifest = ArchiveManifests.fromJson(ledger.findVersion(UUID.fromString(first.getEntryUuid()), response.getVersion()).orElseThrow().manifest);
        assertThat(manifest.getRenditionsList()).allSatisfy(item -> assertThat(item.getStorageObjectId()).isNotBlank());
        assertThat(manifest.getRenditionsList().stream().filter(r -> r.getRendition().getName().equals("original")).findFirst().orElseThrow()
                .getStorageObjectId()).isEqualTo(first.getManifest().getRenditions(0).getStorageObjectId());
        var read = managed.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(request.getAddress()).build());
        assertThat(read.getRenditionsList()).anySatisfy(item -> assertThat(item.getRendition().getName()).isEqualTo("schema"));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void unchangedResultsWaitForTheEntryLockAndRejectConcurrentChanges(boolean bridge) throws Exception {
        var request = request("id;label\n1;first\n").toBuilder().setFilename("rows.csv")
                .setDeclared(ai.protomolt.proto.asset.v1.FormatFact.newBuilder().setDelimited(ai.protomolt.proto.asset.v1.DelimitedTable.newBuilder()
                        .setFilename("rows.csv").setDelimiter(";")
                        .setHeader(ai.protomolt.proto.asset.v1.HeaderPresence.HEADER_PRESENCE_PRESENT))).build();
        var saved = managed.putEntry(CALLER, request);
        if (bridge) managed.bridgeEntry(CALLER, BridgeEntryRequest.newBuilder().setAddress(request.getAddress()).build());
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            java.util.concurrent.Future<?> pending;
            try (var em = database.entityManagerFactory().createEntityManager()) {
                em.getTransaction().begin();
                try {
                    var entry = em.find(ArchiveEntryRecord.class, UUID.fromString(saved.getEntryUuid()), jakarta.persistence.LockModeType.PESSIMISTIC_WRITE);
                    int blocker = ((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                    pending = executor.submit(() -> {
                        if (bridge) return managed.bridgeEntry(CALLER, BridgeEntryRequest.newBuilder().setAddress(request.getAddress()).build());
                        var data = request.getRenditions(0).getData();
                        return managed.uploadStream(CALLER, request.getAddress(), request.getRenditions(0).getRendition(), data.size(), "",
                                null, "rows.csv", null, null, data.newInput());
                    });
                    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
                    boolean waiting = false;
                    while (!pending.isDone() && System.nanoTime() < deadline) {
                        waiting = tx.readOnly(other -> ((Number) other.createNativeQuery(
                                "SELECT count(*) FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))")
                                .setParameter("blocker", blocker).getSingleResult()).longValue() > 0);
                        if (waiting) break;
                        Thread.sleep(10);
                    }
                    assertThat(waiting).as("unchanged result must wait for the retained-version lock").isTrue();
                    if (bridge) entry.title = "changed source metadata";
                    else em.remove(entry);
                    em.getTransaction().commit();
                } finally { if (em.getTransaction().isActive()) em.getTransaction().rollback(); }
            }
            assertThatThrownBy(() -> pending.get(10, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RepositoryException.class)
                    .satisfies(failure -> assertThat(((RepositoryException) failure.getCause()).code()).isEqualTo(RepositoryException.Code.CONFLICT));
        }
    }

    @Test void latestOnlyPublicationDropsTheOldReferenceWithoutDeletingBytesInline() {
        String archive = "latest-" + UUID.randomUUID();
        managed.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setAccountId("account").setName(archive).setDriveName("archive-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_NONE)).build());
        var original = request("original");
        var request = original.toBuilder().setAddress(original.getAddress().toBuilder().setArchive(archive)).build();
        var first = managed.putEntry(CALLER, request);
        var second = managed.putEntry(CALLER, request.toBuilder().setRenditions(0, request.getRenditions(0).toBuilder()
                .setData(ByteString.copyFromUtf8("replacement"))).build());
        UUID entry = UUID.fromString(first.getEntryUuid());
        var object = first.getManifest().getRenditions(0);
        assertThat(ledger.findVersion(entry, first.getVersion())).isEmpty();
        assertThat(ledger.findVersion(entry, second.getVersion())).isPresent();
        assertThat(managed.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(request.getAddress()).build())
                .getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("replacement"));
        assertThat(new ArchiveObjectLedger(tx).readable(entry, first.getVersion(), UUID.fromString(object.getStorageObjectId()))).isEmpty();
        assertThat(opened.store().get("managed-archive", object.getObjectKey()).data())
                .isEqualTo(ByteString.copyFromUtf8("original").toByteArray());
        var recovery = recovery();
        assertThat(recovery.recover(UUID.fromString(second.getManifest().getRenditions(0).getStorageObjectId()),
                java.time.Instant.now().plusSeconds(60))).isEqualTo(ArchiveObjectRecovery.Outcome.SKIPPED);
        assertThat(recovery.recover(UUID.fromString(object.getStorageObjectId()), java.time.Instant.now().plusSeconds(60)))
                .isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThatThrownBy(() -> opened.store().get("managed-archive", object.getObjectKey()))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    static ArchiveObjectRecovery recovery() {
        return new ArchiveObjectRecovery(new ArchiveCleanupLedger(tx), new ManagedBackendLedger(tx), (generation, profile) -> {
            assertThat(generation).isEqualTo("original");
            assertThat(profile.storageRealm()).isEqualTo("original-realm");
            assertThat(profile.identity().location().get("endpoint")).isEqualTo(S3.getEndpoint().toString());
            return opened.reclaimer();
        });
    }

    @Test void failedCleanupRetriesOriginalBackendAndReconcilesLateWrites() {
        byte[] bytes = ByteString.copyFromUtf8("abandoned bytes").toByteArray();
        String key = "abandoned/" + UUID.randomUUID();
        var rendition = RenditionManifestEntry.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setState(RenditionState.RENDITION_STATE_PRESENT).setObjectKey(key).setSizeBytes(bytes.length)
                .setSha256(ArchiveManifests.sha256Hex(bytes)).build();
        var staged = new ArchiveObjectWriter(new ArchiveUploadLedger(tx), opened.store(), "original", opened.capabilities(), Duration.ofMinutes(1))
                .stage(UUID.randomUUID(), "account", "records", "managed-archive", rendition, "text/plain", bytes);
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE archive_object_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE object_id=:id")
                    .setParameter("id", staged.objectId()).executeUpdate();
        });
        var offline = new ArchiveObjectRecovery(new ArchiveCleanupLedger(tx), new ManagedBackendLedger(tx), (generation, profile) -> {
            throw new IllegalStateException("Original backend offline");
        });
        var cutoff = java.time.Instant.now().plusSeconds(60);
        assertThatThrownBy(() -> offline.recover(staged.objectId(), cutoff)).hasRootCauseMessage("Original backend offline");
        tx.readOnly(em -> {
            var row = (Object[]) em.createNativeQuery("SELECT state,cleanup_error FROM archive_object_uploads WHERE object_id=:id")
                    .setParameter("id", staged.objectId()).getSingleResult();
            assertThat(row).containsExactly("DELETING", "BACKEND_RECLAMATION_FAILED");
            return null;
        });
        assertThat(opened.store().get("managed-archive", key).data()).isEqualTo(bytes);
        assertThat(recovery().recover(staged.objectId(), cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThatThrownBy(() -> opened.store().get("managed-archive", key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
        // Simulate an expired upload whose provider request completes after reclamation.
        opened.store().put(new BlobStore.PutSpec("managed-archive", key, "text/plain", Map.of(), rendition.getSha256()), bytes);
        assertThat(new ArchiveCleanupLedger(tx).candidates(cutoff, 1000)).contains(staged.objectId());
        assertThat(recovery().recover(staged.objectId(), cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThatThrownBy(() -> opened.store().get("managed-archive", key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    static PutEntryRequest request(String value) {
        return PutEntryRequest.newBuilder().setAddress(EntryAddress.newBuilder().setAccountId("account")
                .setArchive("records").setEntryId(UUID.randomUUID().toString()))
                .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFromUtf8(value))).build();
    }

    @Test void admittedDeleteRemovesLogicalStateBeforeReclaimingOriginalBytesAndReplaysSafely() {
        var request = request("durable deletion");
        var saved = managed.putEntry(CALLER, request);
        var object = saved.getManifest().getRenditions(0);
        var stats = ledger.findStats("account", "records").orElseThrow();
        var command = mutation(ArchiveMutationRequest.newBuilder()
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(request.getAddress())));
        var receipt = execute(command);
        assertThat(receipt.getEntryDeleted()).isTrue();
        assertThat(receipt.getVersionsRemoved()).isEqualTo(1);
        assertThat(receipt.getObjectsPending()).isEqualTo(1);
        assertThat(ledger.findEntry(ArchiveIds.entryUuid(request.getAddress()))).isEmpty();
        var after = ledger.findStats("account", "records").orElseThrow();
        assertThat(after.entries).isEqualTo(stats.entries - 1);
        assertThat(after.versions).isEqualTo(stats.versions - 1);
        assertThat(after.retainedBytes).isEqualTo(stats.retainedBytes - object.getSizeBytes());
        assertThat(after.currentBytes).isEqualTo(stats.currentBytes - object.getSizeBytes());
        assertThat(opened.store().get("managed-archive", object.getObjectKey()).data())
                .isEqualTo(request.getRenditions(0).getData().toByteArray());
        var replacement = managed.putEntry(CALLER, request);
        assertThat(execute(command)).isEqualTo(receipt);
        assertThat(ledger.findEntry(UUID.fromString(replacement.getEntryUuid()))).isPresent();
        assertThat(recovery().recover(UUID.fromString(object.getStorageObjectId()), java.time.Instant.now().plusSeconds(60)))
                .isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThat(managed.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(request.getAddress()).build())
                .getRenditions(0).getData()).isEqualTo(request.getRenditions(0).getData());
    }

    @Test void admittedPruningKeepsObjectsSharedByRetainedVersions() {
        var request = request("shared");
        var first = managed.putEntry(CALLER, request);
        var secondRequest = request.toBuilder().addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("summary"))
                .setData(ByteString.copyFromUtf8("summary bytes"))).build();
        managed.putEntry(CALLER, secondRequest);
        managed.putEntry(CALLER, secondRequest.toBuilder().setRenditions(0, secondRequest.getRenditions(0).toBuilder()
                .setData(ByteString.copyFromUtf8("new original"))).build());
        var firstPrune = execute(mutation(ArchiveMutationRequest.newBuilder()
                .setPruneVersions(PruneVersionsRequest.newBuilder().setAddress(request.getAddress()).setKeepLatest(2))));
        assertThat(firstPrune.getVersionsRemoved()).isEqualTo(1);
        assertThat(firstPrune.getObjectsTargeted()).isZero();
        assertThat(firstPrune.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
        var secondPrune = execute(mutation(ArchiveMutationRequest.newBuilder()
                .setPruneVersions(PruneVersionsRequest.newBuilder().setAddress(request.getAddress()).setKeepLatest(1))));
        assertThat(secondPrune.getVersionsRemoved()).isEqualTo(1);
        assertThat(secondPrune.getObjectsTargeted()).isEqualTo(1);
        var old = first.getManifest().getRenditions(0);
        assertThat(recovery().recover(UUID.fromString(old.getStorageObjectId()), java.time.Instant.now().plusSeconds(60)))
                .isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThat(managed.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(request.getAddress()).build())
                .getRenditionsList()).extracting(RenditionContent::getData)
                .containsExactlyInAnyOrder(ByteString.copyFromUtf8("new original"), ByteString.copyFromUtf8("summary bytes"));
    }

    @Test void admittedRedactionUpdatesManifestHeadersAndPreservesTombstoneProvenance() {
        var request = request("original");
        var first = managed.putEntry(CALLER, request);
        managed.putEntry(CALLER, request.toBuilder().addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("summary"))
                .setData(ByteString.copyFromUtf8("summary"))).build());
        var command = mutation(ArchiveMutationRequest.newBuilder()
                .setDeleteRendition(DeleteRenditionRequest.newBuilder().setAddress(request.getAddress())
                        .setRendition("original").setReason("redaction requested")));
        var receipt = execute(command);
        assertThat(receipt.getVersionsTombstoned()).isEqualTo(2);
        assertThat(receipt.getObjectsTargeted()).isEqualTo(1);
        for (var version : ledger.allVersions(UUID.fromString(first.getEntryUuid()))) {
            var manifest = ArchiveManifests.fromJson(version.manifest);
            assertThat(manifest.getRootChecksum()).isEqualTo(ArchiveManifests.rootChecksum(manifest.getRenditionsList()))
                    .isEqualTo(version.rootChecksum);
            assertThat(manifest.getTotalBytes()).isEqualTo(ArchiveManifests.totalBytes(manifest.getRenditionsList()))
                    .isEqualTo(version.totalBytes);
            var original = manifest.getRenditionsList().stream().filter(r -> r.getRendition().getName().equals("original")).findFirst().orElseThrow();
            assertThat(original.getState()).isEqualTo(RenditionState.RENDITION_STATE_DELETED);
            assertThat(original.getDeletedReason()).isEqualTo("redaction requested");
            assertThat(original.getStorageObjectId()).isEqualTo(first.getManifest().getRenditions(0).getStorageObjectId());
        }
        var noop = execute(new ArchiveMutationCommand(command.request().toBuilder().setOperationId(UUID.randomUUID().toString()).build()));
        assertThat(noop.getVersionsTombstoned()).isZero();
        assertThat(noop.getObjectsTargeted()).isZero();
        assertThat(execute(command)).isEqualTo(receipt);
    }

    @Test void failedLogicalDeleteLeavesReceiptAbsentAndStoredBytesReadable() {
        var request = request("must survive rollback");
        var saved = managed.putEntry(CALLER, request);
        String function = "reject_delete_" + UUID.randomUUID().toString().replace("-", "");
        var command = mutation(ArchiveMutationRequest.newBuilder()
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(request.getAddress())));
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION " + function + "() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN RAISE EXCEPTION 'injected mutation failure' USING ERRCODE='40001'; END; $$").executeUpdate();
            em.createNativeQuery("CREATE TRIGGER " + function + " BEFORE DELETE ON archive_entries "
                    + "FOR EACH ROW WHEN (OLD.entry_uuid='" + saved.getEntryUuid() + "'::uuid) EXECUTE FUNCTION " + function + "()")
                    .executeUpdate();
        });
        try {
            assertThatThrownBy(() -> execute(command)).hasStackTraceContaining("injected mutation failure");
            assertThat(new ArchiveMutationLedger(tx).find(CALLER.principalName(), "account", command.operationId())).isEmpty();
            assertThat(managed.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(request.getAddress()).build())
                    .getRenditions(0).getData()).isEqualTo(request.getRenditions(0).getData());
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER " + function + " ON archive_entries").executeUpdate();
                em.createNativeQuery("DROP FUNCTION " + function + "()").executeUpdate();
            });
        }
        assertThat(execute(command).getEntryDeleted()).isTrue();
    }

    @Test void receiptObservationsTrackFailureRecoveryAndReopenedCleanup() throws Exception {
        var request = request("observe cleanup");
        var saved = managed.putEntry(CALLER, request);
        var object = saved.getManifest().getRenditions(0);
        UUID objectId = UUID.fromString(object.getStorageObjectId());
        var command = mutation(ArchiveMutationRequest.newBuilder()
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(request.getAddress())));
        var admission = execute(command);
        var observations = new ArchiveMutationObservations(tx);
        java.util.function.Supplier<ArchiveMutationReceipt> observe = () -> observations
                .observe(CALLER.principalName(), "account", command.operationId()).orElseThrow();
        assertThat(observe.get()).isEqualTo(admission);
        assertThat(observations.observe("other", "account", command.operationId())).isEmpty();
        var offline = new ArchiveObjectRecovery(new ArchiveCleanupLedger(tx), new ManagedBackendLedger(tx), (generation, profile) -> {
            throw new IllegalStateException("Original backend offline");
        });
        var cutoff = java.time.Instant.now().plusSeconds(60);
        assertThatThrownBy(() -> offline.recover(objectId, cutoff)).hasRootCauseMessage("Original backend offline");
        var failed = observe.get();
        assertThat(failed.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_RETRY_REQUIRED);
        assertThat(failed.getErrorCode()).isEqualTo("BACKEND_RECLAMATION_FAILED");
        assertThat(failed.getStatusRevision()).isGreaterThan(admission.getStatusRevision());
        assertThat(recovery().recover(objectId, cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        var completed = observe.get();
        assertThat(completed.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
        assertThat(completed.getObjectsConfirmedAbsent()).isEqualTo(1);
        assertThat(completed.hasErrorCode()).isFalse();
        assertThat(completed.getStatusRevision()).isGreaterThan(failed.getStatusRevision());
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var calls = java.util.stream.IntStream.range(0, 8).mapToObj(i -> executor.submit(observe::get)).toList();
            for (var call : calls) assertThat(call.get(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(completed);
        }
        opened.store().put(new BlobStore.PutSpec("managed-archive", object.getObjectKey(), "text/plain", Map.of(), object.getSha256()),
                request.getRenditions(0).getData().toByteArray());
        new ArchiveCleanupLedger(tx).claim(objectId, cutoff).orElseThrow();
        var reopened = observe.get();
        assertThat(reopened.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_RECLAIMING);
        assertThat(reopened.getObjectsPending()).isEqualTo(1);
        assertThat(reopened.getStatusRevision()).isGreaterThan(completed.getStatusRevision());
        assertThat(recovery().recover(objectId, cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThat(observe.get().getStatusRevision()).isGreaterThan(reopened.getStatusRevision());
        assertThat(execute(command)).isEqualTo(admission); // Logical record remains immutable.
    }

    @Test void mutationRecoverySeparatesFailedClaimsFromActiveWorkAndReconciliation() {
        var first = request("first cleanup");
        var second = request("second cleanup");
        UUID firstId = UUID.fromString(managed.putEntry(CALLER, first).getManifest().getRenditions(0).getStorageObjectId());
        UUID secondId = UUID.fromString(managed.putEntry(CALLER, second).getManifest().getRenditions(0).getStorageObjectId());
        execute(mutation(ArchiveMutationRequest.newBuilder().setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(first.getAddress()))));
        execute(mutation(ArchiveMutationRequest.newBuilder().setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(second.getAddress()))));
        var cleanup = new ArchiveCleanupLedger(tx);
        var cutoff = java.time.Instant.now().plusSeconds(60);
        var active = cleanup.claim(firstId, cutoff, java.time.Instant.EPOCH).orElseThrow();
        assertThat(cleanup.claim(firstId, cutoff, java.time.Instant.EPOCH)).isEmpty();
        assertThat(cleanup.mutationCandidates(cutoff, java.time.Instant.EPOCH, 1000)).doesNotContain(firstId).contains(secondId);
        assertThat(cleanup.failed(firstId, active.token(), "PROVIDER_UNAVAILABLE")).isTrue();
        var candidates = cleanup.mutationCandidates(cutoff, java.time.Instant.EPOCH, 1000);
        assertThat(candidates).contains(firstId, secondId);
        assertThat(candidates.indexOf(firstId)).isGreaterThan(candidates.indexOf(secondId));
        assertThat(recovery().recover(secondId, cutoff, java.time.Instant.EPOCH)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThat(cleanup.mutationCandidates(cutoff, java.time.Instant.EPOCH, 1000)).doesNotContain(secondId);
        assertThat(cleanup.candidates(cutoff, 1000)).contains(secondId);
    }

    @ParameterizedTest @ValueSource(strings = {"header", "bytes", "key", "pin"})
    void inconsistentStoredFactsCannotAdmitDestruction(String corruption) {
        var request = request("validate original facts");
        var saved = managed.putEntry(CALLER, request);
        UUID entry = UUID.fromString(saved.getEntryUuid());
        tx.inTransaction(em -> {
            // Inject corruption that could predate the location guard. Disable
            // only that trigger, within this transaction, so handler recovery
            // checks still face real inconsistent persisted facts.
            em.createNativeQuery("ALTER TABLE archive_versions DISABLE TRIGGER archive_location_quarantine").executeUpdate();
            var version = em.find(ArchiveVersionRecord.class, new ArchiveVersionRecord.Key(entry, 1));
            var manifest = ArchiveManifests.fromJson(version.manifest).toBuilder();
            if (corruption.equals("header")) {
                manifest.setTotalBytes(manifest.getTotalBytes() + 1);
                version.totalBytes++;
            } else if (corruption.equals("pin")) {
                em.createNativeQuery("DELETE FROM archive_version_object_refs WHERE entry_uuid=:entry")
                        .setParameter("entry", entry).executeUpdate();
            } else {
                var item = manifest.getRenditions(0).toBuilder();
                if (corruption.equals("key")) item.setObjectKey("wrong-key");
                else item.setSha256("b".repeat(64));
                manifest.setRenditions(0, item);
                manifest.setRootChecksum(ArchiveManifests.rootChecksum(manifest.getRenditionsList()));
                version.rootChecksum = manifest.getRootChecksum();
            }
            version.manifest = ArchiveManifests.toJson(manifest.build());
            em.flush();
            em.createNativeQuery("ALTER TABLE archive_versions ENABLE TRIGGER archive_location_quarantine").executeUpdate();
        });
        var command = mutation(ArchiveMutationRequest.newBuilder()
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(request.getAddress())));
        assertThatThrownBy(() -> execute(command)).isInstanceOf(IllegalStateException.class);
        assertThat(new ArchiveMutationLedger(tx).find(CALLER.principalName(), "account", command.operationId())).isEmpty();
        assertThat(ledger.findEntry(entry)).isPresent();
        assertThat(opened.store().get("managed-archive", saved.getManifest().getRenditions(0).getObjectKey()).data())
                .isEqualTo(request.getRenditions(0).getData().toByteArray());
    }

    static ArchiveMutationCommand mutation(ArchiveMutationRequest.Builder request) {
        return new ArchiveMutationCommand(request.setOperationId(UUID.randomUUID().toString()).build());
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void mutationAndLookupConformanceAcrossLibraryAndAuthenticatedGrpc(boolean transport) throws Exception {
        var operations = new ArchiveMutationOperations(ledger, new ArchiveMutationLedger(tx), new ArchiveMutationObservations(tx));
        String name = "archive-mutations-" + UUID.randomUUID();
        var operator = ai.protomolt.proto.actions.Caller.operator();
        var caller = new RepositoryCaller(operator.name(), operator.unrestricted());
        var server = InProcessServerBuilder.forName(name).addService(io.grpc.ServerInterceptors.intercept(
                new ArchiveMutationGrpcService(operations), new io.grpc.ServerInterceptor() {
                    @Override public <Q, S> io.grpc.ServerCall.Listener<Q> interceptCall(io.grpc.ServerCall<Q, S> call,
                            io.grpc.Metadata headers, io.grpc.ServerCallHandler<Q, S> next) {
                        return io.grpc.Contexts.interceptCall(io.grpc.Context.current()
                                .withValue(ai.protomolt.proto.authz.grpc.CallerContexts.CALLER, operator), call, headers, next);
                    }
                })).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            var stub = ArchiveMutationServiceGrpc.newBlockingStub(channel);
            java.util.function.Function<ArchiveMutationRequest, ArchiveMutationReceipt> mutate = transport
                    ? r -> stub.archiveMutation(r).getReceipt() : r -> operations.mutateArchive(caller, r);
            java.util.function.Function<GetArchiveMutationRequest, ArchiveMutationReceipt> lookup = transport
                    ? r -> stub.getArchiveMutation(r).getReceipt() : r -> operations.getArchiveMutation(caller, r);
            var request = request("version one");
            managed.putEntry(CALLER, request);
            managed.putEntry(CALLER, request.toBuilder().setRenditions(0, request.getRenditions(0).toBuilder()
                    .setData(ByteString.copyFromUtf8("version two"))).build());
            var prune = mutation(ArchiveMutationRequest.newBuilder()
                    .setPruneVersions(PruneVersionsRequest.newBuilder().setAddress(request.getAddress()).setKeepLatest(1))).request();
            var receipt = mutate.apply(prune);
            assertThat(receipt.getVersionsRemoved()).isEqualTo(1);
            assertThat(receipt.getObjectsPending()).isEqualTo(1);
            assertThat(lookup.apply(GetArchiveMutationRequest.newBuilder().setAccountId("account").setOperationId(prune.getOperationId()).build()))
                    .isEqualTo(receipt);
            assertThat(mutate.apply(prune)).isEqualTo(receipt);
            var redaction = mutate.apply(mutation(ArchiveMutationRequest.newBuilder()
                    .setDeleteRendition(DeleteRenditionRequest.newBuilder().setAddress(request.getAddress())
                            .setRendition("original").setReason("remove content"))).request());
            assertThat(redaction.getVersionsTombstoned()).isEqualTo(1);
            var deletion = mutate.apply(mutation(ArchiveMutationRequest.newBuilder()
                    .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(request.getAddress()))).request());
            assertThat(deletion.getEntryDeleted()).isTrue();
            assertThat(deletion.getObjectsTargeted()).isZero();
            var denied = new RepositoryCaller(caller.principalName(), false);
            assertThatThrownBy(() -> operations.mutateArchive(denied, prune)).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            assertThatThrownBy(() -> operations.getArchiveMutation(denied, GetArchiveMutationRequest.newBuilder()
                    .setAccountId("account").setOperationId(prune.getOperationId()).build())).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            var conflict = prune.toBuilder().setPruneVersions(prune.getPruneVersions().toBuilder().setKeepLatest(2)).build();
            assertThatThrownBy(() -> mutate.apply(conflict)).satisfies(failure -> {
                if (transport) assertThat(io.grpc.Status.fromThrowable(failure).getCode()).isEqualTo(io.grpc.Status.Code.ABORTED);
                else assertThat(((RepositoryException) failure).code()).isEqualTo(RepositoryException.Code.CONFLICT);
            });
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }

    @Test void mutationTransportRejectsMissingCallerInsteadOfAssumingOperator() throws Exception {
        String name = "archive-no-caller-" + UUID.randomUUID();
        var operations = new ArchiveMutationOperations(ledger, new ArchiveMutationLedger(tx), new ArchiveMutationObservations(tx));
        var server = InProcessServerBuilder.forName(name).addService(new ArchiveMutationGrpcService(operations)).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            assertThatThrownBy(() -> ArchiveMutationServiceGrpc.newBlockingStub(channel).archiveMutation(ArchiveMutationRequest.getDefaultInstance()))
                    .satisfies(failure -> assertThat(io.grpc.Status.fromThrowable(failure).getCode()).isEqualTo(io.grpc.Status.Code.UNAUTHENTICATED));
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }

    static ArchiveMutationReceipt execute(ArchiveMutationCommand command) {
        long revision = ledger.findEntry(ArchiveIds.entryUuid(command.address())).map(e -> e.mutationRevision).orElse(0L);
        return new ArchiveMutationLedger(tx).execute(CALLER.principalName(), command, revision);
    }
}
