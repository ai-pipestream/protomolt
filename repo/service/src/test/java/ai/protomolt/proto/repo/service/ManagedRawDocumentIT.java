package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.container.lifecycle.JdbcPurgeQueue;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

/** Library/gRPC binding parity over actual PostgreSQL and object-store bytes. */
@Testcontainers
class ManagedRawDocumentIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static final String ACCOUNT = "managed-account";
    static final String BACKEND = "test-localstack-backend-generation-1";
    static final RepositoryCaller CALLER = new RepositoryCaller("managed-test", true);
    static LedgerDatabase database;
    static Tx tx;
    static DocumentLedger documents;
    static DriveLedger drives;
    static RawObjectLedger raw;
    static OpenedBlobStore opened;
    static BlobStore store;
    static DocumentOperations engine;
    static DriveRecord primary;
    static DriveRecord secondary;
    static Server server;
    static ManagedChannel channel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub rpc;

    @BeforeAll static void boot() throws Exception {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        documents = new DocumentLedger(tx);
        drives = new DriveLedger(tx);
        raw = documents.rawObjects();
        opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(),
                "region", S3.getRegion(), "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(),
                "path-style", "true", "conditional-writes", "false"));
        store = opened.store();
        primary = drive("managed-primary");
        secondary = drive("managed-secondary");
        engine = engine(store, BACKEND);
        String name = "managed-document-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(name)
                .addService(new DocumentGrpcService(engine, new BlobOperations(store, drives))).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        rpc = DocumentServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll static void close() throws Exception {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
        if (opened != null) opened.close();
        if (database != null) database.close();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void orphanSweepCannotDeletePartsBetweenProviderWriteAndPublication(boolean transport) throws Exception {
        var doc = Document.newBuilder().setDocId("sweep-race-" + UUID.randomUUID())
                .setOwnership(OwnershipContext.newBuilder().setAccountId(ACCOUNT).setDatasourceId("source")).build();
        var request = save(doc, primary);
        var landed = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var key = new java.util.concurrent.atomic.AtomicReference<String>();
        BlobStore gated = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    try {
                        if (method.getName().equals("put")) assertLegacyReservation(((BlobStore.PutSpec)args[0]).key());
                        var result = method.invoke(store, args);
                        if (method.getName().equals("put")) {
                            key.set(((BlobStore.PutSpec) args[0]).key());
                            landed.countDown();
                            if (!release.await(15, java.util.concurrent.TimeUnit.SECONDS))
                                throw new AssertionError("Timed out waiting for sweep between write and publication");
                        }
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var writer = engine(gated, BACKEND);
        String name = "document-sweep-race-" + UUID.randomUUID();
        var host = InProcessServerBuilder.forName(name).addService(new DocumentGrpcService(writer, new BlobOperations(store, drives))).build().start();
        var connection = InProcessChannelBuilder.forName(name).build();
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> transport ? DocumentServiceGrpc.newBlockingStub(connection).saveDocument(request)
                    : writer.saveDocument(CALLER, request));
            try {
                assertThat(landed.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThat(store.get(primary.bucket, key.get()).data()).isNotEmpty();
                var report = new ai.protomolt.proto.repo.container.lifecycle.StorageReconciler(documents)
                        .reconcile(store, primary.bucket, key.get(), Duration.ZERO, false);
                release.countDown();
                var saved = pending.get(10, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(report.deleted()).as("an uncommitted document part is not an orphan").isZero();
                assertThat(documents.findByNodeId(UUID.fromString(saved.getNodeId()))).isPresent();
                assertThat(writer.getDocument(CALLER, GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build()).getDocument())
                        .isEqualTo(doc);
            } finally { release.countDown(); }
        } finally { connection.shutdownNow(); host.shutdownNow(); }
    }

    @Test void dedupeChecksIdentityAndFullRewriteCanReleaseReferences() {
        for (boolean transport : List.of(false, true)) {
            var seeded = seed();
            var request = save(seeded.document(), primary);
            assertThat(invoke(transport, request).getDeduplicated()).isTrue();
            assertThat(raw.references(seeded.row().nodeId)).containsExactly(seeded.raw().rawId);
            var before = documents.findByNodeId(seeded.row().nodeId).orElseThrow();
            var badBlob = seeded.document().getBlobBag().getBlob().toBuilder().setChecksum("0".repeat(64));
            var bad = request.toBuilder().setDocument(seeded.document().toBuilder()
                    .setBlobBag(BlobBag.newBuilder().setBlob(badBlob))).build();
            assertRejected(transport, bad, RepositoryException.Code.FAILED_PRECONDITION);
            var after = documents.findByNodeId(seeded.row().nodeId).orElseThrow();
            assertThat(after.mutationRevision).isEqualTo(before.mutationRevision);
            assertThat(after.reprocessCount).isEqualTo(before.reprocessCount);
            var removed = request.toBuilder().setDocument(seeded.document().toBuilder().clearBlobBag()).build();
            invoke(transport, removed);
            assertThat(raw.references(seeded.row().nodeId)).isEmpty();
            assertThat(raw.claimCleanup(seeded.raw().rawId, Instant.now().plusSeconds(60))).isPresent();
        }
    }

    @Test void arbitrarySameAccountManagedReferenceCannotAcquireOwnership() {
        for (boolean transport : List.of(false, true)) {
            var seeded = seed();
            var copied = seeded.document().toBuilder().setDocId("unadmitted-" + UUID.randomUUID()).build();
            assertRejected(transport, save(copied, primary), RepositoryException.Code.FAILED_PRECONDITION);
            assertThat(raw.references(seeded.row().nodeId)).containsExactly(seeded.raw().rawId);
        }
    }

    @Test void partialCopyPinsSourceRawBytesAcrossDrivesAndSourceDeletion() {
        for (boolean transport : List.of(false, true)) {
            var seeded = seed();
            var candidate = seeded.document().toBuilder().setDocId("copied-" + UUID.randomUUID()).clearBlobBag().build();
            var request = save(candidate, secondary).toBuilder()
                    .addPartsWritten(DocumentPart.DOCUMENT_PART_CORE)
                    .setCopyUnwrittenPartsFrom(seeded.address()).build();
            var copied = invoke(transport, request);
            UUID copiedId = UUID.fromString(copied.getNodeId());
            assertThat(raw.references(copiedId)).containsExactly(seeded.raw().rawId);
            var stored = engine.getDocument(CALLER, GetDocumentRequest.newBuilder().setNodeId(copied.getNodeId()).build());
            assertThat(stored.getDocument().getBlobBag()).isEqualTo(seeded.document().getBlobBag());
            var delete = DeleteDocumentRequest.newBuilder().setPurgeStorage(true)
                    .setByReference(DeleteDocumentByReferenceCommand.newBuilder().setAddress(seeded.address())).build();
            if (transport) rpc.deleteDocument(delete); else engine.deleteDocument(CALLER, delete);
            assertThat(raw.references(seeded.row().nodeId)).isEmpty();
            assertThat(raw.claimCleanup(seeded.raw().rawId, Instant.now().plusSeconds(60))).isEmpty();
            assertThat(store.get(primary.bucket, seeded.raw().objectKey).data()).isEqualTo(seeded.bytes());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void managedPartialCopyRetainsRawReferences(boolean transport, boolean managedSource) throws Exception {
        var seeded = seed();
        var profile = new ManagedBackendLedger.Profile(new BackendIdentity("s3", "s3/v1", Map.of(
                "endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(), "path-style", "true")), "raw-test-realm");
        new ManagedBackendLedger(tx).bind(BACKEND, profile);
        var budget = new PayloadBudget(1024 * 1024);
        var reader = new DocumentPartReader((generation, retained) -> {
            assertThat(generation).isEqualTo(BACKEND);
            assertThat(retained).isEqualTo(profile);
            return store;
        }, 4, 1024 * 1024, budget);
        var writer = new DocumentAttemptWriter(tx, drives, BACKEND, profile.identity(), opened, budget);
        var operations = new DocumentOperations(documents, drives, tx, store, new PartStorage(),
                new JdbcPurgeQueue(tx), null, BACKEND, reader, writer);
        String name = "managed-partial-raw-" + UUID.randomUUID();
        var host = InProcessServerBuilder.forName(name).addService(new DocumentGrpcService(operations,
                new BlobOperations(store, drives))).build().start();
        var connection = InProcessChannelBuilder.forName(name).build();
        try {
            if (managedSource) operations.saveDocument(CALLER,
                    save(seeded.document(), primary).toBuilder().setForceSave(true).build());
            var candidate = seeded.document().toBuilder().setDocId("managed-copy-" + UUID.randomUUID()).clearBlobBag().build();
            var request = save(candidate, secondary).toBuilder().addPartsWritten(DocumentPart.DOCUMENT_PART_CORE)
                    .setCopyUnwrittenPartsFrom(seeded.address()).build();
            var copied = transport ? DocumentServiceGrpc.newBlockingStub(connection).saveDocument(request)
                    : operations.saveDocument(CALLER, request);
            UUID copiedId = UUID.fromString(copied.getNodeId());
            assertThat(raw.references(copiedId)).containsExactly(seeded.raw().rawId);
            assertThat(raw.references(seeded.row().nodeId)).containsExactly(seeded.raw().rawId);
            var row = documents.findByNodeId(copiedId).orElseThrow();
            assertThat(new DocumentPublicationLedger(tx).findForRead(row)).isPresent();
            var get = GetDocumentRequest.newBuilder().setNodeId(copied.getNodeId()).build();
            var stored = transport ? DocumentServiceGrpc.newBlockingStub(connection).getDocument(get)
                    : operations.getDocument(CALLER, get);
            assertThat(stored.getDocument().getBlobBag()).isEqualTo(seeded.document().getBlobBag());
            var delete = DeleteDocumentRequest.newBuilder().setPurgeStorage(true)
                    .setByReference(DeleteDocumentByReferenceCommand.newBuilder().setAddress(seeded.address())).build();
            if (managedSource) {
                var before = documents.findByNodeId(seeded.row().nodeId).orElseThrow();
                long pendingBefore = tx.readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_purges WHERE node_id=:node")
                        .setParameter("node", before.nodeId).getSingleResult()).longValue());
                for (boolean purge : List.of(false, true)) {
                    var rejected = delete.toBuilder().setPurgeStorage(purge).build();
                    if (transport) assertThatThrownBy(() -> DocumentServiceGrpc.newBlockingStub(connection).deleteDocument(rejected))
                            .isInstanceOfSatisfying(StatusRuntimeException.class,
                                    error -> assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
                    else assertThatThrownBy(() -> operations.deleteDocument(CALLER, rejected))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                }
                var after = documents.findByNodeId(before.nodeId).orElseThrow();
                assertThat(after.status).isEqualTo(before.status);
                assertThat(after.pendingPurgeId).isEqualTo(before.pendingPurgeId);
                assertThat(after.mutationRevision).isEqualTo(before.mutationRevision);
                long pendingAfter = tx.readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_purges WHERE node_id=:node")
                        .setParameter("node", before.nodeId).getSingleResult()).longValue());
                assertThat(pendingAfter).isEqualTo(pendingBefore);
                assertThat(raw.references(before.nodeId)).containsExactly(seeded.raw().rawId);
                assertThat(operations.getDocument(CALLER, GetDocumentRequest.newBuilder().setNodeId(before.nodeId.toString()).build())
                        .getDocument().getBlobBag()).isEqualTo(seeded.document().getBlobBag());
            } else {
                if (transport) DocumentServiceGrpc.newBlockingStub(connection).deleteDocument(delete);
                else operations.deleteDocument(CALLER, delete);
                assertThat(raw.references(seeded.row().nodeId)).isEmpty();
            }
            assertThat(raw.references(copiedId)).containsExactly(seeded.raw().rawId);
            assertThat(operations.getDocument(CALLER, get).getDocument().getBlobBag())
                    .isEqualTo(seeded.document().getBlobBag());
            assertThat(raw.claimCleanup(seeded.raw().rawId, Instant.now().plusSeconds(60))).isEmpty();
            assertThat(store.get(primary.bucket, seeded.raw().objectKey).data()).isEqualTo(seeded.bytes());
        } finally {
            connection.shutdownNow(); host.shutdownNow();
            writer.close(); reader.close();
            assertThat(writer.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(reader.awaitIdle(Duration.ofSeconds(5))).isTrue();
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void corruptedSourceFragmentCannotTransferRawBindings() {
        for (boolean transport : List.of(false, true)) {
            var seeded = seed();
            var part = seeded.row().readManifest().getPartsList().stream()
                    .filter(p -> p.getPart() == DocumentPart.DOCUMENT_PART_BLOBS).findFirst().orElseThrow();
            store.put(new BlobStore.PutSpec(primary.bucket, part.getObjectKey(), "application/x-protobuf", null, null),
                    new byte[] {1, 2, 3});
            var request = save(seeded.document().toBuilder().setDocId("corrupt-copy-" + UUID.randomUUID()).build(), primary)
                    .toBuilder().addPartsWritten(DocumentPart.DOCUMENT_PART_CORE)
                    .setCopyUnwrittenPartsFrom(seeded.address()).build();
            assertRejected(transport, request, RepositoryException.Code.FAILED_PRECONDITION);
        }
    }

    @Test void backendMismatchAndMissingQualificationFailClosed() {
        var seeded = seed();
        for (String identity : new String[] {null, "different-backend"}) {
            assertThatThrownBy(() -> engine(store, identity).saveDocument(CALLER, save(seeded.document(), primary)))
                    .isInstanceOfSatisfying(RepositoryException.class, error -> assertThat(error.code()).isEqualTo(
                            identity == null ? RepositoryException.Code.UNSUPPORTED : RepositoryException.Code.FAILED_PRECONDITION));
        }
        assertThat(documents.findByNodeId(seeded.row().nodeId).orElseThrow().reprocessCount).isZero();
    }

    @Test void sourceBindingChangeDuringCopyAbortsWithoutPublishingDestination() throws Exception {
        for (boolean transport : List.of(false, true)) {
            var seeded = seed();
            var fired = new java.util.concurrent.atomic.AtomicBoolean();
            BlobStore racing = aroundCopies(() -> {
                if (fired.compareAndSet(false, true)) tx.inTransaction(em -> {
                    var source = em.find(DocumentRecord.class, seeded.row().nodeId, jakarta.persistence.LockModeType.PESSIMISTIC_WRITE);
                    raw.replaceReferences(em, source, List.of());
                });
            });
            var request = save(seeded.document().toBuilder().setDocId("race-" + UUID.randomUUID()).build(), secondary)
                    .toBuilder().addPartsWritten(DocumentPart.DOCUMENT_PART_CORE)
                    .setCopyUnwrittenPartsFrom(seeded.address()).build();
            assertRacingCopyRejected(transport, racing, request, Status.Code.ABORTED);
            assertThat(fired).isTrue();
            var address = seeded.address().toBuilder().setDocId(request.getDocument().getDocId()).build();
            assertThat(documents.findByReference(address)).isEmpty();
            // This deliberately changes only the reference table, so the extra
            // binding-set check is exercised independently of the revision guard.
            assertThat(documents.findByNodeId(seeded.row().nodeId).orElseThrow().mutationRevision)
                    .isEqualTo(seeded.row().mutationRevision);
        }
    }

    @Test void sourceBytesChangedAfterPreflightCannotPublishCorruptCopiedBlobs() throws Exception {
        for (boolean transport : List.of(false, true)) {
            var seeded = seed();
            var part = seeded.row().readManifest().getPartsList().stream()
                    .filter(p -> p.getPart() == DocumentPart.DOCUMENT_PART_BLOBS).findFirst().orElseThrow();
            var fired = new java.util.concurrent.atomic.AtomicBoolean();
            BlobStore racing = aroundCopies(() -> {
                if (fired.compareAndSet(false, true)) store.put(new BlobStore.PutSpec(primary.bucket,
                        part.getObjectKey(), "application/x-protobuf", null, null), new byte[] {7, 8, 9});
            });
            var request = save(seeded.document().toBuilder().setDocId("bytes-race-" + UUID.randomUUID()).build(), secondary)
                    .toBuilder().addPartsWritten(DocumentPart.DOCUMENT_PART_CORE)
                    .setCopyUnwrittenPartsFrom(seeded.address()).build();
            assertRacingCopyRejected(transport, racing, request, Status.Code.FAILED_PRECONDITION);
            assertThat(fired).isTrue();
            assertThat(documents.findByReference(seeded.address().toBuilder()
                    .setDocId(request.getDocument().getDocId()).build())).isEmpty();
        }
    }

    @Test void providerChangeDuringCopyAbortsEvenWhenDriveNameAndBucketMatch() throws Exception {
        for (boolean transport : List.of(false, true)) {
            var seeded = seed();
            var fired = new java.util.concurrent.atomic.AtomicBoolean();
            BlobStore racing = aroundCopies(() -> {
                if (fired.compareAndSet(false, true)) tx.inTransaction(em -> {
                    em.find(DriveRecord.class, primary.driveId, jakarta.persistence.LockModeType.PESSIMISTIC_WRITE)
                            .provider = "redis";
                });
            });
            var request = save(seeded.document().toBuilder().setDocId("drive-race-" + UUID.randomUUID()).build(), secondary)
                    .toBuilder().addPartsWritten(DocumentPart.DOCUMENT_PART_CORE)
                    .setCopyUnwrittenPartsFrom(seeded.address()).build();
            try {
                assertRacingCopyRejected(transport, racing, request, Status.Code.ABORTED);
                assertThat(fired).isTrue();
                assertThat(documents.findByReference(seeded.address().toBuilder()
                        .setDocId(request.getDocument().getDocId()).build())).isEmpty();
                assertThat(raw.references(seeded.row().nodeId)).containsExactly(seeded.raw().rawId);
            } finally {
                tx.inTransaction(em -> {
                    em.find(DriveRecord.class, primary.driveId, jakarta.persistence.LockModeType.PESSIMISTIC_WRITE)
                            .provider = "s3";
                });
            }
        }
    }

    private static BlobStore aroundCopies(Runnable beforeCopy) {
        return (BlobStore) java.lang.reflect.Proxy.newProxyInstance(ManagedRawDocumentIT.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("copy")) {
                        assertLegacyReservation((String)args[3]);
                        beforeCopy.run();
                    }
                    try { return method.invoke(store, args); }
                    catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                });
    }

    private static void assertLegacyReservation(String key) {
        long count = tx.readOnly(em -> ((Number)em.createNativeQuery("""
                SELECT count(*) FROM document_part_key_reservations WHERE object_key=:key AND attempt_id IS NULL
                """).setParameter("key",key).getSingleResult()).longValue());
        assertThat(count).as("destination is reserved before provider I/O: %s",key).isEqualTo(1);
    }

    private static void assertRacingCopyRejected(boolean transport, BlobStore racing, SaveDocumentRequest request,
            Status.Code expected) throws Exception {
        var operations = engine(racing, BACKEND);
        if (!transport) {
            assertThatThrownBy(() -> operations.saveDocument(CALLER, request)).isInstanceOfSatisfying(RepositoryException.class,
                    error -> assertThat(error.code()).isEqualTo(expected == Status.Code.ABORTED
                            ? RepositoryException.Code.CONFLICT : RepositoryException.Code.FAILED_PRECONDITION));
            return;
        }
        String name = "managed-race-" + UUID.randomUUID();
        Server endpoint = InProcessServerBuilder.forName(name).addService(new DocumentGrpcService(operations,
                new BlobOperations(racing, drives))).build().start();
        var connection = InProcessChannelBuilder.forName(name).build();
        try {
            assertThatThrownBy(() -> DocumentServiceGrpc.newBlockingStub(connection).saveDocument(request))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            error -> assertThat(error.getStatus().getCode()).isEqualTo(expected));
        } finally {
            connection.shutdownNow();
            endpoint.shutdownNow();
        }
    }

    private static SaveDocumentResponse invoke(boolean transport, SaveDocumentRequest request) {
        return transport ? rpc.saveDocument(request) : engine.saveDocument(CALLER, request);
    }

    private static void assertRejected(boolean transport, SaveDocumentRequest request, RepositoryException.Code code) {
        if (transport) assertThatThrownBy(() -> invoke(true, request)).isInstanceOfSatisfying(StatusRuntimeException.class,
                error -> assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.valueOf(code.name())));
        else assertThatThrownBy(() -> invoke(false, request)).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(code));
    }

    private static DocumentOperations engine(BlobStore bytes, String backend) {
        return new DocumentOperations(documents, drives, tx, bytes, new PartStorage(), new JdbcPurgeQueue(tx), null, backend);
    }

    private static DriveRecord drive(String name) {
        var row = new DriveRecord();
        row.driveId = UUID.randomUUID();
        row.accountId = ACCOUNT;
        row.name = name;
        row.driveType = "INTAKE";
        row.bucket = name;
        row.prefix = name;
        opened.ensureNamespace(row.bucket);
        return drives.insert(row);
    }

    private static SaveDocumentRequest save(Document doc, DriveRecord drive) {
        return SaveDocumentRequest.newBuilder().setDocument(doc).setDrive(drive.name)
                .setUseDatasourceId(true).setGraphId("intake:" + ACCOUNT).build();
    }

    private record Seed(Document document, NodeAddress address, DocumentRecord row, RawObjectRecord raw, byte[] bytes) {}

    /** Seed the reviewed lower-level admission transaction; no HTTP ingestion is implied. */
    private static Seed seed() {
        byte[] bytes = ("raw-body-" + UUID.randomUUID()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var admission = raw.begin(new RawObjectLedger.Location(ACCOUNT, BACKEND, primary.driveId, primary.name,
                primary.bucket, DriveKeys.managedRaw(primary.prefix, UUID.randomUUID())), bytes.length,
                "application/octet-stream", Duration.ofMinutes(5));
        var put = store.put(new BlobStore.PutSpec(primary.bucket, admission.objectKey, admission.contentType, null, null), bytes);
        var verified = raw.verify(admission.rawId, admission.leaseToken, bytes.length,
                DocumentPartCodec.sha256Hex(bytes), put.versionId(), put.eTag());
        var ref = FileStorageReference.newBuilder().setDriveName(primary.name).setObjectKey(verified.objectKey);
        if (verified.providerVersion != null) ref.setVersionId(verified.providerVersion);
        var doc = Document.newBuilder().setDocId("managed-" + UUID.randomUUID())
                .setOwnership(OwnershipContext.newBuilder().setAccountId(ACCOUNT).setDatasourceId("source"))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId(UUID.randomUUID().toString())
                        .setDriveId(primary.name).setStorageRef(ref).setMimeType(verified.contentType)
                        .setSizeBytes(bytes.length).setChecksum(verified.sha256).setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256)))
                .build();
        var address = NodeAddress.newBuilder().setDocId(doc.getDocId()).setAccountId(ACCOUNT)
                .setGraphAddressId("source").setGraphId("intake:" + ACCOUNT).build();
        UUID nodeId = DocumentIds.nodeId(address);
        String prefix = primary.prefix + "/documents/" + ACCOUNT + "/" + nodeId;
        var written = new PartStorage().writeParts(store, primary.bucket, prefix + "/attempts/" + UUID.randomUUID(),
                doc, PartLayouts.document(), address, null, "application/x-protobuf", Map.of(), true, 1);
        var row = new DocumentRecord();
        row.nodeId = nodeId;
        row.docId = doc.getDocId();
        row.accountId = ACCOUNT;
        row.datasourceId = "source";
        row.graphAddressId = "source";
        row.graphId = address.getGraphId();
        row.rowKind = DocumentRowKind.INTAKE;
        row.driveName = primary.name;
        row.objectKey = prefix;
        row.checksum = written.rootChecksum();
        row.etag = written.coreEtag() == null ? "" : written.coreEtag();
        row.sizeBytes = written.totalSizeBytes();
        row.writeManifest(written.manifest());
        var saved = documents.saveIfRevision(row, null, (em, committed) -> raw.replaceReferences(em, committed,
                List.of(new RawObjectLedger.Binding(verified.rawId, verified.leaseToken))));
        return new Seed(doc, address, saved, verified, bytes);
    }
}
