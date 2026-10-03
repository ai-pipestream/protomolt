package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.ledger.DocumentPublicationLedger;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import static org.assertj.core.api.Assertions.*;

/** Real provider byte reads. Publication/authorization SQL is tested separately. */
@Testcontainers
class DocumentPartReaderIT {
    @Container static final org.testcontainers.postgresql.PostgreSQLContainer POSTGRES =
            new org.testcontainers.postgresql.PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static OpenedBlobStore opened;
    static BlobStore store;
    static S3Client admin;
    static final String NAMESPACE = "document-read-original";
    static final String GENERATION = "original-generation";
    static ManagedBackendLedger.Profile profile;
    static LedgerDatabase database;
    static Tx tx;
    static DocumentLedger documents;
    static DriveLedger drives;

    @BeforeAll static void open() {
        opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(),
                "region", S3.getRegion(), "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(),
                "path-style", "true", "conditional-writes", "false"));
        store = opened.store();
        opened.ensureNamespace(NAMESPACE);
        admin = S3Client.builder().endpointOverride(S3.getEndpoint()).region(Region.of(S3.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .forcePathStyle(true).build();
        admin.putBucketVersioning(b -> b.bucket(NAMESPACE).versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
        profile = new ManagedBackendLedger.Profile(new BackendIdentity("s3", "s3/v1", Map.of(
                "endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(), "path-style", "true")), "original-realm");
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        documents = new DocumentLedger(tx);
        drives = new DriveLedger(tx);
        new ManagedBackendLedger(tx).bind(GENERATION, profile);
    }
    @AfterAll static void close() throws Exception {
        if (admin != null) admin.close();
        if (opened != null) opened.close();
        if (database != null) database.close();
    }
    private static DocumentPublicationLedger.Publication publish(byte[] bytes) {
        String key = "documents/read-test/" + UUID.randomUUID() + "/core.pb";
        var put = store.put(new BlobStore.PutSpec(NAMESPACE, key, "application/protobuf", Map.of(), DocumentPartCodec.sha256Hex(bytes)), bytes);
        var measured = store.get(NAMESPACE, key, put.versionId());
        assertThat(measured.data()).isEqualTo(bytes);
        var part = new DocumentPublicationLedger.Part(DocumentPart.DOCUMENT_PART_CORE, "", key, bytes.length,
                DocumentPartCodec.sha256Hex(bytes), measured.versionId(), measured.eTag());
        return new DocumentPublicationLedger.Publication(UUID.randomUUID(), GENERATION, profile, NAMESPACE,
                DocumentManifest.getDefaultInstance(), List.of(part));
    }
    private static DocumentPartReader reader() {
        return new DocumentPartReader((generation, original) -> {
            assertThat(generation).isEqualTo(GENERATION);
            assertThat(original).isEqualTo(profile);
            return store;
        });
    }

    private record Bound(Document expected, DocumentRecord row, DriveRecord drive) {}

    /** Fixture uses guarded SQL publication, not a claim that the public managed writer is enabled. */
    private static Bound bound() {
        String docId = "doc-" + UUID.randomUUID();
        var address = NodeAddress.newBuilder().setAccountId("account").setDocId(docId)
                .setGraphId("intake:account").setGraphAddressId("source").build();
        UUID node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(address);
        UUID attemptId = UUID.randomUUID();
        var doc = Document.newBuilder().setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")).build();
        byte[] bytes = doc.toByteArray();
        String prefix = "documents/account/" + node + "/attempts/" + attemptId + "/";
        String key = prefix + "core.pb";
        String hash = DocumentPartCodec.sha256Hex(bytes);
        var attempts = new DocumentPartAttemptLedger(tx);
        var attempt = attempts.begin(new DocumentPartAttemptLedger.Plan(attemptId,
                new DocumentPartAttemptLedger.Location(node, "account", GENERATION, NAMESPACE), 0, Map.of(),
                List.of(new DocumentPartAttemptLedger.PlannedObject(DocumentPart.DOCUMENT_PART_CORE, "", key,
                        bytes.length, hash, "application/protobuf"))), java.time.Duration.ofMinutes(5));
        var put = store.put(new BlobStore.PutSpec(NAMESPACE, key, "application/protobuf", Map.of(), hash), bytes);
        var actual = store.get(NAMESPACE, key, put.versionId());
        assertThat(actual.data()).isEqualTo(bytes);
        attempts.verify(attempt.id(), attempt.token(), key, actual.data().length,
                DocumentPartCodec.sha256Hex(actual.data()), actual.versionId(), actual.eTag());
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = "account"; drive.name = "read-" + node;
        drive.driveType = "INTAKE"; drive.bucket = NAMESPACE;
        drives.insert(drive);
        var row = new DocumentRecord();
        row.nodeId = node; row.accountId = "account"; row.docId = doc.getDocId();
        row.graphId = "intake:account"; row.graphAddressId = "source";
        row.rowKind = DocumentRowKind.INTAKE; row.datasourceId = "source";
        row.createdAt = java.time.Instant.now(); row.updatedAt = row.createdAt;
        row.driveName = drive.name; row.objectKey = prefix; row.versionId = actual.versionId(); row.etag = actual.eTag();
        row.sizeBytes = (long) bytes.length;
        var manifest = DocumentManifest.newBuilder().setDocVersion(1)
                .setAddress(NodeAddress.newBuilder().setAccountId(row.accountId).setDocId(row.docId)
                        .setGraphId(row.graphId).setGraphAddressId(row.graphAddressId))
                .addParts(PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE)
                        .setState(PartState.PART_STATE_PRESENT).setObjectKey(key).setSizeBytes(bytes.length).setSha256(hash)).build();
        row.writeManifest(manifest); row.checksum = DocumentPartCodec.rootChecksumFromManifest(manifest);
        var saved = documents.saveIfRevision(row, null, (em, committed) -> {
            em.createNativeQuery("""
                    INSERT INTO document_part_publication_history(attempt_id,node_id,publication_revision,body)
                    SELECT :attempt,node_id,mutation_revision,document_publication_body(documents) FROM documents WHERE node_id=:node
                    """).setParameter("attempt", attempt.id()).setParameter("node", node).executeUpdate();
            em.createNativeQuery("INSERT INTO document_part_publications(node_id,attempt_id) VALUES (:node,:attempt)")
                    .setParameter("node", node).setParameter("attempt", attempt.id()).executeUpdate();
        });
        return new Bound(doc, saved, drive);
    }

    private static ai.protomolt.proto.repo.engine.DocumentOperations engine(DocumentPartReader reader) {
        return new ai.protomolt.proto.repo.engine.DocumentOperations(documents, drives, tx, store,
                new ai.protomolt.proto.repo.container.blob.PartStorage(),
                new ai.protomolt.proto.repo.container.lifecycle.JdbcPurgeQueue(tx), null, null, reader);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void legacyWriterCannotRewriteOrCopyBoundPublication(boolean copy) {
        var seeded = bound();
        var touched = new java.util.concurrent.atomic.AtomicInteger();
        BlobStore guarded = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    touched.incrementAndGet();
                    throw new AssertionError("Unqualified legacy writer reached provider " + method.getName());
                });
        var engine = new ai.protomolt.proto.repo.engine.DocumentOperations(documents, drives, tx, guarded,
                new ai.protomolt.proto.repo.container.blob.PartStorage(),
                new ai.protomolt.proto.repo.container.lifecycle.JdbcPurgeQueue(tx), null, null, reader());
        var request = SaveDocumentRequest.newBuilder().setDocument(seeded.expected()).setDrive(seeded.drive().name)
                .setUseDatasourceId(true).setGraphId("intake:account").setForceSave(true);
        if (copy) request.setDocument(seeded.expected().toBuilder().setDocId("copy-" + UUID.randomUUID()))
                .addPartsWritten(DocumentPart.DOCUMENT_PART_CORE)
                .setCopyUnwrittenPartsFrom(seeded.row().readManifest().getAddress());
        assertThatThrownBy(() -> engine.saveDocument(new ai.protomolt.proto.repo.spi.RepositoryCaller("test", true), request.build()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
        assertThat(touched.get()).isZero();
        assertThat(documents.findByNodeId(seeded.row().nodeId).orElseThrow().mutationRevision).isEqualTo(seeded.row().mutationRevision);
        assertThat(engine.getDocument(new ai.protomolt.proto.repo.spi.RepositoryCaller("test", true),
                GetDocumentRequest.newBuilder().setNodeId(seeded.row().nodeId.toString()).build()).getDocument()).isEqualTo(seeded.expected());
    }

    @Test void boundReadUsesOriginalNamespaceLocallyAndOverGrpcAfterDriveChanges() throws Exception {
        var seeded = bound();
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE drives SET bucket='unrelated-current-bucket' WHERE drive_id=:id")
                    .setParameter("id", seeded.drive().driveId).executeUpdate();
        });
        var engine = engine(reader());
        String name = "bound-read-" + UUID.randomUUID();
        var server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                .addService(new DocumentGrpcService(engine, new ai.protomolt.proto.repo.engine.BlobOperations(store, drives))).build().start();
        var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var request = GetDocumentRequest.newBuilder().setNodeId(seeded.row().nodeId.toString()).build();
            assertThat(engine.getDocument(new ai.protomolt.proto.repo.spi.RepositoryCaller("test", true), request).getDocument())
                    .isEqualTo(seeded.expected());
            assertThat(DocumentServiceGrpc.newBlockingStub(channel).getDocument(request).getDocument()).isEqualTo(seeded.expected());
            assertThat(DocumentServiceGrpc.newBlockingStub(channel).getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                    .setAddress(seeded.row().readManifest().getAddress()).build()).getDocument()).isEqualTo(seeded.expected());
        } finally {
            channel.shutdownNow(); server.shutdownNow();
            assertThat(channel.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"missing-reader", "missing-backend", "missing-version", "unavailable", "wrong-etag"})
    void boundFailuresHaveTheSameMeaningLocallyAndOverGrpc(String defect) throws Exception {
        var seeded = bound();
        var publication = new DocumentPublicationLedger(tx).findForRead(seeded.row()).orElseThrow();
        var part = publication.parts().getFirst();
        if (defect.equals("missing-version"))
            admin.deleteObject(b -> b.bucket(publication.namespace()).key(part.key()).versionId(part.providerVersion()));
        BlobStore injected = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    try {
                        var result = method.invoke(store, args);
                        if (method.getName().equals("get")) {
                            if (defect.equals("unavailable"))
                                throw new BlobStoreException(BlobStoreException.Code.UNAVAILABLE, "Injected provider failure", null);
                            if (defect.equals("wrong-etag")) {
                                var actual = (BlobStore.GetResult) result;
                                return new BlobStore.GetResult(actual.data(), actual.contentType(), "wrong-etag", actual.versionId());
                            }
                        }
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var reader = defect.equals("missing-reader") ? null
                : new DocumentPartReader((g, p) -> defect.equals("missing-backend") ? null : injected);
        var engine = engine(reader);
        var expected = switch (defect) {
            case "missing-reader", "missing-backend" -> RepositoryException.Code.FAILED_PRECONDITION;
            case "unavailable" -> RepositoryException.Code.UNAVAILABLE;
            default -> RepositoryException.Code.DATA_LOSS;
        };
        String name = "bound-failure-" + UUID.randomUUID();
        var server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                .addService(new DocumentGrpcService(engine, new ai.protomolt.proto.repo.engine.BlobOperations(store, drives))).build().start();
        var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var request = GetDocumentRequest.newBuilder().setNodeId(seeded.row().nodeId.toString()).build();
            assertThatThrownBy(() -> engine.getDocument(new ai.protomolt.proto.repo.spi.RepositoryCaller("test", true), request))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(expected));
            assertThatThrownBy(() -> DocumentServiceGrpc.newBlockingStub(channel).getDocument(request))
                    .isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode().name()).isEqualTo(expected.name()));
        } finally {
            channel.shutdownNow(); server.shutdownNow();
            assertThat(channel.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void grpcCancellationStopsOutstandingBoundRead(boolean deadline) throws Exception {
        var seeded = bound();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var workerCancelled = new java.util.concurrent.CountDownLatch(1);
        BlobStore delayed = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    try {
                        var result = method.invoke(store, args);
                        if (method.getName().equals("get")) {
                            entered.countDown();
                            try {
                                if (!release.await(30, java.util.concurrent.TimeUnit.SECONDS))
                                    throw new AssertionError("Provider fault gate was not released");
                            } catch (InterruptedException cancelled) {
                                Thread.currentThread().interrupt();
                                workerCancelled.countDown();
                                throw new BlobStoreException(BlobStoreException.Code.CANCELLED, "Injected read cancelled", cancelled);
                            }
                        }
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var engine = engine(new DocumentPartReader((g, p) -> delayed));
        String name = "cancel-bound-read-" + UUID.randomUUID();
        var server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                .addService(new DocumentGrpcService(engine, new ai.protomolt.proto.repo.engine.BlobOperations(store, drives))).build().start();
        var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        var context = io.grpc.Context.current().withCancellation();
        var reply = new java.util.concurrent.CompletableFuture<GetDocumentResponse>();
        try {
            var stub = DocumentServiceGrpc.newStub(channel);
            var timed = deadline ? stub.withDeadlineAfter(10, java.util.concurrent.TimeUnit.SECONDS) : stub;
            context.run(() -> timed.getDocument(GetDocumentRequest.newBuilder().setNodeId(seeded.row().nodeId.toString()).build(),
                    new io.grpc.stub.StreamObserver<>() {
                        @Override public void onNext(GetDocumentResponse value) { reply.complete(value); }
                        @Override public void onError(Throwable failure) { reply.completeExceptionally(failure); }
                        @Override public void onCompleted() {}
                    }));
            assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            if (!deadline) context.cancel(null);
            assertThatThrownBy(() -> reply.get(15, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(io.grpc.StatusRuntimeException.class)
                    .satisfies(e -> assertThat(io.grpc.Status.fromThrowable(e.getCause()).getCode()).isEqualTo(
                            deadline ? io.grpc.Status.Code.DEADLINE_EXCEEDED : io.grpc.Status.Code.CANCELLED));
            // Client completion alone would pass even if the server leaked its worker.
            // The gate follows real GET completion; this does not prove HTTP I/O is interruptible.
            assertThat(workerCancelled.await(5, java.util.concurrent.TimeUnit.SECONDS))
                    .as("server propagates cancellation to its outstanding provider read").isTrue();
        } finally {
            release.countDown(); context.close();
            channel.shutdownNow(); server.shutdownNow();
            assertThat(channel.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }
    @Test void carriedFragmentsPreserveWireBytesThatParsingWouldNormalize() throws Exception {
        var output = new java.io.ByteArrayOutputStream();
        var coded = com.google.protobuf.CodedOutputStream.newInstance(output);
        coded.writeString(Document.DOC_ID_FIELD_NUMBER, "earlier");
        coded.writeString(Document.DOC_ID_FIELD_NUMBER, "final");
        coded.flush();
        byte[] bytes = output.toByteArray();
        assertThat(Document.parseFrom(bytes).getDocId()).isEqualTo("final");
        assertThat(Document.parseFrom(bytes).toByteArray()).isNotEqualTo(bytes);
        var publication = publish(bytes);
        // A replacement at the same key must not change the recorded version being carried.
        opened.store().put(new ai.protomolt.proto.repo.blob.spi.BlobStore.PutSpec(NAMESPACE,
                publication.parts().getFirst().key(), "application/protobuf", java.util.Map.of(), null),
                Document.newBuilder().setDocId("replacement").build().toByteArray());
        var fragments = reader().readFragments(publication, Set.of(), Set.of(),
                ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE);
        assertThat(fragments).hasSize(1);
        assertThat(fragments.getFirst().bytes()).containsExactly(bytes);
        assertThat(fragments.getFirst().sha256()).isEqualTo(publication.parts().getFirst().sha256());
        assertThat(fragments.getFirst().part()).isEqualTo(DocumentPart.DOCUMENT_PART_CORE);
        assertThat(fragments.getFirst().subKey()).isEmpty();
    }

    @Test void carriedFragmentsOwnTheirBytesIndependentlyOfProviderBuffer() {
        byte[] expected = Document.newBuilder().setDocId("detached").build().toByteArray();
        var publication = publish(expected);
        var providerBuffer = new java.util.concurrent.atomic.AtomicReference<byte[]>();
        BlobStore observed = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(store, arguments);
                        if (method.getName().equals("get")) providerBuffer.set(((BlobStore.GetResult) result).data());
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var fragments = new DocumentPartReader((generation, retained) -> observed).readFragments(publication,
                Set.of(), Set.of(), ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE);
        java.util.Arrays.fill(providerBuffer.get(), (byte) 0);
        assertThat(fragments.getFirst().bytes()).containsExactly(expected);
        assertThat(DocumentPartCodec.sha256Hex(fragments.getFirst().bytes())).isEqualTo(fragments.getFirst().sha256());
    }

    @Test void readsRecordedVersionAfterLatestBytesAreReplaced() {
        var expected = Document.newBuilder().setDocId("original").build();
        var publication = publish(expected.toByteArray());
        var part = publication.parts().getFirst();
        assertThat(part.providerVersion()).isNotBlank().isNotEqualTo("null");
        byte[] replacement = Document.newBuilder().setDocId("replacement").build().toByteArray();
        store.put(new BlobStore.PutSpec(NAMESPACE, part.key(), "application/protobuf", Map.of(), null), replacement);
        assertThat(store.get(NAMESPACE, part.key()).data()).isEqualTo(replacement);
        assertThat(reader().read(publication, Set.of(), Set.of(), Document.getDefaultInstance())).isEqualTo(expected);
    }
    @Test void missingRecordedVersionIsDataLossEvenWhenLatestExists() {
        var publication = publish(Document.newBuilder().setDocId("missing").build().toByteArray());
        var part = publication.parts().getFirst();
        store.put(new BlobStore.PutSpec(NAMESPACE, part.key(), "application/protobuf", Map.of(), null), new byte[0]);
        admin.deleteObject(b -> b.bucket(NAMESPACE).key(part.key()).versionId(part.providerVersion()));
        assertThat(store.get(NAMESPACE, part.key()).data()).isEmpty();
        assertThatThrownBy(() -> reader().read(publication, Set.of(), Set.of(), Document.getDefaultInstance()))
                .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
    }
    @Test void checksumMismatchCannotBecomeSuccessfulDocument() {
        var publication = publish(Document.newBuilder().setDocId("corrupt").build().toByteArray());
        var p = publication.parts().getFirst();
        var bad = new DocumentPublicationLedger.Publication(publication.attemptId(), GENERATION, profile, NAMESPACE,
                publication.manifest(), List.of(new DocumentPublicationLedger.Part(p.part(), p.subKey(), p.key(), p.size(),
                        "00".repeat(32), p.providerVersion(), p.etag())));
        assertThatThrownBy(() -> reader().read(bad, Set.of(), Set.of(), Document.getDefaultInstance()))
                .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
    }
    @Test void matchingHashDoesNotMakeMalformedProtobufReadable() {
        var publication = publish(new byte[] {(byte) 0xff});
        assertThatThrownBy(() -> reader().read(publication, Set.of(), Set.of(), Document.getDefaultInstance()))
                .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS))
                .hasCauseInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
    }
    @Test void cancelledCallsRetainCapacityUntilProviderReturns() throws Exception {
        var expected = Document.newBuilder().setDocId("capacity").build();
        var publication = publish(expected.toByteArray());
        var entered = new java.util.concurrent.CountDownLatch(2);
        var release = new java.util.concurrent.CountDownLatch(1);
        var exited = new java.util.concurrent.CountDownLatch(2);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        BlobStore delayed = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    try {
                        var result = method.invoke(store, args);
                        if (method.getName().equals("get")) {
                            calls.incrementAndGet(); entered.countDown();
                            boolean interrupted = false;
                            try {
                                while (release.getCount() != 0) {
                                    try { release.await(); }
                                    catch (InterruptedException injected) { interrupted = true; }
                                }
                            } finally {
                                if (interrupted) Thread.currentThread().interrupt();
                                exited.countDown();
                            }
                        }
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var reader = new DocumentPartReader((g, p) -> delayed, 2);
        var first = new java.util.concurrent.FutureTask<Document>(() -> reader.read(publication, Set.of(), Set.of(), Document.getDefaultInstance()));
        var second = new java.util.concurrent.FutureTask<Document>(() -> reader.read(publication, Set.of(), Set.of(), Document.getDefaultInstance()));
        var one = Thread.ofVirtual().start(first);
        var two = Thread.ofVirtual().start(second);
        try {
            assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            one.interrupt(); two.interrupt();
            one.join(2000); two.join(2000);
            assertThat(one.isAlive()).isFalse(); assertThat(two.isAlive()).isFalse();
            assertThatThrownBy(() -> first.get()).hasCauseInstanceOf(RepositoryException.class);
            assertThatThrownBy(() -> second.get()).hasCauseInstanceOf(RepositoryException.class);
            assertThatThrownBy(() -> reader.read(publication, Set.of(), Set.of(), Document.getDefaultInstance()))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
            assertThat(calls.get()).isEqualTo(2);
        } finally {
            release.countDown();
            assertThat(exited.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            one.join(10000); two.join(10000);
        }
        // The gate's exit precedes permit release by a few instructions; retry only admission contention.
        long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (true) {
            try {
                assertThat(reader.read(publication, Set.of(), Set.of(), Document.getDefaultInstance())).isEqualTo(expected);
                break;
            } catch (RepositoryException contention) {
                if (contention.code() != RepositoryException.Code.RESOURCE_EXHAUSTED || System.nanoTime() >= until) throw contention;
                Thread.sleep(10);
            }
        }
    }

    @Test void smallConcurrencyLimitPreservesMultiPartOrderAndSelection() {
        var chunks = SearchMetadata.newBuilder();
        for (int i = 0; i < 8; i++) chunks.addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("set-" + i));
        var expected = Document.newBuilder().setDocId("ordered").setSearchMetadata(chunks).build();
        var parts = new java.util.ArrayList<DocumentPublicationLedger.Part>();
        for (var fragment : DocumentPartCodec.split(expected, ai.protomolt.proto.repo.codec.PartLayouts.document())) {
            var stored = publish(fragment.bytes()).parts().getFirst();
            parts.add(new DocumentPublicationLedger.Part(fragment.part(), fragment.subKey(), stored.key(), stored.size(),
                    stored.sha256(), stored.providerVersion(), stored.etag()));
        }
        assertThat(parts.size()).isGreaterThan(2);
        var publication = new DocumentPublicationLedger.Publication(UUID.randomUUID(), GENERATION, profile, NAMESPACE,
                DocumentManifest.getDefaultInstance(), parts);
        var lastChunkRead = new java.util.concurrent.CountDownLatch(1);
        BlobStore outOfOrder = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    try {
                        var result = method.invoke(store, args);
                        if (method.getName().equals("get")) {
                            if (args[1].equals(parts.getLast().key())) lastChunkRead.countDown();
                            if (args[1].equals(parts.get(1).key()) && !lastChunkRead.await(10, java.util.concurrent.TimeUnit.SECONDS))
                                throw new AssertionError("Later chunks did not progress while the first chunk was delayed");
                        }
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var reader = new DocumentPartReader((g, p) -> outOfOrder, 2);
        assertThat(reader.read(publication, Set.of(), Set.of(), Document.getDefaultInstance())).isEqualTo(expected);
        var selected = reader.read(publication, Set.of(DocumentPart.DOCUMENT_PART_CHUNKS), Set.of("set-3"), Document.getDefaultInstance());
        assertThat(selected.getSearchMetadata().getSemanticResultsList()).extracting(SemanticProcessingResult::getResultId)
                .containsExactly("set-3");
    }

    @Test void absentOriginalBackendFailsExplicitly() {
        var publication = publish(Document.newBuilder().setDocId("unavailable").build().toByteArray());
        assertThatThrownBy(() -> new DocumentPartReader((generation, original) -> null)
                .read(publication, Set.of(), Set.of(), Document.getDefaultInstance()))
                .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"interrupt", "cancel", "deadline"})
    void cancellationReturnsWithoutWaitingForUncooperativeProvider(String signal) throws Exception {
        var publication = publish(Document.newBuilder().setDocId("cancel").build().toByteArray());
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var workerExited = new java.util.concurrent.CountDownLatch(1);
        // Read real bytes, then model a provider that does not respond to interruption.
        BlobStore delayed = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    try {
                        var result = method.invoke(store, args);
                        if (method.getName().equals("get")) {
                            entered.countDown();
                            boolean interrupted = false;
                            try {
                                while (release.getCount() != 0) {
                                    try { release.await(); }
                                    catch (InterruptedException expected) { interrupted = true; }
                                }
                            } finally {
                                if (interrupted) Thread.currentThread().interrupt();
                                workerExited.countDown();
                            }
                        }
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var remaining = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
        var control = new ai.protomolt.proto.repo.spi.RepositoryReadControl() {
            @Override public boolean isCancelled() { return cancelled.get(); }
            @Override public long remainingNanos() { return remaining.get(); }
        };
        var task = new java.util.concurrent.FutureTask<Document>(() -> new DocumentPartReader((g, p) -> delayed)
                .read(publication, Set.of(), Set.of(), Document.getDefaultInstance(), control));
        var caller = Thread.ofVirtual().start(task);
        try {
            assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            switch (signal) {
                case "interrupt" -> caller.interrupt();
                case "cancel" -> cancelled.set(true);
                case "deadline" -> remaining.set(0);
                default -> throw new AssertionError(signal);
            }
            caller.join(2000);
            assertThat(caller.isAlive()).as("cancelled read must not wait for provider shutdown").isFalse();
            assertThatThrownBy(() -> task.get(1, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RepositoryException.class)
                    .satisfies(e -> assertThat(((RepositoryException) e.getCause()).code()).isEqualTo(
                            signal.equals("deadline") ? RepositoryException.Code.DEADLINE_EXCEEDED : RepositoryException.Code.CANCELLED));
        } finally {
            release.countDown();
            assertThat(workerExited.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            caller.join(10000);
        }
    }
}
