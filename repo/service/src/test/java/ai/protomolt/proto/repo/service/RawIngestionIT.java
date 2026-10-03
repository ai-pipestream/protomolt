package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.container.lifecycle.JdbcPurgeQueue;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.*;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Managed ingestion against real SQL transactions and streamed S3 writes. */
@Testcontainers
class RawIngestionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static final String ACCOUNT = "ingestion-account";
    static final String BACKEND = "ingestion-test-backend";
    static final RepositoryCaller CALLER = new RepositoryCaller("ingestion-test", true);
    static LedgerDatabase database;
    static Tx tx;
    static DocumentLedger documents;
    static DriveLedger drives;
    static OpenedBlobStore opened;
    static BlobStore store;
    static RawIngestionOperations ingestion;

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        documents = new DocumentLedger(tx);
        drives = new DriveLedger(tx);
        opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(),
                "region", S3.getRegion(), "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(),
                "path-style", "true", "conditional-writes", "false"));
        store = opened.store();
        opened.ensureNamespace("raw-ingestion");
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID();
        drive.accountId = ACCOUNT;
        drive.name = "raw-ingestion";
        drive.bucket = "raw-ingestion";
        drive.prefix = "raw-ingestion";
        drive.driveType = "INTAKE";
        drives.insert(drive);
        ingestion = operations(store, opened.capabilities());
    }

    @AfterAll static void close() throws Exception {
        if (opened != null) opened.close();
        if (database != null) database.close();
    }

    @Test void replacementPublishesNewBytesWithoutOverwritingOriginal() {
        String id = UUID.randomUUID().toString();
        var first = upload(id, "original");
        var second = upload(id, "replacement");
        assertThat(second.storageRef()).isNotEqualTo(first.storageRef());
        assertThat(bytes(first)).isEqualTo("original".getBytes(StandardCharsets.UTF_8));
        assertThat(bytes(second)).isEqualTo("replacement".getBytes(StandardCharsets.UTF_8));
        assertThat(documents.rawObjects().references(UUID.fromString(second.document().getNodeId())))
                .containsExactly(second.attemptId());
        assertThat(documents.rawObjects().find(second.attemptId()).orElseThrow().state).isEqualTo(RawObjectRecord.LIVE);
    }

    @Test void duplicateReusesCommittedReferenceAndExpiresUnusedCandidate() {
        String id = UUID.randomUUID().toString();
        var first = upload(id, "same body");
        var duplicate = upload(id, "same body");
        assertThat(duplicate.document().getDeduplicated()).isTrue();
        assertThat(duplicate.storageRef()).isEqualTo(first.storageRef());
        assertThat(duplicate.attemptId()).isNotEqualTo(first.attemptId());
        var unused = documents.rawObjects().find(duplicate.attemptId()).orElseThrow();
        assertThat(unused.state).isEqualTo(RawObjectRecord.VERIFIED);
        assertThat(unused.leaseUntil).isBeforeOrEqualTo(Instant.now());
        UUID node = UUID.fromString(first.document().getNodeId());
        assertThat(documents.rawObjects().references(node)).containsExactly(first.attemptId());
        assertThat(documents.findByNodeId(node).orElseThrow().reprocessCount).isEqualTo(1);
    }

    @Test void checksumFailurePreservesCommittedBodyAndRevision() {
        String id = UUID.randomUUID().toString();
        var first = upload(id, "valid original");
        UUID node = UUID.fromString(first.document().getNodeId());
        long revision = documents.findByNodeId(node).orElseThrow().mutationRevision;
        byte[] bad = "bad replacement".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ingestion.upload(CALLER, request(id, "0".repeat(64)),
                new ByteArrayInputStream(bad), bad.length)).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.INVALID_ARGUMENT));
        assertThat(documents.findByNodeId(node).orElseThrow().mutationRevision).isEqualTo(revision);
        assertThat(documents.rawObjects().references(node)).containsExactly(first.attemptId());
        assertThat(bytes(first)).isEqualTo("valid original".getBytes(StandardCharsets.UTF_8));
    }

    @Test void streamRemainsCallerOwned() {
        var body = new ByteArrayInputStream(new byte[] {1, 2, 3}) {
            boolean closed;
            @Override public void close() { closed = true; }
        };
        ingestion.upload(CALLER, request(UUID.randomUUID().toString(), null), body, 3);
        assertThat(body.closed).isFalse();
        assertThat(body.available()).isZero();
    }

    @Test void truncatedAndOversizedBodiesCannotPublish() {
        for (long declaredLength : new long[] {2, 4}) {
            String id = UUID.randomUUID().toString();
            assertThatThrownBy(() -> ingestion.upload(CALLER, request(id, null),
                    new ByteArrayInputStream(new byte[] {1, 2, 3}), declaredLength))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.INVALID_ARGUMENT));
            assertNoDocument(id);
        }
    }

    @Test void revisionConflictRetriesPublicationWithoutReadingBodyAgain() {
        String id = UUID.randomUUID().toString();
        var original = upload(id, "original");
        UUID node = UUID.fromString(original.document().getNodeId());
        var raced = new java.util.concurrent.atomic.AtomicBoolean();
        var rawWrites = new java.util.concurrent.atomic.AtomicInteger();
        BlobStore racing = afterPut(spec -> {
            if (DriveKeys.isManaged(spec.key())) rawWrites.incrementAndGet();
            else if (raced.compareAndSet(false, true)) tx.inTransaction(em -> {
                em.find(DocumentRecord.class, node, jakarta.persistence.LockModeType.PESSIMISTIC_WRITE)
                        .sourceBlobDeleteReason = "concurrent-policy-change";
            });
        });
        byte[] replacement = "replacement".getBytes(StandardCharsets.UTF_8);
        var result = operations(racing, opened.capabilities()).upload(CALLER, request(id, null),
                new ByteArrayInputStream(replacement), replacement.length);
        assertThat(raced).isTrue();
        assertThat(rawWrites).hasValue(1);
        assertThat(bytes(result)).isEqualTo(replacement);
        assertThat(documents.findByNodeId(node).orElseThrow().sourceBlobDeleteReason)
                .isEqualTo("concurrent-policy-change");
        assertThat(documents.rawObjects().references(node)).containsExactly(result.attemptId());
    }

    @Test void providerAbortedAfterPartWriteIsNotTreatedAsRevisionConflict() {
        String id = UUID.randomUUID().toString();
        var attempts = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
        BlobStore uncertain = afterPut(spec -> {
            if (!DriveKeys.isManaged(spec.key())) {
                String suffix = spec.key().substring(spec.key().indexOf("/attempts/") + "/attempts/".length());
                attempts.add(suffix.substring(0, suffix.indexOf('/')));
                throw new BlobStoreException(BlobStoreException.Code.ABORTED, "ambiguous part write", null);
            }
        });
        assertThatThrownBy(() -> operations(uncertain, opened.capabilities()).upload(CALLER,
                request(id, null), new ByteArrayInputStream(new byte[] {9}), 1))
                .isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CONFLICT));
        assertThat(attempts).hasSize(1);
        assertNoDocument(id);
    }

    @Test void unknownProviderRetentionIsRejected() {
        assertThatThrownBy(() -> operations(store, Set.of(BlobCapability.STREAMING_WRITE)))
                .isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.UNSUPPORTED));
    }

    @Test void lostPutAcknowledgementRetainsCandidateWithoutPublishingOrRetrying() {
        var writes = new java.util.concurrent.atomic.AtomicInteger();
        var receipt = new java.util.concurrent.atomic.AtomicReference<BlobStore.PutSpec>();
        var failure = new IllegalStateException("injected lost acknowledgement after real PUT");
        BlobStore uncertain = afterPut(spec -> {
            writes.incrementAndGet();
            receipt.set(spec);
            throw failure;
        });
        String id = UUID.randomUUID().toString();
        assertThatThrownBy(() -> operations(uncertain, opened.capabilities()).upload(CALLER,
                request(id, null), new ByteArrayInputStream(new byte[] {7}), 1))
                .isInstanceOfSatisfying(RepositoryException.class, error -> {
                    assertThat(error.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE);
                    assertThat(error.getCause()).isSameAs(failure);
                });
        assertThat(writes).hasValue(1);
        assertThat(store.get("raw-ingestion", receipt.get().key()).data()).containsExactly((byte) 7);
        var candidate = tx.inTransaction(em -> { return em.createQuery(
                "select r from RawObjectRecord r where r.objectKey = :key", RawObjectRecord.class)
                .setParameter("key", receipt.get().key()).getSingleResult(); });
        assertThat(candidate.state).isEqualTo(RawObjectRecord.STAGING);
        assertNoDocument(id);
    }

    @Test void driveConfigurationChangeDuringPutPreventsPublication() {
        var fired = new java.util.concurrent.atomic.AtomicBoolean();
        BlobStore racing = afterPut(spec -> {
            if (fired.compareAndSet(false, true)) tx.inTransaction(em -> {
                var drive = em.createQuery("select d from DriveRecord d where d.name = :name", DriveRecord.class)
                        .setParameter("name", "raw-ingestion").getSingleResult();
                drive.provider = "redis";
            });
        });
        String id = UUID.randomUUID().toString();
        try {
            assertThatThrownBy(() -> operations(racing, opened.capabilities()).upload(CALLER,
                    request(id, null), new ByteArrayInputStream(new byte[] {8}), 1))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CONFLICT));
            assertThat(fired).isTrue();
            assertNoDocument(id);
        } finally {
            tx.inTransaction(em -> {
                em.createQuery("select d from DriveRecord d where d.name = :name", DriveRecord.class)
                        .setParameter("name", "raw-ingestion").getSingleResult().provider = "s3";
            });
        }
    }

    private static void assertNoDocument(String id) {
        long count = tx.inTransaction(em -> { return em.createQuery(
                "select count(d) from DocumentRecord d where d.docId = :id", Long.class)
                .setParameter("id", id).getSingleResult(); });
        assertThat(count).isZero();
    }

    /** Fault injection decorates real writes; it never supplies success responses. */
    private static BlobStore afterPut(java.util.function.Consumer<BlobStore.PutSpec> action) {
        return (BlobStore) java.lang.reflect.Proxy.newProxyInstance(RawIngestionIT.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    Object result;
                    try { result = method.invoke(store, args); }
                    catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                    if (method.getName().equals("put")) action.accept((BlobStore.PutSpec) args[0]);
                    return result;
                });
    }

    private static RawIngestionOperations operations(BlobStore bytes, Set<BlobCapability> capabilities) {
        var engine = new DocumentOperations(documents, drives, tx, bytes, new PartStorage(),
                new JdbcPurgeQueue(tx), null, BACKEND);
        return new RawIngestionOperations(engine, documents, drives, bytes, BACKEND, capabilities);
    }

    private static RawIngestionRepository.Request request(String id, String sha) {
        return new RawIngestionRepository.Request(ACCOUNT, "source", "raw-ingestion", id,
                "document.bin", "application/octet-stream", null, null, sha);
    }

    private static RawIngestionRepository.Result upload(String id, String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        return ingestion.upload(CALLER, request(id, null), new ByteArrayInputStream(bytes), bytes.length);
    }

    private static byte[] bytes(RawIngestionRepository.Result result) {
        return store.get("raw-ingestion", result.storageRef().getObjectKey()).data();
    }
}
