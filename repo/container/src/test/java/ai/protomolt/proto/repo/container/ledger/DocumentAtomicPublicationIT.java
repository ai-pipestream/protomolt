package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.protomolt.proto.repo.container.lifecycle.JdbcEventOutbox;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentAtomicPublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DocumentLedger documents;
    private static final String SHA = "ab".repeat(32);
    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); documents = new DocumentLedger(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void readSnapshotRetainsOriginalBindingAfterDriveChanges() {
        var f = fixture(null);
        var saved = publish(f, (em, row) -> {});
        var reader = new DocumentPublicationLedger(tx);
        var original = reader.findForRead(saved).orElseThrow();
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE drives SET bucket='replacement-container' WHERE drive_id=:id")
                    .setParameter("id", f.drive.driveId).executeUpdate();
        });
        assertThat(reader.findForRead(saved)).contains(original);
        assertThat(original.namespace()).isEqualTo("container");
        assertThat(original.attemptId()).isEqualTo(f.attempt.id());
        assertThat(original.parts()).hasSize(1);
        assertThat(original.parts().getFirst().providerVersion()).isEqualTo("v1");
    }

    @Test void staleOrDeletedAuthorizedRevisionCannotBecomeLegacyRead() {
        var first = publish(fixture(null), (em, row) -> {});
        var second = publish(fixture(first), (em, row) -> {});
        var reader = new DocumentPublicationLedger(tx);
        assertThatThrownBy(() -> reader.findForRead(first))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        assertThat(reader.findForRead(second)).isPresent();
        documents.deleteByNodeId(second.nodeId);
        assertThatThrownBy(() -> reader.findForRead(second))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
    }

    @Test void rowPinAndOutboxCommitTogetherWithOneRevision() {
        var f = fixture(null);
        var observedRevision = new java.util.concurrent.atomic.AtomicLong();
        var saved = publish(f, (em, row) -> {
            observedRevision.set(row.mutationRevision);
            new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now()));
        });
        assertThat(saved.mutationRevision).isEqualTo(observedRevision.get());
        assertThat(documents.findByNodeId(saved.nodeId).orElseThrow().mutationRevision).isEqualTo(saved.mutationRevision);
        assertThat(pin(saved.nodeId)).isEqualTo(f.attempt.id());
        assertThat(historyCount(f.attempt.id())).isEqualTo(1);
        assertThat(eventCount(saved.docId)).isEqualTo(1);
        long revision = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT publication_revision FROM document_part_publication_history WHERE attempt_id=:id")
                .setParameter("id", f.attempt.id()).getSingleResult()).longValue());
        assertThat(revision).isEqualTo(saved.mutationRevision);
    }

    @Test void callbackFailureRollsBackRowPinHistoryAndOutboxAndAllowsRetry() {
        var f = fixture(null);
        assertThatThrownBy(() -> publish(f, (em, row) -> {
            new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now()));
            em.flush();
            throw new IllegalStateException("injected after outbox flush");
        })).hasMessage("injected after outbox flush");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(documents.hasPartPublication(f.row.nodeId)).isFalse();
        assertThat(historyCount(f.attempt.id())).isZero();
        assertThat(eventCount(f.row.docId)).isZero();
        assertThat(publish(f, (em, row) -> {}).nodeId).isEqualTo(f.row.nodeId);
    }

    @Test void ordinarySaveCannotExposeAdmittedPartsWithoutAPublication() {
        var f = fixture(null);
        assertThatThrownBy(() -> documents.saveIfRevision(f.row,null,(em,row)->{}))
                .hasStackTraceContaining("Admitted document parts require an atomic publication");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(publish(f,(em,row)->{}).nodeId).isEqualTo(f.row.nodeId);
    }

    @Test void replacingPublicationRetainsHistoryAndRetiredAttemptCannotReturn() {
        var first = fixture(null);
        var saved = publish(first, (em, row) -> {});
        var second = fixture(saved);
        var replaced = publish(second, (em, row) -> {});
        assertThat(pin(saved.nodeId)).isEqualTo(second.attempt.id());
        assertThat(replaced.readManifest().getDocVersion()).isEqualTo(2);
        assertThat(historyCount(first.attempt.id())).isEqualTo(1);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE document_part_publications SET attempt_id=:attempt WHERE node_id=:node")
                    .setParameter("attempt", first.attempt.id()).setParameter("node", saved.nodeId).executeUpdate();
        })).hasStackTraceContaining("requires a new live publication");
        assertThat(pin(saved.nodeId)).isEqualTo(second.attempt.id());
    }

    @ParameterizedTest @ValueSource(strings = {
        "DELETE FROM document_part_publications WHERE node_id=:id",
        "UPDATE documents SET part_manifest='{}'::jsonb WHERE node_id=:id",
        "UPDATE documents SET drive_name='other-drive' WHERE node_id=:id",
        "UPDATE documents SET checksum='other-checksum' WHERE node_id=:id",
        "DELETE FROM document_part_publication_history WHERE node_id=:id" })
    void databaseRejectsBypasses(String sql) {
        var f = fixture(null); var saved = publish(f, (em, row) -> {});
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery(sql).setParameter("id", saved.nodeId).executeUpdate();
        })).hasStackTraceContaining("ERROR:");
        assertThat(pin(saved.nodeId)).isEqualTo(f.attempt.id());
        assertThat(documents.findByNodeId(saved.nodeId).orElseThrow().readManifest()).isEqualTo(saved.readManifest());
    }

    @Test void statusOnlyChangeKeepsPinAndRowDeletionReleasesItButKeepsHistory() {
        var f = fixture(null); var saved = publish(f, (em, row) -> {});
        saved.status = DocumentStatus.PENDING_PURGE;
        saved.pendingPurgeId = UUID.randomUUID();
        documents.saveIfRevision(saved, saved.mutationRevision, (em, row) -> {});
        assertThat(pin(saved.nodeId)).isEqualTo(f.attempt.id());
        documents.deleteByNodeId(saved.nodeId);
        assertThat(documents.hasPartPublication(saved.nodeId)).isFalse();
        assertThat(historyCount(f.attempt.id())).isEqualTo(1);
    }

    @Test void staleRevisionAndWrongNextVersionCannotPublish() {
        var first = fixture(null); var saved = publish(first, (em, row) -> {});
        var f = fixture(saved);
        f.row.writeManifest(f.row.readManifest().toBuilder().setDocVersion(3).build());
        assertThatThrownBy(() -> publish(f, (em, row) -> {})).hasMessageContaining("next locked version");
        f.row.writeManifest(f.row.readManifest().toBuilder().setDocVersion(2).build());
        saved.reprocessCount++;
        documents.save(saved);
        assertThatThrownBy(() -> publish(f, (em, row) -> {})).isInstanceOf(DocumentLedger.RevisionConflictException.class);
        assertThat(pin(saved.nodeId)).isEqualTo(first.attempt.id());
        assertThat(historyCount(f.attempt.id())).isZero();
    }

    @Test void changedDriveAndWrongSelectedBackendCannotPublish() {
        var f = fixture(null);
        var wrong = new DocumentPublicationTarget(new DriveLedger(tx), f.drive, f.attempt.location().backendGeneration(),
                new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "https://different.example")));
        assertThatThrownBy(() -> documents.saveVerifiedAttempt(f.row, null, Map.of(), f.attempt.id(), f.attempt.token(), wrong, (em,row) -> {}))
                .hasMessageContaining("physical location");
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET bucket='moved' WHERE drive_id=:id")
                .setParameter("id", f.drive.driveId).executeUpdate(); });
        assertThatThrownBy(() -> publish(f, (em,row) -> {})).hasMessageContaining("drive changed");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
    }

    @Test void matchingNamespaceDoesNotPermitWrongDriveProviderOrBypassCompositionGate() {
        var f = fixture(null);
        var backend = new ManagedBackendLedger(tx).find(f.attempt.location().backendGeneration()).orElseThrow().identity();
        var gated = new DocumentPublicationTarget(new DriveLedger(tx,drive -> {
            throw new IllegalStateException("selected composition refuses this drive");
        }),f.drive,f.attempt.location().backendGeneration(),backend);
        assertThatThrownBy(() -> documents.saveVerifiedAttempt(f.row,null,Map.of(),f.attempt.id(),f.attempt.token(),gated,(em,row)->{}))
                .hasMessage("selected composition refuses this drive");
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET provider='redis' WHERE drive_id=:id")
                .setParameter("id",f.drive.driveId).executeUpdate(); });
        f.drive.provider = "redis";
        var wrongProvider = new DocumentPublicationTarget(new DriveLedger(tx),f.drive,f.attempt.location().backendGeneration(),backend);
        assertThatThrownBy(() -> documents.saveVerifiedAttempt(f.row,null,Map.of(),f.attempt.id(),f.attempt.token(),wrongProvider,(em,row)->{}))
                .hasMessageContaining("physical location");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(historyCount(f.attempt.id())).isZero();
    }

    @Test void competingWritersCannotPublishAgainstTheSameRevision() throws Exception {
        var initial = publish(fixture(null), (em,row) -> {});
        var first = fixture(initial);
        var second = fixture(initial);
        var insideCommit = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var winner = executor.submit(() -> publish(first, (em,row) -> {
                insideCommit.countDown();
                try { assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
            }));
            try {
                assertThat(insideCommit.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var loser = executor.submit(() -> publish(second,(em,row)->{}));
                release.countDown();
                winner.get(10,java.util.concurrent.TimeUnit.SECONDS);
                assertThatThrownBy(() -> loser.get(10,java.util.concurrent.TimeUnit.SECONDS))
                        .hasCauseInstanceOf(DocumentLedger.RevisionConflictException.class);
            } finally { release.countDown(); }
        }
        assertThat(pin(initial.nodeId)).isEqualTo(first.attempt.id());
        assertThat(historyCount(second.attempt.id())).isZero();
    }

    @Test void deferredGuardRollsBackAChangeMadeAfterPublicationCallback() {
        var f = fixture(null);
        assertThatThrownBy(() -> publish(f,(em,row)->{
            new JdbcEventOutbox(tx).enqueue(em,DocumentEventFactory.saved(row,Instant.now()));
            em.flush();
            em.createNativeQuery("UPDATE documents SET checksum='corrupted-after-binding' WHERE node_id=:id")
                    .setParameter("id",row.nodeId).executeUpdate();
        })).hasStackTraceContaining("Bound document body changed without a new publication");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(historyCount(f.attempt.id())).isZero();
        assertThat(eventCount(f.row.docId)).isZero();
    }

    private record Fixture(DocumentRecord row, Long expected, DocumentPartAttemptLedger.Attempt attempt,
            DriveRecord drive, DocumentPublicationTarget target) {}
    private static Fixture fixture(DocumentRecord previous) {
        UUID node = previous == null ? UUID.randomUUID() : previous.nodeId;
        String generation = "atomic-" + UUID.randomUUID();
        var backend = new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "https://storage.example"));
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(backend, generation));
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = "drive-" + UUID.randomUUID(); drive.provider = "test-location"; drive.driveType = "CUSTOM"; drive.bucket = "container";
        new DriveLedger(tx).insert(drive);
        UUID attemptId = UUID.randomUUID();
        String prefix = "documents/account/" + node + "/attempts/" + attemptId + "/";
        var object = new DocumentPartAttemptLedger.PlannedObject(DocumentPart.DOCUMENT_PART_CORE,"",prefix+"core",3,SHA,"application/protobuf");
        var plan = new DocumentPartAttemptLedger.Plan(attemptId,
                new DocumentPartAttemptLedger.Location(node,"account",generation,"container"),
                previous == null ? 0 : previous.mutationRevision, Map.of(), List.of(object));
        var attempts = new DocumentPartAttemptLedger(tx);
        var attempt = attempts.begin(plan, Duration.ofMinutes(5));
        attempts.verify(attempt.id(),attempt.token(),object.objectKey(),3,SHA,"v1","etag");
        var row = new DocumentRecord(); row.nodeId=node; row.accountId="account"; row.docId="doc-"+node;
        row.createdAt = previous == null ? Instant.now() : previous.createdAt;
        row.updatedAt = Instant.now();
        row.graphId="intake:account"; row.graphAddressId="source"; row.rowKind=DocumentRowKind.INTAKE; row.datasourceId="source";
        row.driveName=drive.name; row.objectKey=prefix; row.versionId="v1"; row.etag="etag"; row.sizeBytes=3L;
        var manifest = DocumentManifest.newBuilder().setDocVersion(previous == null ? 1 : previous.readManifest().getDocVersion()+1)
                .setAddress(NodeAddress.newBuilder().setAccountId(row.accountId).setDocId(row.docId).setGraphId(row.graphId).setGraphAddressId(row.graphAddressId))
                .addParts(PartManifestEntry.newBuilder().setPart(object.part()).setObjectKey(object.objectKey())
                        .setState(PartState.PART_STATE_PRESENT).setSizeBytes(3).setSha256(SHA)).build();
        row.writeManifest(manifest); row.checksum=DocumentPartCodec.rootChecksumFromManifest(manifest);
        return new Fixture(row, previous == null ? null : previous.mutationRevision, attempt, drive, new DocumentPublicationTarget(new DriveLedger(tx), drive,generation,backend));
    }
    private static DocumentRecord publish(Fixture f, java.util.function.BiConsumer<jakarta.persistence.EntityManager,DocumentRecord> callback) {
        return documents.saveVerifiedAttempt(f.row,f.expected,Map.of(),f.attempt.id(),f.attempt.token(),f.target,callback);
    }

    @ParameterizedTest @ValueSource(strings = {"size", "checksum", "state", "version"})
    void directSqlCannotAdmitFalseBodyFacts(String defect) {
        var f = fixture(null);
        switch (defect) {
            case "size" -> f.row.sizeBytes = 4L;
            case "checksum" -> f.row.checksum = "cd".repeat(32);
            case "version" -> f.row.versionId = "another-version";
            case "state" -> f.row.writeManifest(f.row.readManifest().toBuilder().addParts(
                    PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).setStateValue(999)).build());
        }
        assertThatThrownBy(() -> documents.saveIfRevision(f.row, null, (em, row) -> {
            em.createNativeQuery("""
                    INSERT INTO document_part_publication_history(attempt_id,node_id,publication_revision,body)
                    SELECT :attempt,node_id,mutation_revision,document_publication_body(documents) FROM documents WHERE node_id=:id
                    """).setParameter("attempt",f.attempt.id()).setParameter("id",row.nodeId).executeUpdate();
            em.createNativeQuery("INSERT INTO document_part_publications VALUES (:id,:attempt)")
                    .setParameter("id",row.nodeId).setParameter("attempt",f.attempt.id()).executeUpdate();
        })).hasStackTraceContaining("ERROR: Document publication");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(historyCount(f.attempt.id())).isZero();
    }
    private static UUID pin(UUID node) {
        return tx.readOnly(em -> (UUID) em.createNativeQuery("SELECT attempt_id FROM document_part_publications WHERE node_id=:id")
                .setParameter("id",node).getSingleResult());
    }
    private static long historyCount(UUID attempt) {
        return tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_part_publication_history WHERE attempt_id=:id")
                .setParameter("id",attempt).getSingleResult()).longValue());
    }
    private static long eventCount(String doc) {
        return tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_events_outbox WHERE kafka_key=:id")
                .setParameter("id",doc).getSingleResult()).longValue());
    }
}
