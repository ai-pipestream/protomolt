package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.LockModeType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;

/** Real SQL transactions, constraints and competing locks; no provider is simulated here. */
@Testcontainers
class RawObjectLedgerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static RawObjectLedger raw;
    private static DocumentLedger documents;
    private static final String ACCOUNT = "raw-account";
    private static final String SHA = "a".repeat(64);

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        raw = new RawObjectLedger(tx);
        documents = new DocumentLedger(tx);
    }

    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void rejectsUnverifiedWrongTokenWrongLengthAndCrossAccountWithoutPublishing() {
        var upload = begin();
        var target = document(ACCOUNT);
        assertThatThrownBy(() -> publish(target, upload, upload.leaseToken))
                .isInstanceOf(RawObjectLedger.FenceException.class).hasMessageContaining("verified");
        assertThat(documents.findByNodeId(target.nodeId)).isEmpty();
        assertThatThrownBy(() -> raw.verify(upload.rawId, UUID.randomUUID(), 12, SHA, "v1", "tag"))
                .isInstanceOf(RawObjectLedger.FenceException.class);
        assertThatThrownBy(() -> raw.verify(upload.rawId, upload.leaseToken, 13, SHA, "v1", "tag"))
                .isInstanceOf(RawObjectLedger.FenceException.class).hasMessageContaining("length");
        assertThat(raw.find(upload.rawId).orElseThrow().state).isEqualTo(RawObjectRecord.STAGING);
        verify(upload);
        var foreign = document("foreign-account");
        assertThatThrownBy(() -> publish(foreign, upload, upload.leaseToken))
                .isInstanceOf(RawObjectLedger.FenceException.class).hasMessageContaining("account");
        assertThat(documents.findByNodeId(foreign.nodeId)).isEmpty();
        assertThat(raw.references(foreign.nodeId)).isEmpty();
        assertThat(raw.find(upload.rawId).orElseThrow().state).isEqualTo(RawObjectRecord.VERIFIED);
    }

    @Test void sharedReferenceSurvivesOneOwnerDeletionAndOnlyLastReleaseEnablesCleanup() {
        var upload = begin();
        verify(upload);
        var first = publish(document(ACCOUNT), upload, upload.leaseToken);
        var second = publish(document(ACCOUNT), upload, null);
        assertThat(raw.references(first.nodeId)).containsExactly(upload.rawId);
        assertThat(raw.references(second.nodeId)).containsExactly(upload.rawId);
        assertThat(raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60))).isEmpty();
        remove(first.nodeId);
        assertThat(raw.references(first.nodeId)).isEmpty();
        assertThat(raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60))).isEmpty();
        remove(second.nodeId);
        var claimed = raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60)).orElseThrow();
        assertThat(claimed.state).isEqualTo(RawObjectRecord.DELETING);
        assertThat(claimed.backendIdentity).isEqualTo(upload.backendIdentity);
        assertThat(claimed.bucket).isEqualTo(upload.bucket);
        assertThat(claimed.objectKey).isEqualTo(upload.objectKey);
        assertThatThrownBy(() -> publish(document(ACCOUNT), upload, null))
                .isInstanceOf(RawObjectLedger.FenceException.class);
    }

    @Test void publicationAndReferenceChangesRollBackTogether() {
        var upload = begin();
        verify(upload);
        var target = document(ACCOUNT);
        assertThatThrownBy(() -> documents.saveIfRevision(target, null, (em, row) -> {
            raw.replaceReferences(em, row, List.of(new RawObjectLedger.Binding(upload.rawId, upload.leaseToken)));
            throw new IllegalStateException("outbox unavailable");
        })).isInstanceOf(IllegalStateException.class).hasMessage("outbox unavailable");
        assertThat(documents.findByNodeId(target.nodeId)).isEmpty();
        assertThat(raw.references(target.nodeId)).isEmpty();
        assertThat(raw.find(upload.rawId).orElseThrow().state).isEqualTo(RawObjectRecord.VERIFIED);
        // Restarting the ledger facade sees the same durable lease and can publish.
        var restarted = new RawObjectLedger(tx);
        var saved = documents.saveIfRevision(target, null, (em, row) -> restarted.replaceReferences(em, row,
                List.of(new RawObjectLedger.Binding(upload.rawId, upload.leaseToken))));
        assertThat(restarted.references(saved.nodeId)).containsExactly(upload.rawId);
    }

    @Test void expiredLeaseCannotVerifyRenewOrPublishAndDeletionRemainsReconciliable() {
        var upload = begin();
        verify(upload);
        expire(upload.rawId);
        assertThatThrownBy(() -> verify(upload)).isInstanceOf(RawObjectLedger.FenceException.class);
        assertThatThrownBy(() -> raw.renew(upload.rawId, upload.leaseToken, Duration.ofMinutes(1)))
                .isInstanceOf(RawObjectLedger.FenceException.class);
        assertThatThrownBy(() -> publish(document(ACCOUNT), upload, upload.leaseToken))
                .isInstanceOf(RawObjectLedger.FenceException.class);
        var claim = raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60)).orElseThrow();
        assertThat(raw.cleanupFailed(upload.rawId, claim.cleanupToken, "provider unavailable")).isTrue();
        assertThat(raw.find(upload.rawId).orElseThrow().cleanupError).isEqualTo("provider unavailable");
        var retry = raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60)).orElseThrow();
        assertThat(retry.cleanupAttempts).isEqualTo(2);
        assertThat(raw.cleanupSucceeded(upload.rawId, claim.cleanupToken)).isFalse();
        assertThat(raw.cleanupSucceeded(upload.rawId, retry.cleanupToken)).isTrue();
        assertThat(raw.find(upload.rawId).orElseThrow().state).isEqualTo(RawObjectRecord.DELETED);
        assertThat(raw.cleanupCandidates(Instant.now().plusSeconds(60), 1000))
                .extracting(row -> row.rawId).contains(upload.rawId);
        // A late PUT can recreate bytes after that successful delete. SQL must
        // retain its fence and permit another physical cleanup, never republish.
        var reconcile = raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60)).orElseThrow();
        assertThat(reconcile.cleanupAttempts).isEqualTo(3);
        assertThat(raw.cleanupFailed(upload.rawId, retry.cleanupToken, "stale worker")).isFalse();
        assertThatThrownBy(() -> publish(document(ACCOUNT), upload, upload.leaseToken))
                .isInstanceOf(RawObjectLedger.FenceException.class);
    }

    @Test void liveLeasePreventsCleanupAndVerifiedIdentityCannotChange() {
        var upload = begin();
        assertThat(raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60))).isEmpty();
        verify(upload);
        assertThat(raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60))).isEmpty();
        assertThat(verify(upload).sha256).isEqualTo(SHA);
        assertThatThrownBy(() -> raw.verify(upload.rawId, upload.leaseToken, 12, "b".repeat(64), "v1", "tag"))
                .isInstanceOf(RawObjectLedger.FenceException.class).hasMessageContaining("identity");
        assertThatThrownBy(() -> raw.verify(upload.rawId, upload.leaseToken, 12, SHA, "v2", "tag"))
                .isInstanceOf(RawObjectLedger.FenceException.class);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE raw_objects SET object_key = 'other-key' WHERE raw_id = :id")
                    .setParameter("id", upload.rawId).executeUpdate();
        })).isInstanceOf(jakarta.persistence.PersistenceException.class);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE raw_objects SET sha256 = :sha WHERE raw_id = :id")
                    .setParameter("sha", "c".repeat(64)).setParameter("id", upload.rawId).executeUpdate();
        })).isInstanceOf(jakarta.persistence.PersistenceException.class);
        assertThat(raw.find(upload.rawId).orElseThrow().objectKey).isEqualTo(upload.objectKey);
    }

    @Test void cleanupWaitsForPublicationAndSeesCommittedReference() throws Exception {
        var upload = begin();
        verify(upload);
        var existing = publish(document(ACCOUNT), upload, upload.leaseToken);
        // Remove all owners, leaving a LIVE object that either copy or GC may claim.
        remove(existing.nodeId);
        CountDownLatch bound = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        var target = document(ACCOUNT);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var writer = executor.submit(() -> documents.saveIfRevision(target, null, (em, row) -> {
                raw.replaceReferences(em, row, List.of(new RawObjectLedger.Binding(upload.rawId, null)));
                bound.countDown();
                await(release);
            }));
            try {
                assertThat(bound.await(10, TimeUnit.SECONDS)).isTrue();
                var collector = executor.submit(() -> {
                    cleanupStarted.countDown();
                    return raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60));
                });
                assertThat(cleanupStarted.await(10, TimeUnit.SECONDS)).isTrue();
                awaitRawLockWait();
                release.countDown();
                writer.get(10, TimeUnit.SECONDS);
                assertThat(collector.get(10, TimeUnit.SECONDS)).isEmpty();
                assertThat(raw.references(target.nodeId)).containsExactly(upload.rawId);
            } finally { release.countDown(); }
        }
    }

    @Test void cleanupClaimPreventsLaterReferenceEvenBeforePhysicalDeletion() {
        var upload = begin();
        verify(upload);
        var target = publish(document(ACCOUNT), upload, upload.leaseToken);
        remove(target.nodeId);
        raw.claimCleanup(upload.rawId, Instant.now().plusSeconds(60)).orElseThrow();
        var late = document(ACCOUNT);
        assertThatThrownBy(() -> publish(late, upload, null)).isInstanceOf(RawObjectLedger.FenceException.class);
        assertThat(documents.findByNodeId(late.nodeId)).isEmpty();
    }

    @Test void replacingReferencesReleasesOnlyPreviousBinding() {
        var first = begin();
        var second = begin();
        verify(first);
        verify(second);
        var target = publish(document(ACCOUNT), first, first.leaseToken);
        documents.saveIfRevision(target, target.mutationRevision, (em, row) -> raw.replaceReferences(em, row,
                List.of(new RawObjectLedger.Binding(second.rawId, second.leaseToken))));
        assertThat(raw.references(target.nodeId)).containsExactly(second.rawId);
        assertThat(raw.claimCleanup(first.rawId, Instant.now().plusSeconds(60))).isPresent();
        assertThat(raw.claimCleanup(second.rawId, Instant.now().plusSeconds(60))).isEmpty();
    }

    private static RawObjectRecord begin() {
        return raw.begin(new RawObjectLedger.Location(ACCOUNT, "provider-config-generation-1", UUID.randomUUID(),
                "raw-drive", "original-bucket", "blobs/uploads/" + UUID.randomUUID()),
                12, "application/octet-stream", Duration.ofMinutes(5));
    }

    private static RawObjectRecord verify(RawObjectRecord row) {
        return raw.verify(row.rawId, row.leaseToken, 12, SHA, "v1", "tag");
    }

    private static DocumentRecord publish(DocumentRecord candidate, RawObjectRecord upload, UUID token) {
        return documents.saveIfRevision(candidate, null, (em, row) -> raw.replaceReferences(em, row,
                List.of(new RawObjectLedger.Binding(upload.rawId, token))));
    }

    private static void expire(UUID id) {
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE raw_objects SET lease_until = clock_timestamp() - interval '1 second' WHERE raw_id = :id")
                    .setParameter("id", id).executeUpdate();
        });
    }

    private static void remove(UUID id) {
        tx.inTransaction(em -> { em.remove(em.find(DocumentRecord.class, id, LockModeType.PESSIMISTIC_WRITE)); });
    }

    private static DocumentRecord document(String account) {
        var row = new DocumentRecord();
        row.nodeId = UUID.randomUUID();
        row.docId = "raw-doc-" + row.nodeId;
        row.graphAddressId = "raw-source";
        row.graphId = "intake:" + account;
        row.rowKind = DocumentRowKind.INTAKE;
        row.accountId = account;
        row.datasourceId = "raw-source";
        row.checksum = SHA;
        row.driveName = "raw-drive";
        row.objectKey = "parts/" + row.nodeId;
        row.etag = "tag";
        row.sizeBytes = 12L;
        return row;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("Timed out waiting for transaction");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void awaitRawLockWait() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try (var connection = database.dataSource().getConnection(); var statement = connection.createStatement()) {
            while (System.nanoTime() < deadline) {
                try (var rows = statement.executeQuery("""
                        SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database() AND wait_event_type = 'Lock'
                          AND query ILIKE '%raw_objects%'
                        """)) {
                    rows.next();
                    if (rows.getLong(1) > 0) return;
                }
                Thread.sleep(10);
            }
        }
        throw new AssertionError("Collector never waited on the publication's raw-object lock");
    }
}
