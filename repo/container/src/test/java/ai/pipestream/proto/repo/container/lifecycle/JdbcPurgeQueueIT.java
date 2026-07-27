package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.LockModeType;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Queue mechanics against real PostgreSQL: claim ordering, conditional
 * terminal transitions, the attempts ladder, countByStatus, and a genuine
 * SKIP LOCKED proof (a row locked by another open transaction is skipped by
 * claimBatch, then becomes claimable once the lock releases).
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcPurgeQueueIT extends AbstractLifecycleIT {

    private static DocumentPurgeRecord newRecord(Instant requestedAt) {
        DocumentPurgeRecord record = new DocumentPurgeRecord();
        record.purgeId = UUID.randomUUID();
        record.nodeId = UUID.randomUUID();
        record.docId = "doc-" + record.purgeId;
        record.graphAddressId = "ds-q";
        record.accountId = "acct-queue";
        record.graphId = "intake:acct-queue";
        record.driveName = "docs";
        record.writeObjectKeys(List.of("k/" + record.purgeId));
        record.requestedAt = requestedAt;
        return record;
    }

    @Test
    void claimBatchIsOldestFirstAndTerminalTransitionsAreConditional() {
        // countByStatus is queue-global; assert deltas against the pre-test
        // snapshot so method order cannot couple the tests.
        Map<String, Long> before = queueCounts();
        Instant base = Instant.now();
        DocumentPurgeRecord older = newRecord(base.minusSeconds(10));
        DocumentPurgeRecord newer = newRecord(base);
        tx.inTransaction(em -> {
            queue.enqueue(em, newer);
            queue.enqueue(em, older);
        });

        // Batch of 1: oldest requested_at wins regardless of insert order.
        List<DocumentPurgeRecord> first = queue.claimBatch(1);
        assertThat(first).extracting(r -> r.purgeId).containsExactly(older.purgeId);

        // markPurged settles exactly that record; a second mark is a no-op.
        assertThat(queue.markPurged(older.purgeId)).isTrue();
        assertThat(queue.markPurged(older.purgeId)).isFalse();
        assertThat(findPurge(older.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_PURGED);

        // The settled record is never claimed again; the newer one is next.
        assertThat(queue.claimBatch(10)).extracting(r -> r.purgeId)
                .containsExactly(newer.purgeId);
        assertThat(queue.markVoid(newer.purgeId)).isTrue();
        assertThat(queue.markFailed(newer, "too late")).isEmpty();
        assertThat(findPurge(newer.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_VOID);

        Map<String, Long> counts = queueCounts();
        assertThat(counts.getOrDefault(DocumentPurgeRecord.STATUS_PURGED, 0L)
                - before.getOrDefault(DocumentPurgeRecord.STATUS_PURGED, 0L)).isEqualTo(1L);
        assertThat(counts.getOrDefault(DocumentPurgeRecord.STATUS_VOID, 0L)
                - before.getOrDefault(DocumentPurgeRecord.STATUS_VOID, 0L)).isEqualTo(1L);
    }

    @Test
    void markFailedRetriesUntilTheCeilingThenLandsFailed() {
        DocumentPurgeRecord record = newRecord(Instant.now());
        tx.inTransaction(em -> {
            queue.enqueue(em, record);
        });

        for (int i = 1; i < DocumentPurgeRecord.MAX_ATTEMPTS; i++) {
            DocumentPurgeRecord updated = queue.markFailed(record, "boom " + i).orElseThrow();
            assertThat(updated.attempts).isEqualTo(i);
            assertThat(updated.status).isEqualTo(DocumentPurgeRecord.STATUS_PENDING);
            assertThat(updated.lastError).isEqualTo("boom " + i);
        }
        DocumentPurgeRecord terminal = queue.markFailed(record, "boom final").orElseThrow();
        assertThat(terminal.attempts).isEqualTo(DocumentPurgeRecord.MAX_ATTEMPTS);
        assertThat(terminal.status).isEqualTo(DocumentPurgeRecord.STATUS_FAILED);
        // FAILED is terminal: nothing claims it, nothing transitions it.
        assertThat(queue.claimBatch(10)).isEmpty();
        assertThat(queue.markFailed(record, "zombie")).isEmpty();
        assertThat(queue.markPurged(record.purgeId)).isFalse();
        assertThat(queueCounts()).containsEntry(DocumentPurgeRecord.STATUS_FAILED, 1L);
    }

    @Test
    void claimBatchSkipsRowsLockedByAnotherTransaction() throws Exception {
        DocumentPurgeRecord locked = newRecord(Instant.now().minusSeconds(5));
        DocumentPurgeRecord free = newRecord(Instant.now());
        tx.inTransaction(em -> {
            queue.enqueue(em, locked);
            queue.enqueue(em, free);
        });

        // Hold a FOR UPDATE lock on one record from a second, still-open
        // transaction — the competing purger.
        EntityManager rival = database.entityManagerFactory().createEntityManager();
        EntityTransaction rivalTx = rival.getTransaction();
        rivalTx.begin();
        try {
            rival.find(DocumentPurgeRecord.class, locked.purgeId, LockModeType.PESSIMISTIC_WRITE);

            // SKIP LOCKED: the rival's row is invisible to this claim.
            assertThat(queue.claimBatch(10)).extracting(r -> r.purgeId)
                    .containsExactly(free.purgeId);
        } finally {
            rivalTx.rollback();
            rival.close();
        }

        // Lock released: both rows (the free one is still PENDING) claim.
        assertThat(queue.claimBatch(10)).extracting(r -> r.purgeId)
                .containsExactlyInAnyOrder(locked.purgeId, free.purgeId);

        // Settle so no state leaks across test order.
        queue.markVoid(locked.purgeId);
        queue.markVoid(free.purgeId);
    }
}
