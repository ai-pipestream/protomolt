package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentStatus;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.container.ledger.Tx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Recovery sweeper for purges that never got queued: periodically rescans for
 * document rows stuck in {@link DocumentStatus#PENDING_PURGE} with no PENDING
 * purge record — the crash-between-commit-points window of Phase A (should
 * the tombstone and the enqueue ever drift apart) and pre-lifecycle
 * tombstones from before the queue existed — and enqueues a purge record for
 * each, snapshotting keys from the row's manifest (see
 * {@link PurgeSnapshots}).
 * <p>
 * Deliberately NOT swept: rows in PURGE_FAILED and their FAILED records.
 * Exhausted retries are the dead-letter queue — re-enqueueing them
 * automatically would defeat the attempts ceiling, so recovery there is
 * operator territory (re-tombstone or enqueue by hand).
 * <p>
 * Legacy tombstones receive a generation under the same row lock as enqueue.
 * The sampled revision must still match. Existing admissions retain their own
 * retry state and completion mode; the sweeper never substitutes a broader
 * cleanup command for them.
 */
public final class PurgeSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(PurgeSweeper.class);

    /** Upper bound on rows one sweep re-enqueues (the rest is next sweep's work). */
    static final int SWEEP_LIMIT = 500;

    private final Tx tx;
    private final DocumentLedger documents;
    private final DriveLedger drives;
    private final PurgeQueue queue;

    /**
     * @param tx the shared transaction wrapper (the no-pending-record anti-join)
     * @param documents the document-row ledger
     * @param drives the drive ledger (raw-blob key derivation)
     * @param queue the purge queue swept rows are enqueued onto
     */
    public PurgeSweeper(Tx tx, DocumentLedger documents, DriveLedger drives, PurgeQueue queue) {
        this.tx = tx;
        this.documents = documents;
        this.drives = drives;
        this.queue = queue;
    }

    /**
     * One sweep: enqueue a purge record for every PENDING_PURGE row that has
     * no PENDING purge record, up to {@link #SWEEP_LIMIT}. All sampled rows are
     * attempted; enqueue failures are aggregated and propagated after the pass.
     *
     * @return how many purge records were enqueued
     */
    public int sweepOnce() {
        List<DocumentRecord> stuck = tx.readOnly(em -> em.createQuery(
                        "SELECT d FROM DocumentRecord d WHERE d.status = :status AND d.pendingPurgeId IS NULL AND NOT EXISTS ("
                                + "SELECT p FROM DocumentPurgeRecord p WHERE p.nodeId = d.nodeId"
                                + " AND p.status IN (:pending, :failed))"
                                + " ORDER BY d.createdAt ASC, d.nodeId ASC",
                        DocumentRecord.class)
                .setParameter("status", DocumentStatus.PENDING_PURGE)
                .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                .setParameter("failed", DocumentPurgeRecord.STATUS_FAILED)
                .setMaxResults(SWEEP_LIMIT)
                .getResultList());
        List<UUID> missingAdmissions = tx.readOnly(em -> em.createQuery(
                        "SELECT d.nodeId FROM DocumentRecord d WHERE d.status = :status AND d.pendingPurgeId IS NOT NULL"
                                + " AND NOT EXISTS (SELECT p FROM DocumentPurgeRecord p WHERE p.nodeId = d.nodeId"
                                + " AND p.generationId = d.pendingPurgeId)", UUID.class)
                .setParameter("status", DocumentStatus.PENDING_PURGE).setMaxResults(SWEEP_LIMIT).getResultList());
        if (stuck.isEmpty() && missingAdmissions.isEmpty()) {
            return 0;
        }
        LOG.info("Purge sweeper found {} tombstoned row(s) with no pending purge record", stuck.size());

        int enqueued = 0;
        IllegalStateException failures = missingAdmissions.isEmpty() ? null
                : new IllegalStateException("Tombstone generations have no durable purge admission: " + missingAdmissions);
        for (DocumentRecord row : stuck) {
            try {
                if (enqueue(row)) enqueued++;
            } catch (RuntimeException e) {
                LOG.warn("Sweeper failed to enqueue purge for node_id={}: {}", row.nodeId, e.getMessage());
                if (failures == null) failures = new IllegalStateException("Purge recovery failed for one or more documents");
                failures.addSuppressed(e);
            }
        }
        if (failures != null) throw failures;
        return enqueued;
    }

    /** Build and enqueue one row's purge record, in its own transaction. */
    boolean enqueue(DocumentRecord sampled) {
        String drivePrefix = drives.findByName(sampled.accountId, sampled.driveName)
                .map((DriveRecord d) -> d.prefix)
                .orElseThrow(() -> new IllegalStateException("Document drive is unavailable for purge recovery"));
        Instant requestedAt = Instant.now();
        boolean enqueued = tx.inTransaction(em -> {
            DocumentRecord row = em.find(DocumentRecord.class, sampled.nodeId, jakarta.persistence.LockModeType.PESSIMISTIC_WRITE);
            if (row == null || !DocumentStatus.PENDING_PURGE.equals(row.status)
                    || row.mutationRevision != sampled.mutationRevision) return false;
            if (row.pendingPurgeId != null) {
                long admissions = em.createQuery("SELECT count(p) FROM DocumentPurgeRecord p WHERE p.nodeId = :node AND p.generationId = :generation", Long.class)
                        .setParameter("node", row.nodeId).setParameter("generation", row.pendingPurgeId).getSingleResult();
                if (admissions > 0) return false; // Existing admissions own retries and completion mode.
                throw new IllegalStateException("Admitted tombstone has no durable purge record: " + row.nodeId);
            }
            long pending = em.createQuery("SELECT count(p) FROM DocumentPurgeRecord p WHERE p.nodeId = :node AND p.status IN ('PENDING', 'FAILED')", Long.class)
                    .setParameter("node", row.nodeId).getSingleResult();
            if (pending > 0) return false;
            DocumentPurgeRecord record = new DocumentPurgeRecord();
            record.purgeId = UUID.randomUUID();
            record.generationId = record.purgeId;
            record.contentChecksum = row.checksum;
            row.pendingPurgeId = record.generationId;
            record.nodeId = row.nodeId;
            record.docId = row.docId;
            record.graphAddressId = row.graphAddressId;
            record.accountId = row.accountId;
            record.graphId = row.graphId;
            record.driveName = row.driveName;
            record.writeObjectKeys(PurgeSnapshots.objectKeysOf(row, drivePrefix));
            record.requestedAt = requestedAt;
            queue.enqueue(em, record);
            return true;
        });
        if (enqueued) LOG.info("Sweeper enqueued purge for node_id={} (doc_id={}, requested_at={})",
                sampled.nodeId, sampled.docId, requestedAt);
        return enqueued;
    }
}
