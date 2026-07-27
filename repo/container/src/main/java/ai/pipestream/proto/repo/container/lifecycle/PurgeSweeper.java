package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentLedger;
import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentStatus;
import ai.pipestream.proto.repo.container.ledger.DriveLedger;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import ai.pipestream.proto.repo.container.ledger.Tx;
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
 * A record enqueued by the sweeper stamps {@code requested_at = sweep time}:
 * a revive that landed before the sweep moved {@code updated_at} past any
 * earlier request, and the purger's staleness guard then correctly voids the
 * swept purge instead of deleting the fresh body.
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
     * no PENDING purge record, up to {@link #SWEEP_LIMIT}. One bad row never
     * kills the sweep.
     *
     * @return how many purge records were enqueued
     */
    public int sweepOnce() {
        List<DocumentRecord> stuck = tx.readOnly(em -> em.createQuery(
                        "SELECT d FROM DocumentRecord d WHERE d.status = :status AND NOT EXISTS ("
                                + "SELECT p FROM DocumentPurgeRecord p WHERE p.nodeId = d.nodeId"
                                + " AND p.status = :pending)"
                                + " ORDER BY d.createdAt ASC, d.nodeId ASC",
                        DocumentRecord.class)
                .setParameter("status", DocumentStatus.PENDING_PURGE)
                .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                .setMaxResults(SWEEP_LIMIT)
                .getResultList());
        if (stuck.isEmpty()) {
            return 0;
        }
        LOG.info("Purge sweeper found {} tombstoned row(s) with no pending purge record", stuck.size());

        int enqueued = 0;
        for (DocumentRecord row : stuck) {
            try {
                enqueue(row);
                enqueued++;
            } catch (RuntimeException e) {
                // Most likely a concurrent sweeper enqueued first — the drain
                // is idempotent either way, so log and move on.
                LOG.warn("Sweeper failed to enqueue purge for node_id={}: {}", row.nodeId, e.getMessage());
            }
        }
        return enqueued;
    }

    /** Build and enqueue one row's purge record, in its own transaction. */
    private void enqueue(DocumentRecord row) {
        String drivePrefix = drives.findByName(row.accountId, row.driveName)
                .map((DriveRecord d) -> d.prefix)
                .orElse(null);
        Instant requestedAt = Instant.now();
        tx.inTransaction(em -> {
            DocumentPurgeRecord record = new DocumentPurgeRecord();
            record.purgeId = UUID.randomUUID();
            record.nodeId = row.nodeId;
            record.docId = row.docId;
            record.graphAddressId = row.graphAddressId;
            record.accountId = row.accountId;
            record.graphId = row.graphId;
            record.driveName = row.driveName;
            record.writeObjectKeys(PurgeSnapshots.objectKeysOf(row, drivePrefix));
            record.requestedAt = requestedAt;
            queue.enqueue(em, record);
            return null;
        });
        LOG.info("Sweeper enqueued purge for node_id={} (doc_id={}, requested_at={})",
                row.nodeId, row.docId, requestedAt);
    }
}
