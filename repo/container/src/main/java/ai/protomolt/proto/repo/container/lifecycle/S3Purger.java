package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentStatus;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.container.ledger.Tx;
import jakarta.persistence.LockModeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Phase B of the two-phase delete: drains the purge queue
 * ({@code document_purges}) — re-reads each claimed record's document row
 * under a row lock, applies the staleness guard, batch-deletes the snapshot
 * object keys from the record's drive bucket, and removes the row.
 * <p>
 * <b>The staleness guard.</b> A purge is VOID when the row is AVAILABLE
 * (revived) or its {@code updated_at} is strictly after the record's
 * {@code requested_at} (the body was re-staged after the delete was
 * requested): the snapshot keys describe the OLD body, so deleting them is
 * both pointless (they are gone or orphaned, and the reconciler owns orphans)
 * and dangerous-adjacent — the purge is cancelled and objects and row are
 * left alone. The guard is checked TWICE: once to decide whether the S3
 * delete should happen at all, and again under a fresh row lock in the same
 * transaction that removes the row (a revive landing between the delete and
 * the removal still wins).
 * <p>
 * <b>Eligibility.</b> Rows in PENDING_PURGE are the normal case; rows in
 * PURGE_FAILED are also eligible (an operator re-enqueued purge of a
 * previously failed row — PURGE_FAILED is not a revive, only AVAILABLE is).
 * A row already gone means the row removal already happened (a competing
 * drain, a synchronous purge): the snapshot objects are still deleted
 * (NoSuchKey = success) and the record marked PURGED.
 * <p>
 * <b>Failures.</b> Any store/DB error — including a drive that no longer
 * resolves and partial batch-delete failures — is {@link PurgeQueue#markFailed
 * marked failed}: the record retries until {@link DocumentPurgeRecord#MAX_ATTEMPTS},
 * then lands FAILED (the DLQ) and the document row, if still PENDING_PURGE,
 * is flipped to PURGE_FAILED so the sweeper leaves it for an operator.
 * <p>
 * Idempotent throughout: NoSuchKey = success, terminal queue transitions are
 * conditional on PENDING, and a re-drain of a settled record is a no-op.
 * <p>
 * <b>Eventing.</b> When an event outbox is wired (Kafka configured), a
 * {@code DocumentPurged} event is persisted IN THE SAME TRANSACTION as the
 * row removal and the PURGED transition, so the event stream cannot drift
 * from the purge outcome. A VOIDED purge fires nothing: the body lives on.
 */
public final class S3Purger {

    private static final Logger LOG = LoggerFactory.getLogger(S3Purger.class);

    private final Tx tx;
    private final DocumentLedger documents;
    private final DriveLedger drives;
    private final PurgeQueue queue;
    private final JdbcEventOutbox events;

    /**
     * @param tx the shared transaction wrapper (the final guard re-check and
     *        row removal run in one transaction through it)
     * @param documents the document-row ledger
     * @param drives the drive ledger (drive name → bucket)
     * @param queue the purge queue this purger drains
     */
    public S3Purger(Tx tx, DocumentLedger documents, DriveLedger drives, PurgeQueue queue) {
        this(tx, documents, drives, queue, null);
    }

    /**
     * @param tx the shared transaction wrapper (the final guard re-check and
     *        row removal run in one transaction through it)
     * @param documents the document-row ledger
     * @param drives the drive ledger (drive name → bucket)
     * @param queue the purge queue this purger drains
     * @param events the document-event outbox, or null when Kafka is not
     *        configured (no outbox writes then - zero overhead)
     */
    public S3Purger(Tx tx, DocumentLedger documents, DriveLedger drives, PurgeQueue queue,
            JdbcEventOutbox events) {
        this.tx = tx;
        this.documents = documents;
        this.drives = drives;
        this.queue = queue;
        this.events = events;
    }

    /** One claimed record's fate after the guard re-read. */
    private enum Decision { PURGE, VOID, ROW_GONE }

    /**
     * Drain one batch: claim up to {@code batchSize} PENDING records and
     * process each. One bad record never kills the batch — it is marked
     * failed and the loop moves on.
     *
     * @param store the object-storage port the deletes go through
     * @param batchSize the claim batch size
     * @return how many records this call actually transitioned to PURGED
     */
    public int drainOnce(BlobStore store, int batchSize) {
        List<DocumentPurgeRecord> batch = queue.claimBatch(batchSize);
        int purged = 0;
        for (DocumentPurgeRecord record : batch) {
            try {
                if (process(store, record)) {
                    purged++;
                }
            } catch (RuntimeException e) {
                LOG.warn("Purge drain failed for purge_id={} node_id={} (attempt {}): {}",
                        record.purgeId, record.nodeId, record.attempts + 1, e.getMessage());
                fail(record, e);
            }
        }
        return purged;
    }

    /**
     * Process one claimed record. Returns {@code true} only when the record
     * transitioned to PURGED.
     */
    private boolean process(BlobStore store, DocumentPurgeRecord record) {
        DriveRecord drive = drives.findByName(record.accountId, record.driveName)
                .orElseThrow(() -> new IllegalStateException(
                        "drive '" + record.driveName + "' not found for account '"
                                + record.accountId + "' (purge " + record.purgeId + ")"));
        List<String> keys = record.readObjectKeys();

        Decision decision = guardCheck(record);
        switch (decision) {
            case VOID -> {
                // Revived or re-staged after the request: leave objects and
                // row alone, cancel the purge.
                if (queue.markVoid(record.purgeId)) {
                    LOG.info("Purge VOIDED for node_id={} (purge_id={}): row re-staged after {}",
                            record.nodeId, record.purgeId, record.requestedAt);
                }
                return false;
            }
            case ROW_GONE -> {
                // The row removal already happened (competing drain or a
                // synchronous purge): the snapshot objects are orphans now.
                deleteObjects(store, drive.bucket, keys, record);
                if (events == null) {
                    return queue.markPurged(record.purgeId);
                }
                // Same conditional PURGED transition as the queue's, plus the
                // DocumentPurged event in one transaction (checksum unknown:
                // the row was already gone).
                return tx.inTransaction(em -> {
                    int transitioned = em.createQuery(
                                    "UPDATE DocumentPurgeRecord p SET p.status = :purged"
                                            + " WHERE p.purgeId = :id AND p.status = :pending")
                            .setParameter("purged", DocumentPurgeRecord.STATUS_PURGED)
                            .setParameter("id", record.purgeId)
                            .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                            .executeUpdate();
                    if (transitioned == 1) {
                        events.enqueue(em, DocumentEventFactory.purged(record, null, Instant.now()));
                    }
                    return transitioned == 1;
                });
            }
            case PURGE -> {
                deleteObjects(store, drive.bucket, keys, record);
                // Final guard re-check + row removal + PURGED transition in ONE
                // transaction: a revive landing after the S3 delete still wins,
                // because the row lock serializes against its upsert.
                return tx.inTransaction(em -> {
                    DocumentRecord row = em.find(DocumentRecord.class, record.nodeId,
                            LockModeType.PESSIMISTIC_WRITE);
                    if (row != null && !eligible(row, record)) {
                        em.createQuery("UPDATE DocumentPurgeRecord p SET p.status = :void"
                                        + " WHERE p.purgeId = :id AND p.status = :pending")
                                .setParameter("void", DocumentPurgeRecord.STATUS_VOID)
                                .setParameter("id", record.purgeId)
                                .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                                .executeUpdate();
                        LOG.info("Purge VOIDED at finalization for node_id={} (purge_id={}): "
                                        + "row re-staged after {}", record.nodeId, record.purgeId,
                                record.requestedAt);
                        return false;
                    }
                    if (row != null) {
                        em.remove(row);
                    }
                    int transitioned = em.createQuery(
                                    "UPDATE DocumentPurgeRecord p SET p.status = :purged"
                                            + " WHERE p.purgeId = :id AND p.status = :pending")
                            .setParameter("purged", DocumentPurgeRecord.STATUS_PURGED)
                            .setParameter("id", record.purgeId)
                            .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                            .executeUpdate();
                    if (transitioned == 1 && events != null) {
                        // DocumentPurged commits with the row removal and the
                        // PURGED transition: the event stream cannot drift
                        // from the purge outcome.
                        events.enqueue(em, DocumentEventFactory.purged(record,
                                row != null ? row.checksum : null, Instant.now()));
                    }
                    return transitioned == 1;
                });
            }
            default -> throw new IllegalStateException("unhandled decision " + decision);
        }
    }

    /**
     * The first guard check: lock the row, classify. Runs WITHOUT holding the
     * lock across the S3 delete — the finalization re-checks under a fresh
     * lock, so nothing here needs to outlive its transaction.
     */
    private Decision guardCheck(DocumentPurgeRecord record) {
        return tx.inTransaction(em -> {
            DocumentRecord row = em.find(DocumentRecord.class, record.nodeId,
                    LockModeType.PESSIMISTIC_WRITE);
            if (row == null) {
                return Decision.ROW_GONE;
            }
            return eligible(row, record) ? Decision.PURGE : Decision.VOID;
        });
    }

    /**
     * The staleness guard: purge-eligible means the row is still tombstoned
     * (PENDING_PURGE, or PURGE_FAILED from an earlier failed purge — not a
     * revive) AND its body was not re-staged after the purge was requested.
     */
    private static boolean eligible(DocumentRecord row, DocumentPurgeRecord record) {
        boolean tombstoned = DocumentStatus.PENDING_PURGE.equals(row.status)
                || DocumentStatus.PURGE_FAILED.equals(row.status);
        return tombstoned && row.updatedAt != null && !row.updatedAt.isAfter(record.requestedAt);
    }

    /** Batched delete of the snapshot keys; partial failures are drain failures. */
    private static void deleteObjects(BlobStore store, String bucket, List<String> keys,
            DocumentPurgeRecord record) {
        if (keys.isEmpty()) {
            return;
        }
        BlobStore.BatchDeleteResult result = store.deleteAll(bucket, keys);
        if (!result.allSucceeded()) {
            throw new IllegalStateException("batch delete of purge " + record.purgeId
                    + " left " + result.failedKeys().size() + " failed keys, e.g. "
                    + result.failedKeys().entrySet().iterator().next());
        }
    }

    /** Mark the record failed; at the attempts ceiling also land the row in the DLQ. */
    private void fail(DocumentPurgeRecord record, RuntimeException e) {
        Optional<DocumentPurgeRecord> updated;
        try {
            updated = queue.markFailed(record, e.getMessage());
        } catch (RuntimeException markFailure) {
            LOG.error("Failed to mark purge {} failed (original error: {})",
                    record.purgeId, e.getMessage(), markFailure);
            return;
        }
        if (updated.isPresent() && DocumentPurgeRecord.STATUS_FAILED.equals(updated.get().status)) {
            LOG.error("Purge FAILED permanently for node_id={} (purge_id={}) after {} attempts: {}",
                    record.nodeId, record.purgeId, updated.get().attempts, e.getMessage());
            // DLQ landing for the row too: PURGE_FAILED takes it out of the
            // sweeper's PENDING_PURGE scan — operator territory from here.
            try {
                documents.markPurgeFailed(record.nodeId);
            } catch (RuntimeException rowFailure) {
                LOG.error("Failed to flip row {} to PURGE_FAILED (the FAILED purge record "
                        + "still stands as the DLQ entry)", record.nodeId, rowFailure);
            }
        }
    }
}
