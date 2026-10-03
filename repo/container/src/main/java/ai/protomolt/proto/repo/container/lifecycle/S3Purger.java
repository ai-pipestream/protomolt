package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
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
 * Durable object cleanup and document-row removal for admitted purge commands.
 * Each command freezes its object keys. New admissions bind a tombstone
 * generation; a body rewrite clears that generation, so its replacement cannot
 * be removed by the old command. Migrated legacy commands without a generation
 * retain the documented status/updated_at guard until they settle.
 *
 * <p>Guards run before object I/O and again in the transaction that locks the
 * pending command, removes its eligible row and emits the completion event.
 * Storage I/O never holds SQL locks. Missing keys are idempotent success;
 * partial failures remain durable retry work and never produce successful
 * synchronous responses. Recovery preserves the original completion mode:
 * synchronous admissions emit DocumentDeleted, asynchronous admissions emit
 * DocumentPurged. A failed command cannot mark a newer generation PURGE_FAILED.
 *
 * <p>The background drain owns its queue handle, including Kafka consumer
 * acknowledgements. Synchronous request threads must use a separate JDBC handle.
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
    private enum Decision { PURGE, VOID, ROW_GONE, SETTLED }

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
        Decision decision = guardCheck(record);
        if (decision == Decision.SETTLED) {
            acknowledgeTerminal(record.purgeId);
            return false;
        }
        if (decision == Decision.VOID) {
            queue.markVoid(record.purgeId);
            return false;
        }
        DriveRecord drive = drives.findByName(record.accountId, record.driveName)
                .orElseThrow(() -> new IllegalStateException("Document drive is unavailable for purge " + record.purgeId));
        deleteObjects(store, drive.bucket, record.readObjectKeys(), record);
        boolean transitioned = tx.inTransaction(em -> {
            // A competing drain may already have settled this command. Never
            // touch a document until the command itself is locked and pending.
            DocumentPurgeRecord pending = em.find(DocumentPurgeRecord.class, record.purgeId, LockModeType.PESSIMISTIC_WRITE);
            if (pending == null) throw new IllegalStateException("Purge record disappeared: " + record.purgeId);
            if (!DocumentPurgeRecord.STATUS_PENDING.equals(pending.status)) return false;
            DocumentRecord row = em.find(DocumentRecord.class, pending.nodeId, LockModeType.PESSIMISTIC_WRITE);
            if (row != null && !eligible(row, pending)) {
                pending.status = DocumentPurgeRecord.STATUS_VOID;
                return false;
            }
            if (row != null) em.remove(row);
            pending.status = DocumentPurgeRecord.STATUS_PURGED;
            if (events != null) {
                Instant now = Instant.now();
                if (DocumentPurgeRecord.MODE_SYNCHRONOUS.equals(pending.completionMode)) {
                    if (row != null) events.enqueue(em, DocumentEventFactory.deleted(pending, now));
                } else
                    events.enqueue(em, DocumentEventFactory.purged(pending,
                            pending.contentChecksum != null ? pending.contentChecksum : row != null ? row.checksum : null, now));
            }
            return true;
        });
        // Kafka queue acknowledgements belong to its owning drain thread. The
        // synchronous caller constructs this purger with a separate JDBC queue.
        acknowledgeTerminal(record.purgeId);
        return transitioned;
    }

    /**
     * Process exactly one durable admission, returning its persisted status.
     * Request threads must use a JDBC queue handle, never the fleet Kafka consumer.
     * Failures are recorded for recovery and propagated to the request caller.
     */
    public String purgeNow(BlobStore store, java.util.UUID purgeId) {
        DocumentPurgeRecord record = findPurge(purgeId);
        try {
            process(store, record);
        } catch (RuntimeException failure) {
            try { fail(record, failure); }
            catch (RuntimeException recordingFailure) { failure.addSuppressed(recordingFailure); }
            throw failure;
        }
        return findPurge(purgeId).status;
    }

    private DocumentPurgeRecord findPurge(java.util.UUID purgeId) {
        return tx.readOnly(em -> {
            DocumentPurgeRecord record = em.find(DocumentPurgeRecord.class, purgeId);
            if (record == null) throw new IllegalStateException("Purge record is missing: " + purgeId);
            return record;
        });
    }

    private void acknowledgeTerminal(java.util.UUID purgeId) {
        DocumentPurgeRecord record = findPurge(purgeId);
        switch (record.status) {
            case DocumentPurgeRecord.STATUS_PURGED -> queue.markPurged(purgeId);
            case DocumentPurgeRecord.STATUS_VOID -> queue.markVoid(purgeId);
            case DocumentPurgeRecord.STATUS_FAILED -> queue.markFailed(record, record.lastError);
            case DocumentPurgeRecord.STATUS_PENDING -> { }
            default -> throw new IllegalStateException("Unknown purge status: " + record.status);
        }
    }

    /**
     * The first guard check: lock the row, classify. Runs WITHOUT holding the
     * lock across the S3 delete — the finalization re-checks under a fresh
     * lock, so nothing here needs to outlive its transaction.
     */
    private Decision guardCheck(DocumentPurgeRecord record) {
        return tx.inTransaction(em -> {
            DocumentPurgeRecord current = em.find(DocumentPurgeRecord.class, record.purgeId, LockModeType.PESSIMISTIC_WRITE);
            if (current == null) throw new IllegalStateException("Purge record is missing: " + record.purgeId);
            if (!DocumentPurgeRecord.STATUS_PENDING.equals(current.status)) return Decision.SETTLED;
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
        if (record.generationId != null) return tombstoned && record.generationId.equals(row.pendingPurgeId);
        if (row.pendingPurgeId != null) return false; // Legacy work cannot remove a newly admitted generation.
        return tombstoned && row.updatedAt != null && !row.updatedAt.isAfter(record.requestedAt);
    }

    /** Batched delete of the snapshot keys; partial failures are drain failures. */
    private static void deleteObjects(BlobStore store, String bucket, List<String> keys,
            DocumentPurgeRecord record) {
        if (keys.isEmpty()) {
            return;
        }
        // Validate the entire persisted batch, including commands queued before
        // this guard existed. Never partly delete a mixed-ownership snapshot.
        if (keys.stream().anyMatch(key -> ai.protomolt.proto.repo.codec.RepositoryNamespaces.isArchive(key)
                || ai.protomolt.proto.repo.codec.RepositoryNamespaces.isManagedRaw(key)))
            throw new IllegalStateException("Document purge contains an archive or managed-raw key; explicit repair is required");
        BlobStore.BatchDeleteResult result = store.deleteAll(bucket, keys);
        if (!result.allSucceeded()) {
            throw new ai.protomolt.proto.repo.blob.spi.BlobStoreException(ai.protomolt.proto.repo.blob.spi.BlobStoreException.Code.UNAVAILABLE, "batch delete of purge " + record.purgeId
                    + " left " + result.failedKeys().size() + " failed keys, e.g. "
                    + result.failedKeys().entrySet().iterator().next(), null);
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
            throw markFailure;
        }
        if (updated.isPresent() && DocumentPurgeRecord.STATUS_FAILED.equals(updated.get().status)) {
            LOG.error("Purge FAILED permanently for node_id={} (purge_id={}) after {} attempts: {}",
                    record.nodeId, record.purgeId, updated.get().attempts, e.getMessage());
            // DLQ landing for the row too: PURGE_FAILED takes it out of the
            // sweeper's PENDING_PURGE scan — operator territory from here.
            try {
                tx.inTransaction(em -> {
                    DocumentRecord row = em.find(DocumentRecord.class, record.nodeId, LockModeType.PESSIMISTIC_WRITE);
                    if (row != null && DocumentStatus.PENDING_PURGE.equals(row.status) && eligible(row, record))
                        row.status = DocumentStatus.PURGE_FAILED;
                });
            } catch (RuntimeException rowFailure) {
                LOG.error("Failed to flip row {} to PURGE_FAILED (the FAILED purge record "
                        + "still stands as the DLQ entry)", record.nodeId, rowFailure);
                throw rowFailure;
            }
        }
    }
}
