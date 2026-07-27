package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import jakarta.persistence.EntityManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The purge work queue of the two-phase delete: durable, backed by the
 * {@code document_purges} table, so a crash between Phase A (tombstone +
 * enqueue, one transaction) and Phase B (the background drain) never loses a
 * purge.
 * <p>
 * Concurrency contract: {@link #claimBatch(int)} selects with
 * {@code FOR UPDATE SKIP LOCKED} so overlapping claims never return the same
 * rows. The row locks release when the claim transaction commits, so a slow
 * drain can still be re-claimed by a competing purger — every terminal
 * transition ({@link #markPurged}, {@link #markVoid}, {@link #markFailed})
 * is therefore conditional on the record still being PENDING, and the whole
 * drain is idempotent (NoSuchKey = success), so a double-take settles to one
 * winner instead of corrupting anything.
 */
public interface PurgeQueue {

    /**
     * Insert a purge record inside the CALLER'S transaction — the same
     * transaction that tombstones the document row, so the two commit or roll
     * back together (that atomicity is the whole point of Phase A).
     *
     * @param em the caller's transactional EntityManager
     * @param record the fully-populated purge record (snapshot included)
     */
    void enqueue(EntityManager em, DocumentPurgeRecord record);

    /**
     * Claim up to {@code limit} PENDING records, oldest {@code requested_at}
     * first, skipping rows another transaction holds locked
     * ({@code FOR UPDATE SKIP LOCKED}). Returns detached records; terminal
     * transitions are conditional on PENDING, so a record claimed twice is
     * settled once.
     *
     * @param limit the batch size
     * @return the claimed records (detached), possibly empty
     */
    List<DocumentPurgeRecord> claimBatch(int limit);

    /**
     * Transition a record PENDING → PURGED (objects deleted, document row
     * removed).
     *
     * @param purgeId the record id
     * @return {@code true} when this call performed the transition (the record
     *         was still PENDING); {@code false} when it was already settled
     */
    boolean markPurged(UUID purgeId);

    /**
     * Transition a record PENDING → VOID (the staleness guard cancelled the
     * purge; objects and document row are left alone).
     *
     * @param purgeId the record id
     * @return {@code true} when this call performed the transition
     */
    boolean markVoid(UUID purgeId);

    /**
     * Record a drain failure: {@code attempts + 1} and the error detail. At
     * {@link DocumentPurgeRecord#MAX_ATTEMPTS} attempts the record goes FAILED
     * (the DLQ); below the ceiling it returns to PENDING for a later retry.
     * Conditional on PENDING — a record already settled by a competing purger
     * is left alone.
     *
     * @param record the claimed record that failed
     * @param error the failure detail (truncated for the {@code last_error} column)
     * @return the updated record (detached), or empty when it was already settled
     */
    Optional<DocumentPurgeRecord> markFailed(DocumentPurgeRecord record, String error);

    /**
     * Queue depth by status — the operator's drain-lag and DLQ signal.
     *
     * @return status → row count (statuses with no rows are absent)
     */
    Map<String, Long> countByStatus();
}
