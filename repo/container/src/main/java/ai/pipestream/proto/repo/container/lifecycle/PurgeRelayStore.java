package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The database side of {@link KafkaPurgeQueue}: the relay bookkeeping on the
 * {@code document_purges} row, which stays the ledger of record when the
 * queue is Kafka-backed. Package-private seam so the queue's unit tests run
 * without a database; the production implementation is
 * {@link JdbcPurgeRelayStore}.
 */
interface PurgeRelayStore {

    /**
     * Select up to {@code limit} PENDING records never relayed
     * ({@code relayed_at IS NULL}), oldest {@code requested_at} first,
     * skipping rows another transaction holds locked
     * ({@code FOR UPDATE SKIP LOCKED}). The locks release when the select's
     * transaction commits, before any publish, so a crash between the ack and
     * {@link #markRelayed} re-selects the row next time - a duplicate on the
     * topic, tolerated because settling is conditional.
     *
     * @param limit the relay batch size
     * @return the unrelayed PENDING records (detached), possibly empty
     */
    List<DocumentPurgeRecord> claimUnrelayed(int limit);

    /**
     * Stamp {@code relayed_at} after the broker acked the record. Conditional
     * on the row still being PENDING and unrelayed; false means a competing
     * drain settled it meanwhile, which is fine.
     *
     * @param purgeId the record id
     * @param relayedAt the ack instant
     * @return true when this call stamped the row
     */
    boolean markRelayed(UUID purgeId, Instant relayedAt);

    /**
     * Fail a row that can never be relayed (its command fails serialization
     * or validation): straight to FAILED with the error detail, conditional
     * on PENDING. The FAILED record IS the dead-letter queue, same as the
     * attempts ladder's terminal state.
     *
     * @param purgeId the record id
     * @param error the serialization/validation failure detail
     * @return true when this call performed the transition
     */
    boolean failInvalid(UUID purgeId, String error);

    /**
     * Clear {@code relayed_at} so the next claim re-relays the row: the
     * recovery path when a retry republication fails, conditional on PENDING.
     *
     * @param purgeId the record id
     * @return true when this call cleared the stamp
     */
    boolean unrelay(UUID purgeId);

    /**
     * Re-read a claimed command's row, only while it is still PENDING. The
     * Kafka claim carries the id; this is where the full record (and the
     * proof the work is not already settled) comes from.
     *
     * @param purgeId the record id named by the command
     * @return the PENDING record (detached), or empty when the row is gone or
     *         already settled
     */
    Optional<DocumentPurgeRecord> findPending(UUID purgeId);
}
