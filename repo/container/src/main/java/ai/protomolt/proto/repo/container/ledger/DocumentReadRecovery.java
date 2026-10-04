package ai.protomolt.proto.repo.container.ledger;

import java.util.Objects;
import java.util.UUID;

/**
 * Bounded recovery from durable pin identities, without the original plan handle.
 * Only already-QUIESCED readers qualify, even if no pins remain. This class never
 * attests that another process stopped; a crash alone is not proof of quiescence.
 * The caller owns scheduling, concurrency limits and retry after visible failures.
 */
public final class DocumentReadRecovery {
    /** Matches the V47 discovery and V45 release bounds, independently of command size. */
    public static final int MAX_BATCH_SIZE = 10000;
    private final Tx tx;

    /** Borrows the recovery worker's transactional view; does not own its pool. */
    public DocumentReadRecovery(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * Returns the number of claims selected, not the number deleted: concurrent
     * recovery may select overlapping claims. Zero proves no remaining pins for
     * this permanently quiesced reader. Each invocation is one atomic transaction;
     * a failure leaves it retryable. Continue bounded calls until zero, retaining
     * caller control of deadlines and cancellation between batches.
     * Concurrent cleanup can remove identities after another worker releases the
     * selected pins; that losing call may fail. Retry discovery instead of replaying
     * an old claim list. No failure is converted into a successful drain result.
     */
    public int recoverBatch(UUID reader, int limit) {
        Objects.requireNonNull(reader);
        if (limit < 1 || limit > MAX_BATCH_SIZE)
            throw new IllegalArgumentException("Reader recovery batch requires 1 to " + MAX_BATCH_SIZE + " claims");
        return tx.inTransaction(em -> { return ((Number) em.createNativeQuery(
                "SELECT recover_quiesced_document_read_pin_batch(:reader,:limit)")
                .setParameter("reader", reader).setParameter("limit", limit).getSingleResult()).intValue(); });
    }
}
