package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentEventRecord;
import ai.pipestream.proto.repo.container.ledger.Tx;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The transactional outbox ({@code document_events_outbox}) over JDBC/JPA via
 * the {@link Tx} wrapper - every method is one unit of work (see Tx's usage
 * contract), except {@link #enqueue(EntityManager, DocumentEventRecord)},
 * which rides the CALLER's transaction: that is the outbox pattern's whole
 * point, the event row commits or rolls back with the ledger mutation it
 * describes.
 * <p>
 * {@link #claimBatch(int)} is a native query because JPQL has no
 * {@code SKIP LOCKED}; Hibernate maps the result rows back to managed
 * {@link DocumentEventRecord} entities. Terminal transitions are conditional
 * JPQL updates ({@code WHERE status = 'PENDING'}) so a record settled by a
 * competing relay is never re-settled.
 */
public final class JdbcEventOutbox {

    /** Cap on the {@code last_error} column's content. */
    private static final int MAX_ERROR_LENGTH = 4000;

    private final Tx tx;

    /**
     * @param tx the transactional EntityManager wrapper shared by this service
     */
    public JdbcEventOutbox(Tx tx) {
        this.tx = tx;
    }

    /**
     * Insert the event row into the caller's transaction: the outbox write is
     * atomic with the ledger mutation the caller is committing.
     *
     * @param em the caller's EntityManager (its transaction is the commit unit)
     * @param record the event to outbox
     */
    public void enqueue(EntityManager em, DocumentEventRecord record) {
        em.persist(record);
    }

    /**
     * Claim up to {@code limit} PENDING records, oldest first
     * ({@code FOR UPDATE SKIP LOCKED}).
     *
     * @param limit the batch size
     * @return the claimed records (detached), possibly empty
     */
    public List<DocumentEventRecord> claimBatch(int limit) {
        // Native SQL: FOR UPDATE SKIP LOCKED has no JPQL spelling. Oldest
        // event first so relay lag is bounded by the outbox, not by chance.
        return tx.inTransaction(em -> {
            @SuppressWarnings("unchecked")
            List<DocumentEventRecord> claimed = em.createNativeQuery(
                            "SELECT * FROM document_events_outbox WHERE status = 'PENDING'"
                                    + " ORDER BY created_at ASC, event_id ASC"
                                    + " LIMIT :limit FOR UPDATE SKIP LOCKED",
                            DocumentEventRecord.class)
                    .setParameter("limit", limit)
                    .getResultList();
            return claimed;
        });
    }

    /**
     * Mark a claimed record PUBLISHED after the broker ack. Conditional on
     * PENDING: a record settled by a competing relay is not re-settled.
     *
     * @param eventId the record's id
     * @param publishedAt when the broker ack landed
     * @return true when this call moved the record
     */
    public boolean markPublished(UUID eventId, Instant publishedAt) {
        // Cast disambiguates the Tx.inTransaction Function overload.
        return tx.inTransaction((java.util.function.Function<EntityManager, Integer>) em ->
                em.createQuery("UPDATE DocumentEventRecord e SET e.status = :to,"
                                + " e.publishedAt = :when"
                                + " WHERE e.eventId = :id AND e.status = :pending")
                        .setParameter("to", DocumentEventRecord.STATUS_PUBLISHED)
                        .setParameter("when", publishedAt)
                        .setParameter("id", eventId)
                        .setParameter("pending", DocumentEventRecord.STATUS_PENDING)
                        .executeUpdate()) == 1;
    }

    /**
     * Record a relay failure: attempts + 1 and the error detail, landing the
     * record in FAILED (the DLQ) at the attempts ceiling. Returns the updated
     * record, or empty when a competing relay already settled it.
     *
     * @param record the record whose publication failed
     * @param error the failure detail (truncated to the column's cap)
     * @return the updated record, or empty when it was no longer PENDING
     */
    public Optional<DocumentEventRecord> markFailed(DocumentEventRecord record, String error) {
        return tx.inTransaction(em -> {
            DocumentEventRecord managed = em.find(DocumentEventRecord.class, record.eventId,
                    LockModeType.PESSIMISTIC_WRITE);
            if (managed == null || !DocumentEventRecord.STATUS_PENDING.equals(managed.status)) {
                return Optional.empty();
            }
            managed.attempts = managed.attempts + 1;
            managed.lastError = truncate(error);
            if (managed.attempts >= DocumentEventRecord.MAX_ATTEMPTS) {
                managed.status = DocumentEventRecord.STATUS_FAILED;
            }
            return Optional.of(managed);
        });
    }

    /**
     * Look up a record by primary key (test/introspection path).
     *
     * @param eventId the record's id
     * @return the record, or empty
     */
    public Optional<DocumentEventRecord> findById(UUID eventId) {
        return tx.readOnly(em -> Optional.ofNullable(em.find(DocumentEventRecord.class, eventId)));
    }

    /**
     * Row counts per status (test/introspection path).
     *
     * @return status to row count
     */
    public Map<String, Long> countByStatus() {
        return tx.readOnly(em -> {
            List<Object[]> rows = em.createQuery(
                            "SELECT e.status, COUNT(e) FROM DocumentEventRecord e GROUP BY e.status",
                            Object[].class)
                    .getResultList();
            Map<String, Long> counts = new HashMap<>();
            for (Object[] row : rows) {
                counts.put((String) row[0], (Long) row[1]);
            }
            return counts;
        });
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
