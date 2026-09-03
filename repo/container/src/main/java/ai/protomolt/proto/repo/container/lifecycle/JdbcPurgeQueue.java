package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.container.ledger.Tx;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * {@link PurgeQueue} over JDBC/JPA via the {@link Tx} wrapper — every method
 * is one unit of work (see Tx's usage contract).
 * <p>
 * {@link #claimBatch(int)} is a native query because JPQL has no
 * {@code SKIP LOCKED}; Hibernate maps the result rows back to managed
 * {@link DocumentPurgeRecord} entities. Terminal transitions are conditional
 * JPQL updates ({@code WHERE status = 'PENDING'}) so a record settled by a
 * competing purger is never re-settled.
 */
public final class JdbcPurgeQueue implements PurgeQueue {

    /** Cap on the {@code last_error} column's content. */
    private static final int MAX_ERROR_LENGTH = 4000;

    private final Tx tx;

    /**
     * @param tx the transactional EntityManager wrapper shared by this service
     */
    public JdbcPurgeQueue(Tx tx) {
        this.tx = tx;
    }

    @Override
    public void enqueue(EntityManager em, DocumentPurgeRecord record) {
        em.persist(record);
    }

    @Override
    public List<DocumentPurgeRecord> claimBatch(int limit) {
        // Native SQL: FOR UPDATE SKIP LOCKED has no JPQL spelling. Oldest
        // request first so drain lag is bounded by the queue, not by chance.
        return tx.inTransaction(em -> {
            @SuppressWarnings("unchecked")
            List<DocumentPurgeRecord> claimed = em.createNativeQuery(
                            "SELECT * FROM document_purges WHERE status = 'PENDING'"
                                    + " ORDER BY requested_at ASC, purge_id ASC"
                                    + " LIMIT :limit FOR UPDATE SKIP LOCKED",
                            DocumentPurgeRecord.class)
                    .setParameter("limit", limit)
                    .getResultList();
            return claimed;
        });
    }

    @Override
    public boolean markPurged(UUID purgeId) {
        return transition(purgeId, DocumentPurgeRecord.STATUS_PURGED);
    }

    @Override
    public boolean markVoid(UUID purgeId) {
        return transition(purgeId, DocumentPurgeRecord.STATUS_VOID);
    }

    @Override
    public Optional<DocumentPurgeRecord> markFailed(DocumentPurgeRecord record, String error) {
        return tx.inTransaction(em -> {
            DocumentPurgeRecord managed = em.find(DocumentPurgeRecord.class, record.purgeId,
                    LockModeType.PESSIMISTIC_WRITE);
            if (managed == null || !DocumentPurgeRecord.STATUS_PENDING.equals(managed.status)) {
                return Optional.empty();
            }
            managed.attempts = managed.attempts + 1;
            managed.lastError = truncate(error);
            if (managed.attempts >= DocumentPurgeRecord.MAX_ATTEMPTS) {
                managed.status = DocumentPurgeRecord.STATUS_FAILED;
            }
            return Optional.of(managed);
        });
    }

    @Override
    public Map<String, Long> countByStatus() {
        return tx.readOnly(em -> {
            List<Object[]> rows = em.createQuery(
                            "SELECT p.status, COUNT(p) FROM DocumentPurgeRecord p GROUP BY p.status",
                            Object[].class)
                    .getResultList();
            Map<String, Long> counts = new HashMap<>();
            for (Object[] row : rows) {
                counts.put((String) row[0], (Long) row[1]);
            }
            return counts;
        });
    }

    /** Conditional PENDING → {@code to} transition; true when this call moved it. */
    private boolean transition(UUID purgeId, String to) {
        // Cast disambiguates the Tx.inTransaction Function overload.
        return tx.inTransaction((java.util.function.Function<EntityManager, Integer>) em -> em.createQuery(
                        "UPDATE DocumentPurgeRecord p SET p.status = :to"
                                + " WHERE p.purgeId = :id AND p.status = :pending")
                .setParameter("to", to)
                .setParameter("id", purgeId)
                .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                .executeUpdate()) == 1;
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
