package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.pipestream.proto.repo.container.ledger.Tx;
import jakarta.persistence.EntityManager;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * {@link PurgeRelayStore} over JDBC/JPA via the {@link Tx} wrapper, one unit
 * of work per method - the same contract as {@link JdbcPurgeQueue}, whose
 * claim query {@link #claimUnrelayed(int)} mirrors with the extra
 * {@code relayed_at IS NULL} conjunct.
 */
final class JdbcPurgeRelayStore implements PurgeRelayStore {

    /** Cap on the {@code last_error} column's content (as JdbcPurgeQueue). */
    private static final int MAX_ERROR_LENGTH = 4000;

    private final Tx tx;

    /**
     * @param tx the transactional EntityManager wrapper shared by this service
     */
    JdbcPurgeRelayStore(Tx tx) {
        this.tx = tx;
    }

    @Override
    public List<DocumentPurgeRecord> claimUnrelayed(int limit) {
        // Native SQL: FOR UPDATE SKIP LOCKED has no JPQL spelling.
        return tx.inTransaction(em -> {
            @SuppressWarnings("unchecked")
            List<DocumentPurgeRecord> claimed = em.createNativeQuery(
                            "SELECT * FROM document_purges"
                                    + " WHERE status = 'PENDING' AND relayed_at IS NULL"
                                    + " ORDER BY requested_at ASC, purge_id ASC"
                                    + " LIMIT :limit FOR UPDATE SKIP LOCKED",
                            DocumentPurgeRecord.class)
                    .setParameter("limit", limit)
                    .getResultList();
            return claimed;
        });
    }

    @Override
    public boolean markRelayed(UUID purgeId, Instant relayedAt) {
        return tx.inTransaction((java.util.function.Function<EntityManager, Integer>) em ->
                em.createQuery("UPDATE DocumentPurgeRecord p SET p.relayedAt = :at"
                                + " WHERE p.purgeId = :id AND p.status = :pending"
                                + " AND p.relayedAt IS NULL")
                .setParameter("at", relayedAt)
                .setParameter("id", purgeId)
                .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                .executeUpdate()) == 1;
    }

    @Override
    public boolean failInvalid(UUID purgeId, String error) {
        return tx.inTransaction((java.util.function.Function<EntityManager, Integer>) em ->
                em.createQuery("UPDATE DocumentPurgeRecord p"
                                + " SET p.status = :failed, p.lastError = :error,"
                                + " p.attempts = p.attempts + 1"
                                + " WHERE p.purgeId = :id AND p.status = :pending")
                .setParameter("failed", DocumentPurgeRecord.STATUS_FAILED)
                .setParameter("error", truncate(error))
                .setParameter("id", purgeId)
                .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                .executeUpdate()) == 1;
    }

    @Override
    public boolean unrelay(UUID purgeId) {
        return tx.inTransaction((java.util.function.Function<EntityManager, Integer>) em ->
                em.createQuery("UPDATE DocumentPurgeRecord p SET p.relayedAt = NULL"
                                + " WHERE p.purgeId = :id AND p.status = :pending")
                .setParameter("id", purgeId)
                .setParameter("pending", DocumentPurgeRecord.STATUS_PENDING)
                .executeUpdate()) == 1;
    }

    @Override
    public Optional<DocumentPurgeRecord> findPending(UUID purgeId) {
        return tx.readOnly(em -> {
            DocumentPurgeRecord row = em.find(DocumentPurgeRecord.class, purgeId);
            if (row == null || !DocumentPurgeRecord.STATUS_PENDING.equals(row.status)) {
                return Optional.empty();
            }
            return Optional.of(row);
        });
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
