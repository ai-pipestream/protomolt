package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.Objects;

/**
 * Captures the immutable preparation and its fixed modes for one scoped comparison,
 * inside the transaction that already holds the caller's claim and owner fences.
 * Two statements: sizes first, so every byte is reserved before it is fetched, then
 * both rows in one read. Both rows are immutable and a competing mode insert needs
 * the claim row this transaction has locked, so the two reads observe one state.
 * The reservations live until {@link #close()}; the capture grants no authority.
 */
final class DocumentPublicationModeValidation implements AutoCloseable {
    private final PayloadBudget budget;
    private PayloadBudget.Lease preparation;
    private PayloadBudget.Lease modes;

    DocumentPublicationModeValidation(PayloadBudget budget) { this.budget = Objects.requireNonNull(budget); }

    /** Preparation row in the layout {@code DocumentPublicationPreparationJournal.decode} reads; modes null when absent. */
    record Captured(Object[] preparation, Object[] modes) {}

    /**
     * Requires {@code RepositoryOperationLedger.fenceLiveOwner} earlier in the same
     * transaction: that fence locks the execution claim row and stamps the owner.
     * Returns null when no preparation exists for the predecessor, the claim-only case.
     */
    Captured capture(EntityManager em, RepositoryExecutionClaimLedger.Claim claim, long predecessor,
            RepositoryReadControl control) {
        if (predecessor < 0 || predecessor == Long.MAX_VALUE) throw new IllegalArgumentException("Invalid preparation predecessor");
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Mode validation requires the fenced transaction");
        if (preparation != null) throw new IllegalStateException("Mode validation already captured");
        var sizes = bind(em.createNativeQuery("""
                SELECT octet_length(p.preparation_bytes), octet_length(m.modes::text)
                FROM repository_publication_preparations p
                LEFT JOIN repository_publication_modes m USING(account_id,principal,operation_id,predecessor_generation)
                WHERE p.account_id=:a AND p.principal=:p AND p.operation_id=:o AND p.predecessor_generation=:g
                """), claim, predecessor).getResultList();
        if (sizes.isEmpty()) return null;
        var size = (Object[]) sizes.getFirst();
        long preparationBytes = ((Number) size[0]).longValue();
        if (preparationBytes < 1 || preparationBytes > DocumentPublicationPreparationCodec.MAX_BYTES)
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Private preparation integrity check failed");
        // An oversized modes value is never fetched; it is reported after the preparation is validated.
        long modesBytes = size[1] == null || ((Number) size[1]).longValue() > DocumentPublicationModesJournal.MAX_BYTES
                ? 0 : ((Number) size[1]).longValue();
        preparation = budget.reserve(preparationBytes);
        if (modesBytes > 0) modes = budget.reserve(modesBytes);
        control.check();
        var row = (Object[]) bind(em.createNativeQuery("""
                SELECT p.preparation_bytes,p.preparation_sha256,p.owner_nonce,p.command_sha256,m.owner_nonce,
                  CASE WHEN octet_length(m.modes::text)<=:reserved THEN m.modes::text END
                FROM repository_publication_preparations p
                LEFT JOIN repository_publication_modes m USING(account_id,principal,operation_id,predecessor_generation)
                WHERE p.account_id=:a AND p.principal=:p AND p.operation_id=:o AND p.predecessor_generation=:g
                """), claim, predecessor).setParameter("reserved", modesBytes).getSingleResult();
        return new Captured(new Object[]{row[0], row[1], row[2], row[3]},
                row[4] == null ? null : new Object[]{row[4], row[5]});
    }

    long preparationBytes() {
        if (preparation == null) throw new IllegalStateException("No preparation is captured");
        return preparation.bytes();
    }

    @Override public void close() {
        try { if (modes != null) modes.close(); }
        finally { modes = null; if (preparation != null) { preparation.close(); preparation = null; } }
    }

    private static Query bind(Query query, RepositoryExecutionClaimLedger.Claim claim, long predecessor) {
        return query.setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                .setParameter("o", claim.key().operationId()).setParameter("g", predecessor);
    }
}
