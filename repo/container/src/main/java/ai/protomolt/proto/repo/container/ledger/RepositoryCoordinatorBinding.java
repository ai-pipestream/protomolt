package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.Objects;
import java.util.UUID;

/** Private registration identity only: no liveness, quiescence or takeover grant. */
final class RepositoryCoordinatorBinding {
    private RepositoryCoordinatorBinding() {}

    static void requireUnbound(EntityManager em, RepositoryExecutionClaimLedger.Claim claim) {
        // The acquisition already holds the claim lock, serializing registration.
        boolean bound = (Boolean) bind(em.createNativeQuery("""
                SELECT EXISTS(SELECT 1 FROM repository_coordinator_bindings
                WHERE account_id=:a AND principal=:p AND operation_id=:o)
                """), claim).getSingleResult();
        if (bound) throw new IllegalStateException("Coordinator-bound claim cannot use unbound registration");
    }

    static void requireResume(EntityManager em, RepositoryExecutionClaimLedger.Claim claim, UUID incarnation) {
        RepositoryExecutionClaimLedger.lockLive(em, claim);
        var rows = bind(em.createNativeQuery("""
                SELECT claim_token,incarnation,claim_epoch FROM repository_coordinator_bindings
                WHERE account_id=:a AND principal=:p AND operation_id=:o
                """), claim).getResultList();
        if (rows.isEmpty() && incarnation == null) return; // Existing explicitly unbound reconciliation primitive.
        if (rows.isEmpty()) throw new IllegalStateException("Unbound claim cannot resume in a coordinator-bound manager");
        var row = (Object[]) rows.getFirst();
        if (!claim.token().equals(row[0]) || !Objects.equals(incarnation, row[1])
                || ((Number) row[2]).longValue() != claim.epoch())
            throw new IllegalStateException("Coordinator binding differs from the restoring incarnation");
    }

    static void bindInitial(EntityManager em, RepositoryExecutionClaimLedger.Acquisition acquisition, UUID incarnation) {
        Objects.requireNonNull(incarnation);
        var claim = acquisition.claim();
        if (acquisition.created()) {
            bind(em.createNativeQuery("""
                    INSERT INTO repository_coordinator_bindings(account_id,principal,operation_id,claim_epoch,claim_token,incarnation)
                    VALUES(:a,:p,:o,:epoch,:token,:incarnation)
                    """), claim).setParameter("epoch", claim.epoch()).setParameter("token", claim.token())
                    .setParameter("incarnation", incarnation).executeUpdate();
        }
        // Acquisition holds the claim row lock. An existing unbound claim cannot be adopted.
        var rows = bind(em.createNativeQuery("""
                SELECT claim_token,incarnation FROM repository_coordinator_bindings
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=:epoch
                """), claim).setParameter("epoch", claim.epoch()).getResultList();
        if (rows.isEmpty()) throw new IllegalStateException("Existing claim has no coordinator binding; adoption is unavailable");
        var row = (Object[]) rows.getFirst();
        if (!claim.token().equals(row[0]) || !incarnation.equals(row[1]))
            throw new IllegalStateException("Coordinator binding differs from the registered incarnation");
    }

    private static Query bind(Query query, RepositoryExecutionClaimLedger.Claim claim) {
        return query.setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                .setParameter("o", claim.key().operationId());
    }
}
