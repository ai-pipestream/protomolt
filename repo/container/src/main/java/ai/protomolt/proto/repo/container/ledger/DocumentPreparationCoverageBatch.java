package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Keyset scan, scoped to one account and authenticated operation principal. */
final class DocumentPreparationCoverageBatch {
    record Cursor(UUID operation, long generation) {
        Cursor {
            Objects.requireNonNull(operation);
            if (generation < 0 || generation == Long.MAX_VALUE) throw new IllegalArgumentException("Invalid preparation generation");
        }
    }
    record Entry(Cursor cursor, DocumentPreparationCoverageReconciliation.Result result) {}
    /** unresolved is an observation, not authorization to prune; new work may commit afterward. */
    record Page(List<Entry> entries, Optional<Cursor> next, boolean unresolved) {
        Page { entries = List.copyOf(entries); }
    }
    private final Tx tx;
    private final DocumentPreparationCoverageReconciliation reconciliation;

    DocumentPreparationCoverageBatch(Tx tx, PayloadBudget budget, SqlTimeouts timeouts) {
        this.tx = Objects.requireNonNull(tx).withTimeouts(Objects.requireNonNull(timeouts));
        reconciliation = new DocumentPreparationCoverageReconciliation(tx, budget, timeouts);
    }

    /**
     * Each entry commits independently. A failure propagates; retry the same cursor.
     * Already certified entries no longer appear. An exhausted scan with unresolved
     * entries requires a fresh pass or investigation, never a claim of completeness.
     */
    Page reconcile(RepositoryCaller caller, String account, Optional<Cursor> after, int limit, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(after);
        if (caller == null || !caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Preparation reconciliation requires private process authority");
        if (account == null || account.isBlank() || account.length() > 200) throw new IllegalArgumentException("Invalid account");
        if (limit < 1 || limit > 64) throw new IllegalArgumentException("Reconciliation page size must be between 1 and 64");
        List<Cursor> candidates = tx.inTransaction(em -> {
            String seek = after.isPresent() ? " AND (operation_id,predecessor_generation)>(:operation,:generation)" : "";
            var query = em.createNativeQuery("""
                    SELECT operation_id,predecessor_generation FROM repository_preparation_coverage_unresolved
                    WHERE account_id=:account AND principal=:principal
                    """ + seek + " ORDER BY operation_id,predecessor_generation")
                    .setParameter("account", account).setParameter("principal", caller.principalName()).setMaxResults(limit);
            after.ifPresent(cursor -> query.setParameter("operation", cursor.operation()).setParameter("generation", cursor.generation()));
            List<Cursor> result = new ArrayList<>();
            for (Object value : query.getResultList()) {
                var row = (Object[]) value;
                result.add(new Cursor((UUID) row[0], ((Number) row[1]).longValue()));
            }
            return List.copyOf(result);
        });
        List<Entry> entries = new ArrayList<>();
        for (var cursor : candidates) {
            control.check();
            var key = new RepositoryOperationLedger.Key(account, caller.principalName(), cursor.operation());
            entries.add(new Entry(cursor, reconciliation.reconcile(caller, key, cursor.generation(), control)));
        }
        control.check();
        // Check the whole account, not merely rows after the cursor or this principal.
        // Concurrent lower-key entries and other operation principals remain visible.
        boolean unresolved = tx.inTransaction(em -> (Boolean) em.createNativeQuery("""
                SELECT EXISTS(SELECT 1 FROM repository_preparation_coverage_unresolved WHERE account_id=:account)
                """).setParameter("account", account).getSingleResult());
        return new Page(entries, candidates.size() == limit ? Optional.of(candidates.getLast()) : Optional.empty(), unresolved);
    }
}
