package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.HexFormat;
import java.util.Objects;

/** Private durable-state observation. No identity discovery, renewal, takeover or execution grant. */
final class DocumentPublicationRegistrationInspection {
    enum Phase { NO_PREPARATION, PREPARATION_ONLY, MODES_BOUND, OWNER_ADMITTED, OWNER_EXPIRED, ASSESSMENT_STARTED, TERMINAL }
    private DocumentPublicationRegistrationInspection() {}

    static Phase inspect(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            RepositoryExecutionClaimLedger.Claim claim, RepositoryReadControl control) {
        Objects.requireNonNull(tx); Objects.requireNonNull(budget); Objects.requireNonNull(claim);
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, claim.key(), claim.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Registration inspection requires private process authority");
        if (claim.epoch() != 1) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Registration inspection requires the original claim");
        try (var loaded = new DocumentPublicationPreparationJournal(tx, budget).load(caller, claim, 0, control).orElse(null)) {
            var modes = loaded == null ? java.util.Optional.empty()
                    : new DocumentPublicationModesJournal(tx, budget).load(caller, claim, 0, control);
            control.check();
            var phase = tx.inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                // Follow claim -> owner lock order. No large payload is fetched under this lock.
                em.createNativeQuery("""
                        SELECT owner_generation FROM repository_operation_owners
                        WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                        """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                        .setParameter("o", claim.key().operationId()).getResultList();
                var row = (Object[]) em.createNativeQuery("""
                        SELECT p.owner_nonce,p.command_sha256,m.owner_nonce,o.owner_token,o.owner_generation,
                          o.lease_until>clock_timestamp(),s.owner_nonce,s.command_sha256,c.command_sha256,
                          EXISTS(SELECT 1 FROM repository_operation_success r WHERE r.account_id=k.account_id
                            AND r.principal=k.principal AND r.operation_id=k.operation_id),
                          EXISTS(SELECT 1 FROM repository_operation_rejection r WHERE r.account_id=k.account_id
                            AND r.principal=k.principal AND r.operation_id=k.operation_id)
                        FROM repository_execution_claims k
                        LEFT JOIN repository_publication_preparations p ON p.account_id=k.account_id
                          AND p.principal=k.principal AND p.operation_id=k.operation_id AND p.predecessor_generation=0
                        LEFT JOIN repository_publication_modes m ON m.account_id=k.account_id
                          AND m.principal=k.principal AND m.operation_id=k.operation_id AND m.predecessor_generation=0
                        LEFT JOIN repository_operation_owners o ON o.account_id=k.account_id
                          AND o.principal=k.principal AND o.operation_id=k.operation_id
                        LEFT JOIN repository_operations c ON c.account_id=k.account_id
                          AND c.principal=k.principal AND c.operation_id=k.operation_id
                        LEFT JOIN repository_publication_assessment_starts s ON s.account_id=k.account_id
                          AND s.principal=k.principal AND s.operation_id=k.operation_id AND s.predecessor_generation=0
                        WHERE k.account_id=:a AND k.principal=:p AND k.operation_id=:o
                        """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                        .setParameter("o", claim.key().operationId()).getSingleResult();
                if ((row[0] != null) != (loaded != null) || (row[2] != null) != modes.isPresent())
                    throw new RepositoryException(RepositoryException.Code.CONFLICT,
                            "Registration advanced during inspection; inspect again");
                // Explicit low-level claim-only/unjournaled operations are not adoptable registrations.
                if (loaded == null) {
                    if (row[2] != null || row[6] != null) throw corrupt();
                    return Phase.NO_PREPARATION;
                }
                var nonce = loaded.record().seeds().ownerNonce();
                if (!nonce.equals(row[0]) || !digest(row[1], claim)) throw corrupt();
                if (row[2] != null && !nonce.equals(row[2])) throw corrupt();
                if (row[3] == null) {
                    if (row[6] != null || Boolean.TRUE.equals(row[9]) || Boolean.TRUE.equals(row[10])) throw corrupt();
                    return modes.isPresent() ? Phase.MODES_BOUND : Phase.PREPARATION_ONLY;
                }
                if (!nonce.equals(row[3]) || ((Number) row[4]).longValue() != 1)
                    throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                            "Registration owner was replaced");
                if (!modes.isPresent() || !digest(row[8], claim)) throw corrupt();
                if (row[6] != null && (!nonce.equals(row[6]) || !digest(row[7], claim))) throw corrupt();
                boolean success = Boolean.TRUE.equals(row[9]), rejected = Boolean.TRUE.equals(row[10]);
                if (success && rejected) throw corrupt();
                // Presence routes to authorized replay; this inspector never validates or returns receipts.
                if (success || rejected) return Phase.TERMINAL;
                if (!Boolean.TRUE.equals(row[5])) return Phase.OWNER_EXPIRED;
                return row[6] == null ? Phase.OWNER_ADMITTED : Phase.ASSESSMENT_STARTED;
            });
            control.check();
            return phase;
        }
    }

    private static boolean digest(Object value, RepositoryExecutionClaimLedger.Claim claim) {
        return value instanceof byte[] bytes && claim.commandSha256().equals(HexFormat.of().formatHex(bytes));
    }
    private static RepositoryException corrupt() {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Registration journal bindings are inconsistent");
    }
}
