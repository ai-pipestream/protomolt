package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;

/** Private bounded verification of retained preparation metadata. Grants no deletion or execution. */
final class DocumentPreparationCoverageReconciliation {
    enum Result { VERIFIED_LIVE, VERIFIED_RELEASED, ALREADY_CERTIFIED, VERIFIED_SUCCESSOR, ALREADY_VERIFIED_SUCCESSOR, UNKNOWN_ROOTS, PENDING_SUCCESSOR }
    private final Tx tx;
    private final PayloadBudget budget;

    DocumentPreparationCoverageReconciliation(Tx tx, PayloadBudget budget, SqlTimeouts timeouts) {
        this.tx = Objects.requireNonNull(tx).withTimeouts(Objects.requireNonNull(timeouts));
        this.budget = Objects.requireNonNull(budget);
    }

    Result reconcile(RepositoryCaller caller, RepositoryOperationLedger.Key key, long generation,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Preparation reconciliation requires private process authority");
        if (generation < 0 || generation == Long.MAX_VALUE) throw new IllegalArgumentException("Invalid preparation generation");
        // Retain the decode budget through terminal receipt verification and commit.
        try (var memory = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES + 1024 * 1024)) {
            Object[] row = tx.inTransaction(em -> {
                var rows = scope(em.createNativeQuery("""
                        SELECT CASE WHEN octet_length(preparation_bytes)<=:maximum THEN preparation_bytes END,
                         preparation_sha256,owner_nonce,command_sha256,octet_length(preparation_bytes)
                        FROM repository_publication_preparations
                        WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                        """), key, generation).setParameter("maximum", DocumentPublicationPreparationCodec.MAX_BYTES).getResultList();
                if (rows.isEmpty()) throw new RepositoryException(RepositoryException.Code.NOT_FOUND, "Preparation is absent");
                return (Object[]) rows.getFirst();
            });
            control.check();
            int size = ((Number) row[4]).intValue();
            if (size < 1 || size > DocumentPublicationPreparationCodec.MAX_BYTES || !(row[0] instanceof byte[])) throw corrupt();
            String command = HexFormat.of().formatHex((byte[]) row[3]);
            var record = DocumentPublicationPreparationJournal.decode(row, size, key, command, generation);
            byte[] digest = (byte[]) row[1];
            control.check();
            return tx.inTransaction(em -> {
                // Same lock order as terminal release: claim, preparation, header.
                // No source, provider, unresolved-ledger or account lock precedes these.
                var claims = operation(em.createNativeQuery("""
                        SELECT command_sha256,CAST(require_repository_read_committed() AS text)
                        FROM repository_execution_claims WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                        """), key).getResultList();
                if (claims.isEmpty() || !MessageDigest.isEqual((byte[]) ((Object[]) claims.getFirst())[0], (byte[]) row[3]))
                    throw corrupt();
                var identity = scope(em.createNativeQuery("""
                        SELECT preparation_sha256,command_sha256,owner_nonce FROM repository_publication_preparations
                        WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                        """), key, generation).getResultList();
                if (identity.isEmpty()) throw corrupt();
                var current = (Object[]) identity.getFirst();
                if (!MessageDigest.isEqual(digest, (byte[]) current[0]) || !MessageDigest.isEqual((byte[]) row[3], (byte[]) current[1])
                        || !record.seeds().ownerNonce().equals(current[2])) throw corrupt();
                control.check();
                var proofs = (Object[]) scope(em.createNativeQuery("""
                        SELECT EXISTS(SELECT 1 FROM repository_preparation_coverage_certificates
                          WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g),
                         EXISTS(SELECT 1 FROM repository_preparation_coverage_lineage
                          WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g),
                         EXISTS(SELECT 1 FROM repository_preparation_coverage_unresolved
                          WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g)
                        """), key, generation).getSingleResult();
                boolean certified = Boolean.TRUE.equals(proofs[0]), lineage = Boolean.TRUE.equals(proofs[1]);
                boolean unresolved = Boolean.TRUE.equals(proofs[2]);
                if (certified || lineage) {
                    if (unresolved || (certified && lineage)) throw corrupt();
                    return certified ? Result.ALREADY_CERTIFIED : Result.ALREADY_VERIFIED_SUCCESSOR;
                }
                if (!unresolved) throw corrupt();

                // Installation and activation alone do not attest canonical ancestry.
                if (exists(em, key, generation, "repository_successor_installs"))
                    return DocumentPreparationCoverageLineage.certify(em, record, digest, control)
                            ? Result.VERIFIED_SUCCESSOR : Result.PENDING_SUCCESSOR;

                if (exists(em, key, generation, "repository_preparation_root_releases")) {
                    DocumentPreparationCoverageCertificates.certifyReleased(em, caller, record, digest, control);
                    control.check();
                    return Result.VERIFIED_RELEASED;
                }
                if (DocumentPreparationHistoryRoots.coverage(em, record, digest) == DocumentPreparationHistoryRoots.Coverage.UNKNOWN)
                    return Result.UNKNOWN_ROOTS;
                DocumentPreparationCoverageCertificates.certifyNew(em, record, digest);
                control.check();
                // A late cancellation cannot undo a committed certification.
                return Result.VERIFIED_LIVE;
            });
        }
    }

    private static boolean exists(EntityManager em, RepositoryOperationLedger.Key key, long generation, String table) {
        return (Boolean) scope(em.createNativeQuery("SELECT EXISTS(SELECT 1 FROM " + table
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g)"), key, generation).getSingleResult();
    }
    private static Query scope(Query query, RepositoryOperationLedger.Key key, long generation) {
        return operation(query, key).setParameter("g", generation);
    }
    private static Query operation(Query query, RepositoryOperationLedger.Key key) {
        return query.setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId());
    }
    private static RepositoryException corrupt() {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Preparation reconciliation identity is inconsistent");
    }
}
