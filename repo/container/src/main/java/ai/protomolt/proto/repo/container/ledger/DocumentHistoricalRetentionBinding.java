package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;

/** Exact historical preparation and initial-capture anchor; never an execution grant. */
final class DocumentHistoricalRetentionBinding {
    private DocumentHistoricalRetentionBinding() {}

    static void require(EntityManager em, DocumentPublicationPreparationRecord retained, byte[] digest, boolean lock) {
        var rows = scope(em.createNativeQuery("""
                SELECT preparation_sha256 FROM repository_publication_preparations
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:retained
                """ + (lock ? " FOR UPDATE" : "")), retained).getResultList();
        if (rows.size() != 1 || !MessageDigest.isEqual((byte[]) rows.getFirst(), digest)) throw unavailable();
        var roots = scope(em.createNativeQuery("""
                SELECT h.predecessor_generation FROM repository_preparation_history_sets h
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:retained
                AND EXISTS(SELECT 1 FROM repository_preparation_pin_batches b
                  JOIN repository_preparation_pin_owners own USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                  WHERE b.account_id=h.account_id AND b.principal=h.principal AND b.operation_id=h.operation_id
                    AND b.predecessor_generation=h.predecessor_generation AND b.initial_capture AND b.sealed
                    AND b.creation_xid=h.creation_xid)
                """ + (lock ? " FOR UPDATE OF h" : "")), retained).getResultList();
        if (roots.isEmpty() || DocumentPreparationHistoryRoots.coverage(em, retained, digest)
                != DocumentPreparationHistoryRoots.Coverage.EXACT) throw unavailable();
    }

    private static Query scope(Query query, DocumentPublicationPreparationRecord retained) {
        return query.setParameter("a", retained.key().account()).setParameter("p", retained.key().principal())
                .setParameter("o", retained.key().operationId()).setParameter("retained", retained.predecessorGeneration());
    }
    private static RepositoryException unavailable() {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Historical activation retention binding is unavailable");
    }
}
