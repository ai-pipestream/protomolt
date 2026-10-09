package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.HexFormat;

/** Canonical successor ancestry only; never creates roots, captures or execution rights. */
final class DocumentPreparationCoverageLineage {
    private DocumentPreparationCoverageLineage() {}

    /** Caller decoded current canonical bytes and holds claim then current preparation locks. */
    static boolean certify(EntityManager em, DocumentPublicationPreparationRecord record, byte[] digest,
            RepositoryReadControl control) {
        control.check();
        long generation = record.predecessorGeneration();
        if (generation == 0) throw corrupt();
        byte[] command = HexFormat.of().parseHex(record.command().sha256());
        var edges = scope(em.createNativeQuery("""
                SELECT preparation_sha256,command_sha256,owner_nonce,predecessor_preparation_sha256,predecessor_nonce
                FROM repository_successor_installs WHERE account_id=:a AND principal=:p AND operation_id=:o
                 AND predecessor_generation=:g
                """), record).setParameter("g", generation).getResultList();
        if (edges.size() != 1) throw corrupt();
        var edge = (Object[]) edges.getFirst();
        if (!equal(edge[0], digest) || !equal(edge[1], command) || !record.seeds().ownerNonce().equals(edge[2])) throw corrupt();
        var predecessors = scope(em.createNativeQuery("""
                SELECT preparation_sha256,command_sha256,owner_nonce FROM repository_publication_preparations
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:previous FOR UPDATE
                """), record).setParameter("previous", generation - 1).getResultList();
        if (predecessors.size() != 1) throw corrupt();
        var previous = (Object[]) predecessors.getFirst();
        if (!equal(previous[0], (byte[]) edge[3]) || !equal(previous[1], command) || !previous[2].equals(edge[4])) throw corrupt();
        // An immutable predecessor proof attests its canonical decoding. Do not
        // decode another blob while locks are held or allocate an unbounded chain.
        var proofs = scope(em.createNativeQuery("""
                SELECT preparation_sha256,command_sha256,predecessor_generation,preparation_sha256,root_count,roots_sha256,0
                FROM repository_preparation_coverage_certificates
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:previous
                UNION ALL
                SELECT preparation_sha256,command_sha256,anchor_generation,anchor_sha256,root_count,roots_sha256,depth
                FROM repository_preparation_coverage_lineage
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:previous
                """), record).setParameter("previous", generation - 1).getResultList();
        if (proofs.isEmpty()) {
            boolean unresolved = (Boolean) scope(em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM repository_preparation_coverage_unresolved
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:previous)
                    """), record).setParameter("previous", generation - 1).getSingleResult();
            if (!unresolved) throw corrupt();
            return false;
        }

        if (proofs.size() != 1) throw corrupt();
        var proof = (Object[]) proofs.getFirst();
        if (!equal(proof[0], (byte[]) previous[0]) || !equal(proof[1], command)) throw corrupt();
        long anchor = ((Number) proof[2]).longValue();
        int depth = ((Number) proof[6]).intValue() + 1;
        if (depth > 64) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Preparation coverage ancestry exceeds 64 links");
        if (depth < 1 || generation - anchor != depth) throw corrupt();
        var roots = DocumentPreparationHistoryRoots.roots(record.command());
        if (roots.size() != ((Number) proof[4]).intValue()
                || !equal(proof[5], DocumentPreparationHistoryRoots.digest(roots))) throw corrupt();
        control.check();
        int inserted = scope(em.createNativeQuery("""
                INSERT INTO repository_preparation_coverage_lineage(account_id,principal,operation_id,predecessor_generation,
                 preparation_sha256,command_sha256,owner_nonce,previous_preparation_sha256,previous_owner_nonce,
                 anchor_generation,anchor_sha256,root_count,roots_sha256,depth,verifier_version)
                VALUES(:a,:p,:o,:g,:sha,:command,:owner,:previousSha,:previousOwner,:anchor,:anchorSha,:count,:roots,:depth,1)
                """), record).setParameter("g", generation).setParameter("sha", digest).setParameter("command", command)
                .setParameter("owner", record.seeds().ownerNonce()).setParameter("previousSha", previous[0])
                .setParameter("previousOwner", previous[2]).setParameter("anchor", anchor).setParameter("anchorSha", proof[3])
                .setParameter("count", roots.size()).setParameter("roots", proof[5]).setParameter("depth", depth).executeUpdate();
        if (inserted != 1) throw corrupt();
        control.check();
        return true;
    }

    private static boolean equal(Object value, byte[] expected) {
        return value instanceof byte[] actual && MessageDigest.isEqual(actual, expected);
    }
    private static Query scope(Query query, DocumentPublicationPreparationRecord record) {
        return query.setParameter("a", record.key().account()).setParameter("p", record.key().principal())
                .setParameter("o", record.key().operationId());
    }
    private static RepositoryException corrupt() {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Preparation successor coverage ancestry is inconsistent");
    }
}
