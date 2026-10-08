package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import jakarta.persistence.EntityManager;
import java.util.HexFormat;

/** Private handler attestation of canonical coverage, never an execution or pruning capability. */
final class DocumentPreparationCoverageCertificates {
    private DocumentPreparationCoverageCertificates() {}

    /** New journal rows only; caller retains its execution claim and preparation transaction. */
    static void certifyNew(EntityManager em, DocumentPublicationPreparationRecord record, byte[] preparationDigest) {
        if (DocumentPreparationHistoryRoots.coverage(em, record, preparationDigest)
                != DocumentPreparationHistoryRoots.Coverage.EXACT)
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Preparation canonical coverage is unresolved");
        var roots = DocumentPreparationHistoryRoots.roots(record.command());
        int inserted = em.createNativeQuery("""
                INSERT INTO repository_preparation_coverage_certificates(account_id,principal,operation_id,
                    predecessor_generation,preparation_sha256,command_sha256,root_count,roots_sha256,verifier_version,proof_kind)
                VALUES(:a,:p,:o,:g,:preparation,:command,:count,:roots,1,'LIVE_ROOTS')
                """).setParameter("a", record.key().account()).setParameter("p", record.key().principal())
                .setParameter("o", record.key().operationId()).setParameter("g", record.predecessorGeneration())
                .setParameter("preparation", preparationDigest)
                .setParameter("command", HexFormat.of().parseHex(record.command().sha256()))
                .setParameter("count", roots.size()).setParameter("roots", DocumentPreparationHistoryRoots.digest(roots))
                .executeUpdate();
        if (inserted != 1) throw new IllegalStateException("Preparation coverage certification did not insert its record");
    }
}
