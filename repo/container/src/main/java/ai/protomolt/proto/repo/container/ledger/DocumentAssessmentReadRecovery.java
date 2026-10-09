package ai.protomolt.proto.repo.container.ledger;

import java.util.Objects;
import java.util.UUID;

/** Exact bounded session recovery; requires quiescence already established by the host. */
final class DocumentAssessmentReadRecovery {
    private final Tx tx;

    DocumentAssessmentReadRecovery(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Selected count includes sessions another recovery worker may finish concurrently. */
    int recoverBatch(UUID reader, int limit) {
        Objects.requireNonNull(reader);
        if (limit < 1 || limit > DocumentReadRecovery.MAX_BATCH_SIZE)
            throw new IllegalArgumentException("Assessment recovery batch is outside reader bounds");
        return tx.inTransaction(em -> {
            em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
            if (!Boolean.TRUE.equals(em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM repository_reader_incarnations
                    WHERE incarnation=:reader AND state='QUIESCED')
                    """).setParameter("reader", reader).getSingleResult()))
                throw new IllegalStateException("Assessment recovery requires proven reader quiescence");
            @SuppressWarnings("unchecked")
            java.util.List<Object[]> rows = em.createNativeQuery("""
                    SELECT session_id,assessment_id FROM document_assessment_read_sessions
                    WHERE reader_incarnation=:reader ORDER BY session_id LIMIT :limit
                    """).setParameter("reader", reader).setParameter("limit", limit).getResultList();
            for (var row : rows) {
                if (!Boolean.TRUE.equals(em.createNativeQuery(
                        "SELECT recover_quiesced_document_assessment_read_session(:session,:reader,:assessment)")
                        .setParameter("session", row[0]).setParameter("reader", reader)
                        .setParameter("assessment", row[1]).getSingleResult()))
                    throw new IllegalStateException("Assessment session recovery was not acknowledged");
            }
            return rows.size();
        });
    }
}
