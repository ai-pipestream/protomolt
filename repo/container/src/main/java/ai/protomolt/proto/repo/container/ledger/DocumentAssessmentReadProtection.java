package ai.protomolt.proto.repo.container.ledger;

import java.util.Objects;
import java.util.UUID;

/** Protects the whole retained assessment; contains no provider-read authorization. */
record DocumentAssessmentReadProtection(DocumentAssessmentCreation.Created plan, UUID reader, UUID session)
        implements DocumentReadProtection<DocumentAssessmentCreation.Created> {
    DocumentAssessmentReadProtection {
        Objects.requireNonNull(plan); Objects.requireNonNull(reader); Objects.requireNonNull(session);
    }

    @Override public void release(Tx tx) { finish(tx, "release_document_assessment_read_session"); }
    @Override public void recover(Tx tx) { finish(tx, "recover_quiesced_document_assessment_read_session"); }

    private void finish(Tx tx, String function) {
        tx.inTransaction(em -> {
            if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT " + function + "(:session,:reader,:assessment)")
                    .setParameter("session", session).setParameter("reader", reader)
                    .setParameter("assessment", plan.assessment()).getSingleResult()))
                throw new IllegalStateException("Assessment session release was not acknowledged");
        });
    }

    @Override public boolean confirmReleased(Tx tx) {
        return tx.inTransaction(em -> {
            em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
            if (!Boolean.TRUE.equals(em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM repository_reader_incarnations
                    WHERE incarnation=:reader AND state='QUIESCED')
                    """).setParameter("reader", reader).getSingleResult()))
                throw new IllegalStateException("Assessment reconciliation requires proven reader quiescence");
            var identities = em.createNativeQuery("""
                    SELECT reader_incarnation,assessment_id,released FROM document_assessment_read_identities
                    WHERE session_id=:session
                    """).setParameter("session", session).getResultList();
            if (!identities.isEmpty()) {
                var row = (Object[]) identities.getFirst();
                if (!reader.equals(row[0]) || !plan.assessment().equals(row[1]))
                    throw new IllegalStateException("Assessment session identity differs");
                if (!Boolean.TRUE.equals(row[2])) return false;
            }
            return Boolean.TRUE.equals(em.createNativeQuery("""
                    SELECT NOT EXISTS(SELECT 1 FROM document_assessment_read_sessions WHERE session_id=:session)
                    """).setParameter("session", session).getSingleResult());
        });
    }
}
