package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.*;

/** Exact local-disposal classification after the owning activation has stopped admitting work. */
final class RepositoryHistoricalCaptureState {
    private RepositoryHistoricalCaptureState() {}
    enum State { ABSENT, REGISTERED }

    static State classify(Tx tx, PayloadBudget budget, RepositorySuccessorInstall.Plan plan,
            DocumentPublicationPreparationRecord retention, DocumentPreparationCaptureDrain.Identity capture,
            RepositoryReadControl control) {
        var expected = new RepositoryCoordinatorDrain.Identity(plan.next().key(), plan.next().command().sha256(),
                plan.reservation().predecessor().epoch() + 1, plan.reservation().successorToken(),
                plan.reservation().successorIncarnation());
        if (!capture.owner().equals(expected) || capture.generation() != retention.predecessorGeneration())
            throw inconsistent();
        try (var reserved = budget.reserve(2L * DocumentPublicationPreparationCodec.MAX_BYTES + 1024 * 1024)) {
            var sha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.next()));
            var retainedSha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(retention));
            var modes = RepositorySuccessorInstall.encodeModes(plan);
            control.check();
            var state = tx.inTransaction(em -> {
                em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
                var claims = scope(em.createNativeQuery("""
                        SELECT command_sha256 FROM repository_execution_claims
                        WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                        """), capture).getResultList();
                if (claims.size() != 1 || !MessageDigest.isEqual((byte[]) claims.getFirst(),
                        HexFormat.of().parseHex(expected.commandSha256()))) throw inconsistent();
                control.check();
                long execution = epochCount(em, "repository_successor_executions", capture);
                long activation = epochCount(em, "repository_historical_activations", capture);
                long binding = epochCount(em, "repository_coordinator_bindings", capture);
                long batch = captureCount(em, "repository_preparation_pin_batches", capture);
                long owner = captureCount(em, "repository_preparation_pin_owners", capture);
                var pinState = (Object[]) capture(scope(em.createNativeQuery("""
                        SELECT count(*),sha256(convert_to('protomolt/preparation-pins/v1' || E'\\n' ||
                          coalesce(string_agg(reader_incarnation::text || '/' || pin_id::text || '/' || object_id::text || '/' ||
                            node_id::text || '/' || revision_id::text || '/' || publication_revision::text || E'\\n',
                            '' ORDER BY pin_id),''),'UTF8'))
                        FROM repository_preparation_source_pins
                        WHERE account_id=:a AND principal=:p AND operation_id=:o
                          AND predecessor_generation=:g AND pins_sha256=:digest
                        """), capture), capture).getSingleResult();
                long pins = ((Number) pinState[0]).longValue();
                if (execution == 0 && activation == 0 && binding == 0 && batch == 0 && owner == 0 && pins == 0)
                    return State.ABSENT;
                if (execution != 1 || activation != 1 || binding != 1 || batch != 1 || owner != 1 || pins < 1)
                    throw inconsistent();
                if (!MessageDigest.isEqual((byte[]) pinState[1], HexFormat.of().parseHex(capture.pinsSha256())))
                    throw inconsistent();
                var receipt = RepositoryHistoricalActivationEvidence.read(em, plan, retention, sha, retainedSha, modes)
                        .orElseThrow(RepositoryHistoricalCaptureState::inconsistent);
                if (!receipt.execution().equals(expected) || !receipt.captureSha256().equals(capture.pinsSha256()))
                    throw inconsistent();
                var rows = capture(scope(em.createNativeQuery("""
                        SELECT b.sealed,b.expected_count,b.creation_xid::text,o.claim_epoch,o.claim_token,o.incarnation
                        FROM repository_preparation_pin_batches b JOIN repository_preparation_pin_owners o
                          USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                        WHERE b.account_id=:a AND b.principal=:p AND b.operation_id=:o
                          AND b.predecessor_generation=:g AND b.pins_sha256=:digest
                        """), capture), capture).getResultList();
                if (rows.size() != 1) throw inconsistent();
                var row = (Object[]) rows.getFirst();
                if (!Boolean.TRUE.equals(row[0]) || ((Number) row[1]).longValue() != pins
                        || !receipt.activationTransaction().equals(row[2])
                        || ((Number) row[3]).longValue() != expected.epoch()
                        || !expected.token().equals(row[4]) || !expected.incarnation().equals(row[5]))
                    throw inconsistent();
                control.check();
                return State.REGISTERED;
            });
            control.check();
            return state;
        }
    }

    private static long epochCount(EntityManager em, String table, DocumentPreparationCaptureDrain.Identity id) {
        return ((Number) scope(em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=:e"), id)
                .setParameter("e", id.owner().epoch()).getSingleResult()).longValue();
    }
    private static long captureCount(EntityManager em, String table, DocumentPreparationCaptureDrain.Identity id) {
        return ((Number) capture(scope(em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g AND pins_sha256=:digest"), id), id)
                .getSingleResult()).longValue();
    }
    private static Query scope(Query query, DocumentPreparationCaptureDrain.Identity id) {
        return query.setParameter("a", id.owner().key().account()).setParameter("p", id.owner().key().principal())
                .setParameter("o", id.owner().key().operationId());
    }
    private static Query capture(Query query, DocumentPreparationCaptureDrain.Identity id) {
        return query.setParameter("g", id.generation()).setParameter("digest", HexFormat.of().parseHex(id.pinsSha256()));
    }
    private static RepositoryException inconsistent() {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Historical capture disposal evidence is incomplete or inconsistent");
    }
}
