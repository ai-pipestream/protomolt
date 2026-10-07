package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import jakarta.persistence.EntityManager;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/** Immutable readback only. No source handles, session attachment, lease renewal or local drain authority. */
final class RepositoryHistoricalActivationEvidence {
    private RepositoryHistoricalActivationEvidence() {}

    record Receipt(RepositoryCoordinatorDrain.Identity execution, long predecessorGeneration,
            String preparationSha256, long retentionGeneration, String retentionSha256,
            String captureSha256, String activationTransaction) {
        @Override public String toString() { return "HistoricalActivationReceipt[private]"; }
    }

    static Optional<Receipt> confirm(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            RepositorySuccessorInstall.Plan plan, DocumentPublicationPreparationRecord retention,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(plan); Objects.requireNonNull(retention);
        var next = plan.next();
        DocumentAdmissionAuthorization.requireCaller(caller, next.key(), next.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Historical activation confirmation requires private process authority");
        if (!retention.key().equals(next.key()) || !retention.command().sha256().equals(next.command().sha256())
                || retention.predecessorGeneration() >= next.predecessorGeneration())
            throw new IllegalArgumentException("Historical activation retention scope differs");
        try (var reserved = budget.reserve(2L * DocumentPublicationPreparationCodec.MAX_BYTES + 1024 * 1024)) {
            var sha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(next));
            var retainedSha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(retention));
            var modes = RepositorySuccessorInstall.encodeModes(plan);
            control.check();
            var result = tx.inTransaction(em -> { return read(em, plan, retention, sha, retainedSha, modes); });
            control.check(); return result;
        }
    }

    /** The caller has already encoded the bounded expected identities outside SQL locks. */
    static Optional<Receipt> read(EntityManager em, RepositorySuccessorInstall.Plan plan,
            DocumentPublicationPreparationRecord retention, byte[] sha, byte[] retainedSha, String modes) {
        if (!RepositorySuccessorExecution.read(em, plan, sha, modes)) return Optional.empty();
        var key = plan.next().key();
        var rows = em.createNativeQuery("""
                SELECT h.claim_token,h.incarnation,h.predecessor_generation,h.preparation_sha256,h.command_sha256,
                  h.retention_generation,h.retention_sha256,h.pins_sha256,h.creation_xid::text
                FROM repository_historical_activations h JOIN repository_successor_executions e
                  USING(account_id,principal,operation_id,claim_epoch)
                WHERE h.account_id=:a AND h.principal=:p AND h.operation_id=:o AND h.claim_epoch=:e
                  AND e.historical_capture_required AND e.activation_xid=h.creation_xid
                """).setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId())
                .setParameter("e", plan.reservation().predecessor().epoch()+1).getResultList();
        if (rows.isEmpty()) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Historical activation retention binding is unavailable");
        var r = (Object[]) rows.getFirst();
        if (!plan.reservation().successorToken().equals(r[0]) || !plan.reservation().successorIncarnation().equals(r[1])
                || ((Number) r[2]).longValue()!=plan.next().predecessorGeneration() || !MessageDigest.isEqual(sha,(byte[]) r[3])
                || !plan.next().command().sha256().equals(HexFormat.of().formatHex((byte[]) r[4]))
                || ((Number) r[5]).longValue()!=retention.predecessorGeneration()
                || !MessageDigest.isEqual(retainedSha,(byte[]) r[6]))
            throw new RepositoryException(RepositoryException.Code.CONFLICT, "Historical activation differs from retained identity");
        var identity = new RepositoryCoordinatorDrain.Identity(key, plan.next().command().sha256(),
                plan.reservation().predecessor().epoch()+1, plan.reservation().successorToken(), plan.reservation().successorIncarnation());
        return Optional.of(new Receipt(identity, ((Number) r[2]).longValue(), HexFormat.of().formatHex((byte[]) r[3]),
                ((Number) r[5]).longValue(), HexFormat.of().formatHex((byte[]) r[6]), HexFormat.of().formatHex((byte[]) r[7]), (String) r[8]));
    }
}
