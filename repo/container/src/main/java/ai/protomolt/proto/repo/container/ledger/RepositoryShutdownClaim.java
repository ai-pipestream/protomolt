package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.HexFormat;
import java.util.Objects;

/** Identity-only shutdown proof. Does not grant execution, cache retirement or physical reclamation. */
final class RepositoryShutdownClaim {
    private RepositoryShutdownClaim() {}

    enum State {
        CURRENT, UNRESOLVED, FENCED_UNREGISTERED, FENCED_REGISTERED;
        boolean fenced() { return this == FENCED_UNREGISTERED || this == FENCED_REGISTERED; }
    }

    /** The host supplies bounded SQL timeouts; claim lock serializes binding and reservation observations. */
    static State inspect(Tx tx, RepositoryCaller caller, RepositoryCoordinatorDrain.Identity identity,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, identity.key(), identity.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Shutdown claim proof requires private process authority");
        var key = identity.key();
        var result = tx.inTransaction(em -> {
            control.check();
            em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
            var claims = em.createNativeQuery("""
                    SELECT command_sha256,claim_epoch,claim_token FROM repository_execution_claims
                    WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                    """).setParameter("a", key.account()).setParameter("p", key.principal())
                    .setParameter("o", key.operationId()).getResultList();
            if (claims.isEmpty()) return State.UNRESOLVED;
            var claim = (Object[]) claims.getFirst();
            if (!identity.commandSha256().equals(HexFormat.of().formatHex((byte[]) claim[0])))
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Shutdown claim command changed");
            long epoch = ((Number) claim[1]).longValue();
            if (epoch == identity.epoch() && identity.token().equals(claim[2])) return State.CURRENT;
            if (epoch < identity.epoch()) return State.UNRESOLVED;
            var bindings = em.createNativeQuery("""
                    SELECT claim_token,incarnation FROM repository_coordinator_bindings
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=:e
                    """).setParameter("a", key.account()).setParameter("p", key.principal())
                    .setParameter("o", key.operationId()).setParameter("e", identity.epoch()).getResultList();
            if (bindings.isEmpty()) return State.FENCED_UNREGISTERED;
            var binding = (Object[]) bindings.getFirst();
            if (!identity.token().equals(binding[0])) return State.FENCED_UNREGISTERED;
            if (!identity.incarnation().equals(binding[1]))
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Shutdown coordinator binding changed");
            // A bound predecessor needs a complete reviewed handoff path to the current claim.
            // Epochs strictly increase; bounded SQL timeouts cover arbitrarily long histories.
            boolean reviewed = (Boolean) em.createNativeQuery("""
                    WITH RECURSIVE path(successor_epoch,successor_token,successor_incarnation) AS (
                      SELECT successor_epoch,successor_token,successor_incarnation
                      FROM repository_coordinator_reservations
                      WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_epoch=:e
                        AND predecessor_token=:t AND predecessor_incarnation=:i AND command_sha256=:d
                        AND predecessor_remote_state='UNKNOWN'
                      UNION ALL
                      SELECT r.successor_epoch,r.successor_token,r.successor_incarnation
                      FROM path h JOIN repository_coordinator_reservations r
                        ON r.predecessor_epoch=h.successor_epoch AND r.predecessor_token=h.successor_token
                        AND r.predecessor_incarnation=h.successor_incarnation
                      WHERE r.account_id=:a AND r.principal=:p AND r.operation_id=:o
                        AND r.command_sha256=:d AND r.predecessor_remote_state='UNKNOWN'
                        AND r.successor_epoch<=:currentEpoch
                    ) SELECT EXISTS(SELECT 1 FROM path WHERE successor_epoch=:currentEpoch AND successor_token=:currentToken)
                    """).setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId())
                    .setParameter("e", identity.epoch()).setParameter("t", identity.token()).setParameter("i", identity.incarnation())
                    .setParameter("d", HexFormat.of().parseHex(identity.commandSha256()))
                    .setParameter("currentEpoch", epoch).setParameter("currentToken", claim[2]).getSingleResult();
            return reviewed ? State.FENCED_REGISTERED : State.UNRESOLVED;
        });
        control.check();
        return result;
    }
}
