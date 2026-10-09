package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Instant;
import java.util.HexFormat;

/** Private expired-owner reservation. Does not start a provider or release reader resources. */
final class RepositoryCoordinatorExpiration {
    private RepositoryCoordinatorExpiration() {}

    static Instant reserve(Tx tx, RepositoryCaller caller, RepositoryCoordinatorReservation.ExpiredUnquiesced proposal,
            RepositoryReadControl control) {
        RepositoryCoordinatorReservation.require(caller, proposal, control);
        var existing = RepositoryCoordinatorReservation.confirm(tx, caller, proposal, control);
        if (existing.isPresent()) return existing.orElseThrow();
        try {
            var stamp = tx.inTransaction(em -> {
                control.check();
                var p = proposal.predecessor();
                // Plain INSERT preserves atomicity of the trigger's transfer on a losing retry.
                em.createNativeQuery("""
                        INSERT INTO repository_coordinator_expirations
                        (account_id,principal,operation_id,predecessor_epoch,predecessor_token,predecessor_incarnation,
                         command_sha256,successor_epoch,successor_token,successor_incarnation,lease_millis,
                         predecessor_owner_generation,predecessor_owner_nonce)
                        VALUES(:a,:p,:o,:e,:t,:i,:d,:next,:nt,:ni,:lease,:g,:n)
                        """).setParameter("a", p.key().account()).setParameter("p", p.key().principal())
                        .setParameter("o", p.key().operationId()).setParameter("e", p.epoch())
                        .setParameter("t", p.token()).setParameter("i", p.incarnation())
                        .setParameter("d", HexFormat.of().parseHex(p.commandSha256()))
                        .setParameter("next", p.epoch()+1).setParameter("nt", proposal.successorToken())
                        .setParameter("ni", proposal.successorIncarnation()).setParameter("lease", proposal.lease().toMillis())
                        .setParameter("g", proposal.owner().generation()).setParameter("n", proposal.owner().nonce()).executeUpdate();
                control.check();
                return RepositoryCoordinatorReservation.read(em, proposal).orElseThrow();
            });
            control.check(); return stamp;
        } catch (RuntimeException failure) {
            try {
                var committed = RepositoryCoordinatorReservation.confirm(tx, caller, proposal, control);
                if (committed.isPresent()) return committed.orElseThrow();
            } catch (RuntimeException confirmation) {
                if (confirmation != failure) failure.addSuppressed(confirmation);
            }
            throw failure;
        }
    }
}
