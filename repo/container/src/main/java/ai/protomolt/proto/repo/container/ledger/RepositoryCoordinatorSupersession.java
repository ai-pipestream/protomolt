package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.util.HexFormat;

/** Immutable supersession of an expired reservation that never activated. No execution or quiescence grant. */
final class RepositoryCoordinatorSupersession {
    private RepositoryCoordinatorSupersession() {}
    static Instant reserve(Tx tx, RepositoryCaller caller, RepositoryCoordinatorReservation.SupersededUnactivated proposal,
                           RepositoryReadControl control) {
        RepositoryCoordinatorReservation.require(caller,proposal,control);
        var existing = RepositoryCoordinatorReservation.confirm(tx,caller,proposal,control);
        if (existing.isPresent()) return existing.orElseThrow();
        try {
            var stamp = tx.inTransaction(em -> {
                control.check(); var p = proposal.predecessor();
                boolean installed = proposal.installation().isPresent();
                var query = em.createNativeQuery("""
                        INSERT INTO repository_coordinator_supersessions
                        (account_id,principal,operation_id,predecessor_epoch,predecessor_token,predecessor_incarnation,
                         command_sha256,successor_epoch,successor_token,successor_incarnation,lease_millis,
                         predecessor_owner_generation,predecessor_owner_nonce,phase,preparation_sha256
                        """ + (installed ? ",install_predecessor_preparation_sha256,install_preparation_sha256,install_modes_sha256" : "")
                        + ") VALUES(:a,:p,:o,:e,:t,:i,:d,:next,:nt,:ni,:lease,:g,:n,:phase,:sha"
                        + (installed ? ",:oldSha,:installSha,:modesSha" : "") + ")")
                        .setParameter("a",p.key().account()).setParameter("p",p.key().principal()).setParameter("o",p.key().operationId())
                        .setParameter("e",p.epoch()).setParameter("t",p.token()).setParameter("i",p.incarnation())
                        .setParameter("d",bytes(p.commandSha256())).setParameter("next",p.epoch()+1)
                        .setParameter("nt",proposal.successorToken()).setParameter("ni",proposal.successorIncarnation())
                        .setParameter("lease",proposal.lease().toMillis()).setParameter("g",proposal.owner().generation())
                        .setParameter("n",proposal.owner().nonce()).setParameter("phase",proposal.phase())
                        .setParameter("sha",bytes(proposal.preparationSha256()));
                if (installed) {
                    var i = proposal.installation().orElseThrow();
                    query.setParameter("oldSha",bytes(i.predecessorPreparationSha256()))
                            .setParameter("installSha",bytes(i.preparationSha256())).setParameter("modesSha",bytes(i.modesSha256()));
                }
                query.executeUpdate(); control.check();
                return RepositoryCoordinatorReservation.read(em,proposal).orElseThrow();
            });
            control.check(); return stamp;
        } catch (RuntimeException failure) {
            try {
                var committed = RepositoryCoordinatorReservation.confirm(tx,caller,proposal,control);
                if (committed.isPresent()) return committed.orElseThrow();
            } catch (RuntimeException confirmation) { if (confirmation!=failure) failure.addSuppressed(confirmation); }
            throw failure;
        }
    }
    static boolean matches(EntityManager em, RepositoryCoordinatorReservation.SupersededUnactivated proposal) {
        var p = proposal.predecessor();
        var rows = em.createNativeQuery("""
                SELECT phase,preparation_sha256,install_predecessor_preparation_sha256,install_preparation_sha256,install_modes_sha256
                FROM repository_coordinator_supersessions WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_epoch=:e
                """).setParameter("a",p.key().account()).setParameter("p",p.key().principal())
                .setParameter("o",p.key().operationId()).setParameter("e",p.epoch()).getResultList();
        if (rows.size()!=1) return false;
        var row = (Object[]) rows.getFirst();
        if (!proposal.phase().equals(row[0]) || !proposal.preparationSha256().equals(hex(row[1]))) return false;
        if (proposal.installation().isEmpty()) return row[2]==null && row[3]==null && row[4]==null;
        var i = proposal.installation().orElseThrow();
        return i.predecessorPreparationSha256().equals(hex(row[2])) && i.preparationSha256().equals(hex(row[3])) && i.modesSha256().equals(hex(row[4]));
    }
    private static String hex(Object value) { return value == null ? null : HexFormat.of().formatHex((byte[]) value); }
    private static byte[] bytes(String value) { return HexFormat.of().parseHex(value); }
}
