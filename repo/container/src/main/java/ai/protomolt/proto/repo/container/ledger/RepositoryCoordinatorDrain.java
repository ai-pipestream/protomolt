package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Private SQL admission closure; neither local shutdown nor remote-effect quiescence. */
final class RepositoryCoordinatorDrain {
    private RepositoryCoordinatorDrain() {}

    static Instant begin(Tx tx, RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim,
            UUID incarnation, RepositoryReadControl control) {
        require(caller, claim, incarnation, control);
        var started = tx.inTransaction(em -> {
            RepositoryCoordinatorBinding.requireResume(em, claim, incarnation);
            control.check();
            em.createNativeQuery("""
                    INSERT INTO repository_coordinator_drains(account_id,principal,operation_id,claim_epoch,claim_token,incarnation)
                    VALUES(:a,:p,:o,:e,:t,:i)
                    ON CONFLICT(account_id,principal,operation_id,claim_epoch) DO NOTHING
                    """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                    .setParameter("o", claim.key().operationId()).setParameter("e", claim.epoch())
                    .setParameter("t", claim.token()).setParameter("i", incarnation).executeUpdate();
            var value = em.createNativeQuery("""
                    SELECT started_at FROM repository_coordinator_drains
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=:e
                    AND claim_token=:t AND incarnation=:i
                    """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                    .setParameter("o", claim.key().operationId()).setParameter("e", claim.epoch())
                    .setParameter("t", claim.token()).setParameter("i", incarnation).getSingleResult();
            control.check();
            return instant(value);
        });
        control.check();
        return started;
    }

    /** Exact immutable confirmation, including after claim expiry. Grants no execution authority. */
    static Optional<Instant> confirm(Tx tx, RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim,
            UUID incarnation, RepositoryReadControl control) {
        require(caller, claim, incarnation, control);
        var result = tx.readOnly(em -> {
            var rows = em.createNativeQuery("""
                    SELECT d.claim_token,d.incarnation,d.started_at,c.command_sha256
                    FROM repository_coordinator_drains d JOIN repository_execution_claims c
                    USING(account_id,principal,operation_id)
                    WHERE d.account_id=:a AND d.principal=:p AND d.operation_id=:o AND d.claim_epoch=:e
                    """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                    .setParameter("o", claim.key().operationId()).setParameter("e", claim.epoch()).getResultList();
            if (rows.isEmpty()) return Optional.<Instant>empty();
            var row = (Object[]) rows.getFirst();
            if (!claim.token().equals(row[0]) || !incarnation.equals(row[1])
                    || !claim.commandSha256().equals(java.util.HexFormat.of().formatHex((byte[]) row[3])))
                throw new IllegalArgumentException("Drain confirmation differs from original binding");
            return Optional.of(instant(row[2]));
        });
        control.check();
        return result;
    }

    private static void require(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim,
            UUID incarnation, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        Objects.requireNonNull(incarnation); Objects.requireNonNull(claim);
        DocumentAdmissionAuthorization.requireCaller(caller, claim.key(), claim.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Coordinator drain requires private process authority");
    }

    private static Instant instant(Object value) {
        return switch (value) {
            case Instant time -> time;
            case java.time.OffsetDateTime time -> time.toInstant();
            case java.sql.Timestamp time -> time.toInstant();
            default -> throw new IllegalStateException("Unsupported database timestamp representation");
        };
    }
}
