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

    /** Original registration authority without an invented or cached lease timestamp. */
    record Identity(RepositoryOperationLedger.Key key, String commandSha256, long epoch, UUID token, UUID incarnation) {
        Identity {
            Objects.requireNonNull(key); Objects.requireNonNull(commandSha256); Objects.requireNonNull(token); Objects.requireNonNull(incarnation);
            if (epoch < 1) throw new IllegalArgumentException("Claim epoch must be positive");
        }
        @Override public String toString() { return "CoordinatorDrainIdentity[private]"; }
    }

    /** False is unresolved absence, never permission to discard an uncertain registration. */
    static boolean beginRetained(Tx tx, RepositoryCaller caller, Identity identity, RepositoryReadControl control) {
        require(caller, identity, control);
        if (identity.epoch() != 1) throw new IllegalArgumentException("Only initial coordinator registration is supported");
        if (confirm(tx, caller, identity, control).isPresent()) return true;
        try {
            return beginUnconfirmed(tx, caller, identity, control);
        } catch (RuntimeException failure) {
            // A concurrent marker can commit before claim lookup, or our own commit
            // can lose its reply. Only exact durable confirmation resolves either case.
            try {
                if (confirm(tx, caller, identity, control).isPresent()) return true;
            } catch (RuntimeException confirmationFailure) {
                if (confirmationFailure != failure) failure.addSuppressed(confirmationFailure);
            }
            throw failure;
        }
    }

    private static boolean beginUnconfirmed(Tx tx, RepositoryCaller caller, Identity identity, RepositoryReadControl control) {
        var current = tx.readOnly(em -> {
            var rows = em.createNativeQuery("""
                    SELECT command_sha256,claim_epoch,claim_token,lease_until FROM repository_execution_claims
                    WHERE account_id=:a AND principal=:p AND operation_id=:o
                    """).setParameter("a", identity.key().account()).setParameter("p", identity.key().principal())
                    .setParameter("o", identity.key().operationId()).getResultList();
            if (rows.isEmpty()) return Optional.<RepositoryExecutionClaimLedger.Claim>empty();
            var row = (Object[]) rows.getFirst();
            String digest = java.util.HexFormat.of().formatHex((byte[]) row[0]);
            if (!identity.commandSha256().equals(digest) || identity.epoch() != ((Number) row[1]).longValue()
                    || !identity.token().equals(row[2])) throw new RepositoryExecutionClaimLedger.Fenced();
            return Optional.of(new RepositoryExecutionClaimLedger.Claim(identity.key(), digest, identity.epoch(), identity.token(), instant(row[3])));
        });
        control.check();
        if (current.isEmpty()) return false;
        begin(tx, caller, current.orElseThrow(), identity.incarnation(), control);
        return true;
    }

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
        return confirm(tx, caller, new Identity(claim.key(), claim.commandSha256(), claim.epoch(), claim.token(), incarnation), control);
    }

    private static Optional<Instant> confirm(Tx tx, RepositoryCaller caller, Identity identity, RepositoryReadControl control) {
        require(caller, identity, control);
        var result = tx.readOnly(em -> {
            var rows = em.createNativeQuery("""
                    SELECT d.claim_token,d.incarnation,d.started_at,c.command_sha256
                    FROM repository_coordinator_drains d JOIN repository_execution_claims c
                    USING(account_id,principal,operation_id)
                    WHERE d.account_id=:a AND d.principal=:p AND d.operation_id=:o AND d.claim_epoch=:e
                    """).setParameter("a", identity.key().account()).setParameter("p", identity.key().principal())
                    .setParameter("o", identity.key().operationId()).setParameter("e", identity.epoch()).getResultList();
            if (rows.isEmpty()) return Optional.<Instant>empty();
            var row = (Object[]) rows.getFirst();
            if (!identity.token().equals(row[0]) || !identity.incarnation().equals(row[1])
                    || !identity.commandSha256().equals(java.util.HexFormat.of().formatHex((byte[]) row[3])))
                throw new IllegalArgumentException("Drain confirmation differs from original binding");
            return Optional.of(instant(row[2]));
        });
        control.check();
        return result;
    }

    private static void require(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim,
            UUID incarnation, RepositoryReadControl control) {
        Objects.requireNonNull(claim);
        require(caller, new Identity(claim.key(), claim.commandSha256(), claim.epoch(), claim.token(), incarnation), control);
    }

    private static void require(RepositoryCaller caller, Identity identity, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        Objects.requireNonNull(identity);
        DocumentAdmissionAuthorization.requireCaller(caller, identity.key(), identity.key().account());
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
