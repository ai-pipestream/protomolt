package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/**
 * Private SQL attestation primitive. The trusted host must first prove all owned
 * work drained, including external schema workers. No current runtime calls this.
 * This grants no successor execution or provider-effect quiescence.
 */
final class RepositoryCoordinatorLocalDrain {
    private RepositoryCoordinatorLocalDrain() {}

    static Instant record(Tx tx, RepositoryCaller caller, RepositoryCoordinatorDrain.Identity identity,
            RepositoryReadControl control) {
        require(caller, identity, control);
        var recorded = confirm(tx, caller, identity, control);
        if (recorded.isPresent()) return recorded.orElseThrow();
        try {
            var result = tx.inTransaction(em -> {
                control.check();
                em.createNativeQuery("""
                        INSERT INTO repository_coordinator_local_drains
                        (account_id,principal,operation_id,claim_epoch,claim_token,incarnation,command_sha256)
                        VALUES(:a,:p,:o,:e,:t,:i,:d)
                        ON CONFLICT(account_id,principal,operation_id,claim_epoch) DO NOTHING
                        """).setParameter("a", identity.key().account()).setParameter("p", identity.key().principal())
                        .setParameter("o", identity.key().operationId()).setParameter("e", identity.epoch())
                        .setParameter("t", identity.token()).setParameter("i", identity.incarnation())
                        .setParameter("d", HexFormat.of().parseHex(identity.commandSha256())).executeUpdate();
                var value = read(em, identity).orElseThrow(() -> new IllegalStateException("Local drain row is absent after insert"));
                control.check();
                return value;
            });
            control.check();
            return result;
        } catch (RuntimeException failure) {
            try {
                var committed = confirm(tx, caller, identity, control);
                if (committed.isPresent()) return committed.orElseThrow();
            } catch (RuntimeException confirmation) {
                if (confirmation != failure) failure.addSuppressed(confirmation);
            }
            throw failure;
        }
    }

    /** Exact immutable confirmation can outlive the original claim, without granting writes. */
    static Optional<Instant> confirm(Tx tx, RepositoryCaller caller, RepositoryCoordinatorDrain.Identity identity,
            RepositoryReadControl control) {
        require(caller, identity, control);
        var result = tx.readOnly(em -> read(em, identity));
        control.check();
        return result;
    }

    private static Optional<Instant> read(jakarta.persistence.EntityManager em, RepositoryCoordinatorDrain.Identity identity) {
        var rows = em.createNativeQuery("""
                SELECT claim_token,incarnation,command_sha256,recorded_at FROM repository_coordinator_local_drains
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=:e
                """).setParameter("a", identity.key().account()).setParameter("p", identity.key().principal())
                .setParameter("o", identity.key().operationId()).setParameter("e", identity.epoch()).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        var row = (Object[]) rows.getFirst();
        if (!identity.token().equals(row[0]) || !identity.incarnation().equals(row[1])
                || !identity.commandSha256().equals(HexFormat.of().formatHex((byte[]) row[2])))
            throw new IllegalArgumentException("Local drain differs from original binding");
        return Optional.of(switch (row[3]) {
            case Instant time -> time;
            case java.time.OffsetDateTime time -> time.toInstant();
            case java.sql.Timestamp time -> time.toInstant();
            default -> throw new IllegalStateException("Unsupported local drain timestamp");
        });
    }

    private static void require(RepositoryCaller caller, RepositoryCoordinatorDrain.Identity identity,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        Objects.requireNonNull(identity);
        DocumentAdmissionAuthorization.requireCaller(caller, identity.key(), identity.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Local drain requires private process authority");
    }
}
