package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Private graceful reservation. Confirmation grants neither execution nor a renewed lease. */
final class RepositoryCoordinatorHandoff {
    private RepositoryCoordinatorHandoff() {}

    record Proposal(RepositoryCoordinatorDrain.Identity predecessor, UUID successorToken,
            UUID successorIncarnation, Duration lease) {
        Proposal {
            Objects.requireNonNull(predecessor); Objects.requireNonNull(successorToken);
            Objects.requireNonNull(successorIncarnation); Objects.requireNonNull(lease);
            if (predecessor.epoch() == Long.MAX_VALUE || successorToken.equals(predecessor.token())
                    || successorIncarnation.equals(predecessor.incarnation()) || lease.toMillis() < 1000
                    || lease.compareTo(Duration.ofDays(1)) > 0 || !lease.equals(Duration.ofMillis(lease.toMillis())))
                throw new IllegalArgumentException("Handoff requires fresh identities and a bounded millisecond lease");
        }
        @Override public String toString() { return "CoordinatorHandoff[private]"; }
    }

    static Instant reserve(Tx tx, RepositoryCaller caller, Proposal proposal, RepositoryReadControl control) {
        require(caller, proposal, control);
        var existing = confirm(tx, caller, proposal, control);
        if (existing.isPresent()) return existing.orElseThrow();
        try {
            var stamp = tx.inTransaction(em -> {
                control.check();
                var p = proposal.predecessor();
                // Plain INSERT: never combine transfer side effects with ON CONFLICT DO NOTHING.
                em.createNativeQuery("""
                        INSERT INTO repository_coordinator_handoffs
                        (account_id,principal,operation_id,predecessor_epoch,predecessor_token,
                         predecessor_incarnation,command_sha256,successor_epoch,successor_token,
                         successor_incarnation,lease_millis)
                        VALUES(:a,:p,:o,:e,:t,:i,:d,:next,:nt,:ni,:lease)
                        """).setParameter("a", p.key().account()).setParameter("p", p.key().principal())
                        .setParameter("o", p.key().operationId()).setParameter("e", p.epoch())
                        .setParameter("t", p.token()).setParameter("i", p.incarnation())
                        .setParameter("d", HexFormat.of().parseHex(p.commandSha256()))
                        .setParameter("next", p.epoch() + 1).setParameter("nt", proposal.successorToken())
                        .setParameter("ni", proposal.successorIncarnation()).setParameter("lease", proposal.lease().toMillis())
                        .executeUpdate();
                control.check();
                return read(em, proposal).orElseThrow();
            });
            control.check();
            return stamp;
        } catch (RuntimeException failure) {
            try {
                var committed = confirm(tx, caller, proposal, control);
                if (committed.isPresent()) return committed.orElseThrow();
            } catch (RuntimeException confirmation) {
                if (confirmation != failure) failure.addSuppressed(confirmation);
            }
            throw failure;
        }
    }

    static Optional<Instant> confirm(Tx tx, RepositoryCaller caller, Proposal proposal, RepositoryReadControl control) {
        require(caller, proposal, control);
        var result = tx.inTransaction(em -> { return read(em, proposal); });
        control.check();
        return result;
    }

    static Optional<Instant> read(jakarta.persistence.EntityManager em, Proposal proposal) {
        var p = proposal.predecessor();
        var rows = em.createNativeQuery("""
                SELECT predecessor_token,predecessor_incarnation,command_sha256,successor_token,
                       successor_incarnation,lease_millis,recorded_at
                FROM repository_coordinator_handoffs
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_epoch=:e
                """).setParameter("a", p.key().account()).setParameter("p", p.key().principal())
                .setParameter("o", p.key().operationId()).setParameter("e", p.epoch()).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        var row = (Object[]) rows.getFirst();
        if (!p.token().equals(row[0]) || !p.incarnation().equals(row[1])
                || !p.commandSha256().equals(HexFormat.of().formatHex((byte[]) row[2]))
                || !proposal.successorToken().equals(row[3]) || !proposal.successorIncarnation().equals(row[4])
                || proposal.lease().toMillis() != ((Number) row[5]).longValue())
            throw new IllegalArgumentException("Handoff differs from original binding");
        return Optional.of(switch (row[6]) {
            case Instant time -> time;
            case java.time.OffsetDateTime time -> time.toInstant();
            case java.sql.Timestamp time -> time.toInstant();
            default -> throw new IllegalStateException("Unsupported handoff timestamp");
        });
    }

    private static void require(RepositoryCaller caller, Proposal proposal, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(proposal);
        var key = proposal.predecessor().key();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Coordinator handoff requires private process authority");
    }
}
