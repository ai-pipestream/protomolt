package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Private immutable reservation identity. Confirmation never grants execution or proves quiescence. */
final class RepositoryCoordinatorReservation {
    private RepositoryCoordinatorReservation() {}

    sealed interface Proposal permits Graceful, ExpiredUnquiesced, SupersededUnactivated {
        RepositoryCoordinatorDrain.Identity predecessor();
        UUID successorToken();
        UUID successorIncarnation();
        Duration lease();
        String kind();
    }

    record Graceful(RepositoryCoordinatorHandoff.Proposal source) implements Proposal {
        Graceful { Objects.requireNonNull(source); }
        public RepositoryCoordinatorDrain.Identity predecessor() { return source.predecessor(); }
        public UUID successorToken() { return source.successorToken(); }
        public UUID successorIncarnation() { return source.successorIncarnation(); }
        public Duration lease() { return source.lease(); }
        public String kind() { return "GRACEFUL"; }
        @Override public String toString() { return "GracefulReservation[private]"; }
    }

    record OwnerIdentity(long generation, UUID nonce) {
        OwnerIdentity {
            Objects.requireNonNull(nonce);
            if (generation < 1 || generation == Long.MAX_VALUE) throw new IllegalArgumentException("Invalid predecessor owner generation");
        }
        @Override public String toString() { return "PredecessorOwner[private]"; }
    }

    record ExpiredUnquiesced(RepositoryCoordinatorDrain.Identity predecessor, UUID successorToken,
            UUID successorIncarnation, Duration lease, OwnerIdentity owner) implements Proposal {
        ExpiredUnquiesced {
            // Reuse tuple validation only. Constructing this value is not drain evidence.
            new RepositoryCoordinatorHandoff.Proposal(predecessor, successorToken, successorIncarnation, lease);
            Objects.requireNonNull(owner);
        }
        public String kind() { return "EXPIRED_UNQUIESCED"; }
        @Override public String toString() { return "ExpiredUnquiescedReservation[private]"; }
    }

    record Installation(String predecessorPreparationSha256, String preparationSha256, String modesSha256) {
        Installation { sha(predecessorPreparationSha256); sha(preparationSha256); sha(modesSha256); }
        @Override public String toString() { return "PriorInstallation[private]"; }
    }
    record SupersededUnactivated(RepositoryCoordinatorDrain.Identity predecessor, UUID successorToken,
            UUID successorIncarnation, Duration lease, OwnerIdentity owner, String preparationSha256,
            Optional<Installation> installation) implements Proposal {
        SupersededUnactivated {
            new RepositoryCoordinatorHandoff.Proposal(predecessor,successorToken,successorIncarnation,lease);
            if (predecessor.epoch()<2) throw new IllegalArgumentException("Supersession requires a reserved epoch");
            Objects.requireNonNull(owner); Objects.requireNonNull(installation); sha(preparationSha256);
            if (installation.isPresent() && !installation.orElseThrow().preparationSha256().equals(preparationSha256))
                throw new IllegalArgumentException("Supersession preparation differs from installation");
        }
        public String kind() { return "SUPERSEDED_UNACTIVATED"; }
        String phase() { return installation.isPresent() ? "INSTALLED" : "RESERVED_ONLY"; }
        @Override public String toString() { return "UnactivatedSupersession[private]"; }
    }
    static Optional<OwnerIdentity> owner(Proposal proposal) {
        return switch (proposal) {
            case Graceful ignored -> Optional.empty();
            case ExpiredUnquiesced expired -> Optional.of(expired.owner());
            case SupersededUnactivated superseded -> Optional.of(superseded.owner());
        };
    }
    private static void sha(String value) {
        if (value==null || !value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Expected lowercase SHA-256");
    }

    static Optional<Instant> confirm(Tx tx, RepositoryCaller caller, Proposal proposal, RepositoryReadControl control) {
        require(caller, proposal, control);
        // Timeout settings are transaction-local, including for confirmation reads.
        var result = tx.inTransaction(em -> { return read(em, proposal); });
        control.check(); return result;
    }

    /** Used inside already-authorized attachment transactions without borrowing process authority. */
    static Optional<Instant> read(EntityManager em, Proposal proposal) {
        // The immutable V92 source is kind-specific and remains canonical across schema upgrades.
        if (proposal instanceof Graceful graceful) return RepositoryCoordinatorHandoff.read(em, graceful.source());
        var p = proposal.predecessor();
        String source = proposal instanceof SupersededUnactivated ? "repository_coordinator_supersessions" : "repository_coordinator_expirations";
        var rows = em.createNativeQuery("""
                SELECT r.predecessor_token,r.predecessor_incarnation,r.command_sha256,r.successor_token,
                 r.successor_incarnation,r.lease_millis,r.recorded_at,r.kind,r.predecessor_remote_state,
                 r.predecessor_owner_generation,r.predecessor_owner_nonce,
                 s.operation_id IS NOT NULL
                FROM repository_coordinator_reservations r
                LEFT JOIN
                """ + source + " s USING(account_id,principal,operation_id,predecessor_epoch) " + """
                WHERE r.account_id=:a AND r.principal=:p AND r.operation_id=:o AND r.predecessor_epoch=:e
                """).setParameter("a", p.key().account()).setParameter("p", p.key().principal())
                .setParameter("o", p.key().operationId()).setParameter("e", p.epoch()).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        var row = (Object[]) rows.getFirst();
        var expectedOwner = owner(proposal);
        boolean ownerMatches = expectedOwner.isEmpty() ? row[9] == null && row[10] == null
                : row[9] instanceof Number number && number.longValue() == expectedOwner.orElseThrow().generation()
                    && expectedOwner.orElseThrow().nonce().equals(row[10]);
        if (!p.token().equals(row[0]) || !p.incarnation().equals(row[1])
                || !p.commandSha256().equals(HexFormat.of().formatHex((byte[]) row[2]))
                || !proposal.successorToken().equals(row[3]) || !proposal.successorIncarnation().equals(row[4])
                || proposal.lease().toMillis() != ((Number) row[5]).longValue() || !proposal.kind().equals(row[7])
                || !"UNKNOWN".equals(row[8]) || !ownerMatches || !Boolean.TRUE.equals(row[11]))
            throw new IllegalArgumentException("Reservation differs from original binding");
        if (proposal instanceof SupersededUnactivated superseded && !RepositoryCoordinatorSupersession.matches(em,superseded))
            throw new IllegalArgumentException("Supersession differs from original phase or preparation");
        return Optional.of(switch (row[6]) {
            case Instant time -> time;
            case java.time.OffsetDateTime time -> time.toInstant();
            case java.sql.Timestamp time -> time.toInstant();
            default -> throw new IllegalStateException("Unsupported reservation timestamp");
        });
    }

    static void require(RepositoryCaller caller, Proposal proposal, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(proposal);
        var key = proposal.predecessor().key();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Coordinator reservation requires private process authority");
    }
}
