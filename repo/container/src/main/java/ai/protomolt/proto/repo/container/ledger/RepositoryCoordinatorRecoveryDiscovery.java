package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Private exact-operation discovery. Observations are staleable and never authorize mutation or pin release. */
final class RepositoryCoordinatorRecoveryDiscovery {
    enum Status {
        ABSENT, NO_CLAIM, TERMINAL, ABANDONED, LIVE, NO_OWNER, UNBOUND,
        RESERVED_NOT_INSTALLED, INSTALLED_NOT_ACTIVATED, LOCALLY_DRAINED,
        MISSING_JOURNAL, GENERATION_EXHAUSTED, EXPIRED_BOUND
    }
    record Candidate(RepositoryCoordinatorDrain.Identity predecessor,
                     RepositoryCoordinatorReservation.OwnerIdentity owner) {
        Candidate { Objects.requireNonNull(predecessor); Objects.requireNonNull(owner); }
        @Override public String toString() { return "RecoveryCandidate[private]"; }
    }
    record UnactivatedCandidate(RepositoryCoordinatorDrain.Identity predecessor,
            RepositoryCoordinatorReservation.OwnerIdentity owner, String preparationSha256,
            Optional<RepositoryCoordinatorReservation.Installation> installation) {
        UnactivatedCandidate {
            Objects.requireNonNull(predecessor); Objects.requireNonNull(owner); Objects.requireNonNull(preparationSha256); Objects.requireNonNull(installation);
        }
        @Override public String toString() { return "UnactivatedRecoveryCandidate[private]"; }
    }
    record Observation(Status status, Optional<Candidate> candidate, Optional<UnactivatedCandidate> unactivated) {
        Observation {
            Objects.requireNonNull(status); Objects.requireNonNull(candidate); Objects.requireNonNull(unactivated);
            if ((status == Status.EXPIRED_BOUND) != candidate.isPresent())
                throw new IllegalArgumentException("Only expired bound observations carry private candidate identity");
            if (unactivated.isPresent() && status!=Status.RESERVED_NOT_INSTALLED && status!=Status.INSTALLED_NOT_ACTIVATED)
                throw new IllegalArgumentException("Unactivated identity requires a reserved phase");
        }
        @Override public String toString() { return "RecoveryObservation[" + status + "]"; }
    }
    private final Tx tx;
    RepositoryCoordinatorRecoveryDiscovery(Tx tx, SqlTimeouts timeouts) {
        this.tx = Objects.requireNonNull(tx).withTimeouts(Objects.requireNonNull(timeouts));
    }

    /** One indexed key, one fixed-size row, no command/payload decoding, and explicit SQL timeouts. */
    Observation inspect(RepositoryCaller caller, RepositoryOperationLedger.Key key, String expectedDigest,
                        RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(key);
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Recovery discovery requires private process authority");
        if (expectedDigest == null || !expectedDigest.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("Expected command digest must be lowercase SHA-256");
        var result = tx.inTransaction(em -> {
            control.check();
            var row = (Object[]) em.createNativeQuery("""
                    SELECT c.command_sha256,c.claim_epoch,c.claim_token,b.incarnation,o.owner_generation,o.owner_token,
                      c.lease_until<=statement_timestamp(),o.lease_until<=statement_timestamp(),op.command_sha256,
                      EXISTS(SELECT 1 FROM repository_operation_success s WHERE (s.account_id,s.principal,s.operation_id)=(k.account_id,k.principal,k.operation_id)),
                      EXISTS(SELECT 1 FROM repository_operation_rejection s WHERE (s.account_id,s.principal,s.operation_id)=(k.account_id,k.principal,k.operation_id)),
                      EXISTS(SELECT 1 FROM repository_publication_abandonments s WHERE (s.account_id,s.principal,s.operation_id)=(k.account_id,k.principal,k.operation_id)),
                      EXISTS(SELECT 1 FROM repository_coordinator_local_drains d WHERE (d.account_id,d.principal,d.operation_id,d.claim_epoch)=(c.account_id,c.principal,c.operation_id,c.claim_epoch)),
                      EXISTS(SELECT 1 FROM repository_coordinator_reservations r WHERE (r.account_id,r.principal,r.operation_id,r.predecessor_epoch)=(c.account_id,c.principal,c.operation_id,c.claim_epoch-1)
                        AND r.successor_epoch=c.claim_epoch AND r.successor_token=c.claim_token AND r.command_sha256=c.command_sha256),
                      EXISTS(SELECT 1 FROM repository_successor_installs i WHERE (i.account_id,i.principal,i.operation_id,i.successor_epoch)=(c.account_id,c.principal,c.operation_id,c.claim_epoch)
                        AND i.successor_token=c.claim_token AND i.command_sha256=c.command_sha256),
                      EXISTS(SELECT 1 FROM repository_publication_preparations p JOIN repository_publication_modes m
                        USING(account_id,principal,operation_id,predecessor_generation)
                        WHERE (p.account_id,p.principal,p.operation_id,p.predecessor_generation)=(c.account_id,c.principal,c.operation_id,o.owner_generation-1)
                        AND p.owner_nonce=o.owner_token AND m.owner_nonce=o.owner_token
                        AND p.command_sha256=c.command_sha256),
                      r.successor_incarnation,prep.preparation_sha256,
                      i.predecessor_preparation_sha256,i.preparation_sha256,i.modes_sha256
                    FROM (VALUES(CAST(:a AS varchar),CAST(:p AS varchar),CAST(:o AS uuid))) AS k(account_id,principal,operation_id)
                    LEFT JOIN repository_execution_claims c USING(account_id,principal,operation_id)
                    LEFT JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    LEFT JOIN repository_operations op USING(account_id,principal,operation_id)
                    LEFT JOIN repository_coordinator_bindings b ON (b.account_id,b.principal,b.operation_id,b.claim_epoch,b.claim_token)
                      =(c.account_id,c.principal,c.operation_id,c.claim_epoch,c.claim_token)
                    LEFT JOIN repository_coordinator_reservations r ON (r.account_id,r.principal,r.operation_id,r.predecessor_epoch)
                      =(c.account_id,c.principal,c.operation_id,c.claim_epoch-1) AND r.successor_epoch=c.claim_epoch
                      AND r.successor_token=c.claim_token AND r.command_sha256=c.command_sha256
                    LEFT JOIN repository_successor_installs i ON (i.account_id,i.principal,i.operation_id,i.successor_epoch)
                      =(c.account_id,c.principal,c.operation_id,c.claim_epoch) AND i.successor_token=c.claim_token
                      AND i.successor_incarnation=r.successor_incarnation AND i.command_sha256=c.command_sha256
                      AND i.predecessor_generation+1=o.owner_generation AND i.owner_nonce=o.owner_token
                    LEFT JOIN repository_publication_preparations prep ON (prep.account_id,prep.principal,prep.operation_id,prep.predecessor_generation)
                      =(c.account_id,c.principal,c.operation_id,o.owner_generation-1)
                      AND prep.owner_nonce=o.owner_token AND prep.command_sha256=c.command_sha256
                    """).setParameter("a", key.account()).setParameter("p", key.principal())
                    .setParameter("o", key.operationId()).getSingleResult();
            return classify(key, expectedDigest, row);
        });
        control.check(); return result;
    }

    private static Observation classify(RepositoryOperationLedger.Key key, String digest, Object[] row) {
        for (int column : new int[] {0, 8}) if (row[column] != null
                && !digest.equals(HexFormat.of().formatHex((byte[]) row[column])))
            throw new RepositoryException(RepositoryException.Code.CONFLICT, "Recovery command differs from retained operation");
        if (Boolean.TRUE.equals(row[9]) && Boolean.TRUE.equals(row[10]))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Conflicting terminal operation state");
        if (Boolean.TRUE.equals(row[9]) || Boolean.TRUE.equals(row[10])) return state(Status.TERMINAL);
        if (Boolean.TRUE.equals(row[11])) return state(Status.ABANDONED);
        if (row[0] == null) return state(row[8] == null ? Status.ABSENT : Status.NO_CLAIM);
        if (row[3] == null) {
            if (Boolean.TRUE.equals(row[13])) return unactivated(key,digest,row);
            return state(Status.UNBOUND);
        }
        if (row[4] == null) return state(Status.NO_OWNER);
        if (Boolean.TRUE.equals(row[12])) return state(Status.LOCALLY_DRAINED);
        long epoch = ((Number) row[1]).longValue(), generation = ((Number) row[4]).longValue();
        if (epoch == Long.MAX_VALUE || generation == Long.MAX_VALUE) return state(Status.GENERATION_EXHAUSTED);
        if (!Boolean.TRUE.equals(row[15])) return state(Status.MISSING_JOURNAL);
        if (!Boolean.TRUE.equals(row[6]) || !Boolean.TRUE.equals(row[7])) return state(Status.LIVE);
        return new Observation(Status.EXPIRED_BOUND, Optional.of(new Candidate(
                new RepositoryCoordinatorDrain.Identity(key, digest, epoch, (UUID) row[2], (UUID) row[3]),
                new RepositoryCoordinatorReservation.OwnerIdentity(generation, (UUID) row[5]))),Optional.empty());
    }
    private static Observation unactivated(RepositoryOperationLedger.Key key,String digest,Object[] row) {
        boolean installed = Boolean.TRUE.equals(row[14]);
        var status = installed ? Status.INSTALLED_NOT_ACTIVATED : Status.RESERVED_NOT_INSTALLED;
        if (row[4]==null || !Boolean.TRUE.equals(row[6]) || !Boolean.TRUE.equals(row[7])) return state(status);
        long epoch=((Number) row[1]).longValue(),generation=((Number) row[4]).longValue();
        if (epoch==Long.MAX_VALUE || generation==Long.MAX_VALUE) return state(Status.GENERATION_EXHAUSTED);
        if (!Boolean.TRUE.equals(row[15])) return state(Status.MISSING_JOURNAL);
        if (row[16]==null || row[17]==null || (installed && (row[18]==null || row[19]==null || row[20]==null)))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Unactivated recovery bindings are inconsistent");
        Optional<RepositoryCoordinatorReservation.Installation> installation = installed ? Optional.of(
                new RepositoryCoordinatorReservation.Installation(hex(row[18]),hex(row[19]),hex(row[20]))) : Optional.empty();
        if (installed && !hex(row[17]).equals(installation.orElseThrow().preparationSha256()))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Installed preparation differs from current owner");
        return new Observation(status,Optional.empty(),Optional.of(new UnactivatedCandidate(
                new RepositoryCoordinatorDrain.Identity(key,digest,epoch,(UUID) row[2],(UUID) row[16]),
                new RepositoryCoordinatorReservation.OwnerIdentity(generation,(UUID) row[5]),hex(row[17]),installation)));
    }
    private static String hex(Object value) { return HexFormat.of().formatHex((byte[]) value); }
    private static Observation state(Status status) { return new Observation(status, Optional.empty(),Optional.empty()); }
}
