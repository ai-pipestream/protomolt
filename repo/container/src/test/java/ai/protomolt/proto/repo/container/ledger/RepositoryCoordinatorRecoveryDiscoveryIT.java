package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryCoordinatorRecoveryDiscovery.Status.*;
import static org.assertj.core.api.Assertions.*;

/** Discovery over real SQL. It neither starts providers nor authorizes execution. */
@Testcontainers
class RepositoryCoordinatorRecoveryDiscoveryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));
    private record Fixture(DocumentPublicationPreparationRecord previous, RepositoryExecutionClaimLedger.Claim claim,
                           UUID incarnation, RepositoryOperationLedger.Owner owner) {}
    private static Fixture fixture(Context c, boolean bound, boolean modes, boolean owner) {
        var initial = input(c); var incarnation = UUID.randomUUID(); var budget = new PayloadBudget(64_000_000);
        var previous = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(),
                initial.placements(), Duration.ofSeconds(1), 0);
        var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
        var claim = bound ? journal.acquireInitial(CALLER, previous, UUID.randomUUID(), incarnation, NONE)
                : journal.acquireInitial(CALLER, previous, UUID.randomUUID(), NONE);
        if (modes) new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
        var admitted = owner ? new RepositoryOperationLedger(c.tx()).admit(previous.key(), previous.command(),
                previous.seeds().ownerNonce(), Duration.ofSeconds(1), claim).owner().orElseThrow() : null;
        assertThat(budget.reservedBytes()).isZero();
        return new Fixture(previous, claim, incarnation, admitted);
    }
    private static RepositoryCoordinatorRecoveryDiscovery.Observation inspect(Context c, Fixture f) {
        return new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                .inspect(CALLER, f.previous().key(), f.previous().command().sha256(), NONE);
    }
    private static void expire(Context c) { c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult()); }
    private static RepositoryCoordinatorReservation.ExpiredUnquiesced reserve(Context c, RepositoryCoordinatorRecoveryDiscovery.Candidate found, Duration lease) {
        var proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(found.predecessor(), UUID.randomUUID(), UUID.randomUUID(), lease, found.owner());
        RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, proposal, NONE); return proposal;
    }

    @Test void discoversExactExpiredIdentityWithoutRenewingOrGrantingExecution() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c, true, true, true);
            assertThat(inspect(c,f).status()).isEqualTo(LIVE);
            expire(c);
            var before = leases(c,f); var observation = inspect(c,f); var candidate = observation.candidate().orElseThrow();
            assertThat(observation.status()).isEqualTo(EXPIRED_BOUND);
            assertThat(candidate.predecessor()).isEqualTo(new RepositoryCoordinatorDrain.Identity(f.previous().key(),
                    f.previous().command().sha256(), 1, f.claim().token(), f.incarnation()));
            assertThat(candidate.owner()).isEqualTo(new RepositoryCoordinatorReservation.OwnerIdentity(f.owner().generation(), f.owner().token()));
            assertThat(leases(c,f)).containsExactly(before);
            assertThat(observation.toString()).doesNotContain(f.claim().token().toString(), f.owner().token().toString());
            assertThat(candidate.toString()).doesNotContain(f.claim().token().toString(), f.owner().token().toString());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em, f.claim()); }))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            reserve(c,candidate,LEASE);
            assertThat(inspect(c,f).status()).isEqualTo(RESERVED_NOT_INSTALLED);
            assertThatThrownBy(() -> reserve(c,candidate,LEASE)).hasMessageContaining("Reservation differs");
        }
    }
    private static Object[] leases(Context c, Fixture f) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,o.owner_generation,o.owner_token,o.lease_until
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", f.previous().key().operationId()).getSingleResult());
    }

    @Test void absentAndLegacyUnclaimedOperationsAreDistinct() {
        try (var c = context(POSTGRES)) {
            var discovery = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS);
            var source = prepare(c,1);
            var key = new RepositoryOperationLedger.Key("account", "principal", source.command().operationId());
            assertThat(discovery.inspect(CALLER,key,source.command().sha256(),NONE).status()).isEqualTo(NO_CLAIM);
            assertThat(discovery.inspect(CALLER,new RepositoryOperationLedger.Key("account","principal",UUID.randomUUID()),
                    source.command().sha256(),NONE).status()).isEqualTo(ABSENT);
        }
    }

    @Test void unboundPreOwnerAndIncompleteJournalNeverYieldCandidates() {
        for (int variant=0;variant<2;variant++) try (var c = context(POSTGRES)) {
            var f = fixture(c, variant!=0, true, variant!=1);
            assertThat(inspect(c,f).status()).isEqualTo(List.of(UNBOUND,NO_OWNER).get(variant));
            expire(c);
            var observation = inspect(c,f);
            assertThat(observation.status()).isEqualTo(List.of(UNBOUND,NO_OWNER).get(variant));
            assertThat(observation.candidate()).isEmpty();
        }
        // The low-level unjournaled API remains supported, but cannot be recovered as a publication.
        try (var c = context(POSTGRES)) {
            var initial = input(c); var incarnation = UUID.randomUUID();
            var claim = c.tx().inTransaction(em -> {
                var acquired = RepositoryExecutionClaimLedger.acquireInitialInTransaction(em,initial.key(),initial.command(),UUID.randomUUID(),Duration.ofSeconds(1));
                RepositoryCoordinatorBinding.bindInitial(em,acquired,incarnation); return acquired.claim();
            });
            new RepositoryOperationLedger(c.tx()).admit(initial.key(),initial.command(),initial.seeds().ownerNonce(),Duration.ofSeconds(1),claim);
            expire(c);
            var observation = new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS).inspect(CALLER,initial.key(),initial.command().sha256(),NONE);
            assertThat(observation.status()).isEqualTo(MISSING_JOURNAL);
            assertThat(observation.candidate()).isEmpty();
        }
    }

    @Test void localDrainAndTerminalStateCannotBeReservedAsExpired() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c,true,true,true);
            RepositoryCoordinatorDrain.begin(c.tx(),CALLER,f.claim(),f.incarnation(),NONE);
            RepositoryCoordinatorLocalDrain.record(c.tx(),CALLER,new RepositoryCoordinatorDrain.Identity(f.claim().key(),
                    f.claim().commandSha256(),1,f.claim().token(),f.incarnation()),NONE);
            expire(c); assertThat(inspect(c,f).status()).isEqualTo(LOCALLY_DRAINED);
            assertThat(inspect(c,f).candidate()).isEmpty();
        }
        try (var c = context(POSTGRES)) {
            var f = fixture(c,true,true,true);
            new DocumentPublicationRejections(c.tx()).cancel(CALLER,f.owner(),f.previous().command(),NONE);
            assertThat(inspect(c,f).status()).isEqualTo(TERMINAL);
            expire(c); assertThat(inspect(c,f).candidate()).isEmpty();
        }
    }

    @Test void classifiesBothUnactivatedReplacementWindows() {
        for (boolean installed : List.of(false,true)) try (var c = context(POSTGRES)) {
            var f = fixture(c,true,true,true); expire(c);
            var proposal = reserve(c,inspect(c,f).candidate().orElseThrow(),Duration.ofSeconds(1));
            if (installed) {
                var plan = RepositorySuccessorInstall.prepare(proposal,f.previous(),Duration.ofSeconds(1),MODES);
                RepositorySuccessorInstall.install(c.tx(),new PayloadBudget(64_000_000),CALLER,plan,NONE);
            }
            assertThat(inspect(c,f).status()).isEqualTo(installed ? INSTALLED_NOT_ACTIVATED : RESERVED_NOT_INSTALLED);
            expire(c);
            assertThat(inspect(c,f).status()).isEqualTo(installed ? INSTALLED_NOT_ACTIVATED : RESERVED_NOT_INSTALLED);
            assertThat(inspect(c,f).candidate()).isEmpty();
        }
    }

    @Test void activatedSuccessorIsDiscoveredByItsOwnCurrentBinding() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c,true,true,true); expire(c);
            var proposal = reserve(c,inspect(c,f).candidate().orElseThrow(),Duration.ofSeconds(2));
            var plan = RepositorySuccessorInstall.prepare(proposal,f.previous(),Duration.ofSeconds(2),MODES);
            var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorInstall.install(c.tx(),budget,CALLER,plan,NONE);
            RepositorySuccessorExecution.activate(c.tx(),budget,CALLER,CALLER,plan,NONE);
            assertThat(inspect(c,f).status()).isEqualTo(LIVE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(2.1)").getSingleResult());
            var found = inspect(c,f).candidate().orElseThrow();
            assertThat(found.predecessor().epoch()).isEqualTo(2);
            assertThat(found.predecessor().token()).isEqualTo(proposal.successorToken());
            assertThat(found.predecessor().incarnation()).isEqualTo(proposal.successorIncarnation());
            assertThat(found.owner()).isEqualTo(new RepositoryCoordinatorReservation.OwnerIdentity(2,plan.next().seeds().ownerNonce()));
            reserve(c,found,LEASE);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void admissionDrainAloneIsNotQuiescenceAndAbandonmentIsTerminal() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c,true,true,true);
            RepositoryCoordinatorDrain.begin(c.tx(),CALLER,f.claim(),f.incarnation(),NONE);
            expire(c); reserve(c,inspect(c,f).candidate().orElseThrow(),LEASE);
        }
        try (var c = context(POSTGRES)) {
            var f = fixture(c,true,true,false); var budget = new PayloadBudget(64_000_000);
            DocumentPublicationAbandonment.abandon(c.tx(),budget,CALLER,f.claim(),f.previous(),NONE);
            assertThat(inspect(c,f).status()).isEqualTo(ABANDONED);
            assertThat(inspect(c,f).candidate()).isEmpty();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void cancellationAfterReadCommitDoesNotReturnAnObservation() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c,true,true,true); var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(),() -> cancelled.set(true));
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",source,"hibernate.hbm2ddl.auto","validate"))) {
                var discovery = new RepositoryCoordinatorRecoveryDiscovery(new Tx(emf),TIMEOUTS);
                assertThatThrownBy(() -> discovery.inspect(CALLER,f.previous().key(),f.previous().command().sha256(),control))
                        .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                assertThat(cancelled).isTrue();
            }
        }
    }

    @Test void rejectsWrongPrincipalPublicAuthorityDigestAndCancellation() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c,true,true,true); var discovery = new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS);
            for (var caller : List.of(new RepositoryCaller("other",true),new RepositoryCaller("principal",false,Set.of("account"),Set.of())))
                assertThatThrownBy(() -> discovery.inspect(caller,f.previous().key(),f.previous().command().sha256(),NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            assertThatThrownBy(() -> discovery.inspect(CALLER,f.previous().key(),"0".repeat(64),NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CONFLICT));
            assertThatThrownBy(() -> discovery.inspect(CALLER,f.previous().key(),"invalid",NONE)).isInstanceOf(IllegalArgumentException.class);
            var cancelled = new RepositoryReadControl() { public boolean isCancelled() { return true; } public long remainingNanos() { return Long.MAX_VALUE; } };
            assertThatThrownBy(() -> discovery.inspect(CALLER,f.previous().key(),f.previous().command().sha256(),cancelled))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
        }
    }

    @Test void tableLockTimeoutIsVisibleRatherThanAnEmptyObservation() throws Exception {
        try (var c = context(POSTGRES); var blocker = c.pool().getConnection()) {
            var f = fixture(c,true,true,true); blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) { statement.execute("LOCK repository_execution_claims IN ACCESS EXCLUSIVE MODE"); }
            var discovery = new RepositoryCoordinatorRecoveryDiscovery(c.tx(),new SqlTimeouts(Duration.ofMillis(100),Duration.ofMillis(500)));
            try {
                assertThatThrownBy(() -> discovery.inspect(CALLER,f.previous().key(),f.previous().command().sha256(),NONE))
                        .hasStackTraceContaining("lock timeout");
            } finally { blocker.rollback(); }
            assertThat(inspect(c,f).status()).isIn(LIVE,EXPIRED_BOUND);
        }
    }
}
