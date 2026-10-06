package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class RepositoryRecoveryAttemptsIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final SqlTimeouts TIMEOUTS=new SqlTimeouts(Duration.ofSeconds(1),Duration.ofSeconds(5));
    private record Source(DocumentPublicationCommand command, RepositoryCoordinatorRecoveryDiscovery.Observation observation) {}
    private static Source source(Context c) {
        var input=input(c); var budget=new PayloadBudget(64_000_000);
        var previous=new DocumentPublicationPreparationRecord(input.key(),input.command(),input.seeds(),input.placements(),Duration.ofSeconds(1),0);
        var claim=new DocumentPublicationPreparationJournal(c.tx(),budget).acquireInitial(CALLER,previous,UUID.randomUUID(),UUID.randomUUID(),NONE);
        new DocumentPublicationModesJournal(c.tx(),budget).bind(CALLER,claim,0,MODES,NONE);
        new RepositoryOperationLedger(c.tx()).admit(input.key(),input.command(),input.seeds().ownerNonce(),Duration.ofSeconds(1),claim);
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
        return new Source(input.command(),new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS).inspect(CALLER,input.key(),input.command().sha256(),NONE));
    }

    @ParameterizedTest @ValueSource(ints={1,2,3})
    void expiredUnactivatedAttemptKeepsPendingSupersessionAcrossLostReply(int completedPhases) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var cancelled=new AtomicBoolean(); var cancelOnce=new AtomicBoolean(true);
            var failActivation=new AtomicBoolean(completedPhases==3);
            var failingActivation=DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!failActivation.get()) return;
                try (var query=connection.createStatement(); var rows=query.executeQuery("SELECT count(*) FROM repository_successor_executions")) {
                    rows.next();
                    if (rows.getInt(1)==1 && failActivation.compareAndSet(true,false))
                        throw new java.sql.SQLException("Activation commit refused by test", "08006");
                }
            });
            var datasource=DocumentJdbcFaults.afterCommit(failingActivation,() -> {
                if (count(c,"repository_coordinator_supersessions")==1 && cancelOnce.compareAndSet(true,false)) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var budget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(3);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),1,1_000_000,lease,budget);
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),budget,resources.sessions(),lease,TIMEOUTS,1)) {
                RepositoryCoordinatorReservation.Proposal original;
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    original=attempt.proposal();
                    assertThat(attempt.advance(CALLER,CALLER,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.RESERVED);
                    if (completedPhases>=2) assertThat(attempt.advance(CALLER,CALLER,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    if (completedPhases==3) {
                        assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,NONE)).hasStackTraceContaining("Activation commit refused by test");
                        assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                        assertThat(count(c,"repository_successor_executions")).isZero();
                    }
                    assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE)).hasMessageContaining("not expired and unactivated");
                    expire(c,source.command());
                    long held=budget.reservedBytes();
                    assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertThat(cancelOnce).isFalse();
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(budget.reservedBytes()).isEqualTo(held);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,NONE)).hasMessageContaining("Pending supersession");
                }
                Object committed=c.tx().readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_supersessions").getSingleResult());
                try (var retry=attempts.begin(CALLER,source.command(),source.observation())) {
                    assertThat(retry.supersedeExpired(CALLER,CALLER,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.RESERVED);
                    assertThat(retry.proposal().successorToken()).isEqualTo(committed);
                    assertThat(retry.proposal().successorIncarnation()).isNotEqualTo(original.successorIncarnation());
                    assertThat(retry.proposal().predecessor().epoch()).isEqualTo(original.predecessor().epoch()+1);
                    assertThat(budget.reservedBytes()).isEqualTo((long)source.command().canonical().size()+source.command().intent().getSerializedSize());
                    assertThat(retry.advance(CALLER,CALLER,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    assertThat(retry.advance(CALLER,CALLER,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_installs")).isEqualTo(completedPhases==1 ? 1 : 2);
                    assertThat(budget.reservedBytes()).isZero();
                }
            }
        }
    }

    @Test void expiredCommittedActivationCannotBeSupersededAsUnactivated() throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var cancelled=new AtomicBoolean();
            var datasource=DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if (count(c,"repository_successor_executions")==1) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var attemptBudget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(3);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),1,1_000_000,lease,new PayloadBudget(128_000_000));
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),attemptBudget,resources.sessions(),lease,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    var original=attempt.proposal();
                    attempt.advance(CALLER,CALLER,NONE); attempt.advance(CALLER,CALLER,NONE);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    expire(c,source.command());
                    assertThat(new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS)
                            .inspect(CALLER,original.predecessor().key(),source.command().sha256(),NONE).status())
                            .isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
                    long held=attemptBudget.reservedBytes();
                    assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE)).hasMessageContaining("not expired and unactivated");
                    assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(attemptBudget.reservedBytes()).isEqualTo(held).isPositive();
                    attempts.close();
                }
                // Retaining unresolved identity is intentional; close is not reconciliation.
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,1));
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void differentCoordinatorWinnerCannotReplaceRetainedIdentity(boolean pending) throws Exception {
        try (var c=context(POSTGRES);
             var resources=DocumentJournaledSessionsIT.resources(c.tx().withTimeouts(TIMEOUTS),1,1_000_000,LEASE,new PayloadBudget(128_000_000))) {
            var source=source(c); var budget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(3);
            var failOnce=new AtomicBoolean(pending);
            var cancelReadback=new AtomicBoolean(); var cancelled=new AtomicBoolean();
            var failingCommit=DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!failOnce.get()) return;
                try (var query=connection.createStatement(); var rows=query.executeQuery("SELECT count(*) FROM repository_coordinator_supersessions")) {
                    rows.next();
                    if (rows.getInt(1)==1 && failOnce.compareAndSet(true,false))
                        throw new java.sql.SQLException("Supersession commit refused by test", "08006");
                }
            });
            var datasource=DocumentJdbcFaults.afterCommit(failingCommit,() -> {
                if (cancelReadback.compareAndSet(true,false)) cancelled.set(true);
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),budget,resources.sessions(),lease,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    var original=attempt.proposal();
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isFalse();
                    attempt.advance(CALLER,CALLER,NONE); attempt.advance(CALLER,CALLER,NONE);
                    expire(c,source.command());
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isFalse();
                    if (pending) {
                        assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE))
                                .hasStackTraceContaining("Supersession commit refused by test");
                        assertThat(failOnce).isFalse();
                        assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    }
                    var observed=new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS)
                            .inspect(CALLER,original.predecessor().key(),source.command().sha256(),NONE).unactivated().orElseThrow();
                    var winner=new RepositoryCoordinatorReservation.SupersededUnactivated(observed.predecessor(),UUID.randomUUID(),UUID.randomUUID(),
                            lease,observed.owner(),observed.preparationSha256(),observed.installation());
                    RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,winner,NONE);
                    expire(c,source.command());
                    long held=budget.reservedBytes();
                    assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE))
                            .hasMessageContaining(pending ? "differs from original binding" : "differs from retained attempt");
                    if (pending) assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,NONE)).hasMessageContaining("Pending supersession");
                    else assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,NONE))
                            .hasStackTraceContaining("Execution requires exact live successor claim");
                    assertThat(count(c,"repository_successor_executions")).isZero();
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(budget.reservedBytes()).isEqualTo(held).isPositive();
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_supersessions").getSingleResult()))
                            .isEqualTo(winner.successorToken());
                    var before=claimAndOwner(c,source.command());
                    var control=new RepositoryReadControl() {
                        public boolean isCancelled() { return cancelled.get(); }
                        public long remainingNanos() { return Long.MAX_VALUE; }
                    };
                    cancelReadback.set(true);
                    assertThatThrownBy(() -> attempt.retireFenced(CALLER,CALLER,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertThat(cancelReadback).isFalse();
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(budget.reservedBytes()).isEqualTo(held);
                    attempts.close();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,1));
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isTrue();
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isTrue();
                    assertThat(budget.reservedBytes()).isZero();
                    assertThat(claimAndOwner(c,source.command())).containsExactly(before);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,NONE)).hasMessageContaining("retired");
                    assertThat(resources.sessions().retainedSessions()).isEqualTo(pending ? 0 : 1);
                    assertThat(resources.sessions().retireSuperseded(CALLER,source.command(),NONE)).isFalse();
                    attempts.close();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,0));
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
            }
        }
    }

    private static Object[] claimAndOwner(Context c, DocumentPublicationCommand command) {
        return c.tx().readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,o.owner_generation,o.owner_token,o.lease_until
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",command.operationId()).getSingleResult());
    }

    @Test void absentClaimIsNotRetirementProof() throws Exception {
        try (var c=context(POSTGRES);
             var resources=DocumentJournaledSessionsIT.resources(c.tx().withTimeouts(TIMEOUTS),1,1_000_000,LEASE,new PayloadBudget(128_000_000))) {
            var command=input(c).command();
            var key=new RepositoryOperationLedger.Key(command.intent().getAccountId(),CALLER.principalName(),command.operationId());
            // A supplied observation is not authoritative; no claim has been created in SQL.
            var observed=new RepositoryCoordinatorRecoveryDiscovery.Observation(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND,
                    Optional.of(new RepositoryCoordinatorRecoveryDiscovery.Candidate(
                            new RepositoryCoordinatorDrain.Identity(key,command.sha256(),1,UUID.randomUUID(),UUID.randomUUID()),
                            new RepositoryCoordinatorReservation.OwnerIdentity(1,UUID.randomUUID()))),Optional.empty());
            var budget=new PayloadBudget(128_000_000);
            try (var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,command,observed)) {
                    long held=budget.reservedBytes();
                    assertThat(count(c,"repository_execution_claims")).isZero();
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isFalse();
                    assertThat(budget.reservedBytes()).isEqualTo(held).isPositive();
                    attempts.close();
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,1));
            }
        }
    }

    private static void expire(Context c, DocumentPublicationCommand command) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                 (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",command.operationId()).getSingleResult());
    }

    @ParameterizedTest @ValueSource(ints={0,1,2})
    void acceptedCallRetainsItsIdentityAcrossAdmissionClosure(int completedPhases) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var budget=new PayloadBudget(128_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,budget);
                 var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                try (var pressure=budget.reserve(budget.capacity()-1)) {
                    assertThatThrownBy(() -> attempts.begin(CALLER,source.command(),source.observation()))
                            .isInstanceOf(PayloadBudget.CapacityExceededException.class);
                    assertThat(count(c,"repository_coordinator_reservations")).isZero();
                }
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    var changed=new DocumentPublicationCommand(source.command().intent().toBuilder().setMembers(0,
                            source.command().intent().getMembers(0).toBuilder().setMemberId("changed")).build());
                    assertThatThrownBy(() -> attempts.begin(CALLER,changed,source.observation())).hasMessageContaining("command changed");
                    for (int i=0;i<completedPhases;i++) attempt.advance(CALLER,CALLER,NONE);
                    attempts.close();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,1));
                    assertThatThrownBy(() -> attempts.begin(CALLER,source.command(),source.observation())).hasMessageContaining("admission is closed");
                    RepositoryRecoveryAttempts.Phase current;
                    do { current=attempt.advance(CALLER,CALLER,NONE); } while (current!=RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,0));
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @ParameterizedTest @ValueSource(strings={"reservation","installation","activation"})
    void committedPhaseWithCancelledReplyKeepsExactIdentity(String phase) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var armed=new AtomicBoolean(true); var cancelled=new AtomicBoolean();
            String table=switch (phase) {
                case "reservation" -> "repository_coordinator_reservations";
                case "installation" -> "repository_successor_installs";
                case "activation" -> "repository_successor_executions";
                default -> throw new IllegalArgumentException(phase);
            };
            var datasource=DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if (count(c,table)==1 && armed.compareAndSet(true,false)) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var budget=new PayloadBudget(128_000_000);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf),2,1_000_000,LEASE,budget);
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                RepositoryCoordinatorReservation.Proposal proposal;
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    proposal=attempt.proposal();
                    if (!phase.equals("reservation")) assertThat(attempt.advance(CALLER,CALLER,control)).isEqualTo(RepositoryRecoveryAttempts.Phase.RESERVED);
                    if (phase.equals("activation")) assertThat(attempt.advance(CALLER,CALLER,control)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                }
                assertThat(armed).isFalse(); assertThat(count(c,table)).isEqualTo(1);
                var identity=c.tx().readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_reservations").getSingleResult());
                Object ownerBefore=!phase.equals("reservation") ? c.tx().readOnly(em -> em.createNativeQuery("SELECT owner_nonce FROM repository_successor_installs").getSingleResult()) : null;
                try (var retry=attempts.begin(CALLER,source.command(),source.observation())) {
                    assertThat(retry.proposal()).isSameAs(proposal);
                    assertThatThrownBy(() -> attempts.begin(CALLER,source.command(),source.observation())).hasMessageContaining("in use");
                    RepositoryRecoveryAttempts.Phase current;
                    do { current=retry.advance(CALLER,CALLER,NONE); } while (current!=RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    attempts.close();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,0));
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                assertThat(count(c,"repository_coordinator_reservations")).isEqualTo(1);
                assertThat(count(c,"repository_successor_installs")).isEqualTo(1);
                assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_reservations").getSingleResult())).isEqualTo(identity);
                if (ownerBefore!=null) assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery("SELECT owner_nonce FROM repository_successor_installs").getSingleResult())).isEqualTo(ownerBefore);
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }
}
