package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
