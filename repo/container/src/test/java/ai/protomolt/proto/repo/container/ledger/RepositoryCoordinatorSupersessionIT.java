package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL phase-bound supersession. Process termination and provider effects are qualified separately. */
@Testcontainers
class RepositoryCoordinatorSupersessionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final Duration SHORT = Duration.ofSeconds(1);
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(2),Duration.ofSeconds(10));
    private static RepositorySuccessorInstall.Plan initial(Context c,boolean graceful,boolean installed) {
        return initial(c,graceful,installed,SHORT,true);
    }
    private static RepositorySuccessorInstall.Plan initial(Context c,boolean graceful,boolean installed,Duration lease,boolean expired) {
        RepositorySuccessorInstall.Plan plan;
        if (graceful) {
            var p=RepositorySuccessorInstallIT.plan(c,lease);
            plan=RepositorySuccessorInstall.prepare(p.reservation(),p.previous(),lease,MODES);
        } else {
            var input=RepositoryCoordinatorReservationIT.inputFor(c,lease);
            RepositoryCoordinatorExpiration.reserve(c.tx(),CALLER,input.proposal(),NONE);
            plan=RepositorySuccessorInstall.prepare(input.proposal(),input.previous(),lease,MODES);
        }
        if (installed) RepositorySuccessorInstall.install(c.tx(),new PayloadBudget(64_000_000),CALLER,plan,NONE);
        if(expired) expire(c,plan.previous().key()); return plan;
    }
    private static void expire(Context c,RepositoryOperationLedger.Key key) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",key.operationId()).getSingleResult());
    }
    private static RepositoryCoordinatorReservation.SupersededUnactivated proposal(Context c,RepositorySuccessorInstall.Plan initial,Duration lease) {
        var key=initial.previous().key();
        var candidate=new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS)
                .inspect(CALLER,key,initial.previous().command().sha256(),NONE).unactivated().orElseThrow();
        return new RepositoryCoordinatorReservation.SupersededUnactivated(candidate.predecessor(),UUID.randomUUID(),UUID.randomUUID(),lease,
                candidate.owner(),candidate.preparationSha256(),candidate.installation());
    }
    private static Object[] state(Context c,RepositoryOperationLedger.Key key) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,o.owner_generation,o.owner_token,o.lease_until
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",key.operationId()).getSingleResult());
    }

    @ParameterizedTest @CsvSource({"false,false","false,true","true,false","true,true"})
    void bothOriginsAndPhasesInstallAndActivateFreshSuccessor(boolean graceful,boolean installed) {
        try(var c=context(POSTGRES)) {
            var initial=initial(c,graceful,installed); var p=proposal(c,initial,LEASE); var key=initial.previous().key();
            assertThat(p.installation().isPresent()).isEqualTo(installed);
            var before=state(c,key); var stamp=RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE);
            var after=state(c,key); assertThat(after[0]).isEqualTo(3L);
            assertThat(Arrays.copyOfRange(after,3,6)).containsExactly(Arrays.copyOfRange(before,3,6));
            assertThat(RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE)).isEqualTo(stamp);
            assertThat(state(c,key)).containsExactly(after);
            var budget=new PayloadBudget(64_000_000);
            try(var loaded=new RepositoryReservedPreparation(c.tx(),budget,TIMEOUTS).load(CALLER,CALLER,p,p.owner(),NONE)) {
                assertThat(loaded.record().predecessorGeneration()).isEqualTo(installed ? 1 : 0);
                var next=RepositorySuccessorInstall.prepare(p,loaded.record(),LEASE,MODES);
                RepositorySuccessorInstall.install(c.tx(),budget,CALLER,next,NONE);
                RepositorySuccessorExecution.activate(c.tx(),budget,CALLER,CALLER,next,NONE);
                assertThat(RepositorySuccessorExecution.attach(c.tx(),budget,CALLER,next,NONE).owner().generation()).isEqualTo(installed ? 3 : 2);
            }
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(),budget,CALLER,CALLER,initial,NONE))
                    .hasStackTraceContaining(installed ? "exact live successor claim" : "install is not committed");
            for(String table:List.of("repository_coordinator_drains","repository_coordinator_local_drains","repository_coordinator_bindings")) {
                int count=c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE operation_id=:id AND claim_epoch=2")
                        .setParameter("id",key.operationId()).getSingleResult()).intValue());
                assertThat(count).isZero();
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void repeatedUnactivatedDeathsPreserveOwnerAndPreparation(boolean installed) {
        try(var c=context(POSTGRES)) {
            var initial=initial(c,true,installed); var first=proposal(c,initial,SHORT);
            RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,first,NONE); expire(c,initial.previous().key());
            var second=proposal(c,initial,LEASE);
            assertThat(second.predecessor().epoch()).isEqualTo(3);
            assertThat(second.installation()).isEmpty();
            assertThat(second.owner()).isEqualTo(first.owner());
            assertThat(second.preparationSha256()).isEqualTo(first.preparationSha256());
            RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,second,NONE);
            var budget=new PayloadBudget(64_000_000);
            try(var loaded=new RepositoryReservedPreparation(c.tx(),budget,TIMEOUTS).load(CALLER,CALLER,second,second.owner(),NONE)) {
                var plan=RepositorySuccessorInstall.prepare(second,loaded.record(),LEASE,MODES);
                RepositorySuccessorInstall.install(c.tx(),budget,CALLER,plan,NONE);
                RepositorySuccessorExecution.activate(c.tx(),budget,CALLER,CALLER,plan,NONE);
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void wrongPhaseAndHashesNeverTransferClaim() {
        try(var c=context(POSTGRES)) {
            var initial=initial(c,false,true); var p=proposal(c,initial,LEASE); var before=state(c,initial.previous().key());
            var noInstall=new RepositoryCoordinatorReservation.SupersededUnactivated(p.predecessor(),p.successorToken(),p.successorIncarnation(),p.lease(),p.owner(),p.preparationSha256(),Optional.empty());
            assertThatThrownBy(() -> RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,noInstall,NONE)).hasStackTraceContaining("phase changed");
            var i=p.installation().orElseThrow();
            var wrong=new RepositoryCoordinatorReservation.SupersededUnactivated(p.predecessor(),p.successorToken(),p.successorIncarnation(),p.lease(),p.owner(),p.preparationSha256(),
                    Optional.of(new RepositoryCoordinatorReservation.Installation(i.predecessorPreparationSha256(),i.preparationSha256(),"0".repeat(64))));
            assertThatThrownBy(() -> RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,wrong,NONE)).hasStackTraceContaining("exact prior installation");
            assertThat(state(c,initial.previous().key())).containsExactly(before);
            RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE);
            assertThatThrownBy(() -> RepositoryCoordinatorReservation.confirm(c.tx(),CALLER,noInstall,NONE)).hasMessageContaining("original phase");
            assertThatThrownBy(() -> RepositoryCoordinatorReservation.confirm(c.tx(),CALLER,wrong,NONE)).hasMessageContaining("original phase");
        }
    }

    @Test void lostCommitAcknowledgmentConfirmsWithoutRenewal() {
        try(var c=context(POSTGRES)) {
            var initial=initial(c,false,true); var p=proposal(c,initial,LEASE); var once=new AtomicBoolean();
            var source=DocumentJdbcFaults.afterCommit(c.pool(),() -> { if(once.compareAndSet(false,true)) throw new java.sql.SQLException("supersession reply lost","08006"); });
            try(var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of("hibernate.connection.datasource",source,"hibernate.hbm2ddl.auto","validate"))) {
                RepositoryCoordinatorSupersession.reserve(new Tx(emf),CALLER,p,NONE);
            }
            assertThat(once).isTrue(); var saved=state(c,initial.previous().key());
            RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE);
            assertThat(state(c,initial.previous().key())).containsExactly(saved);
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void cancellationBeforeCallOrAfterCommitRemainsVisible(boolean afterCommit) {
        try(var c=context(POSTGRES)) {
            var initial=initial(c,false,true); var p=proposal(c,initial,LEASE);
            var before=state(c,initial.previous().key());
            var cancelled=new AtomicBoolean(!afterCommit); var committed=new AtomicBoolean();
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var source=DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                committed.set(true); cancelled.set(true);
            });
            try(var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource",source,"hibernate.hbm2ddl.auto","validate"))) {
                assertThatThrownBy(() -> RepositoryCoordinatorSupersession.reserve(new Tx(emf),CALLER,p,control))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            }
            assertThat(committed.get()).isEqualTo(afterCommit);
            assertThat(RepositoryCoordinatorReservation.confirm(c.tx(),CALLER,p,NONE).isPresent()).isEqualTo(afterCommit);
            if(!afterCommit) assertThat(state(c,initial.previous().key())).containsExactly(before);
            else {
                var saved=state(c,initial.previous().key());
                assertThat(saved[0]).isEqualTo(3L);
                RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE);
                assertThat(state(c,initial.previous().key())).containsExactly(saved);
            }
        }
    }

    @Test void committedActivationRejectsSupersessionAfterExpiry() {
        try(var c=context(POSTGRES)) {
            var initial=initial(c,true,true,Duration.ofSeconds(3),false);
            var h=initial.reservation(); var key=initial.previous().key();
            var hashes=c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT predecessor_preparation_sha256,preparation_sha256,modes_sha256
                    FROM repository_successor_installs WHERE operation_id=:id AND successor_epoch=2
                    """).setParameter("id",key.operationId()).getSingleResult());
            var hex=HexFormat.of();
            var p=new RepositoryCoordinatorReservation.SupersededUnactivated(
                    new RepositoryCoordinatorDrain.Identity(key,h.predecessor().commandSha256(),2,h.successorToken(),h.successorIncarnation()),
                    UUID.randomUUID(),UUID.randomUUID(),LEASE,
                    new RepositoryCoordinatorReservation.OwnerIdentity(initial.next().predecessorGeneration()+1,initial.next().seeds().ownerNonce()),
                    hex.formatHex((byte[]) hashes[1]),Optional.of(new RepositoryCoordinatorReservation.Installation(
                            hex.formatHex((byte[]) hashes[0]),hex.formatHex((byte[]) hashes[1]),hex.formatHex((byte[]) hashes[2]))));
            var budget=new PayloadBudget(64_000_000);
            RepositorySuccessorExecution.activate(c.tx(),budget,CALLER,CALLER,initial,NONE);
            expire(c,key); var before=state(c,key);
            assertThatThrownBy(() -> RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE))
                    .hasStackTraceContaining("Activated coordinator requires bound recovery");
            assertThat(state(c,key)).containsExactly(before);
            assertThat(RepositoryCoordinatorReservation.confirm(c.tx(),CALLER,p,NONE)).isEmpty();
            var observed=new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS)
                    .inspect(CALLER,key,initial.previous().command().sha256(),NONE);
            assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
            assertThat(observed.unactivated()).isEmpty();
            assertThat(observed.candidate().orElseThrow().predecessor().epoch()).isEqualTo(2);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void concurrentProposalsConvergeOnlyOnExactIdentity(boolean identical) throws Exception {
        try(var c=context(POSTGRES);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            var initial=initial(c,true,true); var a=proposal(c,initial,LEASE); var b=identical?a:proposal(c,initial,LEASE);
            var start=new CountDownLatch(1);
            var x=workers.submit(() -> {start.await();return catchThrowable(() -> RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,a,NONE));});
            var y=workers.submit(() -> {start.await();return catchThrowable(() -> RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,b,NONE));});
            start.countDown(); var first=x.get(10,TimeUnit.SECONDS); var second=y.get(10,TimeUnit.SECONDS);
            if(identical) {assertThat(first).isNull();assertThat(second).isNull();}
            else assertThat((first==null)!=(second==null)).isTrue();
            var winner=first==null?a:b;
            assertThat(RepositoryCoordinatorReservation.confirm(c.tx(),CALLER,winner,NONE)).isPresent();
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void upgradesExistingReservationsBeforeSupersession(boolean installed) {
        try(var c=context(POSTGRES,"97")) {
            var initial=initial(c,true,installed); var before=state(c,initial.previous().key());
            org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(),c.pool().getUsername(),c.pool().getPassword())
                    .schemas(c.pool().getSchema()).defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            assertThat(state(c,initial.previous().key())).containsExactly(before);
            RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,proposal(c,initial,LEASE),NONE);
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void waitingSupersessionRechecksAfterExpiredInstallOrActivationRollsBack(boolean activation) throws Exception {
        try(var c=context(POSTGRES);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            var initial=initial(c,true,activation,Duration.ofSeconds(3),false);
            var held=new CountDownLatch(1);var release=new CountDownLatch(1);var once=new AtomicBoolean();
            var source=DocumentJdbcFaults.beforeCommit(c.pool(),connection -> {
                String table=activation?"repository_successor_executions":"repository_successor_installs";
                try(var q=connection.prepareStatement("SELECT count(*) FROM "+table+" WHERE operation_id=?")) {
                    q.setObject(1,initial.previous().key().operationId());
                    try(var rows=q.executeQuery()) {
                        rows.next();
                        if(rows.getInt(1)==1 && once.compareAndSet(false,true)) {
                            held.countDown();
                            try { if(!release.await(15,TimeUnit.SECONDS)) throw new java.sql.SQLException("Commit gate timed out"); }
                            catch(InterruptedException e) {Thread.currentThread().interrupt();throw new java.sql.SQLException("Commit gate interrupted",e);}
                        }
                    }
                }
            });
            try(var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of("hibernate.connection.datasource",source,"hibernate.hbm2ddl.auto","validate"))) {
                var old=workers.submit(() -> catchThrowable(() -> {
                    var budget=new PayloadBudget(64_000_000);
                    if(activation) RepositorySuccessorExecution.activate(new Tx(emf),budget,CALLER,CALLER,initial,NONE);
                    else RepositorySuccessorInstall.install(new Tx(emf),budget,CALLER,initial,NONE);
                }));
                try {
                    assertThat(held.await(5,TimeUnit.SECONDS)).isTrue();
                    expire(c,initial.previous().key());
                    var p=proposal(c,initial,LEASE);
                    var next=workers.submit(() -> RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE));
                    long deadline=System.nanoTime()+Duration.ofSeconds(5).toNanos(); boolean blocked=false;
                    while(System.nanoTime()<deadline && !next.isDone()) {
                        blocked=c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                                SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
                                 AND wait_event_type='Lock' AND query LIKE '%INSERT INTO repository_coordinator_supersessions%')
                                """).getSingleResult());
                        if(blocked) break; Thread.sleep(10);
                    }
                    assertThat(blocked).isTrue(); release.countDown();
                    assertThat(old.get(10,TimeUnit.SECONDS)).hasStackTraceContaining(activation?
                            "Successor activation requires live exact coordinator binding":"Successor claim expired before install commit");
                    assertThat(next.get(10,TimeUnit.SECONDS)).isNotNull();
                    assertThat(state(c,initial.previous().key())[0]).isEqualTo(3L);
                } finally {release.countDown();}
            }
        }
    }

    @Test void parentFailureRollsBackChildAndClaimAndRejectsForgery() {
        try(var c=context(POSTGRES)) {
            var initial=initial(c,false,false);var p=proposal(c,initial,LEASE);var before=state(c,initial.previous().key());
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION reject_supersession_parent() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN IF NEW.kind='SUPERSEDED_UNACTIVATED' THEN RAISE EXCEPTION 'controlled parent failure'; END IF; RETURN NULL; END; $$
                        """).executeUpdate();
                em.createNativeQuery("CREATE TRIGGER reject_supersession_parent AFTER INSERT ON repository_coordinator_reservations FOR EACH ROW EXECUTE FUNCTION reject_supersession_parent()")
                        .executeUpdate();
            });
            assertThatThrownBy(() -> RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE)).hasStackTraceContaining("controlled parent failure");
            assertThat(state(c,initial.previous().key())).containsExactly(before);
            assertThat(RepositoryCoordinatorReservation.confirm(c.tx(),CALLER,p,NONE)).isEmpty();
            int children=c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_supersessions").getSingleResult()).intValue());
            assertThat(children).isZero();
            c.tx().inTransaction(em -> {em.createNativeQuery("DROP TRIGGER reject_supersession_parent ON repository_coordinator_reservations").executeUpdate();});
            RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,p,NONE);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {em.createNativeQuery("""
                    INSERT INTO repository_coordinator_reservations SELECT
                    (jsonb_populate_record(NULL::repository_coordinator_reservations,to_jsonb(r)||jsonb_build_object('successor_token',gen_random_uuid()))).*
                    FROM repository_coordinator_reservations r WHERE kind='SUPERSEDED_UNACTIVATED'
                    """).executeUpdate();})).hasStackTraceContaining("exact source evidence");
            for(String sql:List.of("DELETE FROM repository_coordinator_supersessions","UPDATE repository_coordinator_supersessions SET phase=phase"))
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {em.createNativeQuery(sql).executeUpdate();})).hasStackTraceContaining("supersession is immutable");
        }
    }
}
