package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Actual PostgreSQL contention; initial provider observations are synthetic. */
@Testcontainers
class RepositoryHistoricalLimitConcurrencyIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void exhaustedActivationWaitsForDecisionThenRefusesWithoutPartialCapture() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,Duration.ofSeconds(10))) {
            for (int i=0;i<15;i++) append(c,rig);
            var plan=installedHistoricalSuccessor(c,rig,Duration.ofMinutes(5));
            var before=leases(c,rig);
            var entered=new CountDownLatch(1);
            var release=new CountDownLatch(1);
            var firstPid=new AtomicInteger();
            var datasource=DocumentJdbcFaults.beforeCommit(c.pool(),connection -> {
                try (var statement=connection.prepareStatement("""
                        SELECT count(*),pg_backend_pid() FROM repository_recovery_limit_decisions d
                        JOIN repository_operation_rejection r USING(account_id,principal,operation_id)
                        WHERE d.creation_xid=r.creation_xid AND d.operation_id=?
                        """)) {
                    statement.setObject(1,rig.record().key().operationId());
                    try (var rows=statement.executeQuery()) {
                        rows.next();
                        if (rows.getInt(1)!=1) return;
                        firstPid.set(rows.getInt(2));
                        entered.countDown();
                        try {
                            if (!release.await(15,TimeUnit.SECONDS)) throw new java.sql.SQLException("decision gate timed out");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new java.sql.SQLException("decision gate interrupted",e);
                        }
                    }
                }
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var fresh=capture(c,rig);
                 var workers=Executors.newVirtualThreadPerTaskExecutor()) {
                var budget=new PayloadBudget(128L*1024*1024);
                var decision=new RepositoryHistoricalLimitDecisions(new Tx(emf),budget);
                var activation=new RepositoryHistoricalSuccessorActivation(c.tx(),budget,plan,rig.record(),
                        fresh.sources(),new DriveLedger(c.tx()));
                var decided=workers.submit(() -> decision.decide(CALLER,CALLER,plan,rig.record(),NONE).orElseThrow());
                try {
                    assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();
                    var activated=workers.submit(() -> activation.activate(CALLER,CALLER,NONE));
                    long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                    boolean waiting=false;
                    while (!waiting && System.nanoTime()<until) {
                        if (activated.isDone()) activated.get();
                        waiting=c.tx().readOnly(em -> ((Number)em.createNativeQuery("""
                                SELECT count(*) FROM pg_stat_activity
                                WHERE datname=current_database() AND pid<>pg_backend_pid()
                                  AND wait_event_type='Lock' AND state='active'
                                  AND query ILIKE '%INSERT INTO repository_successor_executions%'
                                  AND :blocker=ANY(pg_blocking_pids(pid))
                                """).setParameter("blocker",firstPid.get()).getSingleResult()).longValue()>0);
                        if (!waiting) Thread.sleep(10);
                    }
                    assertThat(waiting).as("activation waits for decision's claim lock").isTrue();
                    release.countDown();
                    var receipt=decided.get(10,TimeUnit.SECONDS);
                    assertThatThrownBy(() -> activated.get(10,TimeUnit.SECONDS))
                            .hasStackTraceContaining("Closed successor cannot activate");
                    assertThat(activation.tentativeCapture()).isEmpty();
                    assertThat(new RepositoryHistoricalLimitDecisions(c.tx(),budget)
                            .decide(CALLER,CALLER,plan,rig.record(),NONE)).contains(receipt);
                    assertThat(count(c,"repository_recovery_limit_decisions")).isEqualTo(1);
                    assertThat(count(c,"repository_operation_rejection")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_executions")).isZero();
                    assertThat(count(c,"repository_historical_activations")).isZero();
                    assertThat(count(c,"repository_preparation_pin_batches")).isEqualTo(16);
                    assertThat(count(c,"repository_preparation_capture_drains")).isZero();
                    assertThat(leases(c,rig)).containsExactly(before);
                    assertThat(budget.reservedBytes()).isZero();
                } finally { release.countDown(); }
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false,true})
    void competingDecisionsConvergeAfterCommitOrRollback(boolean abortFirst) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c,Duration.ofSeconds(10))) {
            for (int i=0;i<15;i++) append(c,rig);
            var plan = installedHistoricalSuccessor(c,rig,Duration.ofMinutes(5));
            var before = leases(c,rig);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var firstPid = new AtomicInteger();
            var datasource = DocumentJdbcFaults.beforeCommit(c.pool(),connection -> {
                try (var statement=connection.createStatement(); var rows=statement.executeQuery("""
                        SELECT count(*),pg_backend_pid() FROM repository_recovery_limit_decisions d
                        JOIN repository_operation_rejection r USING(account_id,principal,operation_id)
                        WHERE d.creation_xid=r.creation_xid
                        """)) {
                    rows.next();
                    if (rows.getInt(1)!=1) return;
                    firstPid.set(rows.getInt(2));
                    entered.countDown();
                    try {
                        if (!release.await(15,TimeUnit.SECONDS)) throw new java.sql.SQLException("decision gate timed out");
                        if (abortFirst) throw new java.sql.SQLException("first decision deliberately aborted","40001");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new java.sql.SQLException("decision gate interrupted",e);
                    }
                }
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var workers=Executors.newVirtualThreadPerTaskExecutor()) {
                var budget = new PayloadBudget(128L*1024*1024);
                var first = new RepositoryHistoricalLimitDecisions(new Tx(emf),budget);
                var second = new RepositoryHistoricalLimitDecisions(c.tx(),budget);
                var firstResult = workers.submit(() -> first.decide(CALLER,CALLER,plan,rig.record(),NONE).orElseThrow());
                try {
                    assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();
                    var secondResult = workers.submit(() -> second.decide(CALLER,CALLER,plan,rig.record(),NONE).orElseThrow());
                    long until = System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                    boolean waiting=false;
                    while (!waiting && System.nanoTime()<until) {
                        if (secondResult.isDone()) secondResult.get();
                        waiting=c.tx().readOnly(em -> ((Number)em.createNativeQuery("""
                                SELECT count(*) FROM pg_stat_activity
                                WHERE datname=current_database() AND pid<>pg_backend_pid()
                                  AND wait_event_type='Lock' AND state='active'
                                  AND query ILIKE '%SELECT claim_epoch FROM repository_execution_claims%FOR UPDATE%'
                                  AND :blocker=ANY(pg_blocking_pids(pid))
                                """).setParameter("blocker",firstPid.get()).getSingleResult()).longValue()>0);
                        if (!waiting) Thread.sleep(10);
                    }
                    assertThat(waiting).as("second decision waits on first transaction's claim lock").isTrue();
                    release.countDown();
                    var receipt=secondResult.get(10,TimeUnit.SECONDS);
                    if (abortFirst) {
                        assertThatThrownBy(() -> firstResult.get(10,TimeUnit.SECONDS))
                                .hasStackTraceContaining("first decision deliberately aborted");
                    } else {
                        assertThat(firstResult.get(10,TimeUnit.SECONDS)).isEqualTo(receipt);
                    }
                    assertThat(second.decide(CALLER,CALLER,plan,rig.record(),NONE)).contains(receipt);
                    assertThat(count(c,"repository_recovery_limit_decisions")).isEqualTo(1);
                    assertThat(count(c,"repository_operation_rejection")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_executions")).isZero();
                    assertThat(count(c,"repository_historical_activations")).isZero();
                    assertThat(count(c,"repository_preparation_pin_batches")).isEqualTo(16);
                    assertThat(count(c,"repository_preparation_capture_drains")).isZero();
                    assertThat(leases(c,rig)).containsExactly(before);
                    assertThat(budget.reservedBytes()).isZero();
                } finally { release.countDown(); }
            }
        }
    }
}
