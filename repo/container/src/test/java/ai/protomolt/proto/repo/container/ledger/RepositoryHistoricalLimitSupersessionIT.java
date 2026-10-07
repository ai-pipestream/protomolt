package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real lease expiry with a successor blocked on the deciding transaction's claim lock. */
@Testcontainers
class RepositoryHistoricalLimitSupersessionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;

    @Test void expiredDecisionRollsBackBeforeSupersessionAndFreshOwnerCanDecide() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,Duration.ofSeconds(10))) {
            for (int i=0;i<15;i++) append(c,rig);
            var plan=installedHistoricalSuccessor(c,rig,Duration.ofSeconds(5));
            var entered=new CountDownLatch(1);
            var release=new CountDownLatch(1);
            var decidingPid=new AtomicInteger();
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
                        decidingPid.set(rows.getInt(2));
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
                 var workers=Executors.newVirtualThreadPerTaskExecutor()) {
                var decision=new RepositoryHistoricalLimitDecisions(new Tx(emf),rig.budget());
                var deciding=workers.submit(() -> decision.decide(CALLER,CALLER,plan,rig.record(),NONE));
                try {
                    assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();
                    c.tx().inTransaction(em -> {
                        em.createNativeQuery("""
                                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                                  (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                                WHERE c.operation_id=:id
                                """).setParameter("id",rig.record().key().operationId()).getSingleResult();
                        boolean expired=(Boolean)em.createNativeQuery("""
                                SELECT clock_timestamp()>GREATEST(c.lease_until,o.lease_until)
                                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                                WHERE c.operation_id=:id
                                """).setParameter("id",rig.record().key().operationId()).getSingleResult();
                        assertThat(expired).as("both actual leases expired").isTrue();
                    });
                    var observed=new RepositoryCoordinatorRecoveryDiscovery(c.tx(),new SqlTimeouts(Duration.ofSeconds(2),Duration.ofSeconds(10)))
                            .inspect(CALLER,rig.record().key(),rig.record().command().sha256(),NONE);
                    assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.INSTALLED_NOT_ACTIVATED);
                    var candidate=observed.unactivated().orElseThrow();
                    var nextLease=Duration.ofMinutes(5);
                    var proposal=new RepositoryCoordinatorReservation.SupersededUnactivated(candidate.predecessor(),
                            UUID.randomUUID(),UUID.randomUUID(),nextLease,candidate.owner(),
                            candidate.preparationSha256(),candidate.installation());
                    var superseding=workers.submit(() -> RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,proposal,NONE));
                    long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                    boolean waiting=false;
                    while (!waiting && System.nanoTime()<until) {
                        if (superseding.isDone()) superseding.get();
                        waiting=c.tx().readOnly(em -> ((Number)em.createNativeQuery("""
                                SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
                                  AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND state='active'
                                  AND query ILIKE '%INSERT INTO repository_coordinator_supersessions%'
                                  AND :blocker=ANY(pg_blocking_pids(pid))
                                """).setParameter("blocker",decidingPid.get()).getSingleResult()).longValue()>0);
                        if (!waiting) Thread.sleep(10);
                    }
                    assertThat(waiting).as("supersession waits on the expired deciding transaction").isTrue();
                    release.countDown();
                    assertThatThrownBy(() -> deciding.get(10,TimeUnit.SECONDS))
                            .hasStackTraceContaining("Recovery limit decision requires exact live installed identity");
                    var stamp=superseding.get(10,TimeUnit.SECONDS);
                    assertThat(RepositoryCoordinatorReservation.confirm(c.tx(),CALLER,proposal,NONE)).contains(stamp);
                    assertThat(count(c,"repository_recovery_limit_decisions")).isZero();
                    assertThat(count(c,"repository_operation_rejection")).isZero();
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    var identity=c.tx().readOnly(em -> (Object[])em.createNativeQuery("""
                            SELECT c.claim_epoch,c.claim_token,o.owner_generation,o.owner_token
                            FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                            WHERE c.operation_id=:id
                            """).setParameter("id",rig.record().key().operationId()).getSingleResult());
                    assertThat(((Number)identity[0]).longValue()).isEqualTo(proposal.predecessor().epoch()+1);
                    assertThat(identity[1]).isEqualTo(proposal.successorToken());
                    assertThat(((Number)identity[2]).longValue()).isEqualTo(plan.next().predecessorGeneration()+1);
                    assertThat(identity[3]).isEqualTo(plan.next().seeds().ownerNonce());
                    assertThatThrownBy(() -> new RepositoryHistoricalLimitDecisions(c.tx(),rig.budget())
                            .decide(CALLER,CALLER,plan,rig.record(),NONE)).hasMessageContaining("exact live unactivated ownership");
                    var next=RepositorySuccessorInstall.prepare(proposal,plan.next(),nextLease,plan.modes());
                    RepositorySuccessorInstall.install(c.tx(),rig.budget(),CALLER,next,NONE);
                    var leases=leases(c,rig);
                    var clean=new RepositoryHistoricalLimitDecisions(c.tx(),rig.budget());
                    var receipt=clean.decide(CALLER,CALLER,next,rig.record(),NONE).orElseThrow();
                    assertThat(receipt.getOwnerGeneration()).isEqualTo(next.next().predecessorGeneration()+1);
                    assertThat(clean.decide(CALLER,CALLER,next,rig.record(),NONE)).contains(receipt);
                    assertThat(count(c,"repository_recovery_limit_decisions")).isEqualTo(1);
                    assertThat(count(c,"repository_operation_rejection")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_installs")).isEqualTo(2);
                    assertThat(count(c,"repository_successor_executions")).isZero();
                    assertThat(count(c,"repository_historical_activations")).isZero();
                    assertThat(count(c,"repository_preparation_pin_batches")).isEqualTo(16);
                    assertThat(count(c,"repository_preparation_capture_drains")).isZero();
                    assertThat(leases(c,rig)).containsExactly(leases);
                    assertThat(rig.budget().reservedBytes()).isZero();
                } finally { release.countDown(); }
            }
        }
    }
}
