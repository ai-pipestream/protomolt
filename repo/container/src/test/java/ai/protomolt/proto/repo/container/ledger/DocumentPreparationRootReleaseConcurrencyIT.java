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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationRootReleaseIT.*;
import static org.assertj.core.api.Assertions.*;

/** Actual SQL lock waits and commit outcomes; provider observations are fixture supplied. */
@Testcontainers
class DocumentPreparationRootReleaseConcurrencyIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(booleans={false,true})
    void concurrentReleaseConvergesAfterCommitOrRollback(boolean abortFirst) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,Duration.ofMinutes(5))) {
            abandon(c,rig); drain(c,rig);
            var entered=new CountDownLatch(1);
            var release=new CountDownLatch(1);
            var blocker=new AtomicInteger();
            var before=c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o",rig.record().command().operationId()).getSingleResult());
            var datasource=DocumentJdbcFaults.beforeCommit(c.pool(),connection -> {
                try (var statement=connection.prepareStatement("""
                        SELECT count(*),pg_backend_pid(),(SELECT count(*) FROM repository_preparation_history_roots WHERE operation_id=?)
                        FROM repository_preparation_root_releases WHERE operation_id=? AND creation_xid=pg_current_xact_id()
                        """)) {
                    statement.setObject(1,rig.record().command().operationId());
                    statement.setObject(2,rig.record().command().operationId());
                    try (var rows=statement.executeQuery()) {
                        rows.next();
                        if (rows.getInt(1)!=1) return;
                        assertThat(rows.getInt(3)).as("first transaction tentatively removed every root").isZero();
                        blocker.set(rows.getInt(2)); entered.countDown();
                        try {
                            if (!release.await(15,TimeUnit.SECONDS)) throw new java.sql.SQLException("release gate timed out");
                            if (abortFirst) throw new java.sql.SQLException("first release deliberately aborted","40001");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt(); throw new java.sql.SQLException("release gate interrupted",interrupted);
                        }
                    }
                }
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var workers=Executors.newVirtualThreadPerTaskExecutor()) {
                var budget=new PayloadBudget(128L*1024*1024);
                var first=workers.submit(() -> DocumentPreparationRootReleases.release(new Tx(emf),budget,CALLER,rig.record(),NONE));
                try {
                    assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();
                    // Other transactions still see the complete old state until commit.
                    assertLive(c,rig);
                    var second=workers.submit(() -> DocumentPreparationRootReleases.release(c.tx(),budget,CALLER,rig.record(),NONE));
                    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                    boolean waiting=false;
                    while (!waiting && System.nanoTime()<deadline) {
                        if (second.isDone()) second.get();
                        waiting=c.tx().readOnly(em -> ((Number)em.createNativeQuery("""
                                SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
                                 AND state='active' AND wait_event_type='Lock'
                                 AND query ILIKE '%repository_execution_claims%FOR UPDATE%'
                                 AND :blocker=ANY(pg_blocking_pids(pid))
                                """).setParameter("blocker",blocker.get()).getSingleResult()).longValue()>0);
                        if (!waiting) Thread.sleep(10);
                    }
                    assertThat(waiting).as("second release waits on the first release claim lock").isTrue();
                    release.countDown();
                    var receipt=second.get(10,TimeUnit.SECONDS);
                    if (abortFirst) assertThatThrownBy(() -> first.get(10,TimeUnit.SECONDS))
                            .hasStackTraceContaining("first release deliberately aborted");
                    else assertThat(first.get(10,TimeUnit.SECONDS)).isEqualTo(receipt);
                    assertThat(DocumentPreparationRootReleases.release(c.tx(),budget,CALLER,rig.record(),NONE)).isEqualTo(receipt);
                    assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(1);
                    assertThat(count(c,rig,"repository_preparation_history_roots")).isZero();
                    assertThat(count(c,rig,"repository_preparation_capture_drains")).isEqualTo(1);
                    var after=c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                            .setParameter("o",rig.record().command().operationId()).getSingleResult());
                    assertThat(after).isEqualTo(before);
                    assertThat(budget.reservedBytes()).isZero();
                } finally { release.countDown(); }
            }
        }
    }
}
