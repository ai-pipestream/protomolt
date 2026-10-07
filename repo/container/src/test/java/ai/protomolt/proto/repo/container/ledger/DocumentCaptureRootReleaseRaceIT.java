package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
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

@Testcontainers
class DocumentCaptureRootReleaseRaceIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(booleans={false,true})
    void captureWaitsForReleaseAndCannotReopenTerminalRetention(boolean abortRelease) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,Duration.ofMinutes(5))) {
            abandon(c,rig); drain(c,rig);
            try (var fresh=RepositoryHistoricalSuccessorActivationIT.capture(c,rig)) {
                var pins=DocumentPreparationSourcePins.prepare(rig.record().command(),fresh.sources().references(rig.record().command(),() -> {}),() -> {});
                for (var pin:pins.pins()) {
                    long stored=c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM repository_preparation_source_pins WHERE pin_id=:pin")
                            .setParameter("pin",pin.pin()).getSingleResult()).longValue());
                    assertThat(stored).as("independent reader is not a persisted capture").isZero();
                }
                var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var blocker=new AtomicInteger();
                var waiter=new AtomicInteger();
                var datasource=DocumentJdbcFaults.beforeCommit(c.pool(),connection -> {
                    try (var statement=connection.createStatement(); var rows=statement.executeQuery("""
                            SELECT count(*),pg_backend_pid() FROM repository_preparation_root_releases WHERE creation_xid=pg_current_xact_id()
                            """)) {
                        rows.next(); if (rows.getInt(1)!=1) return;
                        blocker.set(rows.getInt(2)); entered.countDown();
                        try {
                            if (!release.await(15,TimeUnit.SECONDS)) throw new java.sql.SQLException("capture/release gate timed out");
                            if (abortRelease) throw new java.sql.SQLException("release deliberately aborted","40001");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); throw new java.sql.SQLException("capture/release gate interrupted",e);
                        }
                    }
                });
                try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                        "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                     var workers=Executors.newVirtualThreadPerTaskExecutor()) {
                    var releasing=workers.submit(() -> DocumentPreparationRootReleases.release(new Tx(emf),rig.budget(),CALLER,rig.record(),NONE));
                    try {
                        assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();
                        var capturing=workers.submit(() -> c.tx().inTransaction(em -> {
                            waiter.set(((Number)em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                            RepositoryExecutionClaimLedger.lockLive(em,rig.claim());
                            var nodes=pins.pins().stream().map(DocumentHistoricalSourcePin::node).collect(java.util.stream.Collectors.toSet());
                            var objects=pins.pins().stream().map(DocumentHistoricalSourcePin::object).collect(java.util.stream.Collectors.toSet());
                            var locks=DocumentPublicationLocks.lockIndependentOrigins(em,nodes,objects,Set.of());
                            DocumentPublicationLocks.lockIndependentRetention(em,locks);
                            DocumentPreparationSourcePins.insert(em,rig.record(),pins,rig.claim(),rig.coordinator(),() -> {});
                        }));
                        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5); boolean waiting=false;
                        while (!waiting && System.nanoTime()<deadline) {
                            if (capturing.isDone()) capturing.get();
                            waiting=c.tx().readOnly(em -> ((Number)em.createNativeQuery("""
                                    SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid=:waiter
                                     AND state='active' AND wait_event_type='Lock' AND query ILIKE '%fence_repository_execution_claim%'
                                     AND :blocker=ANY(pg_blocking_pids(pid))
                                    """).setParameter("blocker",blocker.get()).setParameter("waiter",waiter.get()).getSingleResult()).longValue()>0);
                            if (!waiting) Thread.sleep(10);
                        }
                        assertThat(waiting).as("capture waits for the actual release claim lock").isTrue();
                        release.countDown();
                        if (abortRelease) assertThatThrownBy(() -> releasing.get(10,TimeUnit.SECONDS)).hasStackTraceContaining("release deliberately aborted");
                        else assertThat(releasing.get(10,TimeUnit.SECONDS).rootCount()).isEqualTo(1);
                        assertThatThrownBy(() -> capturing.get(10,TimeUnit.SECONDS)).hasStackTraceContaining("Publication capture admission is closed");
                        assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(abortRelease?0:1);
                        assertThat(count(c,rig,"repository_preparation_history_roots")).isEqualTo(abortRelease?1:0);
                        assertThat(count(c,rig,"repository_preparation_pin_batches")).isEqualTo(1);
                        assertThat(count(c,rig,"repository_preparation_pin_owners")).isEqualTo(1);
                        for (var pin:pins.pins()) {
                            long live=c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_read_pins WHERE pin_id=:pin")
                                    .setParameter("pin",pin.pin()).getSingleResult()).longValue());
                            assertThat(live).as("unrelated reader keeps its own live pin").isEqualTo(1);
                        }
                        assertThat(rig.budget().reservedBytes()).isZero();
                    } finally { release.countDown(); }
                }
            }
        }
    }
}
