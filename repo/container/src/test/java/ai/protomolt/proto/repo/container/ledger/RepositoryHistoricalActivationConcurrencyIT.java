package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL contention; source publication uses synthetic provider observations. */
@Testcontainers
class RepositoryHistoricalActivationConcurrencyIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void competingFreshCaptureCannotBorrowWinningActivation() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var winnerPid = new java.util.concurrent.atomic.AtomicInteger();
            var datasource = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement();
                     var rows = statement.executeQuery("SELECT count(*),pg_backend_pid() FROM repository_historical_activations")) {
                    rows.next();
                    if (rows.getLong(1) != 1) return;
                    winnerPid.set(rows.getInt(2));
                    entered.countDown();
                    try {
                        if (!release.await(15, TimeUnit.SECONDS)) throw new java.sql.SQLException("activation gate timed out");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); throw new java.sql.SQLException("activation gate interrupted", e);
                    }
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var first = capture(c, rig); var second = capture(c, rig);
                 var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(128L * 1024 * 1024);
                var winner = new RepositoryHistoricalSuccessorActivation(new Tx(emf), budget, plan, rig.record(),
                        first.sources(), new DriveLedger(c.tx()));
                var loser = new RepositoryHistoricalSuccessorActivation(c.tx(), budget, plan, rig.record(),
                        second.sources(), new DriveLedger(c.tx()));
                var firstPins = DocumentPreparationSourcePins.prepare(rig.record().command(),
                        first.sources().references(rig.record().command(), () -> {}), () -> {});
                var secondPins = DocumentPreparationSourcePins.prepare(rig.record().command(),
                        second.sources().references(rig.record().command(), () -> {}), () -> {});
                assertThat(firstPins.digest()).isNotEqualTo(secondPins.digest());
                var before = leases(c, rig);
                var winning = workers.submit(() -> winner.activate(CALLER, CALLER, NONE));
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    var losing = workers.submit(() -> loser.activate(CALLER, CALLER, NONE));
                    // Observe the actual database lock wait, not merely an unfinished Java future.
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    boolean waiting = false;
                    while (!waiting && System.nanoTime() < until) {
                        if (losing.isDone()) losing.get(); // Surface early refusal rather than a misleading barrier timeout.
                        waiting = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                                SELECT count(*) FROM pg_stat_activity
                                WHERE datname=current_database() AND pid<>pg_backend_pid()
                                  AND wait_event_type='Lock' AND state='active'
                                  AND query ILIKE '%INSERT INTO repository_successor_executions%'
                                  AND :blocker=ANY(pg_blocking_pids(pid))
                                """).setParameter("blocker", winnerPid.get()).getSingleResult()).longValue() > 0);
                        if (!waiting) Thread.sleep(10);
                    }
                    assertThat(waiting).as("competing activation waits on the winning SQL transaction").isTrue();
                    release.countDown();
                    var committed = winning.get(10, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> losing.get(10, TimeUnit.SECONDS))
                            .hasStackTraceContaining("duplicate key value");
                    assertThat(loser.tentativeCapture()).isEmpty();
                    assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                    assertThat(count(c, "repository_successor_executions")).isEqualTo(1);
                    assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(2);
                    var evidence = RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER,
                            plan, rig.record(), NONE).orElseThrow();
                    assertThat(evidence.captureSha256()).isEqualTo(committed.identity().pinsSha256());
                    assertThat(winner.activate(CALLER, CALLER, NONE)).isSameAs(committed);
                    assertThat(committed.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                    assertThat(leases(c, rig)).containsExactly(before);
                } finally { release.countDown(); }
            }
        }
    }
}
