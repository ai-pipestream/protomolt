package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.capture;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.count;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL lifecycle checks. The source fixture uses synthetic provider observations. */
@Testcontainers
class RepositoryInstalledHistoricalAttemptsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void cancelledWorkerDrainRetainsEntryAndCanResumeCleanup() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
            var checked = new java.util.concurrent.CountDownLatch(1);
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { checked.countDown(); return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var later = capture(c, rig); var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                DocumentHistoricalAssessmentSources.Work worker;
                try (var attempt = owner.beginInstalled(CALLER, plan, rig.record())) {
                    var root = later.sources().work();
                    attempt.attachSources(later.sources(), root, NONE);
                    worker = root.fork();
                }
                owner.close();
                try (worker) {
                    var draining = executor.submit(() -> owner.detachClosed(Duration.ofSeconds(30), ignored -> CALLER, control));
                    assertThat(checked.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    cancelled.set(true);
                    assertThatThrownBy(() -> draining.get(2, java.util.concurrent.TimeUnit.SECONDS))
                            .hasCauseInstanceOf(RepositoryException.class).hasStackTraceContaining("Document read cancelled");
                    assertThat(owner.drain().unresolved()).isEqualTo(1);
                    assertThat(later.history().isReleased()).isFalse();
                    assertThat(budget.reservedBytes()).isPositive();
                }
                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void uncertainActivationRetainsOneOwnerAndDisposesExactDurableOutcome(boolean committed) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            var datasource = committed ? DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c, "repository_historical_activations") == 1 && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("owner activation reply lost", "08006");
            }) : DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement();
                     var rows = statement.executeQuery("SELECT count(*) FROM repository_historical_activations")) {
                    rows.next();
                    if (rows.getLong(1) == 1 && armed.compareAndSet(true, false))
                        throw new java.sql.SQLException("owner activation rolled back", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", java.util.Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var later = capture(c, rig)) {
                var owner = new RepositoryInstalledHistoricalAttempts(new Tx(emf), budget, new DriveLedger(c.tx()), 1);
                try (var attempt = owner.beginInstalled(CALLER, plan, rig.record())) {
                    attempt.attachSources(later.sources(), later.sources().work(), NONE);
                    assertThatThrownBy(() -> attempt.openExecution(CALLER, NONE))
                            .hasStackTraceContaining(committed ? "reply lost" : "rolled back");
                }
                assertThat(armed).isFalse();
                assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                assertThat(later.history().isReleased()).isFalse();
                if (committed) {
                    try (var resumed = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                        resumed.openExecution(CALLER, NONE);
                        resumed.start(Duration.ofMinutes(5), NONE);
                    }
                    assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(2);
                }
                owner.close();
                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                assertThat(count(c, "repository_preparation_capture_drains")).isEqualTo(committed ? 1 : 0);
                assertThat(later.history().isReleased()).isTrue();
                assertThat(rig.history().isReleased()).isFalse();
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @Test void capacityAndIdentityRefuseBeforeActivationAndAttemptCloseOnlyReleasesExclusion() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var attempt = owner.beginInstalled(CALLER, plan, rig.record())) {
                assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(1, 1));
                assertThatThrownBy(() -> owner.resume(CALLER, rig.record().command())).hasMessageContaining("in use");
                assertThat(count(c, "repository_historical_activations")).isZero();
            }
            long retained = budget.reservedBytes();
            assertThat(retained).isPositive();
            var changed = new RepositoryCaller("principal", false, Set.of(plan.next().key().account()), Set.of());
            assertThatThrownBy(() -> owner.resume(changed, rig.record().command())).hasMessageContaining("identity changed");
            assertThat(budget.reservedBytes()).isEqualTo(retained);
            try (var pressure = budget.reserve(budget.capacity() - budget.reservedBytes());
                 var exactRetry = owner.beginInstalled(CALLER, plan, rig.record())) {
                assertThat(owner.drain().active()).isEqualTo(1);
            }
            try (var resumed = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(1, 1));
            }
            owner.close();
            assertThatThrownBy(() -> owner.beginInstalled(CALLER, plan, rig.record())).hasMessageContaining("admission is closed");
            assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
            assertThat(budget.reservedBytes()).isZero();
            assertThat(count(c, "repository_historical_activations")).isZero();
        }
    }

    @Test void byteCapacityRefusalCreatesNoLocalEntryOrActivation() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var budget = new PayloadBudget(1);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            assertThatThrownBy(() -> owner.beginInstalled(CALLER, plan, rig.record()))
                    .isInstanceOf(PayloadBudget.CapacityExceededException.class);
            assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
            assertThat(budget.reservedBytes()).isZero();
            assertThat(count(c, "repository_historical_activations")).isZero();
            owner.close();
            assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
        }
    }

    @Test void failedTransferLeavesSourcesWithCallerAndNoCaptureDisposalWaitsActualWorker() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var later = capture(c, rig); var other = capture(c, rig);
                 var wrong = other.sources().work()) {
                DocumentHistoricalAssessmentSources.Work worker;
                try (var attempt = owner.beginInstalled(CALLER, plan, rig.record())) {
                    assertThatThrownBy(() -> attempt.attachSources(later.sources(), wrong, NONE))
                            .hasMessageContaining("Source Work owner differs");
                    wrong.authorize(NONE); // Failed transfer did not take or close foreign Work.
                    var root = later.sources().work();
                    attempt.attachSources(later.sources(), root, NONE);
                    worker = root.fork();
                    owner.close();
                    assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isFalse();
                    assertThat(later.history().isReleased()).isFalse();
                }
                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isFalse();
                assertThat(budget.reservedBytes()).isPositive();
                assertThat(later.history().isReleased()).isFalse();
                worker.close();
                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                assertThat(later.history().isReleased()).isTrue();
                assertThat(other.history().isReleased()).isFalse();
                assertThat(rig.history().isReleased()).isFalse();
                assertThat(budget.reservedBytes()).isZero();
                assertThat(count(c, "repository_preparation_capture_drains")).isZero();
            }
        }
    }

    @Test void executionSurvivesClientCallsWithoutHoldingRuntimeBarrierAndDrainsRegisteredCapture() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            var runtime = new DocumentPublicationScopeCalls();
            try (var later = capture(c, rig)) {
                DocumentAssessmentStartJournal.Started start;
                try (var call = runtime.enter(); var attempt = owner.beginInstalled(CALLER, plan, rig.record())) {
                    attempt.attachSources(later.sources(), later.sources().work(), NONE);
                    attempt.openExecution(CALLER, NONE);
                    start = attempt.start(Duration.ofMinutes(5), NONE);
                }
                assertThat(runtime.isIdle()).isTrue();
                assertThat(later.history().isReleased()).isFalse();
                later.sources().close(); // Continuation must use accepted Work, never reacquire admission.
                try (var call = runtime.enter(); var attempt = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                    attempt.openExecution(CALLER, NONE);
                    assertThat(attempt.start(Duration.ofMinutes(5), NONE)).isEqualTo(start);
                }
                assertThat(runtime.isIdle()).isTrue();
                assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                owner.close(); runtime.close();
                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                assertThat(later.history().isReleased()).isTrue();
                assertThat(count(c, "repository_preparation_capture_drains")).isEqualTo(1);
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }
}
