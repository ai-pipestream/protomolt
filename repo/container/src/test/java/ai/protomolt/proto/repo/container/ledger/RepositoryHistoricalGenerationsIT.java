package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
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

/** Real PostgreSQL ownership transitions; source setup uses synthetic provider observations. */
@Testcontainers
class RepositoryHistoricalGenerationsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void successorActivatesWhileOldCallAndWorkerRemainOwned(boolean shutdown) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(3));
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 2);
            try (var oldSources = capture(c, rig);
                 var old = owner.beginInstalled(CALLER, second, rig.record())) {
                var work = oldSources.sources().work();
                old.attachSources(oldSources.sources(), work, NONE); old.openExecution(CALLER, NONE);
                var worker = work.fork();
                RepositoryHistoricalSuccessorActivationIT.Captured newSources = null;
                try {
                    var observed = expired(c, rig);
                    java.util.UUID nextId;
                    try (var next = owner.beginSuccessor(CALLER, CALLER, old.identity(), second.next().command(),
                            second.modes(), observed, Duration.ofMinutes(2), TIMEOUTS)) {
                        nextId = next.identity();
                        assertThat(nextId).isNotEqualTo(old.identity());
                        assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(2, 2));
                        assertThatThrownBy(() -> old.start(Duration.ofMinutes(1), NONE)).hasMessageContaining("disposal only");
                        assertThat(next.advancePreparation(CALLER, second.modes(), Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                        assertThat(next.advancePreparation(CALLER, second.modes(), Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                        newSources = capture(c, rig);
                        next.attachSources(newSources.sources(), newSources.sources().work(), NONE);
                        next.openExecution(CALLER, NONE);
                        assertThat(next.start(Duration.ofMinutes(1), NONE)).isNotNull();
                    }
                    assertThat(oldSources.history().isReleased()).isFalse();
                    assertThat(count(c, "repository_historical_activations")).isEqualTo(2);
                    assertThat(count(c, "repository_preparation_capture_drains")).isZero();
                    if (shutdown) {
                        old.close(); owner.close();
                        assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isFalse();
                        // A held old generation does not stop ready generations from being disposed.
                        assertThat(newSources.history().isReleased()).isTrue();
                        assertThat(oldSources.history().isReleased()).isFalse();
                        assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                    } else {
                        assertThat(old.retireFenced(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETAINED);
                        try (var resumed = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                            assertThat(resumed.identity()).isEqualTo(nextId);
                            assertThat(resumed.start(Duration.ofMinutes(1), NONE)).isNotNull();
                        }
                    }
                    worker.close();
                    if (!shutdown) {
                        assertThat(old.retireFenced(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                        try (var resumed = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                            assertThat(resumed.identity()).isEqualTo(nextId);
                        }
                        owner.close();
                    }
                    assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                    assertThat(oldSources.history().isReleased()).isTrue();
                    assertThat(budget.reservedBytes()).isZero();
                } finally { worker.close(); if (newSources != null) newSources.close(); }
            }
        }
    }

    @Test void capacityRefusalLeavesOldRouteAndBudgetIntactWithoutReservation() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(2));
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var sources = capture(c, rig)) {
                java.util.UUID id;
                try (var old = owner.beginInstalled(CALLER, second, rig.record())) {
                    id = old.identity(); old.attachSources(sources.sources(), sources.sources().work(), NONE);
                    old.openExecution(CALLER, NONE);
                }
                var observed = expired(c, rig); long before = budget.reservedBytes();
                var reservations = count(c, "repository_coordinator_expirations");
                assertThatThrownBy(() -> owner.beginSuccessor(CALLER, CALLER, id, second.next().command(),
                        second.modes(), observed, Duration.ofMinutes(2), TIMEOUTS))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(reservations);
                assertThat(budget.reservedBytes()).isEqualTo(before);
                try (var old = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                    assertThat(old.identity()).isEqualTo(id);
                }
                owner.close(); assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void takeoverReplyFailurePreservesOneSuccessorAndRejectsChangedCommand(boolean committed) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(2));
            long previousReservations = count(c, "repository_coordinator_expirations");
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
            var data = committed ? DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c, "repository_coordinator_expirations") == previousReservations + 1 && armed.compareAndSet(true, false)) {
                    cancelled.set(true);
                    throw new java.sql.SQLException("generation takeover reply lost", "08006");
                }
            }) : DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT count(*) FROM repository_coordinator_expirations")) {
                    rows.next();
                    if (rows.getLong(1) == previousReservations + 1 && armed.compareAndSet(true, false)) {
                        cancelled.set(true);
                        throw new java.sql.SQLException("generation takeover rolled back", "08006");
                    }
                }
            });
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", data, "hibernate.hbm2ddl.auto", "validate"));
                 var oldSources = capture(c, rig)) {
                var budget = new PayloadBudget(256L * 1024 * 1024);
                var owner = new RepositoryInstalledHistoricalAttempts(new Tx(emf), budget, new DriveLedger(c.tx()), 2);
                java.util.UUID oldId;
                try (var old = owner.beginInstalled(CALLER, second, rig.record())) {
                    oldId = old.identity(); old.attachSources(oldSources.sources(), oldSources.sources().work(), NONE);
                    old.openExecution(CALLER, NONE);
                }
                var observed = expired(c, rig); java.util.UUID successorId;
                try (var next = owner.beginSuccessor(CALLER, CALLER, oldId, second.next().command(), second.modes(),
                        observed, Duration.ofMinutes(2), TIMEOUTS)) {
                    successorId = next.identity();
                    assertThatThrownBy(() -> next.advancePreparation(CALLER, second.modes(), Map.of(), control))
                            .hasStackTraceContaining(committed ? "generation takeover reply lost" : "generation takeover rolled back");
                    assertThat(armed).isFalse();
                }
                assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(previousReservations + (committed ? 1 : 0));
                var exact = claim(c, rig);
                long retained = budget.reservedBytes();
                var changed = new DocumentPublicationCommand(second.next().command().intent().toBuilder()
                        .setMembers(0, second.next().command().intent().getMembers(0).toBuilder().setClusterId("changed-retry")).build());
                assertThat(changed.canonical()).isNotEqualTo(second.next().command().canonical());
                assertThatThrownBy(() -> owner.beginSuccessor(CALLER, CALLER, oldId, changed, second.modes(),
                        observed, Duration.ofMinutes(2), TIMEOUTS)).hasMessageContaining("retry identity changed");
                assertThat(budget.reservedBytes()).isEqualTo(retained);
                cancelled.set(false);
                var fresh = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                        .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
                try (var next = owner.beginSuccessor(CALLER, CALLER, oldId, second.next().command(), second.modes(),
                        fresh, Duration.ofMinutes(2), TIMEOUTS)) {
                    assertThat(next.identity()).isEqualTo(successorId);
                    assertThat(next.advancePreparation(CALLER, second.modes(), Map.of(), NONE))
                            .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                    if (committed) assertThat(claim(c, rig)).containsExactly(exact);
                    assertThat(next.advancePreparation(CALLER, second.modes(), Map.of(), NONE))
                            .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                }
                assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(previousReservations + 1);
                try (var old = owner.resumeGeneration(CALLER, CALLER, rig.record().command(), oldId).orElseThrow()) {
                    assertThatThrownBy(() -> old.start(Duration.ofMinutes(1), NONE)).hasMessageContaining("disposal only");
                    assertThat(old.retireFenced(CALLER, Duration.ZERO, NONE))
                            .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                }
                try (var next = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                    assertThat(next.identity()).isEqualTo(successorId);
                }
                owner.close(); assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    private static Object[] claim(Context c, Rig rig) {
        return (Object[]) c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT claim_epoch,claim_token FROM repository_execution_claims WHERE operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
    }

    private static RepositoryCoordinatorRecoveryDiscovery.Observation expired(Context c, Rig rig) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
        var result = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
        assertThat(result.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
        return result;
    }
}
