package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
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

/** Real SQL generation limits; historical source fixtures use synthetic provider observations. */
@Testcontainers
class RepositoryHistoricalGenerationLimitsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));

    @Test void byteExhaustionRestoresOldRouteWithoutAnyReservationWrite() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(2));
            try (var sources = capture(c, rig)) {
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 2);
            try {
                UUID id;
                try (var old = owner.beginInstalled(CALLER, plan, rig.record())) {
                    id = old.identity(); old.attachSources(sources.sources(), sources.sources().work(), NONE);
                    old.openExecution(CALLER, NONE);
                }
                var observed = expired(c, rig);
                long reservations = count(c, "repository_coordinator_expirations");
                long installs = count(c, "repository_successor_installs");
                long retained = budget.reservedBytes();
                // Real shared-budget pressure, without changing the two-entry capacity.
                try (var pressure = budget.reserve(budget.capacity() - retained)) {
                    assertThatThrownBy(() -> owner.beginSuccessor(CALLER, CALLER, id, plan.next().command(),
                            plan.modes(), observed, Duration.ofMinutes(2), TIMEOUTS))
                            .isInstanceOf(PayloadBudget.CapacityExceededException.class);
                    assertThat(budget.reservedBytes()).isEqualTo(budget.capacity());
                    assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                    assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(reservations);
                    assertThat(count(c, "repository_successor_installs")).isEqualTo(installs);
                    assertThat(sources.history().isReleased()).isFalse();
                    try (var old = owner.resume(CALLER, plan.next().command()).orElseThrow()) {
                        assertThat(old.identity()).isEqualTo(id);
                    }
                }
                assertThat(budget.reservedBytes()).isEqualTo(retained);
                // Releasing pressure permits the same request; failed allocation left no hidden entry.
                try (var next = owner.beginSuccessor(CALLER, CALLER, id, plan.next().command(), plan.modes(),
                        observed, Duration.ofMinutes(2), TIMEOUTS)) {
                    assertThat(next.identity()).isNotEqualTo(id);
                    assertThat(next.advancePreparation(CALLER, plan.modes(), Map.of(), NONE))
                            .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                }
                assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(reservations + 1);
            } finally {
                owner.close();
                assertThat(owner.detachClosed(Duration.ofSeconds(1), ignored -> CALLER, NONE)).isTrue();
                assertThat(budget.reservedBytes()).isZero();
            }
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void unactivatedReplacementKeepsStableEntryWhileOlderWorkerDrains(boolean installed) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(2));
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 2);
            try (var sources = capture(c, rig)) {
                DocumentHistoricalAssessmentSources.Work worker = null;
                try {
                    UUID oldId;
                    try (var old = owner.beginInstalled(CALLER, plan, rig.record())) {
                        oldId = old.identity();
                        var accepted = sources.sources().work();
                        old.attachSources(sources.sources(), accepted, NONE); old.openExecution(CALLER, NONE);
                        worker = accepted.fork();
                    }
                    var observed = expired(c, rig);
                    UUID nextId;
                    Identity initial;
                    long initialInstalls;
                    try (var next = owner.beginSuccessor(CALLER, CALLER, oldId, plan.next().command(), plan.modes(),
                            observed, Duration.ofSeconds(3), TIMEOUTS)) {
                        nextId = next.identity();
                        assertThat(next.advancePreparation(CALLER, plan.modes(), Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                        if (installed) assertThat(next.advancePreparation(CALLER, plan.modes(), Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                        initial = identity(c, rig);
                        initialInstalls = count(c, "repository_successor_installs");
                    }
                    waitExpired(c, rig);
                    try (var next = owner.resume(CALLER, plan.next().command()).orElseThrow()) {
                        assertThat(next.identity()).isEqualTo(nextId);
                        assertThat(next.reconcileUnactivated(CALLER, plan.modes(), Map.of(), NONE)).isTrue();
                        assertThat(next.identity()).isEqualTo(nextId);
                        assertThat(currentToken(c, rig)).isNotEqualTo(initial.token());
                        assertThat(next.advancePreparation(CALLER, plan.modes(), Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                        var replacement = next.installedPlan(CALLER, NONE);
                        var actual = identity(c, rig);
                        assertThat(replacement.reservation().predecessor().epoch()).isEqualTo(initial.epoch());
                        assertThat(replacement.reservation().predecessor().token()).isEqualTo(initial.token());
                        assertThat(actual.epoch()).isEqualTo(initial.epoch() + 1);
                        assertThat(actual.token()).isEqualTo(replacement.reservation().successorToken());
                        assertThat(replacement.next().predecessorGeneration()).isEqualTo(initial.ownerGeneration());
                        assertThat(actual.ownerGeneration()).isEqualTo(initial.ownerGeneration() + 1);
                        assertThat(actual.ownerToken()).isEqualTo(replacement.next().seeds().ownerNonce());
                        assertThat(actual.ownerToken()).isNotEqualTo(initial.ownerToken());
                        assertThat(count(c, "repository_successor_installs")).isEqualTo(initialInstalls + 1);

                    }
                    assertThat(count(c, "repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                    assertThat(count(c, "repository_preparation_capture_drains")).isZero();
                    assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 2));
                    try (var old = owner.resumeGeneration(CALLER, CALLER, plan.next().command(), oldId).orElseThrow()) {
                        assertThat(old.retireFenced(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETAINED);
                    }
                    assertThat(sources.history().isReleased()).isFalse();
                    worker.close(); worker = null;
                    try (var old = owner.resumeGeneration(CALLER, CALLER, plan.next().command(), oldId).orElseThrow()) {
                        assertThat(old.retireFenced(CALLER, Duration.ofSeconds(1), NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                    }
                    assertThat(sources.history().isReleased()).isTrue();
                    try (var next = owner.resume(CALLER, plan.next().command()).orElseThrow()) {
                        assertThat(next.identity()).isEqualTo(nextId);
                    }
                } finally {
                    if (worker != null) worker.close();
                    owner.close();
                    assertThat(owner.detachClosed(Duration.ofSeconds(1), ignored -> CALLER, NONE)).isTrue();
                    assertThat(budget.reservedBytes()).isZero();
                }
            }
        }
    }

    private record Identity(long epoch, UUID token, long ownerGeneration, UUID ownerToken) {}
    private static Identity identity(Context c, Rig rig) {
        return c.tx().readOnly(em -> {
            var row = (Object[]) em.createNativeQuery("""
                    SELECT c.claim_epoch,c.claim_token,o.owner_generation,o.owner_token
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id", rig.record().command().operationId()).getSingleResult();
            return new Identity(((Number) row[0]).longValue(), (UUID) row[1], ((Number) row[2]).longValue(), (UUID) row[3]);
        });
    }
    private static UUID currentToken(Context c, Rig rig) {
        return c.tx().readOnly(em -> (UUID) em.createNativeQuery(
                "SELECT claim_token FROM repository_execution_claims WHERE operation_id=:id")
                .setParameter("id", rig.record().command().operationId()).getSingleResult());
    }
    private static void waitExpired(Context c, Rig rig) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
    }
    private static RepositoryCoordinatorRecoveryDiscovery.Observation expired(Context c, Rig rig) {
        waitExpired(c, rig);
        var observation = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
        assertThat(observation.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
        return observation;
    }
}
