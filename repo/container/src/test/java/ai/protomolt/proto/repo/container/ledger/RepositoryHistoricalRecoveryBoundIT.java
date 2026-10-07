package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Actual bounded recovery chain; no invented install edges or disabled SQL guards. */
@Testcontainers
class RepositoryHistoricalRecoveryBoundIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final Duration LEASE = Duration.ofSeconds(2);

    @Test void sixtyFourInstalledEdgesActivateButSixtyFiveRollBack() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var shortLease = Duration.ofSeconds(1);
            var plan = installedHistoricalSuccessor(c, rig, shortLease);
            var timeouts = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(10));
            for (int edge = 2; edge <= 64; edge++) {
                expire(c, rig);
                var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), timeouts)
                        .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE)
                        .unactivated().orElseThrow();
                assertThat(observed.installation()).isPresent();
                var reservation = new RepositoryCoordinatorReservation.SupersededUnactivated(observed.predecessor(),
                        UUID.randomUUID(), UUID.randomUUID(), shortLease, observed.owner(),
                        observed.preparationSha256(), observed.installation());
                RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, reservation, NONE);
                plan = RepositorySuccessorInstall.prepare(reservation, plan.next(), shortLease, plan.modes());
                RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, plan, NONE);
            }
            assertThat(count(c, "repository_successor_installs")).isEqualTo(64);
            assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(1);
            try (var sources = capture(c, rig)) {
                var accepted = activation(c.tx(), c, rig, plan, sources).activate(CALLER, CALLER, NONE);
                var evidence = RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER,
                        plan, rig.record(), NONE).orElseThrow();
                assertThat(evidence.predecessorGeneration()).isEqualTo(64);
                assertThat(evidence.retentionGeneration()).isZero();
                assertThat(accepted.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                expire(c, rig);
                var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(accepted.identity().owner(),
                        UUID.randomUUID(), UUID.randomUUID(), LEASE,
                        new RepositoryCoordinatorReservation.OwnerIdentity(65, plan.next().seeds().ownerNonce()));
                RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, reservation, NONE);
                var next = RepositorySuccessorInstall.prepare(reservation, plan.next(), LEASE, plan.modes());
                RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, next, NONE);
                try (var refusedSources = capture(c, rig)) {
                    var attempt = activation(c.tx(), c, rig, next, refusedSources);
                    var before = leases(c, rig);
                    assertThatThrownBy(() -> attempt.activate(CALLER, CALLER, NONE))
                            .hasStackTraceContaining("Historical activation ancestry exceeds 64 links");
                    assertThat(RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER,
                            next, rig.record(), NONE)).isEmpty();
                    assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                    assertThat(count(c, "repository_successor_executions")).isEqualTo(1);
                    assertThat(count(c, "repository_coordinator_bindings")).isEqualTo(2);
                    assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(2);
                    assertThat(count(c, "repository_preparation_capture_drains")).isEqualTo(1);
                    assertThat(leases(c, rig)).containsExactly(before);
                    assertThat(rig.budget().reservedBytes()).isZero();
                }
            }
        }
    }

    private static void expire(DocumentNativePublicationFixture.Context c, Rig rig) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
    }

    @Test void seventeenthCaptureRefusesWithoutPartialActivationEvenAfterEarlierCapturesDrain() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig, LEASE);
            for (int epoch = 2; epoch <= 17; epoch++) {
                try (var sources = capture(c, rig)) {
                    var attempt = activation(c.tx(), c, rig, plan, sources);
                    if (epoch == 17) {
                        var before = leases(c, rig);
                        assertThatThrownBy(() -> attempt.activate(CALLER, CALLER, NONE))
                                .hasStackTraceContaining("Preparation source capture batch limit exceeded");
                        assertThat(attempt.tentativeCapture()).isEmpty();
                        assertThat(RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER,
                                plan, rig.record(), NONE)).isEmpty();
                        assertThat(count(c, "repository_historical_activations")).isEqualTo(15);
                        assertThat(count(c, "repository_successor_executions")).isEqualTo(15);
                        assertThat(count(c, "repository_coordinator_bindings")).isEqualTo(16);
                        assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(16);
                        assertThat(count(c, "repository_preparation_capture_drains")).isEqualTo(15);
                        assertThat(leases(c, rig)).containsExactly(before);
                        assertThat(rig.budget().reservedBytes()).isZero();
                        break;
                    }
                    var capture = attempt.activate(CALLER, CALLER, NONE);
                    assertThat(capture.identity().owner().epoch()).isEqualTo(epoch);
                    var evidence = RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER,
                            plan, rig.record(), NONE).orElseThrow();
                    assertThat(evidence.predecessorGeneration()).isEqualTo(epoch - 1);
                    assertThat(evidence.retentionGeneration()).isZero();
                    assertThat(capture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                    // Real expiry of both owner and claim; no direct mutation of lease rows.
                    c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(2.05)").getSingleResult());
                    var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(capture.identity().owner(),
                            UUID.randomUUID(), UUID.randomUUID(), LEASE,
                            new RepositoryCoordinatorReservation.OwnerIdentity(epoch, plan.next().seeds().ownerNonce()));
                    RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, reservation, NONE);
                    plan = RepositorySuccessorInstall.prepare(reservation, plan.next(), LEASE, plan.modes());
                    RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, plan, NONE);
                }
            }
        }
    }
}
