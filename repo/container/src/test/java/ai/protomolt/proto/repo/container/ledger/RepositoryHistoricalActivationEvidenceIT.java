package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Cold immutable readback using real SQL. No process-local source capability is restored. */
@Testcontainers
class RepositoryHistoricalActivationEvidenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void thirdEpochUsesOriginalRetentionWithoutAdoptingEitherOlderCapture() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            var original = DocumentPreparationSourcePins.prepare(rig.record().command(),
                    rig.sources().references(rig.record().command(), () -> {}), () -> {});
            try (var secondSources = capture(c, rig)) {
                var secondCapture = activation(c.tx(), c, rig, second, secondSources).activate(CALLER, CALLER, NONE);
                c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
                var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(secondCapture.identity().owner(),
                        UUID.randomUUID(), UUID.randomUUID(), Duration.ofMinutes(5),
                        new RepositoryCoordinatorReservation.OwnerIdentity(2, second.next().seeds().ownerNonce()));
                RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, reservation, NONE);
                var third = RepositorySuccessorInstall.prepare(reservation, second.next(), Duration.ofMinutes(5), second.modes());
                RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, third, NONE);
                try (var thirdSources = capture(c, rig)) {
                    var thirdCapture = activation(c.tx(), c, rig, third, thirdSources).activate(CALLER, CALLER, NONE);
                    var evidence = RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER, third, rig.record(), NONE).orElseThrow();
                    assertThat(evidence.execution().epoch()).isEqualTo(3);
                    assertThat(evidence.execution().incarnation()).isNotEqualTo(secondCapture.identity().owner().incarnation());
                    assertThat(evidence.predecessorGeneration()).isEqualTo(2);
                    assertThat(evidence.retentionGeneration()).isZero();
                    assertThat(count(c, "repository_historical_activations")).isEqualTo(2);
                    assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(3);
                    var before = leases(c, rig);
                    var coverage = DocumentPreparationCaptureCoverage.prepare(rig.record(), NONE);
                    assertThat(thirdCapture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                    assertThatThrownBy(() -> c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); }))
                            .hasMessageContaining("has not drained");
                    assertThat(secondCapture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                    assertThatThrownBy(() -> c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); }))
                            .hasMessageContaining("has not drained");
                    rig.sources().close(); rig.history().close(); rig.history().release();
                    rig.reads().fence(); rig.reads().attestLocalQuiescence();
                    var originalIdentity = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(
                            rig.record().key(), rig.record().command().sha256(), rig.claim().epoch(), rig.claim().token(), rig.coordinator()),
                            0, HexFormat.of().formatHex(original.digest()));
                    assertThat(DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, originalIdentity, NONE).kind()).isEqualTo("QUIESCED");
                    int qualified = c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); });
                    assertThat(qualified).isEqualTo(3);
                    assertThat(leases(c, rig)).containsExactly(before);
                }
            }
        }
    }

    @Test void separateConnectionConfirmsBeforeAndAfterCaptureDrainWithoutChangingLeases() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            assertThat(RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER, plan, rig.record(), NONE)).isEmpty();
            try (var later = capture(c, rig);
                 var coldEmf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                         "hibernate.connection.datasource", c.pool(), "hibernate.hbm2ddl.auto", "validate"))) {
                var capture = activation(c.tx(), c, rig, plan, later).activate(CALLER, CALLER, NONE);
                var before = leases(c, rig);
                var coldTx = new Tx(coldEmf);
                var evidence = RepositoryHistoricalActivationEvidence.confirm(coldTx, rig.budget(), CALLER, plan, rig.record(), NONE).orElseThrow();
                assertThat(evidence.execution()).isEqualTo(capture.identity().owner());
                assertThat(evidence.captureSha256()).isEqualTo(capture.identity().pinsSha256());
                assertThat(evidence.retentionGeneration()).isZero();
                assertThat(evidence.predecessorGeneration()).isEqualTo(1);
                assertThat(evidence.activationTransaction()).matches("[0-9]+");
                assertThat(capture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                assertThat(RepositoryHistoricalActivationEvidence.confirm(coldTx, rig.budget(), CALLER, plan, rig.record(), NONE)).contains(evidence);
                assertThat(leases(c, rig)).containsExactly(before);
                assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(2);
                // The historical session path is still unavailable: a read receipt cannot bypass it.
                assertThatThrownBy(() -> RepositorySuccessorExecution.attach(coldTx, rig.budget(), CALLER, plan, NONE))
                        .isInstanceOf(UnsupportedOperationException.class);
            }
        }
    }

    @Test void wrongRetentionAndUnprivilegedConfirmationCannotBorrowAnActivation() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            try (var later = capture(c, rig)) {
                var capture = activation(c.tx(), c, rig, plan, later).activate(CALLER, CALLER, NONE);
                var wrong = new DocumentPublicationPreparationRecord(rig.record().key(), rig.record().command(), rig.record().seeds(),
                        rig.record().placements(), Duration.ofSeconds(2), rig.record().predecessorGeneration());
                assertThatThrownBy(() -> RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER, plan, wrong, NONE))
                        .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
                assertThatThrownBy(() -> RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), scoped, plan, rig.record(), NONE))
                        .hasMessageContaining("private process authority");
                assertThatThrownBy(() -> RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(),
                        new RepositoryCaller("another-principal", true), plan, rig.record(), NONE))
                        .hasMessageContaining("principal differs");
                assertThat(capture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
            }
        }
    }

    @Test void bareOrdinaryActivationIsNotHistoricalEvidence() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            assertThatThrownBy(() -> RepositoryHistoricalActivationEvidence.confirm(c.tx(), budget, CALLER, plan, plan.previous(), NONE))
                    .hasMessageContaining("retention binding is unavailable");
            assertThat(count(c, "repository_historical_activations")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }
}
