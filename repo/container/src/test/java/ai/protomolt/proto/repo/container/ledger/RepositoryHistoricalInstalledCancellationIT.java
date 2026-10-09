package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Explicit cancellation is distinct from classifying a recovery failure as terminal. */
@Testcontainers
class RepositoryHistoricalInstalledCancellationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void ordinaryCancellationCannotBypassMissingSuccessorActivation() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var owner = c.tx().inTransaction(em -> {
                var claim = RepositoryExecutionClaimLedger.lockLive(em, rig.record().key(), rig.record().command().sha256(),
                        plan.reservation().predecessor().epoch() + 1, plan.reservation().successorToken());
                return RepositoryOperationLedger.lockLiveOwner(em, rig.record().key(),
                        plan.next().predecessorGeneration() + 1, plan.next().seeds().ownerNonce(), Optional.of(claim));
            });
            var before = leases(c, rig);
            var decisions = new DocumentPublicationRejections(c.tx());
            assertThatThrownBy(() -> decisions.cancel(CALLER, owner, rig.record().command(), NONE))
                    .hasStackTraceContaining("Coordinator successor requires exact activation");
            assertThat(count(c, "repository_operation_rejection")).isZero();
            assertThat(count(c, "repository_successor_executions")).isZero();
            assertThat(count(c, "repository_historical_activations")).isZero();
            assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(1);
            assertThat(count(c, "repository_preparation_capture_drains")).isZero();
            assertThat(leases(c, rig)).containsExactly(before);
            try (var later = capture(c, rig)) {
                var activated = activation(c.tx(), c, rig, plan, later).activate(CALLER, CALLER, NONE);
                var result = decisions.cancel(CALLER, owner, rig.record().command(), NONE);
                assertThat(result.state()).isEqualTo(DocumentPublicationReplay.State.TERMINATED);
                assertThat(decisions.cancel(CALLER, owner, rig.record().command(), NONE)).isEqualTo(result);
                assertThat(activated.complete(CALLER, java.time.Duration.ZERO, NONE)).isPresent();
            }
        }
    }
}
