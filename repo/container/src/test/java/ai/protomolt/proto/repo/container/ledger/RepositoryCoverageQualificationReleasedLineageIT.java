package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.historicalInitial;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.installedHistoricalSuccessor;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.Context;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationCoverageReconciliation.Result.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryCoverageQualificationSupport.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.activation;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.capture;
import static org.assertj.core.api.Assertions.*;

/**
 * Successor lineage anchored to a historical root owner whose roots were genuinely
 * released: real successor activation with a fresh capture, canonical cancellation,
 * reader drain and the qualified root release. No receipt is fabricated and no
 * guard is disabled. Source provider observations are the fixture's synthetic ones.
 */
@Testcontainers
class RepositoryCoverageQualificationReleasedLineageIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final String UNRESOLVED = "repository_preparation_coverage_unresolved";
    private static final String CERTIFICATES = "repository_preparation_coverage_certificates";
    private static final String LINEAGE = "repository_preparation_coverage_lineage";

    /** legacyAnchor=true starts the root owner under V117 and migrates; false uses the current writer. */
    @ParameterizedTest(name = "legacyAnchor={0}") @ValueSource(booleans = {true, false})
    void successorLineageAnchorsToGenuinelyReleasedHistoricalRoot(boolean legacyAnchor) throws Exception {
        try (var c = legacyAnchor ? context(POSTGRES, "117") : context(POSTGRES); var rig = historicalInitial(c)) {
            var record = rig.record();
            var key = record.key();
            var op = key.operationId();
            var command = record.command();
            var plan = installedHistoricalSuccessor(c, rig, Duration.ofMinutes(1));

            // Real activation of the installed successor with its own capture, then canonical
            // cancellation through the live owner: the operation reaches a REJECTION terminal.
            try (var later = capture(c, rig)) {
                var activated = activation(c.tx(), c, rig, plan, later).activate(CALLER, CALLER, NONE);
                var owner = c.tx().inTransaction(em -> {
                    var claim = RepositoryExecutionClaimLedger.lockLive(em, key, command.sha256(),
                            plan.reservation().predecessor().epoch() + 1, plan.reservation().successorToken());
                    return RepositoryOperationLedger.lockLiveOwner(em, key, plan.next().predecessorGeneration() + 1,
                            plan.next().seeds().ownerNonce(), Optional.of(claim));
                });
                var rejection = new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, command, NONE).rejection().orElseThrow();
                assertThat(rejection.getOwnerGeneration()).isEqualTo(2);
                later.sources().close();
                assertThat(activated.complete(CALLER, Duration.ofSeconds(1), NONE)).isPresent();
            }
            // Drain the initial capture through the real reader lifecycle and recover its drain.
            rig.sources().close(); rig.history().close();
            assertThat(rig.history().awaitDrained(Duration.ofSeconds(1))).isTrue();
            rig.history().release(); rig.reads().fence(); rig.reads().attestLocalQuiescence();
            var pins = c.tx().readOnly(em -> (byte[]) em.createNativeQuery(
                    "SELECT pins_sha256 FROM repository_preparation_pin_batches WHERE operation_id=:o AND initial_capture")
                    .setParameter("o", op).getSingleResult());
            var initial = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(key,
                    command.sha256(), rig.claim().epoch(), rig.claim().token(), rig.coordinator()), 0, HexFormat.of().formatHex(pins));
            assertThat(DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, initial, NONE).kind()).isEqualTo("QUIESCED");
            assertThat(count(c, "repository_preparation_capture_drains", op)).isEqualTo(2);

            var receipt = DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, record, NONE);
            assertThat(receipt.captureCount()).isEqualTo(2);
            assertThat(receipt.terminal()).isInstanceOfSatisfying(DocumentPreparationTerminalEvidence.Outcome.class, outcome -> {
                assertThat(outcome.kind()).isEqualTo("REJECTION");
                assertThat(outcome.generation()).isEqualTo(2);
            });
            assertThat(count(c, "repository_preparation_history_roots", op)).isZero();
            assertThat(count(c, "repository_preparation_root_releases", op)).isEqualTo(1);

            if (legacyAnchor) {
                DocumentPreparationCoverageCertificatesIT.migrate(c);
                assertThat(count(c, UNRESOLVED, op)).as("root owner and successor are both unresolved").isEqualTo(2);
                assertThat(count(c, CERTIFICATES, op)).isZero();
            } else {
                assertThat(count(c, UNRESOLVED, op)).as("only the install-only successor is unresolved").isEqualTo(1);
                assertThat(certificate(c, op, 0)[0]).isEqualTo("LIVE_ROOTS");
            }
            var before = effects(c, op);
            var leasesBefore = leases(c, op);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), rig.budget(), TIMEOUTS);

            if (legacyAnchor) {
                // The successor stays pending until its released anchor is verified exactly.
                assertThat(reconciliation.reconcile(CALLER, key, 1, NONE)).isEqualTo(PENDING_SUCCESSOR);
                assertThat(count(c, UNRESOLVED, op, 1)).isEqualTo(1);
                assertThat(count(c, LINEAGE, op)).isZero();
                assertThat(reconciliation.reconcile(CALLER, key, 0, NONE)).isEqualTo(VERIFIED_RELEASED);
            }
            var anchor = certificate(c, op, 0);
            assertThat(anchor[0]).isEqualTo(legacyAnchor ? "RELEASE_RECEIPT" : "LIVE_ROOTS");
            assertThat(anchor[1]).isEqualTo(preparationSha(record)).isEqualTo(receipt.preparationSha256());
            assertThat(anchor[2]).isEqualTo(command.sha256());
            assertThat(((Number) anchor[3]).intValue()).isEqualTo(1);
            assertThat(anchor[4]).isEqualTo(rootsSha(record)).isEqualTo(receipt.rootsSha256());
            var release = release(c, op, 0);
            assertThat(release[0]).isEqualTo(anchor[1]);
            assertThat(release[3]).isEqualTo(anchor[4]);
            assertThat(release[5]).isEqualTo(release[7]).isEqualTo(receipt.capturesSha256());

            assertThat(reconciliation.reconcile(CALLER, key, 1, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
            var proof = lineage(c, op, 1);
            assertThat(((Number) proof[0]).longValue()).as("anchor generation").isZero();
            assertThat(proof[1]).as("anchor is the original released root owner").isEqualTo(anchor[1]);
            assertThat(((Number) proof[2]).intValue()).as("depth").isEqualTo(1);
            assertThat(((Number) proof[3]).intValue()).as("inherited root count").isEqualTo(1);
            assertThat(proof[4]).as("inherited root fingerprint").isEqualTo(anchor[4]);
            assertThat(proof[5]).isEqualTo(preparationSha(plan.next()));
            assertThat(proof[6]).isEqualTo(preparationSha(record));
            assertThat(proof[7]).isEqualTo(command.sha256());

            // Exact retries return the recorded state and change nothing.
            assertThat(reconciliation.reconcile(CALLER, key, 0, NONE)).isEqualTo(ALREADY_CERTIFIED);
            assertThat(reconciliation.reconcile(CALLER, key, 1, NONE)).isEqualTo(ALREADY_VERIFIED_SUCCESSOR);
            assertThat(DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, record, NONE)).isEqualTo(receipt);

            // No invented captures, drains, headers, activations or executions.
            assertThat(effects(c, op)).containsExactly(before);
            assertThat(count(c, "repository_preparation_history_sets", op, 1)).isZero();
            assertThat(count(c, CERTIFICATES, op)).isEqualTo(1);
            assertThat(count(c, LINEAGE, op)).isEqualTo(1);
            assertThat(count(c, UNRESOLVED, op)).isZero();
            assertThat(count(c, "repository_preparation_history_roots", op)).isZero();
            assertThat(leases(c, op)).containsExactly(leasesBefore);
            assertThat(rig.budget().reservedBytes()).isZero();
        }
    }

    private static long[] effects(Context c, UUID op) {
        return new long[] {
                count(c, "repository_preparation_pin_batches", op), count(c, "repository_preparation_capture_drains", op),
                count(c, "repository_preparation_history_sets", op), count(c, "repository_preparation_root_releases", op),
                count(c, "repository_successor_installs", op), count(c, "repository_operation_rejection", op),
                total(c, "repository_historical_activations"), total(c, "repository_successor_executions")};
    }
}
