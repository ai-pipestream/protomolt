package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.Rig;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.historicalInitial;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.Context;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationCoverageReconciliation.Result.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryCoverageQualificationSupport.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Bounded reconciliation against root release in both commit orders. One operation's
 * real SQL commit is held open; the other is proven blocked on the execution-claim
 * row lock through pg_blocking_pids before the barrier opens or aborts. Source
 * provider observations come from the existing synthetic publication fixture.
 */
@Testcontainers
class RepositoryCoverageQualificationReleaseRaceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final String UNRESOLVED = "repository_preparation_coverage_unresolved";
    private static final String CERTIFICATES = "repository_preparation_coverage_certificates";

    enum Terminal { ABANDONMENT, REJECTION }

    static Stream<Arguments> orderings() {
        return Stream.of("RECONCILE", "RELEASE").flatMap(first -> Stream.of(false, true).flatMap(abort ->
                Stream.of(Terminal.values()).map(terminal -> Arguments.of(first, abort, terminal))));
    }

    @ParameterizedTest(name = "first={0} abortFirst={1} terminal={2}")
    @MethodSource("orderings")
    void reconciliationAndReleaseConvergeInEitherCommitOrder(String first, boolean abortFirst, Terminal terminal) throws Exception {
        boolean reconcileFirst = first.equals("RECONCILE");
        try (var c = context(POSTGRES, "117"); var rig = historicalInitial(c, LEASE)) {
            var record = rig.record();
            var op = record.key().operationId();
            if (terminal == Terminal.REJECTION) cancel(c, rig); else DocumentPreparationRootReleaseIT.abandon(c, rig);
            DocumentPreparationRootReleaseIT.drain(c, rig);
            DocumentPreparationCoverageCertificatesIT.migrate(c);
            // Populated V117 state after migrating to the current schema: indexed as
            // unresolved, no proof, one live root, real terminal and capture-drain evidence.
            assertThat(count(c, UNRESOLVED, op)).isEqualTo(1);
            assertThat(count(c, CERTIFICATES, op)).isZero();
            DocumentPreparationRootReleaseIT.assertLive(c, rig);
            assertThat(count(c, "repository_preparation_capture_drains", op)).isEqualTo(1);
            assertThat(count(c, "repository_preparation_pin_batches", op)).isEqualTo(1);
            var leasesBefore = leases(c, op);

            var gate = new CommitGate(reconcileFirst ? CERTIFICATE_MARKER : RELEASE_MARKER, op, abortFirst);
            var reconcileBudget = new PayloadBudget(64L * 1024 * 1024);
            Object firstResult = null; Throwable firstFailure = null; Object secondResult;
            try (var gated = factory(gate.wrap(c.pool())); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var gatedTx = new Tx(gated);
                Future<Object> leader = workers.submit((Callable<Object>) () -> reconcileFirst
                        ? reconcile(gatedTx, reconcileBudget, record)
                        : DocumentPreparationRootReleases.release(gatedTx, rig.budget(), CALLER, record, NONE));
                try {
                    assertThat(gate.entered.await(10, TimeUnit.SECONDS)).as("first operation reached its commit").isTrue();
                    // Other sessions still observe the complete pre-race state while the commit is held.
                    assertThat(count(c, UNRESOLVED, op)).isEqualTo(1);
                    assertThat(count(c, CERTIFICATES, op)).isZero();
                    DocumentPreparationRootReleaseIT.assertLive(c, rig);
                    Future<Object> follower = workers.submit((Callable<Object>) () -> reconcileFirst
                            ? DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, record, NONE)
                            : reconcile(c.tx(), reconcileBudget, record));
                    assertThat(awaitBlockedOnClaim(c, gate.pid.get(), follower, Duration.ofSeconds(5)))
                            .as("second operation waits on the first operation's execution-claim lock").isTrue();
                    gate.open();
                    try { firstResult = leader.get(20, TimeUnit.SECONDS); }
                    catch (ExecutionException failure) { firstFailure = failure.getCause(); }
                    secondResult = follower.get(20, TimeUnit.SECONDS);
                } finally { gate.open(); }
            }
            assertThat(gate.fired()).isTrue();

            DocumentPreparationRootReleases.Receipt receipt;
            String expectedProof;
            if (abortFirst) {
                assertThat(firstFailure).as("gated first commit aborted").isNotNull()
                        .hasStackTraceContaining("gated commit deliberately aborted");
                if (reconcileFirst) {
                    // Aborted certification left nothing; release committed; a fresh reconciliation
                    // must now verify the exact release receipt rather than live roots.
                    receipt = (DocumentPreparationRootReleases.Receipt) secondResult;
                    assertThat(count(c, CERTIFICATES, op)).isZero();
                    assertThat(count(c, UNRESOLVED, op)).isEqualTo(1);
                    assertThat(reconcile(c.tx(), reconcileBudget, record)).isEqualTo(VERIFIED_RELEASED);
                    expectedProof = "RELEASE_RECEIPT";
                } else {
                    // Aborted release left the roots live; reconciliation certified live roots;
                    // a fresh release then completes against the certified preparation.
                    assertThat(secondResult).isEqualTo(VERIFIED_LIVE);
                    DocumentPreparationRootReleaseIT.assertLive(c, rig);
                    receipt = DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, record, NONE);
                    expectedProof = "LIVE_ROOTS";
                }
            } else {
                assertThat(firstFailure).isNull();
                if (reconcileFirst) {
                    assertThat(firstResult).isEqualTo(VERIFIED_LIVE);
                    receipt = (DocumentPreparationRootReleases.Receipt) secondResult;
                    expectedProof = "LIVE_ROOTS";
                } else {
                    receipt = (DocumentPreparationRootReleases.Receipt) firstResult;
                    assertThat(secondResult).isEqualTo(VERIFIED_RELEASED);
                    expectedProof = "RELEASE_RECEIPT";
                }
            }

            // Converged state: exactly one immutable proof of the expected kind, no unresolved
            // row, no roots, one release, one drained capture, and exact retries on both sides.
            assertThat(reconcile(c.tx(), reconcileBudget, record)).isEqualTo(ALREADY_CERTIFIED);
            assertThat(DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, record, NONE)).isEqualTo(receipt);
            assertThat(count(c, "repository_preparation_history_roots", op)).isZero();
            assertThat(count(c, "repository_preparation_root_releases", op)).isEqualTo(1);
            assertThat(count(c, CERTIFICATES, op)).isEqualTo(1);
            assertThat(count(c, "repository_preparation_coverage_lineage", op)).isZero();
            assertThat(count(c, UNRESOLVED, op)).isZero();
            assertThat(count(c, "repository_preparation_capture_drains", op)).isEqualTo(1);
            assertThat(count(c, "repository_preparation_pin_batches", op)).isEqualTo(1);
            assertThat(count(c, "repository_preparation_history_sets", op)).isEqualTo(1);

            var certificate = certificate(c, op, 0);
            assertThat(certificate[0]).isEqualTo(expectedProof);
            assertThat(certificate[1]).isEqualTo(preparationSha(record)).isEqualTo(receipt.preparationSha256());
            assertThat(certificate[2]).isEqualTo(record.command().sha256());
            assertThat(((Number) certificate[3]).intValue()).isEqualTo(1).isEqualTo(receipt.rootCount());
            assertThat(certificate[4]).isEqualTo(rootsSha(record)).isEqualTo(receipt.rootsSha256());
            assertThat(((Number) certificate[5]).intValue()).isEqualTo(1);

            var release = release(c, op, 0);
            assertThat(release[0]).isEqualTo(certificate[1]);
            assertThat(release[1]).isEqualTo(certificate[2]);
            assertThat(((Number) release[2]).intValue()).isEqualTo(1);
            assertThat(release[3]).isEqualTo(certificate[4]);
            assertThat(((Number) release[4]).intValue()).isEqualTo(1).isEqualTo(receipt.captureCount());
            assertThat(release[5]).as("stored capture fingerprint equals its recomputation and the receipt")
                    .isEqualTo(release[7]).isEqualTo(receipt.capturesSha256());
            assertThat(release[6]).isEqualTo(terminal.name());
            if (terminal == Terminal.ABANDONMENT) assertThat(receipt.terminal())
                    .isEqualTo(new DocumentPreparationTerminalEvidence.Abandonment(rig.claim().token(), record.seeds().ownerNonce()));
            else assertThat(receipt.terminal()).isInstanceOfSatisfying(DocumentPreparationTerminalEvidence.Outcome.class, outcome -> {
                assertThat(outcome.kind()).isEqualTo("REJECTION");
                assertThat(outcome.generation()).isEqualTo(1);
            });
            assertThat(leases(c, op)).containsExactly(leasesBefore);
            assertThat(reconcileBudget.reservedBytes()).isZero();
            assertThat(rig.budget().reservedBytes()).isZero();
        }
    }

    private static DocumentPreparationCoverageReconciliation.Result reconcile(Tx tx, PayloadBudget budget,
            DocumentPublicationPreparationRecord record) {
        return new DocumentPreparationCoverageReconciliation(tx, budget, RACE_TIMEOUTS).reconcile(CALLER, record.key(), 0, NONE);
    }

    /** Real owner admission followed by canonical cancellation: a REJECTION terminal receipt. */
    private static void cancel(Context c, Rig rig) throws Exception {
        var command = rig.record().command();
        try (var work = rig.sources().work()) {
            var admission = RepositoryOperationLedger.prepareHistoricalAdmission(rig.record().key(), command,
                    rig.record().seeds().ownerNonce(), LEASE, work);
            var owner = c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                DocumentPublicationModesJournal.insert(em, rig.claim(), rig.record(), DocumentPublicationModesJournal.encode(command,
                        Map.of(command.intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED)));
                return admission.apply(em, rig.claim()).owner().orElseThrow();
            });
            assertThat(new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, command, NONE).rejection()).isPresent();
        }
    }
}
