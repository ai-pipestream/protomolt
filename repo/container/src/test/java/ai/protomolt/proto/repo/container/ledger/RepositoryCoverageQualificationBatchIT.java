package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationCoverageReconciliation.Result.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryCoverageQualificationSupport.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Partial batch completion, commit-failure retry and the whole-account unresolved
 * observation. Operation ids are fixed so PostgreSQL uuid order is the keyset order.
 * The only injected fault is a real certification commit replaced by a serialization
 * failure; everything else is the supported protocol.
 */
@Testcontainers
class RepositoryCoverageQualificationBatchIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final String UNRESOLVED = "repository_preparation_coverage_unresolved";
    private static final String CERTIFICATES = "repository_preparation_coverage_certificates";
    private static final UUID OP1 = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final UUID OP2 = UUID.fromString("00000000-0000-4000-8000-000000000002");
    private static final UUID OP3 = UUID.fromString("00000000-0000-4000-8000-000000000003");
    private static final UUID OTHER = UUID.fromString("00000000-0000-4000-8000-000000000005");

    @Test void earlierProofCommitsLaterCommitFailurePropagatesAndRetryNeitherDuplicatesNorLoses() {
        try (var c = context(POSTGRES, "117")) {
            var template = DocumentPublicationRegistrationInspectionIT.input(c);
            legacyRootOwner(c, template, "principal", OP1);
            legacyRootOwner(c, template, "principal", OP2);
            legacyRootOwner(c, template, "principal", OP3);
            DocumentPreparationCoverageCertificatesIT.migrate(c);
            assertThat(total(c, UNRESOLVED)).isEqualTo(3);
            assertThat(total(c, CERTIFICATES)).isZero();

            var gate = new CommitGate(CERTIFICATE_MARKER, OP2, true);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try (var gated = factory(gate.wrap(c.pool()))) {
                var batch = new DocumentPreparationCoverageBatch(new Tx(gated), budget, TIMEOUTS);
                gate.open();
                assertThatThrownBy(() -> batch.reconcile(CALLER, "account", Optional.empty(), 3, NONE))
                        .hasStackTraceContaining("gated commit deliberately aborted");
            }
            assertThat(gate.fired()).isTrue();
            // The first proof really committed; the second rolled back at commit; the third never ran.
            assertThat(count(c, CERTIFICATES, OP1)).isEqualTo(1);
            assertThat(count(c, CERTIFICATES, OP2)).isZero();
            assertThat(count(c, CERTIFICATES, OP3)).isZero();
            assertThat(count(c, UNRESOLVED, OP1)).isZero();
            assertThat(count(c, UNRESOLVED, OP2)).isEqualTo(1);
            assertThat(count(c, UNRESOLVED, OP3)).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();

            // Retrying the same cursor on a healthy connection resumes at the first unresolved key.
            var retry = new DocumentPreparationCoverageBatch(c.tx(), budget, TIMEOUTS);
            var page = retry.reconcile(CALLER, "account", Optional.empty(), 3, NONE);
            assertThat(page.entries()).extracting(entry -> entry.cursor().operation()).containsExactly(OP2, OP3);
            assertThat(page.entries()).extracting(DocumentPreparationCoverageBatch.Entry::result).containsExactly(VERIFIED_LIVE, VERIFIED_LIVE);
            assertThat(page.next()).isEmpty();
            assertThat(page.unresolved()).isFalse();
            for (var op : List.of(OP1, OP2, OP3)) {
                assertThat(count(c, CERTIFICATES, op)).as("one certificate for " + op).isEqualTo(1);
                assertThat(certificate(c, op, 0)[0]).isEqualTo("LIVE_ROOTS");
            }
            assertThat(total(c, CERTIFICATES)).isEqualTo(3);
            assertThat(total(c, UNRESOLVED)).isZero();
            var exhausted = retry.reconcile(CALLER, "account", Optional.empty(), 3, NONE);
            assertThat(exhausted.entries()).isEmpty();
            assertThat(exhausted.unresolved()).isFalse();
            assertThat(total(c, CERTIFICATES)).isEqualTo(3);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void wholeAccountObservationSeesOtherPrincipalsAndLowerKeyWorkCommittedAfterTheScan() {
        try (var c = context(POSTGRES, "117")) {
            var template = DocumentPublicationRegistrationInspectionIT.input(c);
            legacyRootOwner(c, template, "principal", OP2);
            legacyRootOwner(c, template, "principal", OP3);
            legacyRootOwner(c, template, "other", OTHER);
            DocumentPreparationCoverageCertificatesIT.migrate(c);
            assertThat(total(c, UNRESOLVED)).isEqualTo(3);

            // A lower-key operation whose root owner certifies atomically under the current
            // writer. Its install-only successor is the unresolved key that lands after the scan.
            var lowerCommand = new DocumentPublicationCommand(template.command().intent().toBuilder().setOperationId(OP1.toString()).build());
            var lowerKey = new RepositoryOperationLedger.Key("account", "principal", lowerCommand.operationId());
            var lower = new DocumentPublicationPreparationRecord(lowerKey, lowerCommand, DocumentPublicationSeeds.mint(lowerKey, lowerCommand),
                    template.placements(), template.lease(), 0);
            var plan = RepositorySuccessorInstallIT.plan(c, lower, Duration.ofMinutes(1));
            assertThat(count(c, UNRESOLVED, OP1)).isZero();
            assertThat(certificate(c, OP1, 0)[0]).isEqualTo("LIVE_ROOTS");

            var installed = new AtomicBoolean();
            var datasource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                // After the last scanned entry's certification commits and before the final
                // observation, a real V93 install adds unresolved key (OP1,1) below the cursor.
                if (installed.get() || count(c, CERTIFICATES, OP3) != 1 || !installed.compareAndSet(false, true)) return;
                RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64L * 1024 * 1024), CALLER, plan, NONE);
            });
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try (var gated = factory(datasource)) {
                var batch = new DocumentPreparationCoverageBatch(new Tx(gated), budget, TIMEOUTS);
                var page = batch.reconcile(CALLER, "account", Optional.empty(), 1, NONE);
                assertThat(page.entries()).extracting(entry -> entry.cursor().operation()).containsExactly(OP2);
                assertThat(page.entries()).extracting(DocumentPreparationCoverageBatch.Entry::result).containsExactly(VERIFIED_LIVE);
                assertThat(page.next()).contains(new DocumentPreparationCoverageBatch.Cursor(OP2, 0));
                assertThat(page.unresolved()).as("OP3 and the other principal remain").isTrue();
                assertThat(installed).isFalse();
                var last = batch.reconcile(CALLER, "account", page.next(), 2, NONE);
                assertThat(last.entries()).extracting(entry -> entry.cursor().operation()).containsExactly(OP3);
                assertThat(last.next()).isEmpty();
                assertThat(installed).as("lower key committed between scan and observation").isTrue();
                assertThat(last.unresolved()).as("whole-account observation sees both").isTrue();
            }
            assertThat(budget.reservedBytes()).isZero();
            List<String> remaining = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT principal||'/'||operation_id||'/'||predecessor_generation FROM repository_preparation_coverage_unresolved
                    ORDER BY principal,operation_id,predecessor_generation
                    """).getResultList());
            assertThat(remaining).containsExactly("other/" + OTHER + "/0", "principal/" + OP1 + "/1");

            // The other principal's scan certifies its root and still observes the lower key.
            var otherPage = new DocumentPreparationCoverageBatch(c.tx(), budget, TIMEOUTS)
                    .reconcile(new RepositoryCaller("other", true), "account", Optional.empty(), 64, NONE);
            assertThat(otherPage.entries()).extracting(entry -> entry.cursor().operation()).containsExactly(OTHER);
            assertThat(otherPage.entries()).extracting(DocumentPreparationCoverageBatch.Entry::result).containsExactly(VERIFIED_LIVE);
            assertThat(otherPage.unresolved()).isTrue();

            // A fresh scan for the original principal starts at the lowest key and resolves it.
            var fresh = new DocumentPreparationCoverageBatch(c.tx(), budget, TIMEOUTS)
                    .reconcile(CALLER, "account", Optional.empty(), 64, NONE);
            assertThat(fresh.entries()).extracting(DocumentPreparationCoverageBatch.Entry::cursor)
                    .containsExactly(new DocumentPreparationCoverageBatch.Cursor(OP1, 1));
            assertThat(fresh.entries()).extracting(DocumentPreparationCoverageBatch.Entry::result).containsExactly(VERIFIED_SUCCESSOR);
            assertThat(fresh.next()).isEmpty();
            assertThat(fresh.unresolved()).isFalse();
            assertThat(total(c, UNRESOLVED)).isZero();
            assertThat(total(c, CERTIFICATES)).isEqualTo(4);
            assertThat(total(c, "repository_preparation_coverage_lineage")).isEqualTo(1);
            assertThat(total(c, "repository_successor_executions")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }
}
