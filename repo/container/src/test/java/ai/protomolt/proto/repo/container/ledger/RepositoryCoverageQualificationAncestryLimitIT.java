package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.historicalInitial;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.installedHistoricalSuccessor;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.Context;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationCoverageReconciliation.Result.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryCoverageQualificationSupport.*;
import static org.assertj.core.api.Assertions.*;

/**
 * The 64-link ancestry limit at its boundary. Sixty-five successors are installed through
 * the supported expiry, discovery, supersession and V93 install protocol with one-second
 * leases; no lease or recovery limit is removed. Lineage verification accepts depth 64 and
 * refuses depth 65 without clearing the unresolved row. A separate, clearly labeled
 * adversarial SQL case shows the table constraint and trigger refuse direct rows.
 */
@Testcontainers
class RepositoryCoverageQualificationAncestryLimitIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final String UNRESOLVED = "repository_preparation_coverage_unresolved";
    private static final String LINEAGE = "repository_preparation_coverage_lineage";

    @Test void sixtyFourLinksVerifyAndTheSixtyFifthIsRefusedWithoutClearingUnresolved() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var record = rig.record();
            var key = record.key();
            var op = key.operationId();
            assertThat(certificate(c, op, 0)[0]).isEqualTo("LIVE_ROOTS");

            var plans = chain(c, rig);
            assertThat(count(c, UNRESOLVED, op)).isEqualTo(65);
            var before = effects(c, op);
            var leasesBefore = leases(c, op);

            var budget = new PayloadBudget(64L * 1024 * 1024);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            for (int generation = 1; generation <= 64; generation++) {
                assertThat(reconciliation.reconcile(CALLER, key, generation, NONE)).as("generation " + generation).isEqualTo(VERIFIED_SUCCESSOR);
                var proof = lineage(c, op, generation);
                assertThat(((Number) proof[0]).longValue()).as("anchor generation").isZero();
                assertThat(proof[1]).as("anchor identity").isEqualTo(preparationSha(record));
                assertThat(((Number) proof[2]).intValue()).as("depth").isEqualTo(generation);
                assertThat(proof[5]).isEqualTo(preparationSha(plans.get(generation - 1).next()));
                assertThat(proof[6]).isEqualTo(preparationSha(generation == 1 ? record : plans.get(generation - 2).next()));
            }
            assertThat(count(c, LINEAGE, op)).isEqualTo(64);
            assertThat(count(c, UNRESOLVED, op)).isEqualTo(1);
            assertThat(count(c, UNRESOLVED, op, 65)).isEqualTo(1);

            // The 65th link: refused by the handler, retry refused identically, nothing cleared.
            for (int attempt = 0; attempt < 2; attempt++)
                assertThatThrownBy(() -> reconciliation.reconcile(CALLER, key, 65, NONE)).as("attempt")
                        .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                            assertThat(failure).hasMessageContaining("exceeds 64 links");
                        });
            assertThat(lineage(c, op, 65)).isNull();
            assertThat(count(c, LINEAGE, op)).isEqualTo(64);
            assertThat(count(c, UNRESOLVED, op, 65)).isEqualTo(1);
            assertThat(reconciliation.reconcile(CALLER, key, 64, NONE)).isEqualTo(ALREADY_VERIFIED_SUCCESSOR);

            // A bounded batch reaching the same entry propagates the same refusal and keeps the
            // whole-account unresolved observation true.
            var batch = new DocumentPreparationCoverageBatch(c.tx(), budget, TIMEOUTS);
            assertThatThrownBy(() -> batch.reconcile(CALLER, "account", Optional.empty(), 64, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure ->
                            assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
            assertThat(count(c, UNRESOLVED, op, 65)).isEqualTo(1);
            var beyond = batch.reconcile(CALLER, "account", Optional.of(new DocumentPreparationCoverageBatch.Cursor(op, 65)), 64, NONE);
            assertThat(beyond.entries()).isEmpty();
            assertThat(beyond.unresolved()).isTrue();

            assertThat(effects(c, op)).containsExactly(before);
            assertThat(leases(c, op)).containsExactly(leasesBefore);
            assertThat(budget.reservedBytes()).isZero();
            assertThat(rig.budget().reservedBytes()).isZero();
        }
    }


    /**
     * ADVERSARIAL SQL, clearly separated from the supported protocol above. The same
     * 65-link fixture is built, generations 1 through 64 are verified by the handler, and
     * direct SQL then tries to record generation 65. The table's depth constraint refuses a
     * depth-65 row even though every V119 trigger check passes, and the trigger refuses an
     * understated depth because no depth-63 predecessor exists at generation 64. Neither
     * attempt clears the unresolved row.
     */
    @Test void adversarialSqlCannotRecordLineageBeyondTheLimit() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var key = rig.record().key();
            var op = key.operationId();
            chain(c, rig);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            for (int generation = 1; generation <= 64; generation++)
                assertThat(reconciliation.reconcile(CALLER, key, generation, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
            var before = effects(c, op);
            var leasesBefore = leases(c, op);
            // Depth 65: trigger checks pass, the check constraint refuses.
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO repository_preparation_coverage_lineage(account_id,principal,operation_id,predecessor_generation,
                         preparation_sha256,command_sha256,owner_nonce,previous_preparation_sha256,previous_owner_nonce,
                         anchor_generation,anchor_sha256,root_count,roots_sha256,depth,verifier_version)
                        SELECT p.account_id,p.principal,p.operation_id,p.predecessor_generation,p.preparation_sha256,p.command_sha256,
                         p.owner_nonce,prior.preparation_sha256,prior.owner_nonce,l.anchor_generation,l.anchor_sha256,l.root_count,l.roots_sha256,65,1
                        FROM repository_publication_preparations p
                        JOIN repository_publication_preparations prior ON prior.account_id=p.account_id AND prior.principal=p.principal
                         AND prior.operation_id=p.operation_id AND prior.predecessor_generation=64
                        JOIN repository_preparation_coverage_lineage l ON l.account_id=p.account_id AND l.principal=p.principal
                         AND l.operation_id=p.operation_id AND l.predecessor_generation=64
                        WHERE p.operation_id=:o AND p.predecessor_generation=65
                        """).setParameter("o", op).executeUpdate();
            })).hasStackTraceContaining("violates check constraint").hasStackTraceContaining("depth");
            // Depth 64 at generation 65: the trigger refuses the missing depth-63 predecessor.
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO repository_preparation_coverage_lineage(account_id,principal,operation_id,predecessor_generation,
                         preparation_sha256,command_sha256,owner_nonce,previous_preparation_sha256,previous_owner_nonce,
                         anchor_generation,anchor_sha256,root_count,roots_sha256,depth,verifier_version)
                        SELECT p.account_id,p.principal,p.operation_id,p.predecessor_generation,p.preparation_sha256,p.command_sha256,
                         p.owner_nonce,prior.preparation_sha256,prior.owner_nonce,l.anchor_generation+1,l.anchor_sha256,l.root_count,l.roots_sha256,64,1
                        FROM repository_publication_preparations p
                        JOIN repository_publication_preparations prior ON prior.account_id=p.account_id AND prior.principal=p.principal
                         AND prior.operation_id=p.operation_id AND prior.predecessor_generation=64
                        JOIN repository_preparation_coverage_lineage l ON l.account_id=p.account_id AND l.principal=p.principal
                         AND l.operation_id=p.operation_id AND l.predecessor_generation=64
                        WHERE p.operation_id=:o AND p.predecessor_generation=65
                        """).setParameter("o", op).executeUpdate();
            })).isInstanceOf(RuntimeException.class);
            assertThat(lineage(c, op, 65)).isNull();
            assertThat(count(c, LINEAGE, op)).isEqualTo(64);
            assertThat(count(c, UNRESOLVED, op, 65)).isEqualTo(1);

            assertThat(effects(c, op)).containsExactly(before);
            assertThat(leases(c, op)).containsExactly(leasesBefore);
            assertThat(budget.reservedBytes()).isZero();
            assertThat(rig.budget().reservedBytes()).isZero();
        }
    }

    /** 65 installs through the supported protocol; returns the plans for generations 1 through 65. */
    private static ArrayList<RepositorySuccessorInstall.Plan> chain(Context c, DocumentCaptureAdmissionClosureIT.Rig rig) {
        var plans = new ArrayList<RepositorySuccessorInstall.Plan>();
        var current = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
        plans.add(current);
        for (int generation = 2; generation <= 65; generation++) {
            current = installNextUnactivated(c, rig, current, Duration.ofSeconds(1));
            assertThat(current.next().predecessorGeneration()).isEqualTo(generation);
            plans.add(current);
        }
        assertThat(count(c, "repository_successor_installs", rig.record().key().operationId())).isEqualTo(65);
        return plans;
    }

    private static long[] effects(Context c, UUID op) {
        return new long[] {
                count(c, "repository_preparation_pin_batches", op), count(c, "repository_preparation_capture_drains", op),
                count(c, "repository_preparation_history_sets", op), count(c, "repository_preparation_history_roots", op),
                count(c, "repository_preparation_root_releases", op), count(c, "repository_successor_installs", op),
                total(c, "repository_historical_activations"), total(c, "repository_successor_executions")};
    }
}
