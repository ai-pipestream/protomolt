package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationCoverageReconciliation.Result.*;
import static ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE;
import static org.assertj.core.api.Assertions.*;

/** Real SQL ancestry and migration tests; source fixture provider observations are synthetic. */
@Testcontainers
class DocumentPreparationCoverageLineageIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(10));

    @Test void ordinarySuccessorCertifiesAnEmptyAnchorWithoutGrantingExecution() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
            assertThat(reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE)).isEqualTo(ALREADY_VERIFIED_SUCCESSOR);
            assertThat(count(c, "repository_preparation_coverage_unresolved", plan.next())).isZero();
            assertThat(count(c, "repository_preparation_history_sets", plan.next())).isZero();
            assertThat(count(c, "repository_preparation_coverage_certificates", plan.next())).isZero();
            assertThat(total(c, "repository_successor_executions")).isZero();
            assertThat(total(c, "repository_preparation_pin_batches")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void twoUnactivatedSuccessorsRequireEachImmediateProofAndKeepOriginalRoots() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var first = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            var second = installNext(c, rig, first);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), rig.budget(), TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, second.next().key(), 2, NONE)).isEqualTo(PENDING_SUCCESSOR);
            assertThat(count(c, "repository_preparation_coverage_unresolved", second.next())).isEqualTo(1);
            assertThat(reconciliation.reconcile(CALLER, first.next().key(), 1, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
            assertThat(reconciliation.reconcile(CALLER, second.next().key(), 2, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
            Object[] proof = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT anchor_generation,depth,root_count FROM repository_preparation_coverage_lineage
                    WHERE operation_id=:operation AND predecessor_generation=2
                    """).setParameter("operation", rig.record().key().operationId()).getSingleResult());
            assertThat(((Number) proof[0]).intValue()).isZero();
            assertThat(((Number) proof[1]).intValue()).isEqualTo(2);
            assertThat(((Number) proof[2]).intValue()).isEqualTo(1);
            assertThat(count(c, "repository_preparation_history_roots", rig.record())).isEqualTo(1);
            assertThat(count(c, "repository_preparation_history_sets", first.next())).isZero();
            assertThat(count(c, "repository_preparation_history_sets", second.next())).isZero();
            assertThat(total(c, "repository_successor_executions")).isZero();
            assertThat(total(c, "repository_preparation_pin_batches")).isEqualTo(1);
            for (String mutation : java.util.List.of("DELETE FROM repository_preparation_coverage_lineage",
                    "UPDATE repository_preparation_coverage_lineage SET depth=depth"))
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(mutation).executeUpdate(); }))
                        .hasStackTraceContaining("lineage is immutable");
        }
    }

    @Test void migrationDoesNotGuessPredecessorProof() throws Exception {
        try (var c = context(POSTGRES, "117"); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            DocumentPreparationCoverageCertificatesIT.migrate(c);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), rig.budget(), TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE)).isEqualTo(PENDING_SUCCESSOR);
            assertThat(count(c, "repository_preparation_coverage_unresolved", plan.next())).isEqualTo(1);
            assertThat(reconciliation.reconcile(CALLER, rig.record().key(), 0, NONE)).isEqualTo(VERIFIED_LIVE);
            assertThat(reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
            assertThat(total(c, "repository_preparation_coverage_unresolved")).isZero();
        }
    }

    @Test void forgedAnchorAndEdgeFieldsCannotClearUnresolved() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            for (String changed : java.util.List.of("previous", "owner", "anchor", "roots", "depth")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    String previous = changed.equals("previous") ? "decode(repeat('00',32),'hex')" : "prior.preparation_sha256";
                    String owner = changed.equals("owner") ? "gen_random_uuid()" : "p.owner_nonce";
                    String anchor = changed.equals("anchor") ? "decode(repeat('00',32),'hex')" : "c.preparation_sha256";
                    String roots = changed.equals("roots") ? "decode(repeat('00',32),'hex')" : "c.roots_sha256";
                    String depth = changed.equals("depth") ? "2" : "1";
                    em.createNativeQuery("""
                            INSERT INTO repository_preparation_coverage_lineage(account_id,principal,operation_id,predecessor_generation,
                             preparation_sha256,command_sha256,owner_nonce,previous_preparation_sha256,previous_owner_nonce,
                             anchor_generation,anchor_sha256,root_count,roots_sha256,depth,verifier_version)
                            SELECT p.account_id,p.principal,p.operation_id,p.predecessor_generation,p.preparation_sha256,p.command_sha256,
                            """ + owner + "," + previous + ",prior.owner_nonce,c.predecessor_generation," + anchor + ",c.root_count,"
                            + roots + "," + depth + ",1 " + """
                            FROM repository_publication_preparations p JOIN repository_publication_preparations prior
                             ON prior.account_id=p.account_id AND prior.principal=p.principal AND prior.operation_id=p.operation_id
                              AND prior.predecessor_generation=p.predecessor_generation-1
                            JOIN repository_preparation_coverage_certificates c ON c.account_id=prior.account_id
                             AND c.principal=prior.principal AND c.operation_id=prior.operation_id AND c.predecessor_generation=prior.predecessor_generation
                            WHERE p.operation_id=:operation AND p.predecessor_generation=1
                            """).setParameter("operation", plan.next().key().operationId()).executeUpdate();
                })).as(changed).isInstanceOf(RuntimeException.class);
                assertThat(count(c, "repository_preparation_coverage_unresolved", plan.next())).isEqualTo(1);
                assertThat(count(c, "repository_preparation_coverage_lineage", plan.next())).isZero();
            }
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
        }
    }

    @Test void cancellationAfterLineageInsertRollsBackProofAndUnresolvedDeletion() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            var checks = new java.util.concurrent.atomic.AtomicInteger();
            var abort = new IllegalStateException("injected cancellation after lineage insert");
            var control = new ai.protomolt.proto.repo.spi.RepositoryReadControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                public void check() { if (checks.incrementAndGet() == 7) throw abort; }
            };
            assertThatThrownBy(() -> reconciliation.reconcile(CALLER, plan.next().key(), 1, control)).isSameAs(abort);
            assertThat(count(c, "repository_preparation_coverage_unresolved", plan.next())).isEqualTo(1);
            assertThat(count(c, "repository_preparation_coverage_lineage", plan.next())).isZero();
            assertThat(reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void corruptInstallLinkIsDataLossAndRemainsUnresolved() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            c.tx().inTransaction(em -> {
                // Explicit disk/catalog corruption simulation, outside the immutable API.
                em.createNativeQuery("ALTER TABLE repository_successor_installs DISABLE TRIGGER repository_successor_install_guard").executeUpdate();
                em.createNativeQuery("UPDATE repository_successor_installs SET predecessor_preparation_sha256=decode(repeat('00',32),'hex')").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_successor_installs ENABLE TRIGGER repository_successor_install_guard").executeUpdate();
            });
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            assertThatThrownBy(() -> reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.DATA_LOSS));
            assertThat(count(c, "repository_preparation_coverage_unresolved", plan.next())).isEqualTo(1);
            assertThat(count(c, "repository_preparation_coverage_lineage", plan.next())).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void missingPredecessorProofAndUnresolvedMarkerIsCorruption() throws Exception {
        try (var c = context(POSTGRES, "117"); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            DocumentPreparationCoverageCertificatesIT.migrate(c);
            c.tx().inTransaction(em -> {
                // Catalog corruption, not a supported reconciliation operation.
                em.createNativeQuery("ALTER TABLE repository_preparation_coverage_unresolved DISABLE TRIGGER preparation_coverage_unresolved_guard").executeUpdate();
                em.createNativeQuery("DELETE FROM repository_preparation_coverage_unresolved WHERE predecessor_generation=0").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_preparation_coverage_unresolved ENABLE TRIGGER preparation_coverage_unresolved_guard").executeUpdate();
            });
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), rig.budget(), TIMEOUTS);
            assertThatThrownBy(() -> reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.DATA_LOSS));
            assertThat(count(c, "repository_preparation_coverage_unresolved", plan.next())).isEqualTo(1);
            assertThat(count(c, "repository_preparation_coverage_lineage", plan.next())).isZero();
        }
    }

    @Test void verifiedLineageWithAnUnresolvedMarkerIsCorruption() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE)).isEqualTo(VERIFIED_SUCCESSOR);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO repository_preparation_coverage_unresolved
                        SELECT account_id,principal,operation_id,predecessor_generation FROM repository_preparation_coverage_lineage
                        WHERE operation_id=:operation
                        """).setParameter("operation", plan.next().key().operationId()).executeUpdate();
            });
            assertThatThrownBy(() -> reconciliation.reconcile(CALLER, plan.next().key(), 1, NONE))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.DATA_LOSS));
            assertThat(count(c, "repository_preparation_coverage_unresolved", plan.next())).isEqualTo(1);
        }
    }

    private static RepositorySuccessorInstall.Plan installNext(Context c, Rig rig, RepositorySuccessorInstall.Plan first) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:operation
                """).setParameter("operation", rig.record().key().operationId()).getSingleResult());
        var observation = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE).unactivated().orElseThrow();
        var reservation = new RepositoryCoordinatorReservation.SupersededUnactivated(observation.predecessor(),
                UUID.randomUUID(), UUID.randomUUID(), Duration.ofMinutes(2), observation.owner(),
                observation.preparationSha256(), observation.installation());
        RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, reservation, NONE);
        var next = RepositorySuccessorInstall.prepare(reservation, first.next(), Duration.ofMinutes(2), first.modes());
        RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, next, NONE);
        return next;
    }
    private static long count(Context c, String table, DocumentPublicationPreparationRecord record) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE operation_id=:operation AND predecessor_generation=:generation")
                .setParameter("operation", record.key().operationId()).setParameter("generation", record.predecessorGeneration())
                .getSingleResult()).longValue());
    }
    private static long total(Context c, String table) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table).getSingleResult()).longValue());
    }
}
