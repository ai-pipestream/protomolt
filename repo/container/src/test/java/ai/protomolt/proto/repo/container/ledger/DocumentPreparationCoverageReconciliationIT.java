package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationCoverageCertificatesIT.*;
import static ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentPreparationCoverageReconciliationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(10));

    @Test void liveLegacyRootOwnerIsCertifiedAndRetryIsIdempotent() {
        try (var c = context(POSTGRES, "117")) {
            var record = ordinary(c);
            migrate(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, record.key(), 0, NONE))
                    .isEqualTo(DocumentPreparationCoverageReconciliation.Result.VERIFIED_LIVE);
            assertThat(reconciliation.reconcile(CALLER, record.key(), 0, NONE))
                    .isEqualTo(DocumentPreparationCoverageReconciliation.Result.ALREADY_CERTIFIED);
            assertThat(count(c, record, "repository_preparation_coverage_certificates")).isEqualTo(1);
            assertThat(count(c, record, "repository_preparation_coverage_unresolved")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void releasedLegacyRootsRequireTheirTerminalAndDrainProof() throws Exception {
        try (var c = context(POSTGRES, "117");
             var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c, Duration.ofMinutes(5))) {
            DocumentPreparationRootReleaseIT.abandon(c, rig);
            DocumentPreparationRootReleaseIT.drain(c, rig);
            DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, rig.record(), NONE);
            migrate(c);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), rig.budget(), TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, rig.record().key(), 0, NONE))
                    .isEqualTo(DocumentPreparationCoverageReconciliation.Result.VERIFIED_RELEASED);
            assertThat(count(c, rig.record(), "repository_preparation_history_roots")).isZero();
            assertThat(count(c, rig.record(), "repository_preparation_coverage_unresolved")).isZero();
            String proof = c.tx().readOnly(em -> (String) em.createNativeQuery(
                    "SELECT proof_kind FROM repository_preparation_coverage_certificates WHERE operation_id=:operation")
                    .setParameter("operation", rig.record().key().operationId()).getSingleResult());
            assertThat(proof).isEqualTo("RELEASE_RECEIPT");
        }
    }

    @Test void missingLegacyHeaderStaysExplicitlyUnknown() {
        try (var c = context(POSTGRES, "102")) {
            var record = DocumentPublicationPreparationCodecIT.input(c);
            LegacyPublicationPreparationFixture.acquire(c.tx(), record, UUID.randomUUID(), UUID.randomUUID());
            migrate(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, record.key(), 0, NONE))
                    .isEqualTo(DocumentPreparationCoverageReconciliation.Result.UNKNOWN_ROOTS);
            assertThat(count(c, record, "repository_preparation_coverage_unresolved")).isEqualTo(1);
            assertThat(count(c, record, "repository_preparation_coverage_certificates")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void callerMismatchAndCancellationCannotClearUnresolved() {
        try (var c = context(POSTGRES, "117")) {
            var record = ordinary(c);
            migrate(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            assertThatThrownBy(() -> reconciliation.reconcile(new RepositoryCaller("other", true), record.key(), 0, NONE))
                    .hasMessageContaining("principal differs");
            var unprivileged = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
            assertThatThrownBy(() -> reconciliation.reconcile(unprivileged, record.key(), 0, NONE))
                    .hasMessageContaining("private process authority");
            var checks = new java.util.concurrent.atomic.AtomicInteger();

            var abort = new IllegalStateException("injected cancellation before certification commit");
            assertThatThrownBy(() -> reconciliation.reconcile(CALLER, record.key(), 0, new ai.protomolt.proto.repo.spi.RepositoryReadControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                public void check() { if (checks.incrementAndGet() == 5) throw abort; }
            })).isSameAs(abort);
            assertThat(count(c, record, "repository_preparation_coverage_unresolved")).isEqualTo(1);
            assertThat(count(c, record, "repository_preparation_coverage_certificates")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void boundedPagesDoNotHideUnresolvedRowsBeforeTheCursor() {
        try (var c = context(POSTGRES, "117")) {
            var first = ordinary(c);
            var command = new ai.protomolt.proto.repo.spi.DocumentPublicationCommand(first.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
            var second = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                    first.placements(), first.lease(), 0);
            c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.acquireInitialInTransaction(em, key, command, UUID.randomUUID(), second.lease());
                LegacyPublicationPreparationFixture.insertProjected(em, second, List.of());
            });

            migrate(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var batch = new DocumentPreparationCoverageBatch(c.tx(), budget, TIMEOUTS);
            var beyondAll = new DocumentPreparationCoverageBatch.Cursor(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 0);
            var empty = batch.reconcile(CALLER, "account", java.util.Optional.of(beyondAll), 1, NONE);
            assertThat(empty.entries()).isEmpty();
            assertThat(empty.next()).isEmpty();
            assertThat(empty.unresolved()).isTrue();
            var page = batch.reconcile(CALLER, "account", java.util.Optional.empty(), 1, NONE);
            assertThat(page.entries()).hasSize(1);
            assertThat(page.unresolved()).isTrue();
            var last = batch.reconcile(CALLER, "account", page.next(), 2, NONE);
            assertThat(last.entries()).hasSize(1);
            assertThat(last.next()).isEmpty();
            assertThat(last.unresolved()).isFalse();
            assertThat(count(c, first, "repository_preparation_coverage_certificates")).isEqualTo(1);
            assertThat(count(c, second, "repository_preparation_coverage_certificates")).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(() -> batch.reconcile(CALLER, "account", java.util.Optional.empty(), 65, NONE))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void corruptStoredBytesNeverBecomeCertified() {
        try (var c = context(POSTGRES, "117")) {
            var record = ordinary(c);
            migrate(c);
            // Simulated storage corruption in this isolated database, outside the
            // supported immutable writer. Re-enable the guard before verification.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE repository_publication_preparations DISABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_publication_preparations DROP CONSTRAINT repository_preparation_digest").executeUpdate();
                em.createNativeQuery("UPDATE repository_publication_preparations SET preparation_bytes=set_byte(preparation_bytes,0,0) WHERE operation_id=:operation")
                        .setParameter("operation", record.key().operationId()).executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_publication_preparations ENABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
            });
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), budget, TIMEOUTS);
            assertThatThrownBy(() -> reconciliation.reconcile(CALLER, record.key(), 0, NONE))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.DATA_LOSS));
            assertThat(count(c, record, "repository_preparation_coverage_unresolved")).isEqualTo(1);
            assertThat(count(c, record, "repository_preparation_coverage_certificates")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void certifiedRootCanReleaseAndReconcileAgainWithoutChangingItsCertificate() throws Exception {
        try (var c = context(POSTGRES); var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c, Duration.ofMinutes(5))) {
            DocumentPreparationRootReleaseIT.abandon(c, rig);
            DocumentPreparationRootReleaseIT.drain(c, rig);
            DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, rig.record(), NONE);
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), rig.budget(), TIMEOUTS);
            assertThat(reconciliation.reconcile(CALLER, rig.record().key(), 0, NONE))
                    .isEqualTo(DocumentPreparationCoverageReconciliation.Result.ALREADY_CERTIFIED);
            assertThat(count(c, rig.record(), "repository_preparation_coverage_certificates")).isEqualTo(1);
            assertThat(count(c, rig.record(), "repository_preparation_history_roots")).isZero();
        }
    }

    @Test void corruptedReleaseReceiptCannotResolveLegacyPreparation() throws Exception {
        try (var c = context(POSTGRES, "117"); var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c, Duration.ofMinutes(5))) {
            DocumentPreparationRootReleaseIT.abandon(c, rig);
            DocumentPreparationRootReleaseIT.drain(c, rig);
            DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, rig.record(), NONE);
            migrate(c);
            c.tx().inTransaction(em -> {
                // Storage corruption outside the immutable release API.
                em.createNativeQuery("ALTER TABLE repository_preparation_root_releases DISABLE TRIGGER repository_preparation_root_release_guard").executeUpdate();
                em.createNativeQuery("UPDATE repository_preparation_root_releases SET roots_sha256=decode(repeat('00',32),'hex') WHERE operation_id=:operation")
                        .setParameter("operation", rig.record().key().operationId()).executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_preparation_root_releases ENABLE TRIGGER repository_preparation_root_release_guard").executeUpdate();
            });
            var reconciliation = new DocumentPreparationCoverageReconciliation(c.tx(), rig.budget(), TIMEOUTS);
            assertThatThrownBy(() -> reconciliation.reconcile(CALLER, rig.record().key(), 0, NONE))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.DATA_LOSS));
            assertThat(count(c, rig.record(), "repository_preparation_coverage_unresolved")).isEqualTo(1);
            assertThat(count(c, rig.record(), "repository_preparation_coverage_certificates")).isZero();
        }
    }

    private static DocumentPublicationPreparationRecord ordinary(Context c) {
        var record = DocumentPublicationPreparationCodecIT.input(c);
        c.tx().inTransaction(em -> {
            RepositoryExecutionClaimLedger.acquireInitialInTransaction(em, record.key(), record.command(), UUID.randomUUID(), record.lease());
            LegacyPublicationPreparationFixture.insertProjected(em, record, List.of());
        });
        return record;
    }
}
