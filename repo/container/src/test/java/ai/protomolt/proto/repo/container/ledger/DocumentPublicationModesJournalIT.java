package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentPublicationModesJournalIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final Map<String, DocumentPublicationCandidate.Mode> MODES = Map.of(
            "member-0", DocumentPublicationCandidate.Mode.TYPED, "member-1", DocumentPublicationCandidate.Mode.OPAQUE);

    @Test void exactRetrySurvivesLostAcknowledgmentAndOwnerAdmission() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, LEASE);
            var journal = new DocumentPublicationModesJournal(c.tx(), budget);
            // Observe the committed row from another transaction before injecting lost acknowledgment.
            var lost = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return false; }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
                @Override public void check() {
                    long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM repository_publication_modes").getSingleResult()).longValue());
                    if (count == 1) throw new RepositoryException(RepositoryException.Code.CANCELLED, "Lost acknowledgment");
                }
            };
            assertThatThrownBy(() -> journal.bind(CALLER, claim, 0, MODES, lost)).isInstanceOf(RepositoryException.class);
            assertThat(budget.reservedBytes()).isZero();
            new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim);
            new DocumentPublicationModesJournal(new Tx(c.emf()), budget).bind(CALLER, claim, 0, MODES, NONE);
            assertThat(new DocumentPublicationModesJournal(new Tx(c.emf()), budget).load(CALLER, claim, 0, NONE))
                    .contains(MODES);
            assertThat(budget.reservedBytes()).isZero();
            assertThat(c.tx().readOnly(em -> em.createNativeQuery("SELECT modes::text FROM repository_publication_modes").getSingleResult()).toString())
                    .contains("TYPED", "OPAQUE");
        }
    }

    @Test void changedModesAndDirectUpdateOrDeleteAreRejected() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, LEASE);
            var journal = new DocumentPublicationModesJournal(c.tx(), budget); journal.bind(CALLER, claim, 0, MODES, NONE);
            assertThatThrownBy(() -> journal.bind(CALLER, claim, 0, Map.of("member-0", DocumentPublicationCandidate.Mode.OPAQUE,
                    "member-1", DocumentPublicationCandidate.Mode.OPAQUE), NONE)).hasStackTraceContaining("modes changed");
            for (String sql : new String[]{"UPDATE repository_publication_modes SET modes=modes", "DELETE FROM repository_publication_modes"})
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("modes are immutable");
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void missingExtraMembersAndAccountOnlyCallerAreRejected() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, LEASE);
            var journal = new DocumentPublicationModesJournal(c.tx(), budget);
            assertThatThrownBy(() -> journal.bind(CALLER, claim, 0, Map.of("member-0", DocumentPublicationCandidate.Mode.TYPED), NONE))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> journal.bind(CALLER, claim, 0, Map.of("member-0", DocumentPublicationCandidate.Mode.TYPED,
                    "member-1", DocumentPublicationCandidate.Mode.TYPED, "extra", DocumentPublicationCandidate.Mode.TYPED), NONE))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> journal.bind(new RepositoryCaller("principal", false, Set.of(value.key().account()), Set.of()),
                    claim, 0, MODES, NONE)).isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void journaledOwnerAdmissionRequiresFixedModes() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, LEASE);
            assertThatThrownBy(() -> new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim))
                    .hasStackTraceContaining("Journaled owner requires fixed modes");
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            assertThat(new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim).owner()).isPresent();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void journaledOwnerAdmissionRequiresSavedNonce() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, LEASE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            assertThatThrownBy(() -> new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), UUID.randomUUID(), LEASE, claim))
                    .hasStackTraceContaining("Journaled owner differs from preparation");
        }
    }

    @Test void staleClaimCannotBindAndNewClaimCanRetryOriginalChoices() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var old = save(c, value, budget, Duration.ofSeconds(1));
            var journal = new DocumentPublicationModesJournal(c.tx(), budget); journal.bind(CALLER, old, 0, MODES, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var next = new RepositoryExecutionClaimLedger(c.tx()).takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> journal.bind(CALLER, old, 0, MODES, NONE)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            journal.bind(CALLER, next, 0, MODES, NONE);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static PayloadBudget budget() { return new PayloadBudget(32L * 1024 * 1024); }

    @Test void scopedCallerCanCheckObservedModesWithoutReadingPrivateChoices() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, LEASE);
            var journal = new DocumentPublicationModesJournal(c.tx(), budget); journal.bind(CALLER, claim, 0, MODES, NONE);
            var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim).owner().orElseThrow();
            var scoped = new RepositoryCaller("principal", false, Set.of(value.key().account()), Set.of());
            journal.requireObservedModes(scoped, owner, value.command(), MODES, NONE);
            assertThatThrownBy(() -> journal.load(scoped, claim, 0, NONE)).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            assertThatThrownBy(() -> journal.requireObservedModes(scoped, owner, value.command(),
                    Map.of("member-0", DocumentPublicationCandidate.Mode.TYPED, "member-1", DocumentPublicationCandidate.Mode.TYPED), NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                        assertThat(failure.getMessage()).isEqualTo("Observed publication modes differ from fixed modes");
                    });
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void expiredJournaledClaimStillAllowsRecoveryFenceButNotWriteAuthority() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, Duration.ofSeconds(1));
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), Duration.ofSeconds(1), claim)
                    .owner().orElseThrow();
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT fence_repository_operation_recovery(:a,:p,:o)")
                        .setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                        .setParameter("o", value.key().operationId()).getSingleResult();
                assertThat(em.createNativeQuery("SELECT require_repository_operation_recovery_fence(:a,:p,:o)")
                        .setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                        .setParameter("o", value.key().operationId()).getSingleResult()).isEqualTo(true); return null;
            });
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, owner); }))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
        }
    }

    @Test void loadRefusesSyntacticallyValidWrongMembersFromDirectSql() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, LEASE);
            c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                insertModes(em, value, "{\"wrong-member\":\"TYPED\"}"); return null;
            });
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(c.tx(), budget).load(CALLER, claim, 0, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void opposingFirstWritersSerializeOnClaimAndLoserRefusesChangedModes() throws Exception {
        try (var c = context(POSTGRES); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var value = input(c); var budget = budget(); var claim = save(c, value, budget, LEASE);
            var inserted = new CountDownLatch(1); var release = new CountDownLatch(1);
            var first = workers.submit(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                insertModes(em, value, "{\"member-0\":\"TYPED\",\"member-1\":\"OPAQUE\"}");
                inserted.countDown();
                try { if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Release timed out"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                return null;
            }));
            try {
                assertThat(inserted.await(5, TimeUnit.SECONDS)).isTrue();
                var second = workers.submit(() -> new DocumentPublicationModesJournal(new Tx(c.emf()), budget).bind(CALLER, claim, 0,
                        Map.of("member-0", DocumentPublicationCandidate.Mode.OPAQUE, "member-1", DocumentPublicationCandidate.Mode.OPAQUE), NONE));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                boolean waiting = false;
                while (!waiting && System.nanoTime() < until) {
                    waiting = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                            SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
                              AND wait_event_type='Lock' AND query LIKE '%fence_repository_execution_claim%'
                            """).getSingleResult()).intValue()>0);
                    if (!waiting) Thread.sleep(10);
                }
                assertThat(waiting).as("second writer waits on the real claim lock").isTrue();
                release.countDown(); first.get(5, TimeUnit.SECONDS);
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).hasStackTraceContaining("modes changed");
                assertThat(new DocumentPublicationModesJournal(c.tx(), budget).load(CALLER, claim, 0, NONE)).contains(MODES);
            } finally { release.countDown(); }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static void insertModes(jakarta.persistence.EntityManager em, DocumentPublicationPreparationRecord value, String modes) {
        em.createNativeQuery("""
                INSERT INTO repository_publication_modes(account_id,principal,operation_id,predecessor_generation,owner_nonce,modes)
                VALUES (:a,:p,:o,0,:owner,CAST(:modes AS jsonb))
                """).setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                .setParameter("o", value.key().operationId()).setParameter("owner", value.seeds().ownerNonce())
                .setParameter("modes", modes).executeUpdate();
    }
    private static RepositoryExecutionClaimLedger.Claim save(Context c, DocumentPublicationPreparationRecord value, PayloadBudget budget, Duration lease) {
        var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), UUID.randomUUID(), lease);
        new DocumentPublicationPreparationJournal(c.tx(), budget).save(CALLER, claim, value, NONE); return claim;
    }
    private static DocumentPublicationPreparationRecord input(Context c) {
        var source = prepare(c, 2, true);
        var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
        UUID id = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var placement = DocumentUploadPlan.Placement.sample(new DriveLedger(c.tx()).findById(id).orElseThrow(), "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
        return new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), Map.of(id, placement), LEASE, 0);
    }
}
