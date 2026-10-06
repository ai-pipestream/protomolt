package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Shared SQL journal evidence, not a forced-process-crash or provider recovery qualification. */
@Testcontainers
class DocumentPublicationPreparationJournalIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2, 3, 4, 5})
    void coordinatorBindingCommitsWithClaimAndPreparation(int checkpoint) {
        try (var c = context(POSTGRES)) {
            var value = input(c); var token = UUID.randomUUID(); var coordinator = UUID.randomUUID();
            var budget = budget(); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
            assertThatThrownBy(() -> journal.acquireInitial(CALLER, value, token, coordinator, cancelAt(checkpoint)))
                    .isInstanceOf(RepositoryException.class);
            for (String table : java.util.List.of("repository_execution_claims", "repository_publication_preparations", "repository_coordinator_bindings")) {
                long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                        .setParameter("id", value.key().operationId()).getSingleResult()).longValue());
                assertThat(count).as("%s checkpoint %s", table, checkpoint).isEqualTo(checkpoint == 5 ? 1 : 0);
            }
            var original = journal.acquireInitial(CALLER, value, token, coordinator, NONE);
            assertThat(journal.acquireInitial(CALLER, value, token, coordinator, NONE)).isEqualTo(original);
            assertThatThrownBy(() -> journal.acquireInitial(CALLER, value, token, NONE))
                    .hasMessageContaining("cannot use unbound registration");
            assertThat(journal.acquireInitial(CALLER, value, token, coordinator, NONE)).isEqualTo(original);
            assertThatThrownBy(() -> journal.acquireInitial(CALLER, value, token, UUID.randomUUID(), NONE))
                    .hasMessageContaining("Coordinator binding differs");
            assertThat(budget.reservedBytes()).isZero();
            for (String statement : java.util.List.of("UPDATE repository_coordinator_bindings SET incarnation=incarnation",
                    "DELETE FROM repository_coordinator_bindings"))
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return em.createNativeQuery(statement).executeUpdate(); }))
                        .hasStackTraceContaining("Coordinator binding is immutable");
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void boundRegistrationCannotAdoptAnExistingUnboundClaim(boolean preparationPresent) {
        try (var c = context(POSTGRES)) {
            var value = input(c); var token = UUID.randomUUID(); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget());
            if (preparationPresent) journal.acquireInitial(CALLER, value, token, NONE);
            else new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), token, LEASE);
            assertThatThrownBy(() -> journal.acquireInitial(CALLER, value, token, UUID.randomUUID(), NONE))
                    .hasMessageContaining("no coordinator binding");
            long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_bindings")
                    .getSingleResult()).longValue());
            assertThat(count).isZero();
        }
    }

    @Test void transferredBoundClaimCannotBecomeLegacyUnboundRestoration() {
        try (var c = context(POSTGRES)) {
            var source = input(c);
            var value = new DocumentPublicationPreparationRecord(source.key(), source.command(), source.seeds(),
                    source.placements(), Duration.ofSeconds(1), 0);
            var journal = new DocumentPublicationPreparationJournal(c.tx(), budget());
            journal.acquireInitial(CALLER, value, UUID.randomUUID(), UUID.randomUUID(), NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var next = new RepositoryExecutionClaimLedger(c.tx()).takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryCoordinatorBinding.requireResume(em, next, null); return null;
            })).hasMessageContaining("Coordinator-bound claim cannot use unbound registration");
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2, 3, 4, 5})
    void initialRegistrationCancellationNeverSeparatesClaimFromPreparation(int checkpoint) {
        try (var c = context(POSTGRES)) {
            var budget = budget(); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
            // Entry, encoded-before-SQL, claim-before-preparation, preparation-before-commit, after-commit.
            var value = input(c); var token = UUID.randomUUID();
            var control = cancelAt(checkpoint);
            assertThatThrownBy(() -> journal.acquireInitial(CALLER, value, token, control))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            for (String table : java.util.List.of("repository_execution_claims", "repository_publication_preparations")) {
                int count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                        .setParameter("id", value.command().operationId()).getSingleResult()).intValue());
                assertThat(count).as("%s at checkpoint %s", table, checkpoint).isEqualTo(checkpoint == 5 ? 1 : 0);
            }
            assertThat(budget.reservedBytes()).isZero();
            var claim = journal.acquireInitial(CALLER, value, token, NONE);
            assertThat(journal.acquireInitial(CALLER, value, token, NONE)).isEqualTo(claim);
            try (var loaded = journal.load(CALLER, claim, 0, NONE).orElseThrow()) {
                assertThat(loaded.record().seeds().ownerNonce()).isEqualTo(value.seeds().ownerNonce());
                assertThat(loaded.record().seeds().attempts()).isEqualTo(value.seeds().attempts());
                assertThat(loaded.record().seeds().uploadTokens()).isEqualTo(value.seeds().uploadTokens());
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void journalRoundTripAdmitsOriginalIdentitiesAndRetainsBudgetUntilClose() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
            var claim = claim(c, value, LEASE);
            journal.save(CALLER, claim, value, NONE);
            assertThat(budget.reservedBytes()).isZero();
            // A fresh host-side journal instance reads shared SQL, with no captured record passed to it.
            var reader = new DocumentPublicationPreparationJournal(new Tx(c.emf()), budget);
            assertThat(reader.readCommand(CALLER, value.key(), 0, NONE).orElseThrow().intent()).isEqualTo(value.command().intent());
            var loaded = reader.load(CALLER, claim, 0, NONE).orElseThrow();
            try (loaded) {
                assertThat(budget.reservedBytes()).isEqualTo(DocumentPublicationPreparationCodec.encode(value).size());
                var restored = loaded.record();
                assertThat(restored.placements()).isEqualTo(value.placements());
                bindModes(c, claim, 0);
                var owner = new RepositoryOperationLedger(c.tx()).admit(restored.key(), restored.command(), restored.seeds().ownerNonce(), LEASE, claim)
                        .owner().orElseThrow();
                var attempts = new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, restored.prepare());
                assertThat(attempts.getFirst().id()).isEqualTo(value.seeds().attempts().get("member-0"));
                assertThat(attempts.getFirst().token()).isEqualTo(value.seeds().uploadTokens().get("member-0"));
                journal.save(CALLER, claim, value, NONE); // Exact retry remains valid after admission.
            }
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(loaded::record).isInstanceOf(IllegalStateException.class);
            loaded.close(); assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void bootstrapCommandAllowsNewClaimWithoutExposingPriorClaimOrPreparation() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget());
            var oldClaim = claim(c, value, Duration.ofSeconds(1)); journal.save(CALLER, oldClaim, value, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var command = new DocumentPublicationPreparationJournal(new Tx(c.emf()), budget()).readCommand(CALLER, value.key(), 0, NONE).orElseThrow();
            var next = new RepositoryExecutionClaimLedger(c.tx()).takeOver(value.key(), command, 1, UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> journal.load(CALLER, oldClaim, 0, NONE)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            try (var loaded = journal.load(CALLER, next, 0, NONE).orElseThrow()) {
                assertThat(loaded.record().seeds().ownerNonce()).isEqualTo(value.seeds().ownerNonce());
            }
        }
    }

    @Test void conflictingPreparationAndDirectMutationAreRejected() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget); var claim = claim(c, value, LEASE);
            journal.save(CALLER, claim, value, NONE);
            var different = new DocumentPublicationPreparationRecord(value.key(), value.command(), DocumentPublicationSeeds.mint(value.key(), value.command()),
                    value.placements(), value.lease(), 0);
            assertThatThrownBy(() -> journal.save(CALLER, claim, different, NONE)).hasStackTraceContaining("identity conflicts");
            assertThat(budget.reservedBytes()).isZero();
            for (String sql : new String[]{"UPDATE repository_publication_preparations SET preparation_bytes=preparation_bytes", "DELETE FROM repository_publication_preparations"})
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("preparation is immutable");
        }
    }

    @Test void newPreparationCannotAdoptAnAdmittedInitialOwner() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget()); var claim = claim(c, value, LEASE);
            new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim);
            assertThatThrownBy(() -> journal.save(CALLER, claim, value, NONE)).hasStackTraceContaining("must precede operation admission");
            assertThat(journal.readCommand(CALLER, value.key(), 0, NONE)).isEmpty();
        }
    }

    @Test void recoveryPreparationRequiresExpiredExactPredecessorAndRetainsNextNonce() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget()); var claim = claim(c, value, LEASE);
            journal.save(CALLER, claim, value, NONE);
            var operations = new RepositoryOperationLedger(c.tx());
            bindModes(c, claim, 0);
            operations.admit(value.key(), value.command(), value.seeds().ownerNonce(), Duration.ofSeconds(1), claim);
            var next = new DocumentPublicationPreparationRecord(value.key(), value.command(), DocumentPublicationSeeds.mint(value.key(), value.command()),
                    value.placements(), value.lease(), 1);
            assertThatThrownBy(() -> journal.save(CALLER, claim, next, NONE)).hasStackTraceContaining("exact expired predecessor");
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            journal.save(CALLER, claim, next, NONE);
            bindModes(c, claim, 1);
            operations.takeOver(value.key(), value.command(), 1, next.seeds().ownerNonce(), LEASE, claim);
            journal.save(CALLER, claim, next, NONE);
            try (var loaded = journal.load(CALLER, claim, 1, NONE).orElseThrow()) {
                assertThat(loaded.record().seeds().ownerNonce()).isEqualTo(next.seeds().ownerNonce());
            }
        }
    }

    @Test void wrongCallerCapacityAndCancellationDoNotLeakPrivateLoads() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget); var claim = claim(c, value, LEASE);
            journal.save(CALLER, claim, value, NONE);
            var wrong = new RepositoryCaller("another", true);
            assertThatThrownBy(() -> journal.readCommand(wrong, value.key(), 0, NONE)).isInstanceOf(RepositoryException.class);
            assertThatThrownBy(() -> journal.load(wrong, claim, 0, NONE)).isInstanceOf(RepositoryException.class);
            var tiny = new PayloadBudget(1);
            assertThatThrownBy(() -> new DocumentPublicationPreparationJournal(c.tx(), tiny).load(CALLER, claim, 0, NONE))
                    .isInstanceOf(PayloadBudget.CapacityExceededException.class);
            assertThat(tiny.reservedBytes()).isZero();
            assertThatThrownBy(() -> journal.load(CALLER, claim, 0, cancelAt(3))).isInstanceOf(RepositoryException.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void cancellationAfterSaveCommitCanReconcileExactRecord() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget); var claim = claim(c, value, LEASE);
            assertThatThrownBy(() -> journal.save(CALLER, claim, value, cancelAt(4))).isInstanceOf(RepositoryException.class);
            assertThat(budget.reservedBytes()).isZero();
            journal.save(CALLER, claim, value, NONE);
            assertThat(journal.readCommand(CALLER, value.key(), 0, NONE)).isPresent();
        }
    }

    @Test void corruptedStoredBytesFailWholeBlobIntegrityAndReleaseReservation() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget(); var journal = new DocumentPublicationPreparationJournal(c.tx(), budget); var claim = claim(c, value, LEASE);
            journal.save(CALLER, claim, value, NONE);
            // Controlled corruption of real SQL storage, outside the normal immutable write API.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE repository_publication_preparations DISABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_publication_preparations DROP CONSTRAINT repository_preparation_digest").executeUpdate();
                em.createNativeQuery("UPDATE repository_publication_preparations SET preparation_bytes=set_byte(preparation_bytes,0,0)").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_publication_preparations ENABLE TRIGGER repository_publication_preparation_guard").executeUpdate(); return null;
            });
            assertThatThrownBy(() -> journal.load(CALLER, claim, 0, NONE)).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> { assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS); assertThat(failure.getCause()).isNull(); });
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void transferredClaimCannotDeliverDecodedPreparation() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget();
            var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
            var oldClaim = claim(c, value, Duration.ofSeconds(1));
            journal.save(CALLER, oldClaim, value, NONE);
            var transfer = new RepositoryReadControl() {
                int checks;
                @Override public boolean isCancelled() { return false; }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
                @Override public void check() {
                    if (++checks == 3) {
                        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
                        new RepositoryExecutionClaimLedger(c.tx()).takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE);
                    }
                }
            };
            assertThatThrownBy(() -> {
                try (var loaded = journal.load(CALLER, oldClaim, 0, transfer).orElseThrow()) { loaded.record(); }
            }).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void accountMembershipCannotBootstrapPrivateRecoveryCommand() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = budget();
            var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
            journal.save(CALLER, claim(c, value, LEASE), value, NONE);
            var scoped = new RepositoryCaller("principal", false, java.util.Set.of(value.key().account()), java.util.Set.of());
            assertThatThrownBy(() -> journal.readCommand(scoped, value.key(), 0, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static RepositoryReadControl cancelAt(int count) {
        return new RepositoryReadControl() {
            int checks;
            @Override public boolean isCancelled() { return false; }
            @Override public long remainingNanos() { return Long.MAX_VALUE; }
            @Override public void check() { if (++checks==count) throw new RepositoryException(RepositoryException.Code.CANCELLED, "Injected cancellation"); }
        };
    }
    private static PayloadBudget budget() { return new PayloadBudget(2L*DocumentPublicationPreparationCodec.MAX_BYTES); }
    private static void bindModes(Context c, RepositoryExecutionClaimLedger.Claim claim, long predecessor) {
        new DocumentPublicationModesJournal(c.tx(), budget()).bind(CALLER, claim, predecessor,
                Map.of("member-0", DocumentPublicationCandidate.Mode.TYPED, "member-1", DocumentPublicationCandidate.Mode.TYPED), NONE);
    }
    private static RepositoryExecutionClaimLedger.Claim claim(Context c, DocumentPublicationPreparationRecord value, Duration lease) {
        return new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), UUID.randomUUID(), lease);
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
