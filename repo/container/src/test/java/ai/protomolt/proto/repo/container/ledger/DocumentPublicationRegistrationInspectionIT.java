package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspection.Phase.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL classification; synthetic document fixture, no provider or process-restart proof. */
@Testcontainers
class DocumentPublicationRegistrationInspectionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    static final Duration LEASE = Duration.ofMinutes(5);
    static final Map<String, DocumentPublicationCandidate.Mode> MODES = Map.of("member-0", DocumentPublicationCandidate.Mode.TYPED);

    @Test void observesEveryRegistrationBoundaryWithoutMintingOrRenewingIdentity() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), UUID.randomUUID(), LEASE);
            assertThat(inspect(c, budget, claim)).isEqualTo(NO_PREPARATION);
            new DocumentPublicationPreparationJournal(c.tx(), budget).save(CALLER, claim, value, NONE);
            assertThat(inspect(c, budget, claim)).isEqualTo(PREPARATION_ONLY);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            assertThat(inspect(c, budget, claim)).isEqualTo(MODES_BOUND);
            var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim).owner().orElseThrow();
            assertThat(inspect(c, budget, claim)).isEqualTo(OWNER_ADMITTED);
            new DocumentAssessmentStartJournal(c.tx(), budget).start(CALLER, owner, value.command(), UUID.randomUUID(), LEASE, NONE);
            assertThat(inspect(c, budget, claim)).isEqualTo(ASSESSMENT_STARTED);
            new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, value.command(), NONE);
            assertThat(inspect(c, budget, claim)).isEqualTo(TERMINAL);
            var observed = new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), claim.token(), LEASE);
            assertThat(observed).isEqualTo(claim);
            try (var loaded = new DocumentPublicationPreparationJournal(c.tx(), budget).load(CALLER, claim, 0, NONE).orElseThrow()) {
                assertThat(loaded.record().seeds().ownerNonce()).isEqualTo(value.seeds().ownerNonce());
                assertThat(loaded.record().seeds().attempts()).isEqualTo(value.seeds().attempts());
                assertThat(loaded.record().seeds().uploadTokens()).isEqualTo(value.seeds().uploadTokens());
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void requiresPrivateAuthorityAndOriginalLiveClaim() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var shortLease = new DocumentPublicationPreparationRecord(value.key(), value.command(), value.seeds(),
                    value.placements(), Duration.ofSeconds(1), 0);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, shortLease, UUID.randomUUID(), NONE);
            var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> DocumentPublicationRegistrationInspection.inspect(c.tx(), budget, scoped, claim, NONE))
                    .isInstanceOf(RepositoryException.class).hasMessageContaining("private process authority");
            var wrong = new RepositoryExecutionClaimLedger.Claim(claim.key(), claim.commandSha256(), claim.epoch(), UUID.randomUUID(), claim.leaseUntil());
            assertThatThrownBy(() -> inspect(c, budget, wrong)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            expire(c);
            assertThatThrownBy(() -> inspect(c, budget, claim)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var successor = new RepositoryExecutionClaimLedger(c.tx()).takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> inspect(c, budget, claim)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThatThrownBy(() -> inspect(c, budget, successor)).isInstanceOf(RepositoryException.class).hasMessageContaining("original claim");
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void expiredOwnerIsNotPresentedAsExecutable() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), Duration.ofSeconds(1), claim).owner().orElseThrow();
            expire(c);
            assertThat(inspect(c, budget, claim)).isEqualTo(OWNER_EXPIRED);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void rowAppearingDuringDecodeRequiresFreshInspection() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            var checkpoints = new AtomicInteger();
            DocumentPublicationRegistrationInspection.inspect(c.tx(), budget, CALLER, claim, control(checkpoints::incrementAndGet));
            int beforeFinalTransaction = checkpoints.get() - 1;
            var calls = new AtomicInteger();
            assertThatThrownBy(() -> DocumentPublicationRegistrationInspection.inspect(c.tx(), budget, CALLER, claim, control(() -> {
                if (calls.incrementAndGet() == beforeFinalTransaction)
                    new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            }))).isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CONFLICT));
            assertThat(budget.reservedBytes()).isZero();
            assertThat(inspect(c, budget, claim)).isEqualTo(MODES_BOUND);
        }
    }

    @Test void cancellationAtEveryCheckpointReleasesPreparationAndModesReservations() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            var checkpoints = new AtomicInteger();
            DocumentPublicationRegistrationInspection.inspect(c.tx(), budget, CALLER, claim, control(checkpoints::incrementAndGet));
            for (int i = 1; i <= checkpoints.get(); i++) {
                int target = i; var calls = new AtomicInteger();
                assertThatThrownBy(() -> DocumentPublicationRegistrationInspection.inspect(c.tx(), budget, CALLER, claim, control(() -> {
                    if (calls.incrementAndGet() == target) throw new RepositoryException(RepositoryException.Code.CANCELLED, "cancel inspection");
                }))).isInstanceOf(RepositoryException.class).hasMessage("cancel inspection");
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    static DocumentPublicationRegistrationInspection.Phase inspect(Context c, PayloadBudget budget, RepositoryExecutionClaimLedger.Claim claim) {
        return DocumentPublicationRegistrationInspection.inspect(new Tx(c.emf()), budget, CALLER, claim, NONE);
    }
    private static void expire(Context c) {
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
    }
    private static RepositoryReadControl control(Runnable check) {
        return new RepositoryReadControl() {
            public boolean isCancelled() { return false; }
            public long remainingNanos() { return Long.MAX_VALUE; }
            public void check() { check.run(); }
        };
    }
    static DocumentPublicationPreparationRecord input(Context c) {
        return input(c, 1);
    }
    static DocumentPublicationPreparationRecord input(Context c, int members) {
        var seed = prepare(c, members, true);
        var command = new DocumentPublicationCommand(seed.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
        UUID drive = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var placement = DocumentUploadPlan.Placement.sample(new DriveLedger(c.tx()).findById(drive).orElseThrow(), "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
        return new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), Map.of(drive, placement), LEASE, 0);
    }
}
