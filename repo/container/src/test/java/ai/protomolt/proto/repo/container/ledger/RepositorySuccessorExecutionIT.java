package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import java.time.Instant;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** SQL boundary tests; activation is not yet exposed through a host or public API. */
@Testcontainers
class RepositorySuccessorExecutionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void exactSuccessorBindingResumesAndRetainedDrainClosesExecution() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64_000_000), CALLER, plan, NONE);
            activate(c, plan, true);
            var claim = claim(plan);
            c.tx().inTransaction(em -> {
                RepositoryCoordinatorBinding.requireResume(em, claim, plan.handoff().successorIncarnation());
            });
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryCoordinatorBinding.requireResume(em, claim, plan.handoff().predecessor().incarnation());
            })).hasMessageContaining("differs");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, plan.previous().key(), plan.previous().command().sha256(),
                        plan.handoff().predecessor().epoch(), plan.handoff().predecessor().token());
            })).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), claim.epoch(),
                    claim.token(), plan.handoff().successorIncarnation());
            assertThat(RepositoryCoordinatorDrain.beginRetained(c.tx(), CALLER, identity, NONE)).isTrue();
            // Admission drain still allows already admitted work to settle.
            c.tx().inTransaction(em -> { RepositoryExecutionClaimLedger.lockLive(em, claim); });
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
            })).hasStackTraceContaining("locally drained");
        }
    }

    @Test void activationCannotCommitWithoutExactBinding() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64_000_000), CALLER, plan, NONE);
            assertThatThrownBy(() -> activate(c, plan, false)).hasStackTraceContaining("exact coordinator binding");
            long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_successor_executions").getSingleResult()).longValue());
            assertThat(count).isZero();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim(plan));
            })).hasStackTraceContaining("locally drained");
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void exactGenerationAdmitsAssessmentAndAttemptsOnlyBeforeDrain(boolean draining) {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            activate(c, plan, true);
            var claim = claim(plan);
            var owner = new RepositoryOperationLedger.Owner(plan.next().key(), 2,
                    plan.next().seeds().ownerNonce(), Instant.EPOCH).withClaim(claim);
            var previousOwner = new RepositoryOperationLedger.Owner(plan.previous().key(), 1,
                    plan.previous().seeds().ownerNonce(), Instant.EPOCH).withClaim(claim);
            var starts = new DocumentAssessmentStartJournal(c.tx(), budget);
            var uploads = new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx()));
            assertThatThrownBy(() -> starts.start(CALLER, previousOwner, plan.previous().command(), UUID.randomUUID(), LEASE, NONE))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            assertThatThrownBy(() -> uploads.admit(CALLER, previousOwner, plan.previous().prepare()))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            long oldAttempts = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_part_attempts WHERE operation_generation=1 AND operation_id=:o")
                        .setParameter("o", plan.next().key().operationId()).getSingleResult()).longValue());
            assertThat(oldAttempts).isZero();
            if (draining) {
                RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, plan.handoff().successorIncarnation(), NONE);
                assertThatThrownBy(() -> starts.start(CALLER, owner, plan.next().command(), UUID.randomUUID(), LEASE, NONE))
                        .hasStackTraceContaining("new admission is closed");
                assertThatThrownBy(() -> uploads.admit(CALLER, owner, plan.next().prepare()))
                        .hasStackTraceContaining("new admission is closed");
                assertThat(new RepositoryOperationLedger(c.tx()).renew(owner, LEASE).generation()).isEqualTo(2);
            } else {
                assertThat(starts.start(CALLER, owner, plan.next().command(), UUID.randomUUID(), LEASE, NONE)).isNotNull();
                assertThat(uploads.admit(CALLER, owner, plan.next().prepare())).isNotNull();
                long attempts = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_part_attempts WHERE operation_generation=2 AND operation_id=:o")
                        .setParameter("o", plan.next().key().operationId()).getSingleResult()).longValue());
                assertThat(attempts).isPositive();
            }
        }
    }

    @Test void ownerExpiryBeforeActivationCommitRollsBackBindingAndGrant() {
        try (var c = context(POSTGRES)) {
            var original = RepositorySuccessorInstallIT.plan(c);
            var plan = RepositorySuccessorInstall.prepare(original.handoff(), original.previous(), Duration.ofSeconds(1), MODES);
            RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64_000_000), CALLER, plan, NONE);
            assertThatThrownBy(() -> activate(c, plan, true, true)).hasStackTraceContaining("exact coordinator binding");
            assertThat(c.tx().<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_successor_executions").getSingleResult()).longValue())).isZero();
            assertThat(c.tx().<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_coordinator_bindings WHERE claim_epoch=2").getSingleResult()).longValue())).isZero();
        }
    }

    @Test void activationDoesNotPermitAnotherPreparationGeneration() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            activate(c, plan, true);
            var next = new DocumentPublicationPreparationRecord(plan.next().key(), plan.next().command(),
                    DocumentPublicationSeeds.mint(plan.next().key(), plan.next().command()), plan.next().placements(), LEASE, 2);
            assertThatThrownBy(() -> new DocumentPublicationPreparationJournal(c.tx(), budget)
                    .save(CALLER, claim(plan), next, NONE)).hasStackTraceContaining("Recovery preparation requires exact expired predecessor");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim(plan));
                em.createNativeQuery("SELECT require_repository_coordinator_admission(:a,:p,:o)")
                        .setParameter("a", plan.next().key().account()).setParameter("p", plan.next().key().principal())
                        .setParameter("o", plan.next().key().operationId()).getSingleResult();
            })).hasStackTraceContaining("new admission is closed");
            long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_publication_preparations WHERE predecessor_generation=2").getSingleResult()).longValue());
            assertThat(count).isZero();
        }
    }

    @Test void ungrantedThirdEpochCannotInheritExecution() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c, Duration.ofSeconds(1));
            RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64_000_000), CALLER, plan, NONE);
            activate(c, plan, true);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var next = new RepositoryExecutionClaimLedger(c.tx()).takeOver(plan.next().key(), plan.next().command(),
                    2, UUID.randomUUID(), LEASE);
            assertThat(next.epoch()).isEqualTo(3);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, next);
            })).hasStackTraceContaining("locally drained");
        }
    }

    private static RepositoryExecutionClaimLedger.Claim claim(RepositorySuccessorInstall.Plan plan) {
        return new RepositoryExecutionClaimLedger.Claim(plan.next().key(), plan.next().command().sha256(),
                plan.handoff().predecessor().epoch() + 1, plan.handoff().successorToken(), Instant.EPOCH);
    }

    private static void activate(Context c, RepositorySuccessorInstall.Plan plan, boolean bind) {
        activate(c, plan, bind, false);
    }

    private static void activate(Context c, RepositorySuccessorInstall.Plan plan, boolean bind, boolean expire) {
        c.tx().inTransaction(em -> {
            // Exercise database guards directly, using the exact committed install.
            em.createNativeQuery("""
                    INSERT INTO repository_successor_executions(account_id,principal,operation_id,claim_epoch,
                     claim_token,incarnation,owner_generation,owner_nonce,command_sha256,preparation_sha256,modes_sha256,activation_xid)
                    SELECT account_id,principal,operation_id,successor_epoch,successor_token,successor_incarnation,
                     predecessor_generation+1,owner_nonce,command_sha256,preparation_sha256,modes_sha256,pg_current_xact_id()
                    FROM repository_successor_installs WHERE operation_id=:o
                    """).setParameter("o", plan.next().key().operationId()).executeUpdate();
            if (bind) em.createNativeQuery("""
                    INSERT INTO repository_coordinator_bindings(account_id,principal,operation_id,claim_epoch,claim_token,incarnation)
                    SELECT account_id,principal,operation_id,claim_epoch,claim_token,incarnation
                    FROM repository_successor_executions WHERE operation_id=:o
                    """).setParameter("o", plan.next().key().operationId()).executeUpdate();
            if (expire) em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult();
        });
    }
}
