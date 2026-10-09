package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Private Java/SQL recovery, not a provider or process-termination qualification. */
@Testcontainers
class RepositoryCoordinatorReservationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    record Input(DocumentPublicationPreparationRecord previous, RepositoryCoordinatorReservation.ExpiredUnquiesced proposal) {}

    static Input inputFor(Context c, Duration successorLease) {
        var original = input(c); var incarnation = UUID.randomUUID();
        var previous = new DocumentPublicationPreparationRecord(original.key(), original.command(), original.seeds(),
                original.placements(), Duration.ofSeconds(1), 0);
        var budget = new PayloadBudget(64_000_000);
        var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                .acquireInitial(CALLER, previous, UUID.randomUUID(), incarnation, NONE);
        new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
        var owner = new RepositoryOperationLedger(c.tx()).admit(previous.key(), previous.command(),
                previous.seeds().ownerNonce(), Duration.ofSeconds(1), claim).owner().orElseThrow();
        var proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(
                new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), incarnation),
                UUID.randomUUID(), UUID.randomUUID(), successorLease,
                new RepositoryCoordinatorReservation.OwnerIdentity(owner.generation(), owner.token()));
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
        assertThat(budget.reservedBytes()).isZero();
        return new Input(previous, proposal);
    }

    @Test void loadsRetainedPreparationUnderSuccessorAndAttachesOnlyExactKind() {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, LEASE); var proposal = input.proposal(); var budget = new PayloadBudget(64_000_000);
            RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, proposal, NONE);
            var claim = c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em,
                    input.previous().key(), input.previous().command().sha256(), 2, proposal.successorToken()); });
            try (var retained = new DocumentPublicationPreparationJournal(c.tx(), budget).load(CALLER, claim, 0, NONE).orElseThrow()) {
                var previous = retained.record();
                var plan = RepositorySuccessorInstall.prepare(proposal, previous, LEASE, MODES);
                RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
                RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
                var attached = RepositorySuccessorExecution.attach(c.tx(), budget, CALLER, plan, NONE);
                assertThat(attached.owner().generation()).isEqualTo(2);
                assertThat(attached.owner().token()).isEqualTo(plan.next().seeds().ownerNonce());
                var graceful = new RepositoryCoordinatorReservation.Graceful(new RepositoryCoordinatorHandoff.Proposal(
                        proposal.predecessor(), proposal.successorToken(), proposal.successorIncarnation(), proposal.lease()));
                var changed = new RepositorySuccessorInstall.Plan(graceful, previous, plan.next(), plan.modes());
                assertThat(DocumentSuccessorFingerprint.of(changed)).isNotEqualTo(DocumentSuccessorFingerprint.of(plan));
                assertThatThrownBy(() -> RepositorySuccessorExecution.attach(c.tx(), budget, CALLER, changed, NONE))
                        .hasMessageContaining("activation is not committed");
                assertThatThrownBy(() -> RepositorySuccessorInstall.install(c.tx(), budget, CALLER, changed, NONE))
                        .hasMessageContaining("handoff is not committed");
                var wrongOwner = new RepositoryCoordinatorReservation.ExpiredUnquiesced(proposal.predecessor(),
                        proposal.successorToken(), proposal.successorIncarnation(), proposal.lease(),
                        new RepositoryCoordinatorReservation.OwnerIdentity(2, proposal.owner().nonce()));
                assertThatThrownBy(() -> RepositorySuccessorInstall.prepare(wrongOwner, previous, LEASE, MODES))
                        .hasMessageContaining("owner differs");
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void lostCommitReplyConfirmsExactReservationWithoutRenewal() {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, LEASE); var armed = new AtomicBoolean(true);
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                int count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_expirations")
                        .getSingleResult()).intValue());
                if (count == 1 && armed.compareAndSet(true, false)) throw new java.sql.SQLException("reservation reply lost", "08006");
            });
            java.time.Instant stamp;
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                stamp = RepositoryCoordinatorExpiration.reserve(new Tx(emf), CALLER, input.proposal(), NONE);
            }
            assertThat(armed).isFalse();
            var lease = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims").getSingleResult());
            assertThat(RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, input.proposal(), NONE)).isEqualTo(stamp);
            var after = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims").getSingleResult());
            assertThat(after).isEqualTo(lease);
        }
    }

    @Test void gracefulReservationCannotBeReinterpretedAsExpiredAtAttachment() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c); var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            var original = plan.reservation();
            var expired = new RepositoryCoordinatorReservation.ExpiredUnquiesced(original.predecessor(), original.successorToken(),
                    original.successorIncarnation(), original.lease(), new RepositoryCoordinatorReservation.OwnerIdentity(
                    plan.previous().predecessorGeneration()+1, plan.previous().seeds().ownerNonce()));
            assertThatThrownBy(() -> RepositoryCoordinatorReservation.confirm(c.tx(), CALLER, expired, NONE))
                    .hasMessageContaining("Reservation differs");
            var changed = new RepositorySuccessorInstall.Plan(expired, plan.previous(), plan.next(), plan.modes());
            assertThatThrownBy(() -> RepositorySuccessorExecution.attach(c.tx(), budget, CALLER, changed, NONE))
                    .hasMessageContaining("Reservation differs");
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void cancellationAfterCommitIsVisibleAndLaterRetryConfirmsSavedReservation() {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, LEASE); var cancelled = new AtomicBoolean();
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                int count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_expirations")
                        .getSingleResult()).intValue());
                if (count == 1) cancelled.set(true);
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                assertThatThrownBy(() -> RepositoryCoordinatorExpiration.reserve(new Tx(emf), CALLER, input.proposal(), control))
                        .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            }
            assertThat(cancelled).isTrue();
            var saved = RepositoryCoordinatorReservation.confirm(c.tx(), CALLER, input.proposal(), NONE).orElseThrow();
            var lease = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims").getSingleResult());
            assertThat(RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, input.proposal(), NONE)).isEqualTo(saved);
            var after = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims").getSingleResult());
            assertThat(after).isEqualTo(lease);
        }
    }

    @Test void expiredConfirmationGrantsNoExecutionAndRejectsDifferentOwnerOrKind() {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, Duration.ofSeconds(1)); var proposal = input.proposal();
            var stamp = RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, proposal, NONE);
            var lease = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims").getSingleResult());
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            assertThat(RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, proposal, NONE)).isEqualTo(stamp);
            var after = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims").getSingleResult());
            assertThat(after).isEqualTo(lease);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em,
                    input.previous().key(), input.previous().command().sha256(), 2, proposal.successorToken()); }))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var changed = new RepositoryCoordinatorReservation.ExpiredUnquiesced(proposal.predecessor(), proposal.successorToken(),
                    proposal.successorIncarnation(), proposal.lease(), new RepositoryCoordinatorReservation.OwnerIdentity(1, UUID.randomUUID()));
            assertThatThrownBy(() -> RepositoryCoordinatorReservation.confirm(c.tx(), CALLER, changed, NONE)).hasMessageContaining("differs");
            var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> RepositoryCoordinatorExpiration.reserve(c.tx(), scoped, proposal, NONE))
                    .hasMessageContaining("private process authority");
            assertThatThrownBy(() -> RepositoryCoordinatorReservation.confirm(c.tx(), scoped, proposal, NONE))
                    .hasMessageContaining("private process authority");
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void concurrentJavaReservationsConfirmOnlyExactWinner(boolean identical) throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var input = inputFor(c, LEASE); var a = input.proposal();
            var b = identical ? a : new RepositoryCoordinatorReservation.ExpiredUnquiesced(a.predecessor(),
                    UUID.randomUUID(), UUID.randomUUID(), a.lease(), a.owner());
            var start = new java.util.concurrent.CountDownLatch(1);
            var first = workers.submit(() -> { start.await(); return catchThrowable(() -> RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, a, NONE)); });
            var second = workers.submit(() -> { start.await(); return catchThrowable(() -> RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, b, NONE)); });
            start.countDown();
            var x = first.get(10, java.util.concurrent.TimeUnit.SECONDS); var y = second.get(10, java.util.concurrent.TimeUnit.SECONDS);
            if (identical) { assertThat(x).isNull(); assertThat(y).isNull(); }
            else assertThat((x == null) != (y == null)).isTrue();
            var winner = x == null ? a : b;
            assertThat(RepositoryCoordinatorReservation.confirm(c.tx(), CALLER, winner, NONE)).isPresent();
            int count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_expirations").getSingleResult()).intValue());
            assertThat(count).isEqualTo(1);
        }
    }
}
