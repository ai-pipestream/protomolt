package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class RepositoryCoordinatorHandoffIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void exactRetryAfterSuccessorExpiryDoesNotRenewOrOpenExecution() {
        try (var c = context(POSTGRES)) {
            var initial = input(c); var incarnation = UUID.randomUUID();
            var value = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(),
                    initial.placements(), Duration.ofSeconds(1), 0);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), incarnation);
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            var proposal = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), Duration.ofSeconds(1));
            assertThatThrownBy(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE))
                    .hasStackTraceContaining("expired predecessor");
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var stamp = RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE);
            Object lease = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o", value.key().operationId()).getSingleResult());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return em.createNativeQuery("SELECT require_repository_execution_claim(:a,:p,:o)")
                    .setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                    .setParameter("o", value.key().operationId()).getSingleResult(); })).hasStackTraceContaining("locally drained");
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            assertThat(RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE)).isEqualTo(stamp);
            Object after = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o", value.key().operationId()).getSingleResult());
            assertThat(after).isEqualTo(lease);
            var conflict = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), Duration.ofSeconds(1));
            assertThatThrownBy(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, conflict, NONE)).hasMessageContaining("differs");
            for (String sql : java.util.List.of("DELETE FROM repository_coordinator_handoffs",
                    "UPDATE repository_coordinator_handoffs SET recorded_at=clock_timestamp()"))
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("handoff is immutable");
        }
    }

    @Test void missingDrainAndWrongIdentityCannotTransfer() {
        try (var c = context(POSTGRES)) {
            var initial = input(c); var incarnation = UUID.randomUUID();
            var value = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(),
                    initial.placements(), Duration.ofSeconds(1), 0);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), incarnation);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var proposal = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE))
                    .hasStackTraceContaining("exact local drain");
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            for (var wrong : java.util.List.of(
                    new RepositoryCoordinatorDrain.Identity(claim.key(), "0".repeat(64), 1, claim.token(), incarnation),
                    new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, UUID.randomUUID(), incarnation),
                    new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), UUID.randomUUID()))) {
                assertThatThrownBy(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER,
                        new RepositoryCoordinatorHandoff.Proposal(wrong, UUID.randomUUID(), UUID.randomUUID(), LEASE), NONE))
                        .hasStackTraceContaining("Handoff requires exact");
            }
            var untrusted = new RepositoryCaller(claim.key().principal(), false, java.util.Set.of(claim.key().account()), java.util.Set.of());
            assertThatThrownBy(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), untrusted, proposal, NONE))
                    .hasMessageContaining("private process authority");
            assertThat(RepositoryCoordinatorHandoff.confirm(c.tx(), CALLER, proposal, NONE)).isEmpty();
            RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE);
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void concurrentProposalsProduceOneTransfer(boolean exactRetry) throws Exception {
        try (var c = context(POSTGRES); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var initial = input(c); var incarnation = UUID.randomUUID();
            var value = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(),
                    initial.placements(), Duration.ofSeconds(1), 0);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), incarnation);
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var gate = new CountDownLatch(1);
            var first = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), LEASE);
            var proposals = java.util.List.of(first, exactRetry ? first :
                    new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), LEASE));
            var futures = proposals.stream().map(p -> workers.submit(() -> {
                gate.await();
                return catchThrowable(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, p, NONE));
            })).toList();
            gate.countDown();
            int successes = 0;
            for (var future : futures) {
                var failure = future.get(10, TimeUnit.SECONDS);
                if (failure == null) successes++;
                else assertThat(failure.toString()).satisfiesAnyOf(
                        text -> assertThat(text).contains("Handoff requires exact expired predecessor"),
                        text -> assertThat(text).contains("Handoff differs from original binding"));
            }
            assertThat(successes).isEqualTo(exactRetry ? 2 : 1);
            long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_handoffs").getSingleResult()).longValue());
            assertThat(count).isEqualTo(1);
            var state = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("SELECT claim_epoch,claim_token FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o", value.key().operationId()).getSingleResult());
            assertThat(((Number) state[0]).longValue()).isEqualTo(2);
            var winner = proposals.stream().filter(p -> p.successorToken().equals(state[1])).findFirst().orElseThrow();
            assertThat(RepositoryCoordinatorHandoff.confirm(c.tx(), CALLER, winner, NONE)).isPresent();
        }
    }

    @Test void rollbackLeavesPredecessorAndLostCommitReplyConfirmsExactTransfer() {
        try (var c = context(POSTGRES)) {
            var initial = input(c); var incarnation = UUID.randomUUID();
            var value = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(),
                    initial.placements(), Duration.ofSeconds(1), 0);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), incarnation);
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var proposal = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), LEASE);
            var checks = new java.util.concurrent.atomic.AtomicInteger();
            var cancelled = new RepositoryException(RepositoryException.Code.CANCELLED, "cancel after transfer before commit");
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                public void check() { if (checks.incrementAndGet() >= 5) throw cancelled; }
            };
            assertThat(catchThrowable(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, control))).isSameAs(cancelled);
            assertThat(RepositoryCoordinatorHandoff.confirm(c.tx(), CALLER, proposal, NONE)).isEmpty();
            long epoch = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT claim_epoch FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o", value.key().operationId()).getSingleResult()).longValue());
            assertThat(epoch).isEqualTo(1);
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_handoffs").getSingleResult()).longValue());
                if (count == 1 && armed.compareAndSet(true, false)) throw new java.sql.SQLException("handoff reply lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", java.util.Map.of(
                    "hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var stamp = RepositoryCoordinatorHandoff.reserve(new Tx(emf), CALLER, proposal, NONE);
                assertThat(armed).isFalse();
                assertThat(RepositoryCoordinatorHandoff.confirm(c.tx(), CALLER, proposal, NONE)).contains(stamp);
            }
        }
    }

    @Test void terminalCancellationCannotAcquireSuccessor() {
        try (var c = context(POSTGRES)) {
            var initial = input(c); var incarnation = UUID.randomUUID();
            var value = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(),
                    initial.placements(), Duration.ofSeconds(2), 0);
            var budget = new PayloadBudget(64_000_000);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim)
                    .owner().orElseThrow();
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            assertThat(new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, value.command(), NONE).rejection()).isPresent();
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), incarnation);
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(2.1)").getSingleResult());
            var proposal = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE))
                    .hasStackTraceContaining("Terminal operation cannot hand off");
            assertThat(RepositoryCoordinatorHandoff.confirm(c.tx(), CALLER, proposal, NONE)).isEmpty();
        }
    }
}
