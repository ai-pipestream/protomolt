package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Actual SQL admission/settlement checks, not provider-start or local-drain qualification. */
@Testcontainers
class RepositoryCoordinatorDrainIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void committedDrainSerializesBeforeWaitingAdmission() throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var value = input(c); var budget = new PayloadBudget(64_000_000); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            var inserted = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var blocker = new AtomicInteger();
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var dataSource = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!armed.compareAndSet(true, false)) return;
                try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                    result.next(); blocker.set(result.getInt(1));
                }
                inserted.countDown();
                try {
                    if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new java.sql.SQLException("release timeout");
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.sql.SQLException(interrupted); }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    java.util.Map.of("hibernate.connection.datasource", dataSource, "hibernate.hbm2ddl.auto", "validate"))) {
                armed.set(true);
                var drain = workers.submit(() -> RepositoryCoordinatorDrain.begin(new Tx(emf), CALLER, claim, incarnation, NONE));
                try {
                    assertThat(inserted.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    var admission = workers.submit(() -> {
                        new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
                    });
                    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                    boolean blocked = false;
                    while (System.nanoTime() < deadline) {
                        blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                                SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                                WHERE :blocker=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'
                                AND query LIKE '%fence_repository_execution_claim%')
                                """).setParameter("blocker", blocker.get()).getSingleResult());
                        if (blocked) break;
                        Thread.sleep(10);
                    }
                    assertThat(blocked).as("mode admission waits on the drain transaction's claim lock").isTrue();
                    release.countDown();
                    drain.get(5, java.util.concurrent.TimeUnit.SECONDS);
                    assertThatThrownBy(() -> admission.get(5, java.util.concurrent.TimeUnit.SECONDS))
                            .hasStackTraceContaining("new admission is closed");
                } finally { release.countDown(); }
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void lostCommitReplyCanBeConfirmedAfterExpiryAndTransferWithoutReopeningAdmissions() throws Exception {
        try (var c = context(POSTGRES)) {
            var original = input(c); var incarnation = UUID.randomUUID(); var budget = new PayloadBudget(64_000_000);
            var value = new DocumentPublicationPreparationRecord(original.key(), original.command(), original.seeds(),
                    original.placements(), java.time.Duration.ofSeconds(1), 0);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            assertThat(RepositoryCoordinatorDrain.confirm(c.tx(), CALLER, claim, incarnation, NONE)).isEmpty();
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            var dataSource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) throw new java.sql.SQLException("drain acknowledgement lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    java.util.Map.of("hibernate.connection.datasource", dataSource, "hibernate.hbm2ddl.auto", "validate"))) {
                assertThatThrownBy(() -> RepositoryCoordinatorDrain.begin(new Tx(emf), CALLER, claim, incarnation, NONE))
                        .hasStackTraceContaining("drain acknowledgement lost");
            }
            var saved = RepositoryCoordinatorDrain.confirm(c.tx(), CALLER, claim, incarnation, NONE).orElseThrow();
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            assertThatThrownBy(() -> RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var successor = new RepositoryExecutionClaimLedger(c.tx()).takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE);
            assertThat(RepositoryCoordinatorDrain.confirm(c.tx(), CALLER, claim, incarnation, NONE)).contains(saved);
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, successor, 0, MODES, NONE))
                    .hasStackTraceContaining("new admission is closed");
            assertThatThrownBy(() -> RepositoryCoordinatorDrain.confirm(c.tx(), CALLER, claim, UUID.randomUUID(), NONE))
                    .hasMessageContaining("differs from original binding");
            var wrongCommand = new RepositoryExecutionClaimLedger.Claim(claim.key(), "0".repeat(64), claim.epoch(), claim.token(), claim.leaseUntil());
            assertThatThrownBy(() -> RepositoryCoordinatorDrain.confirm(c.tx(), CALLER, wrongCommand, incarnation, NONE))
                    .hasMessageContaining("differs from original binding");
            var scoped = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
            assertThatThrownBy(() -> RepositoryCoordinatorDrain.confirm(c.tx(), scoped, claim, incarnation, NONE))
                    .hasMessageContaining("private process authority");
            assertThatThrownBy(() -> RepositoryCoordinatorDrain.begin(c.tx(), scoped, successor, incarnation, NONE))
                    .hasMessageContaining("private process authority");
        }
    }

    @Test void ownerGenerationCannotAdvanceAfterDrain() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            var operations = new RepositoryOperationLedger(c.tx());
            operations.admit(value.key(), value.command(), value.seeds().ownerNonce(), java.time.Duration.ofSeconds(1), claim);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            assertThatThrownBy(() -> operations.takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE, claim))
                    .hasStackTraceContaining("new admission is closed");
        }
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 3})
    void closesNewRowsWhilePreservingExactRetriesAndOwnerSettlement(int phase) {
        try (var c = context(POSTGRES, "89")) {
            var value = input(c); var budget = new PayloadBudget(64_000_000); var incarnation = UUID.randomUUID();
            var preparations = new DocumentPublicationPreparationJournal(c.tx(), budget);
            var claim = preparations.acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            var modes = new DocumentPublicationModesJournal(c.tx(), budget);
            var operations = new RepositoryOperationLedger(c.tx());
            var starts = new DocumentAssessmentStartJournal(c.tx(), budget);
            if (phase >= 1) modes.bind(CALLER, claim, 0, MODES, NONE);
            var owner = phase >= 2 ? operations.admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim)
                    .owner().orElseThrow() : null;
            var assessment = UUID.randomUUID();
            var started = phase >= 3 ? starts.start(CALLER, owner, value.command(), assessment, LEASE, NONE) : null;
            String schema = c.pool().getSchema();
            org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
            var drain = RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            assertThat(RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE)).isEqualTo(drain);
            assertThat(preparations.acquireInitial(CALLER, value, claim.token(), incarnation, NONE)).isEqualTo(claim);
            if (phase == 0) {
                assertThatThrownBy(() -> modes.bind(CALLER, claim, 0, MODES, NONE)).hasStackTraceContaining("new admission is closed");
            } else {
                modes.bind(CALLER, claim, 0, MODES, NONE);
                if (phase == 1) assertThatThrownBy(() -> operations.admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim))
                        .hasStackTraceContaining("new admission is closed");
                else {
                    assertThat(operations.admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim)
                            .owner().orElseThrow().token()).isEqualTo(owner.token());
                    assertThat(operations.renew(owner, LEASE).token()).isEqualTo(owner.token());
                    if (phase == 2) assertThatThrownBy(() -> starts.start(CALLER, owner, value.command(), assessment, LEASE, NONE))
                            .hasStackTraceContaining("new admission is closed");
                    else assertThat(starts.start(CALLER, owner, value.command(), assessment, LEASE, NONE)).isEqualTo(started);
                    new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, value.command(), NONE);
                    long rejected = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:o")
                            .setParameter("o", claim.key().operationId()).getSingleResult()).longValue());
                    assertThat(rejected).isEqualTo(1);
                }
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void newUploadAttemptIsRefusedBeforeAnyProviderWork() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim)
                    .owner().orElseThrow();
            var prepared = DocumentOperationUploadAdmission.prepare(value.command(), value.placements(), value.seeds().attempts(), LEASE,
                    value.seeds().uploadTokens());
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            assertThatThrownBy(() -> new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, prepared))
                    .hasStackTraceContaining("new admission is closed");
            long attempts = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_part_attempts WHERE operation_id=:o")
                    .setParameter("o", claim.key().operationId()).getSingleResult()).longValue());
            assertThat(attempts).isZero();
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3, 4})
    void cancellationAndWrongIdentityCannotInventOrRemoveDrain(int checkpoint) {
        try (var c = context(POSTGRES)) {
            var value = input(c); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            assertThatThrownBy(() -> RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, UUID.randomUUID(), NONE))
                    .hasMessageContaining("restoring incarnation");
            var count = new AtomicInteger();
            var cancelled = new IllegalStateException("cancelled");
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                public void check() { if (count.incrementAndGet() == checkpoint) throw cancelled; }
            };
            assertThatThrownBy(() -> RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, control)).isSameAs(cancelled);
            long rows = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_drains").getSingleResult()).longValue());
            assertThat(rows).isEqualTo(checkpoint == 4 ? 1 : 0);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            for (var statement : java.util.List.of("DELETE FROM repository_coordinator_drains", "UPDATE repository_coordinator_drains SET incarnation=incarnation"))
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return em.createNativeQuery(statement).executeUpdate(); }))
                        .hasStackTraceContaining("Coordinator drain is immutable");
        }
    }
}
