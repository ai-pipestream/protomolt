package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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

/** Private SQL attestation/fence tests; these do not simulate or attest host worker quiescence. */
@Testcontainers
class RepositoryCoordinatorLocalDrainIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void localDrainClosesSettlementButPreservesRecoveryFenceAndSchemaRetention() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            var operations = new RepositoryOperationLedger(c.tx());
            var owner = operations.admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim).owner().orElseThrow();
            var attempts = new DocumentOperationUploadAdmission(c.tx(),new DriveLedger(c.tx())).admit(CALLER,owner,
                    DocumentOperationUploadAdmission.prepare(value.command(),value.placements(),
                            java.util.Map.of("member-0",UUID.randomUUID()),LEASE));
            var attempt = attempts.getFirst();
            var selections = java.util.List.of(new DocumentSelectedAttemptLedger.Selected("member-0",1,attempt.id(),attempt.token()));
            var selected = new DocumentSelectedAttemptLedger(c.tx());
            selected.renewOwnerAndSelections(owner,selections,LEASE);
            var descriptor = ai.protomolt.proto.descriptors.DescriptorFingerprints.closure(com.google.protobuf.StringValue.getDescriptor()).toByteString();
            new RepositorySchemaArtifacts(c.tx()).stage(owner, value.command(), java.util.List.of(descriptor), () -> {});
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            assertThat(operations.renew(owner, LEASE).token()).isEqualTo(owner.token());
            var identity = identity(claim, incarnation);
            var stamp = RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            assertThat(RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE)).isEqualTo(stamp);
            var leaseQuery = "SELECT c.lease_until,o.lease_until,a.lease_until FROM repository_execution_claims c "
                    + "JOIN repository_operation_owners o USING(account_id,principal,operation_id) "
                    + "JOIN document_part_attempts a ON a.operation_id=o.operation_id WHERE a.attempt_id=:id";
            Object[] before = c.tx().readOnly(em -> (Object[]) em.createNativeQuery(leaseQuery)
                    .setParameter("id",attempt.id()).getSingleResult());
            assertThatThrownBy(() -> selected.renewOwnerAndSelections(owner,selections,LEASE))
                    .hasStackTraceContaining("locally drained");
            Object[] after = c.tx().readOnly(em -> (Object[]) em.createNativeQuery(leaseQuery)
                    .setParameter("id",attempt.id()).getSingleResult());
            assertThat(after).containsExactly(before);
            assertThatThrownBy(() -> operations.renew(owner, LEASE)).hasStackTraceContaining("locally drained");
            assertThatThrownBy(() -> new RepositoryExecutionClaimLedger(c.tx()).renew(claim, LEASE)).hasStackTraceContaining("locally drained");
            assertThatThrownBy(() -> new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, value.command(), NONE))
                    .hasStackTraceContaining("locally drained");
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT fence_repository_operation_recovery(:a,:p,:o)")
                        .setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                        .setParameter("o", value.key().operationId()).getSingleResult();
                int released = ((Number) em.createNativeQuery("SELECT release_repository_replaced_schema_claims(:a,:p,:o,10)")
                        .setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                        .setParameter("o", value.key().operationId()).getSingleResult()).intValue();
                assertThat(released).as("current-generation schema claims remain protected").isZero();
            });
            for (String verb : java.util.List.of("DELETE FROM", "UPDATE")) {
                String sql = verb.equals("UPDATE") ? "UPDATE repository_coordinator_local_drains SET recorded_at=clock_timestamp()"
                        : "DELETE FROM repository_coordinator_local_drains";
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("Coordinator local drain is immutable");
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void expiredExactIdentityCanAttestButTransferredIdentityCannot(boolean transferFirst) {
        try (var c = context(POSTGRES)) {
            var initial = input(c); var incarnation = UUID.randomUUID(); var budget = new PayloadBudget(64_000_000);
            var value = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(), initial.placements(), Duration.ofSeconds(1), 0);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var identity = identity(claim, incarnation);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var claims = new RepositoryExecutionClaimLedger(c.tx());
            if (transferFirst) {
                claims.takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE);
                assertThatThrownBy(() -> RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE))
                        .hasStackTraceContaining("original current claim");
                assertThat(RepositoryCoordinatorLocalDrain.confirm(c.tx(), CALLER, identity, NONE)).isEmpty();
            } else {
                var before = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                        .setParameter("o", value.key().operationId()).getSingleResult());
                var stamp = RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
                Object after = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                        .setParameter("o", value.key().operationId()).getSingleResult());
                assertThat(after).isEqualTo(before);
                var successor = claims.takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE);
                assertThat(RepositoryCoordinatorLocalDrain.confirm(c.tx(), CALLER, identity, NONE)).contains(stamp);
                assertThat(RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE)).isEqualTo(stamp);
                assertThatThrownBy(() -> claims.renew(successor, LEASE)).hasStackTraceContaining("locally drained");
                assertThatThrownBy(() -> new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, successor, 0, MODES, NONE))
                        .hasStackTraceContaining("locally drained");
            }
        }
    }

    @Test void requiresAdmissionClosureAndExactPrivateIdentity() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            var identity = identity(claim, incarnation);
            assertThatThrownBy(() -> RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE))
                    .hasStackTraceContaining("exact coordinator admission closure");
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var scoped = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
            assertThatThrownBy(() -> RepositoryCoordinatorLocalDrain.record(c.tx(), scoped, identity, NONE))
                    .hasMessageContaining("private process authority");
            assertThatThrownBy(() -> RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity(claim, UUID.randomUUID()), NONE))
                    .hasStackTraceContaining("exact coordinator admission closure");
            var wrong = new RepositoryCoordinatorDrain.Identity(value.key(), "0".repeat(64), 1, claim.token(), incarnation);
            assertThatThrownBy(() -> RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, wrong, NONE))
                    .hasStackTraceContaining("original current claim");
            assertThat(RepositoryCoordinatorLocalDrain.confirm(c.tx(), CALLER, identity, NONE)).isEmpty();
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            for (var incorrect : java.util.List.of(wrong, identity(claim, UUID.randomUUID()),
                    new RepositoryCoordinatorDrain.Identity(value.key(), claim.commandSha256(), 1, UUID.randomUUID(), incarnation))) {
                assertThatThrownBy(() -> RepositoryCoordinatorLocalDrain.confirm(c.tx(), CALLER, incorrect, NONE))
                        .hasMessageContaining("differs from original binding");
            }
        }
    }

    @Test void earlierFenceInSameTransactionCannotAuthorizeWritesAfterAttestation() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                insert(em, identity(claim, incarnation));
                em.createNativeQuery("SELECT require_repository_execution_claim(:a,:p,:o)")
                        .setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                        .setParameter("o", claim.key().operationId()).getSingleResult();
            })).hasStackTraceContaining("locally drained");
            assertThat(RepositoryCoordinatorLocalDrain.confirm(c.tx(), CALLER, identity(claim, incarnation), NONE)).isEmpty();
        }
    }

    @Test void upgradePreservesExistingDrainingOperationUntilExplicitAttestation() {
        try (var c = context(POSTGRES, "90")) {
            var value = input(c); var incarnation = UUID.randomUUID();
            var claim = LegacyPublicationPreparationFixture.acquire(c.tx(), value, UUID.randomUUID(), incarnation);
            var draining = RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            String schema = c.pool().getSchema();
            org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
            var identity = identity(claim, incarnation);
            assertThat(RepositoryCoordinatorLocalDrain.confirm(c.tx(), CALLER, identity, NONE)).isEmpty();
            assertThat(RepositoryCoordinatorDrain.confirm(c.tx(), CALLER, claim, incarnation, NONE)).contains(draining);
            assertThat(new RepositoryExecutionClaimLedger(c.tx()).renew(claim, LEASE).token()).isEqualTo(claim.token());
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            assertThatThrownBy(() -> new RepositoryExecutionClaimLedger(c.tx()).renew(claim, LEASE)).hasStackTraceContaining("locally drained");
        }
    }

    @Test void renewalWaitingBehindLocalDrainCommitIsRefused() throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var value = input(c); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var entered = new java.util.concurrent.CountDownLatch(1); var release = new java.util.concurrent.CountDownLatch(1);
            var armed = new AtomicBoolean(true); var blocker = new AtomicInteger();
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!armed.get()) return;
                try (var statement = connection.createStatement(); var rows = statement.executeQuery(
                        "SELECT pg_backend_pid() WHERE EXISTS(SELECT 1 FROM repository_coordinator_local_drains)")) {
                    if (!rows.next() || !armed.compareAndSet(true, false)) return;
                    blocker.set(rows.getInt(1));
                }
                entered.countDown();
                try { if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new java.sql.SQLException("commit release timeout"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.sql.SQLException(interrupted); }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var drain = workers.submit(() -> RepositoryCoordinatorLocalDrain.record(new Tx(emf), CALLER, identity(claim, incarnation), NONE));
                try {
                    assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    var renewal = workers.submit(() -> new RepositoryExecutionClaimLedger(c.tx()).renew(claim, LEASE));
                    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                    boolean blocked = false;
                    while (System.nanoTime() < deadline) {
                        blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                                SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))
                                AND wait_event_type='Lock' AND query LIKE '%fence_repository_execution_claim%')
                                """).setParameter("blocker", blocker.get()).getSingleResult());
                        if (blocked) break;
                        Thread.sleep(10);
                    }
                    assertThat(blocked).as("renewal waits on the actual attestation transaction").isTrue();
                    release.countDown();
                    drain.get(5, java.util.concurrent.TimeUnit.SECONDS);
                    assertThatThrownBy(() -> renewal.get(5, java.util.concurrent.TimeUnit.SECONDS)).hasStackTraceContaining("locally drained");
                } finally { release.countDown(); }
            }
        }
    }

    private static void insert(jakarta.persistence.EntityManager em, RepositoryCoordinatorDrain.Identity identity) {
        em.createNativeQuery("""
                INSERT INTO repository_coordinator_local_drains(account_id,principal,operation_id,claim_epoch,claim_token,incarnation,command_sha256)
                VALUES(:a,:p,:o,:e,:t,:i,:d)
                """).setParameter("a", identity.key().account()).setParameter("p", identity.key().principal())
                .setParameter("o", identity.key().operationId()).setParameter("e", identity.epoch())
                .setParameter("t", identity.token()).setParameter("i", identity.incarnation())
                .setParameter("d", java.util.HexFormat.of().parseHex(identity.commandSha256())).executeUpdate();
    }

    @Test void realLostCommitReplyIsConfirmedExactly() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var armed = new AtomicBoolean(true);
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_local_drains").getSingleResult()).longValue());
                if (count == 1 && armed.compareAndSet(true, false)) throw new java.sql.SQLException("local drain reply lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var stamp = RepositoryCoordinatorLocalDrain.record(new Tx(emf), CALLER, identity(claim, incarnation), NONE);
                assertThat(armed).isFalse();
                assertThat(RepositoryCoordinatorLocalDrain.confirm(c.tx(), CALLER, identity(claim, incarnation), NONE)).contains(stamp);
            }
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3, 4, 5, 6})
    void cancellationNeverBecomesAnAttestationGrant(int checkpoint) {
        try (var c = context(POSTGRES)) {
            var value = input(c); var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64_000_000))
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var calls = new AtomicInteger();
            var failure = new RepositoryException(RepositoryException.Code.CANCELLED, "cancel local drain");
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                public void check() { if (calls.incrementAndGet() >= checkpoint) throw failure; }
            };
            assertThat(catchThrowable(() -> RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity(claim, incarnation), control)))
                    .isSameAs(failure);
            assertThat(RepositoryCoordinatorLocalDrain.confirm(c.tx(), CALLER, identity(claim, incarnation), NONE).isPresent())
                    .isEqualTo(checkpoint == 6);
        }
    }

    private static RepositoryCoordinatorDrain.Identity identity(RepositoryExecutionClaimLedger.Claim claim, UUID incarnation) {
        return new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), claim.epoch(), claim.token(), incarnation);
    }
}
