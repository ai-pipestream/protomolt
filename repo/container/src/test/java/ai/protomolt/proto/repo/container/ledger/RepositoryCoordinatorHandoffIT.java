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

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void exactRetryAfterSuccessorExpiryDoesNotRenewOrOpenExecution(boolean migrateExisting) {
        try (var c = migrateExisting ? context(POSTGRES, "95") : context(POSTGRES)) {
            var initial = input(c); var incarnation = UUID.randomUUID();
            var value = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(),
                    initial.placements(), Duration.ofSeconds(1), 0);
            // A V95 schema is seeded by its own era's preparation writer, never the V103+ journal.
            var claim = LegacyPublicationPreparationFixture.acquireInitial(c.tx(), new PayloadBudget(64_000_000), CALLER, value,
                    UUID.randomUUID(), incarnation, NONE);
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), incarnation);
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            var proposal = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), Duration.ofSeconds(1));
            assertThatThrownBy(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE))
                    .hasStackTraceContaining("expired predecessor");
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var stamp = RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE);
            var legacyState = retainedState(c, value.key().operationId());
            assertThat(((Number) legacyState[6]).longValue()).isEqualTo(1);
            if (migrateExisting) {
                assertThat(LegacyPublicationPreparationFixture.schemaVersion(c.tx())).isEqualTo(95);
                var schema = c.pool().getSchema();
                org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                        .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
                assertThat(LegacyPublicationPreparationFixture.schemaVersion(c.tx()))
                        .isGreaterThanOrEqualTo(LegacyPublicationPreparationFixture.HISTORY_SETS_VERSION);
                // V103 deliberately leaves existing preparations unindexed: migration invents no history set.
                assertThat(count(c, "repository_preparation_history_sets")).isZero();
            }
            // Migration keeps the stored claim/owner identity, leases and preparation row intact.
            Object[] migratedState = retainedState(c, value.key().operationId());
            assertThat(migratedState).containsExactly(legacyState);
            assertThat(((Number) migratedState[6]).longValue()).isEqualTo(1);
            var reservation = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT kind,predecessor_remote_state,successor_token,successor_incarnation,recorded_at
                    FROM repository_coordinator_reservations WHERE operation_id=:o
                    """).setParameter("o", value.key().operationId()).getSingleResult());
            assertThat(reservation).containsExactly("GRACEFUL", "UNKNOWN", proposal.successorToken(),
                    proposal.successorIncarnation(), stamp);
            // The parent is evidence-backed, not an alternate reservation entry point.
            for (String mutation : java.util.List.of(
                    "jsonb_build_object('operation_id', gen_random_uuid())",
                    "jsonb_build_object('successor_token', gen_random_uuid())",
                    "jsonb_build_object('recorded_at', clock_timestamp()+interval '1 second')")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    em.createNativeQuery("INSERT INTO repository_coordinator_reservations SELECT "
                            + "(jsonb_populate_record(NULL::repository_coordinator_reservations,to_jsonb(r)||" + mutation + ")).* "
                            + "FROM repository_coordinator_reservations r").executeUpdate();
                })).hasStackTraceContaining("requires exact source evidence");
            }
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
            for (String sql : java.util.List.of("DELETE FROM repository_coordinator_reservations",
                    "UPDATE repository_coordinator_reservations SET recorded_at=clock_timestamp()"))
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("reservation is immutable");
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

    @Test void parentPublicationFailureRollsBackClaimAndHandoff() {
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
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION fail_reservation_publication() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN RAISE EXCEPTION 'injected reservation publication failure'; END; $$
                        """).executeUpdate();
                em.createNativeQuery("""
                        CREATE TRIGGER fail_reservation_publication AFTER INSERT ON repository_coordinator_reservations
                        FOR EACH ROW EXECUTE FUNCTION fail_reservation_publication()
                        """).executeUpdate();
            });
            assertThatThrownBy(() -> RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE))
                    .hasStackTraceContaining("injected reservation publication failure");
            var retained = c.tx().readOnly(em -> (Object[]) em.createNativeQuery(
                    "SELECT claim_epoch,claim_token FROM repository_execution_claims WHERE operation_id=:id")
                    .setParameter("id", value.key().operationId()).getSingleResult());
            assertThat(((Number) retained[0]).longValue()).isEqualTo(1);
            assertThat(retained[1]).isEqualTo(claim.token());
            for (var table : java.util.List.of("repository_coordinator_handoffs", "repository_coordinator_reservations")) {
                int count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table)
                        .getSingleResult()).intValue());
                assertThat(count).isZero();
            }
            c.tx().inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER fail_reservation_publication ON repository_coordinator_reservations").executeUpdate();
            });
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

    @Test void reservedSuccessorCannotUseExistingRecoveryEntryPoints() {
        try (var c = context(POSTGRES)) {
            var initial = input(c); var incarnation = UUID.randomUUID();
            var value = new DocumentPublicationPreparationRecord(initial.key(), initial.command(), initial.seeds(),
                    initial.placements(), Duration.ofSeconds(1), 0);
            var budget = new PayloadBudget(64_000_000);
            var preparations = new DocumentPublicationPreparationJournal(c.tx(), budget);
            var modes = new DocumentPublicationModesJournal(c.tx(), budget);
            var operations = new RepositoryOperationLedger(c.tx());
            var claim = preparations.acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            modes.bind(CALLER, claim, 0, MODES, NONE);
            var owner = operations.admit(value.key(), value.command(), value.seeds().ownerNonce(), Duration.ofSeconds(1), claim)
                    .owner().orElseThrow();
            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), 1, claim.token(), incarnation);
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var proposal = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), LEASE);
            RepositoryCoordinatorHandoff.reserve(c.tx(), CALLER, proposal, NONE);
            var successor = new RepositoryExecutionClaimLedger(c.tx()).takeOver(value.key(), value.command(),
                    claim.epoch(), proposal.successorToken(), LEASE);
            assertThat(successor.epoch()).isEqualTo(2);
            var fresh = new DocumentPublicationPreparationRecord(value.key(), value.command(),
                    DocumentPublicationSeeds.mint(value.key(), value.command()), value.placements(), LEASE, owner.generation());
            assertThatThrownBy(() -> preparations.save(CALLER, successor, fresh, NONE)).hasStackTraceContaining("locally drained");
            assertThatThrownBy(() -> modes.bind(CALLER, successor, owner.generation(), MODES, NONE)).hasStackTraceContaining("locally drained");
            assertThatThrownBy(() -> operations.takeOver(value.key(), value.command(), owner.generation(),
                    fresh.seeds().ownerNonce(), LEASE, successor)).hasStackTraceContaining("locally drained");
            long generation = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT owner_generation FROM repository_operation_owners WHERE operation_id=:o")
                    .setParameter("o", value.key().operationId()).getSingleResult()).longValue());
            assertThat(generation).isEqualTo(owner.generation());
            for (String table : java.util.List.of("repository_publication_preparations", "repository_publication_modes")) {
                long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                        + " WHERE operation_id=:o AND predecessor_generation=1")
                        .setParameter("o", value.key().operationId()).getSingleResult()).longValue());
                assertThat(count).isZero();
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    /** Stable scalar/hex snapshot of the claim, owner lease and exact preparation bytes across migration. */
    private static Object[] retainedState(Context c, UUID operation) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,o.owner_generation,o.owner_token,o.lease_until,
                 (SELECT count(*) FROM repository_publication_preparations p WHERE p.operation_id=c.operation_id),
                 p.account_id::text,p.principal,p.operation_id::text,p.predecessor_generation,p.owner_nonce::text,
                 p.command_codec,p.command_version,encode(p.command_bytes,'hex'),encode(p.command_sha256,'hex'),
                 encode(p.preparation_bytes,'hex'),encode(p.preparation_sha256,'hex')
                FROM repository_execution_claims c
                LEFT JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                JOIN repository_publication_preparations p USING(account_id,principal,operation_id)
                WHERE c.operation_id=:o
                """).setParameter("o", operation).getSingleResult());
    }
}
