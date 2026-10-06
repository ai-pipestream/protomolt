package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL activation/manager lifecycle. Provider publication is a separate qualification. */
@Testcontainers
class DocumentSuccessorManagerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void exactRetryAndOrdinaryExecutionReuseSuccessor() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c.tx(), 2, 1_000_000)) {
            var plan = installed(c, r.sessions());
            r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE);
            var before = identity(c, plan);
            r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE);
            assertThat(r.sessions().retainedSessions()).isEqualTo(1);
            assertThat(identity(c, plan)).containsExactly(before);
            assertThatThrownBy(() -> execute(r.sessions(), plan)).isInstanceOf(DocumentSchemaPolicies.StalePolicy.class)
                    .hasMessage("No active schema policy for account");
            assertThat(identity(c, plan)).containsExactly(before);
            assertThat(r.sessions().retainedSessions()).isEqualTo(1);
            var changed = RepositorySuccessorInstall.prepare(plan.reservation(), plan.previous(), LEASE, MODES);
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, changed, NONE))
                    .hasMessageContaining("proposal changed");
            assertThat(count(c)).isEqualTo(1);
        }
    }

    @Test void failedActivationRetainsIdentityAndExactRetryAttaches() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c.tx(), 2, 1_000_000)) {
            var plan = plan(c, r.sessions());
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE))
                    .hasMessageContaining("install is not committed");
            assertThat(r.sessions().retainedSessions()).isEqualTo(1);
            assertThat(count(c)).isZero();
            RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, plan, NONE);
            r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE);
            assertThat(count(c)).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void failedActivationRetiresOnlyAfterReplacementOwnerIsInstalled(boolean beforeInstall) throws Exception {
        try (var c = context(POSTGRES); var r = resources(c.tx(), 1, 1_000_000);
             var other = resources(c.tx(), 1, 1_000_000)) {
            assertThat(r.sessions().coordinatorIdentity()).isNotEqualTo(other.sessions().coordinatorIdentity());
            var shortLease = Duration.ofSeconds(1);
            var reserved = RepositorySuccessorInstallIT.plan(c, input(c), shortLease, r.sessions().coordinatorIdentity());
            var first = RepositorySuccessorInstall.prepare(reserved.reservation(), reserved.previous(), shortLease, MODES);
            var command = first.next().command();
            if (beforeInstall) assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, first, NONE))
                    .hasMessageContaining("install is not committed");
            RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, first, NONE);
            if (!beforeInstall) {
                var denied = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
                assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, denied, first, NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            }
            assertThat(r.sessions().retainedSessions()).isEqualTo(1);
            assertThat(count(c)).isZero();
            c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                     (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id",command.operationId()).getSingleResult());
            assertThat(r.sessions().retireSuperseded(CALLER, command, NONE)).isFalse();
            var timeouts = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));
            var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), timeouts)
                    .inspect(CALLER, first.next().key(), command.sha256(), NONE).unactivated().orElseThrow();
            var proposal = new RepositoryCoordinatorReservation.SupersededUnactivated(observed.predecessor(),
                    UUID.randomUUID(), other.sessions().coordinatorIdentity(), LEASE, observed.owner(),
                    observed.preparationSha256(), observed.installation());
            RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, proposal, NONE);
            // Claim transfer alone leaves the old owner in place. It is not retirement proof.
            assertThat(r.sessions().retireSuperseded(CALLER, command, NONE)).isFalse();
            try (var loaded = new RepositoryReservedPreparation(c.tx(), r.budget(), timeouts)
                    .load(CALLER, CALLER, proposal, proposal.owner(), NONE)) {
                var replacement = RepositorySuccessorInstall.prepare(proposal, loaded.record(), LEASE, loaded.modes());
                RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, replacement, NONE);
                assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, replacement, NONE))
                        .hasMessageContaining("incarnation");
                var durable = identity(c, replacement);
                assertThat(r.sessions().retireSuperseded(CALLER, command, NONE)).isTrue();
                assertThat(r.sessions().retainedSessions()).isZero();
                assertThat(r.sessions().retainedCommandBytes()).isZero();
                assertThat(identity(c, replacement)).containsExactly(durable);
                assertThat(count(c)).isZero();
                other.sessions().activateSuccessor(CALLER, CALLER, replacement, NONE);
                assertThat(other.sessions().retainedSessions()).isEqualTo(1);
                assertThat(count(c)).isEqualTo(1);
                assertThat(r.sessions().retireSuperseded(CALLER, command, NONE)).isFalse();
                assertThat(other.sessions().retireSuperseded(CALLER, command, NONE)).isFalse();
                var unrelated = anotherInstalled(c, r.sessions(), first.previous());
                r.sessions().activateSuccessor(CALLER, CALLER, unrelated, NONE);
                assertThat(r.sessions().retainedSessions()).isEqualTo(1);
                assertThat(count(c)).isEqualTo(2);
            }
        }
    }

    @Test void capacityAndClosedManagerRejectBeforeActivation() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c.tx(), 1, 1_000_000)) {
            var first = installed(c, r.sessions());
            r.sessions().activateSuccessor(CALLER, CALLER, first, NONE);
            var second = anotherInstalled(c, r.sessions(), first.previous());
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, second, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
            assertThat(count(c)).isEqualTo(1);
            r.sessions().close();
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, second, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
            assertThat(count(c)).isEqualTo(1);
        }
    }

    @Test void managerIdentityAndCurrentCallerRightsAreRequired() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c.tx(), 2, 1_000_000)) {
            var foreign = RepositorySuccessorInstallIT.plan(c);
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, foreign, NONE))
                    .hasMessageContaining("incarnation");
            assertThat(r.sessions().retainedSessions()).isZero();
            var plan = anotherInstalled(c, r.sessions(), foreign.previous());
            var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> r.sessions().activateSuccessor(scoped, CALLER, plan, NONE))
                    .hasMessageContaining("private process authority");
            assertThat(r.sessions().retainedSessions()).isZero();
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, scoped, plan, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThat(count(c)).isZero();
            assertThat(r.sessions().retainedSessions()).isEqualTo(1);
        }
    }

    @Test void lostCommitAcknowledgmentFindsAlreadyRetainedSession() throws Exception {
        try (var c = context(POSTGRES)) {
            var armed = new AtomicBoolean(true);
            var manager = new AtomicReference<DocumentPublicationSessions>();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c) == 1 && armed.compareAndSet(true, false)) {
                    assertThat(manager.get().retainedSessions()).isEqualTo(1);
                    throw new java.sql.SQLException("activation reply lost", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                 var r = resources(new Tx(emf), 2, 1_000_000)) {
                manager.set(r.sessions());
                var plan = installed(c, r.sessions());
                r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE);
                assertThat(armed).isFalse();
                assertThat(count(c)).isEqualTo(1);
                assertThat(r.sessions().retainedSessions()).isEqualTo(1);
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void shutdownWaitsForAcceptedActivationThenDrainsRefusedAttachment(boolean cancel) throws Exception {
        try (var c = context(POSTGRES); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var committed = new CountDownLatch(1); var release = new CountDownLatch(1);
            var armed = new AtomicBoolean(true); var cancelled = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c) == 1 && armed.compareAndSet(true, false)) {
                    committed.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("release timeout");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt(); throw new AssertionError(interrupted);
                    }
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                 var r = resources(new Tx(emf), 2, 1_000_000)) {
                var plan = installed(c, r.sessions());
                var control = new RepositoryReadControl() {
                    public boolean isCancelled() { return cancelled.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                var future = workers.submit(() -> r.sessions().activateSuccessor(CALLER, CALLER, plan, control));
                try {
                    assertThat(committed.await(10, TimeUnit.SECONDS)).isTrue();
                    assertThat(r.sessions().retainedSessions()).isEqualTo(1);
                    assertThatThrownBy(() -> execute(r.sessions(), plan)).hasMessageContaining("in progress");
                    assertThatThrownBy(() -> r.sessions().retireClaimFenced(CALLER,plan.next().command(),NONE))
                            .hasMessageContaining("in use");
                    assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE).registrationsIdle()).isFalse();
                    assertThat(r.sessions().awaitIdle(Duration.ZERO)).isFalse();
                    cancelled.set(cancel);
                } finally { release.countDown(); }
                assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS)).isInstanceOfSatisfying(ExecutionException.class,
                        failure -> assertThat(failure.getCause()).isInstanceOfSatisfying(RepositoryException.class,
                                cause -> assertThat(cause.code()).isEqualTo(cancel ? RepositoryException.Code.CANCELLED
                                        : RepositoryException.Code.UNAVAILABLE)));
                assertThat(r.sessions().awaitIdle(Duration.ofSeconds(1))).isTrue();
                var drained = r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE);
                assertThat(drained.registrationsIdle()).isTrue();
                assertThat(drained.confirmed()).isEqualTo(1);
                assertThat(drained.unresolved()).isZero();
                r.sessions().attestLocalDrain(key -> CALLER, NONE);
                int drains = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM repository_coordinator_drains WHERE claim_epoch=2").getSingleResult()).intValue());
                assertThat(drains).isEqualTo(1);
            }
        }
    }

    @Test void serializedCommandCapacityRefusesBeforeActivation() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c.tx(), 2, 1)) {
            var plan = installed(c, r.sessions());
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
            assertThat(count(c)).isZero();
            assertThat(r.sessions().retainedSessions()).isZero();
            assertThat(r.sessions().retainedCommandBytes()).isZero();
        }
    }

    private static void execute(DocumentPublicationSessions sessions, RepositorySuccessorInstall.Plan plan) throws Exception {
        sessions.execute(CALLER, plan.next().command(), Map.of(), Map.of(), Map.of(), plan.modes(), Optional.empty(),
                (member, control) -> { throw new AssertionError("Missing policy must stop resolution"); }, NONE);
    }
    private static RepositorySuccessorInstall.Plan plan(Context c, DocumentPublicationSessions sessions) {
        return RepositorySuccessorInstallIT.plan(c, input(c), LEASE, sessions.coordinatorIdentity());
    }
    private static RepositorySuccessorInstall.Plan installed(Context c, DocumentPublicationSessions sessions) {
        var plan = plan(c, sessions);
        RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64_000_000), CALLER, plan, NONE);
        return plan;
    }
    private static RepositorySuccessorInstall.Plan anotherInstalled(Context c, DocumentPublicationSessions sessions,
            DocumentPublicationPreparationRecord source) {
        var command = new DocumentPublicationCommand(source.command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(source.key().account(), source.key().principal(), command.operationId());
        var input = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                source.placements(), LEASE, 0);
        var plan = RepositorySuccessorInstallIT.plan(c, input, LEASE, sessions.coordinatorIdentity());
        RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64_000_000), CALLER, plan, NONE);
        return plan;
    }
    private static DocumentJournaledSessionsIT.Resources resources(Tx tx, int capacity, long bytes) {
        return DocumentJournaledSessionsIT.resources(tx, capacity, bytes, LEASE, new PayloadBudget(64_000_000));
    }
    private static int count(Context c) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_successor_executions")
                .getSingleResult()).intValue());
    }
    private static Object[] identity(Context c, RepositorySuccessorInstall.Plan plan) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,o.owner_generation,o.owner_token,o.lease_until
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE operation_id=:id
                """).setParameter("id", plan.next().key().operationId()).getSingleResult());
    }
}
