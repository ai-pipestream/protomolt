package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Private target ownership and per-operation SQL/drain identities, without provider I/O. */
@Testcontainers
class DocumentSuccessorTargetIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));

    @Test void independentTargetsKeepFixedSessionsAndDrainTheirOwnIncarnations() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c, 3); var foreign = resources(c, 1)) {
            var fixed = r.sessions().coordinatorIdentity();
            var source = input(c);
            var ordinary = installed(c, r, source, fixed);
            var firstInput = another(source); var secondInput = another(source);
            var first = r.sessions().successorTarget(firstInput.key(), firstInput.command().sha256(), fixed);
            var second = r.sessions().successorTarget(secondInput.key(), secondInput.command().sha256(), fixed);
            assertThat(first.incarnation()).isNotEqualTo(second.incarnation()).isNotEqualTo(fixed);
            var firstPlan = installed(c, r, firstInput, first.incarnation());
            var secondPlan = installed(c, r, secondInput, second.incarnation());
            var mismatchedDigest = r.sessions().successorTarget(firstInput.key(), "f".repeat(64), fixed);
            var mismatchedIncarnation = r.sessions().successorTarget(firstInput.key(), firstInput.command().sha256(), fixed);
            assertThatThrownBy(() -> foreign.sessions().activateSuccessor(first, CALLER, CALLER, firstPlan, NONE))
                    .hasMessageContaining("target differs");
            assertThatThrownBy(() -> r.sessions().activateSuccessor(first, CALLER, CALLER, secondPlan, NONE))
                    .hasMessageContaining("target differs");
            assertThatThrownBy(() -> r.sessions().activateSuccessor(mismatchedDigest, CALLER, CALLER, firstPlan, NONE))
                    .hasMessageContaining("target differs");
            assertThatThrownBy(() -> r.sessions().activateSuccessor(mismatchedIncarnation, CALLER, CALLER, firstPlan, NONE))
                    .hasMessageContaining("target differs");
            assertThat(count(c, "repository_successor_executions")).isZero();
            assertThat(r.sessions().retainedSessions()).isZero();
            assertThat(foreign.sessions().retainedSessions()).isZero();
            r.sessions().activateSuccessor(CALLER, CALLER, ordinary, NONE);
            r.sessions().activateSuccessor(first, CALLER, CALLER, firstPlan, NONE);
            r.sessions().activateSuccessor(second, CALLER, CALLER, secondPlan, NONE);
            r.sessions().activateSuccessor(first, CALLER, CALLER, firstPlan, NONE);
            assertThat(count(c, "repository_successor_executions")).isEqualTo(3);
            assertThat(r.sessions().coordinatorIdentity()).isEqualTo(fixed);
            var attached = RepositorySuccessorExecution.attach(c.tx(), r.budget(), CALLER, firstPlan, NONE);
            assertThatThrownBy(() -> r.sessions().resumeStarted(CALLER, firstInput.command(), attached.owner(), NONE))
                    .hasMessageContaining("restoring incarnation");
            assertThat(r.sessions().retainedSessions()).isEqualTo(3);
            assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE))
                    .isEqualTo(new DocumentPublicationSessions.DrainProgress(true, 3, 0));
            var incarnations = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT incarnation FROM repository_coordinator_drains WHERE claim_epoch=2").getResultList());
            assertThat(incarnations).containsExactlyInAnyOrder(fixed, first.incarnation(), second.incarnation());
            assertThatThrownBy(() -> r.sessions().successorTarget(firstInput.key(), firstInput.command().sha256(), fixed))
                    .hasMessageContaining("closed");
        }
    }

    @Test void freshTargetCanSupersedeSameManagersExpiredUnactivatedSession() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c, 1)) {
            var fixed = r.sessions().coordinatorIdentity();
            var shortLease = Duration.ofSeconds(1);
            var source = RepositorySuccessorInstallIT.plan(c, input(c), shortLease, fixed);
            var first = RepositorySuccessorInstall.prepare(source.reservation(), source.previous(), shortLease, MODES);
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, first, NONE))
                    .hasMessageContaining("install is not committed");
            RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, first, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                     (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id", first.next().key().operationId()).getSingleResult());
            var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                    .inspect(CALLER, first.next().key(), first.next().command().sha256(), NONE).unactivated().orElseThrow();
            var target = r.sessions().successorTarget(first.next().key(), first.next().command().sha256(), observed.predecessor().incarnation());
            assertThat(target.incarnation()).isNotEqualTo(fixed);
            var proposal = new RepositoryCoordinatorReservation.SupersededUnactivated(observed.predecessor(),
                    UUID.randomUUID(), target.incarnation(), LEASE, observed.owner(), observed.preparationSha256(), observed.installation());
            RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, proposal, NONE);
            assertThat(r.sessions().retireSuperseded(CALLER, first.next().command(), NONE)).isFalse();
            try (var loaded = new RepositoryReservedPreparation(c.tx(), r.budget(), TIMEOUTS)
                    .load(CALLER, CALLER, proposal, proposal.owner(), NONE)) {
                var next = RepositorySuccessorInstall.prepare(proposal, loaded.record(), LEASE, loaded.modes());
                RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, next, NONE);
                assertThatThrownBy(() -> r.sessions().activateSuccessor(target, CALLER, CALLER, next, NONE))
                        .hasMessageContaining("proposal changed");
                assertThat(r.sessions().retireSuperseded(CALLER, next.next().command(), NONE)).isTrue();
                r.sessions().activateSuccessor(target, CALLER, CALLER, next, NONE);
                assertThat(r.sessions().retainedSessions()).isEqualTo(1);
                assertThat(r.sessions().coordinatorIdentity()).isEqualTo(fixed);
                assertThat(count(c, "repository_successor_executions")).isEqualTo(1);
            }
        }
    }

    @Test void supersededUnactivatedSessionCanFinishShutdownWithoutInventingDrainMarkers() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c, 1)) {
            var source = RepositorySuccessorInstallIT.plan(c, input(c), Duration.ofSeconds(1),
                    r.sessions().coordinatorIdentity());
            var first = RepositorySuccessorInstall.prepare(source.reservation(), source.previous(), Duration.ofSeconds(1), MODES);
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, first, NONE))
                    .hasMessageContaining("install is not committed");
            RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, first, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                     (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id", first.next().key().operationId()).getSingleResult());
            var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                    .inspect(CALLER, first.next().key(), first.next().command().sha256(), NONE).unactivated().orElseThrow();
            var proposal = new RepositoryCoordinatorReservation.SupersededUnactivated(observed.predecessor(),
                    UUID.randomUUID(), UUID.randomUUID(), LEASE, observed.owner(), observed.preparationSha256(), observed.installation());
            RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, proposal, NONE);
            var originalDrains = count(c, "repository_coordinator_drains");
            var originalLocalDrains = count(c, "repository_coordinator_local_drains");
            var progress = r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE);
            assertThat(progress.registrationsIdle()).isTrue();
            assertThat(progress.confirmed()).isZero();
            assertThat(progress.unresolved()).isZero();
            assertThat(progress.fenced()).isEqualTo(1);
            assertThat(r.sessions().awaitIdle(Duration.ZERO)).isTrue();
            // This registration never activated and this fixture has no provider/schema workers.
            assertThat(r.sessions().attestLocalDrain(key -> CALLER, NONE)).isTrue();
            assertThat(count(c, "repository_coordinator_drains")).isEqualTo(originalDrains);
            assertThat(count(c, "repository_coordinator_local_drains")).isEqualTo(originalLocalDrains);
            assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE)).isEqualTo(progress);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void rolledBackActivationCanStopWithoutWaitingForAnotherCoordinator(boolean lateActivation) throws Exception {
        try (var c = context(POSTGRES)) {
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement();
                     var rows = statement.executeQuery("SELECT count(*) FROM repository_successor_executions")) {
                    rows.next();
                    if (rows.getInt(1) != 0 && armed.compareAndSet(true, false))
                        throw new java.sql.SQLException("Activation rolled back by shutdown test", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                 var r = DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS), 1, 1_000_000,
                         LEASE, new PayloadBudget(128_000_000))) {
                var plan = installed(c, r, input(c), r.sessions().coordinatorIdentity());
                var originalDrains = count(c, "repository_coordinator_drains");
                var originalLocalDrains = count(c, "repository_coordinator_local_drains");
                assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE))
                        .hasStackTraceContaining("Activation rolled back by shutdown test");
                assertThat(count(c, "repository_successor_executions")).isZero();
                var claim = c.tx().readOnly(em -> em.createNativeQuery("""
                        SELECT claim_token FROM repository_execution_claims WHERE operation_id=:id AND lease_until>clock_timestamp()
                        """).setParameter("id", plan.next().key().operationId()).getSingleResult());
                assertThat(claim).isEqualTo(plan.reservation().successorToken());
                var drained = r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE);
                assertThat(drained.registrationsIdle()).isTrue();
                assertThat(drained.unresolved()).isZero();
                assertThat(drained.detached()).isEqualTo(1);
                assertThat(drained.confirmed()).isZero();
                assertThat(drained.fenced()).isZero();
                if (lateActivation) {
                    RepositorySuccessorExecution.activate(c.tx(), r.budget(), CALLER, CALLER, plan, NONE);
                    assertThat(count(c, "repository_successor_executions")).isEqualTo(1);
                }
                assertThat(r.sessions().awaitIdle(Duration.ZERO)).isTrue();
                assertThat(r.sessions().attestLocalDrain(key -> CALLER, NONE)).isTrue();
                assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE)).isEqualTo(drained);
                assertThat(count(c, "repository_coordinator_drains")).isEqualTo(originalDrains);
                assertThat(count(c, "repository_coordinator_local_drains")).isEqualTo(originalLocalDrains);
                assertThat(RepositoryCoordinatorReservation.confirm(c.tx(), CALLER, plan.reservation(), NONE)).isPresent();
            }
        }
    }

    @Test void shutdownProofRequiresExactFingerprintAndPrivateAuthority() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c, 1)) {
            var plan = installed(c, r, input(c), r.sessions().coordinatorIdentity());
            var fingerprint = DocumentSuccessorFingerprint.of(plan);
            var tx = c.tx().withTimeouts(TIMEOUTS);
            var storedModeHash = (byte[]) c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT modes_sha256 FROM repository_successor_installs WHERE operation_id=:id")
                    .setParameter("id", plan.next().key().operationId()).getSingleResult());
            assertThat(storedModeHash).isNotEqualTo(fingerprint.modes().toByteArray());
            assertThat(RepositorySuccessorShutdown.inspect(tx, r.budget(), CALLER, fingerprint, NONE))
                    .isEqualTo(RepositorySuccessorShutdown.State.UNACTIVATED);
            var changed = com.google.protobuf.ByteString.copyFrom(new byte[32]);
            for (var wrong : List.of(
                    new DocumentSuccessorFingerprint(fingerprint.reservation(), changed, fingerprint.next(), fingerprint.modes()),
                    new DocumentSuccessorFingerprint(fingerprint.reservation(), fingerprint.previous(), changed, fingerprint.modes()),
                    new DocumentSuccessorFingerprint(fingerprint.reservation(), fingerprint.previous(), fingerprint.next(), changed))) {
                assertThat(RepositorySuccessorShutdown.inspect(tx, r.budget(), CALLER, wrong, NONE))
                        .isEqualTo(RepositorySuccessorShutdown.State.UNRESOLVED);
            }
            var scoped = new RepositoryCaller(CALLER.principalName(), false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> RepositorySuccessorShutdown.inspect(tx, r.budget(), scoped, fingerprint, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            RepositorySuccessorExecution.activate(tx, r.budget(), CALLER, CALLER, plan, NONE);
            assertThat(RepositorySuccessorShutdown.inspect(tx, r.budget(), CALLER, fingerprint, NONE))
                    .isEqualTo(RepositorySuccessorShutdown.State.ACTIVATED);
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void detachedShutdownRequiresReviewedTakeover(boolean reviewed, boolean classifyBeforeTransfer) throws Exception {
        try (var c = context(POSTGRES); var r = resources(c, 1)) {
            var lease = Duration.ofSeconds(1);
            var source = RepositorySuccessorInstallIT.plan(c, input(c), lease, r.sessions().coordinatorIdentity());
            var plan = RepositorySuccessorInstall.prepare(source.reservation(), source.previous(), lease, MODES);
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE))
                    .hasMessageContaining("install is not committed");
            RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, plan, NONE);
            if (classifyBeforeTransfer)
                assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE).detached()).isEqualTo(1);
            c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                     (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id", plan.next().key().operationId()).getSingleResult());
            if (reviewed) {
                var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                        .inspect(CALLER, plan.next().key(), plan.next().command().sha256(), NONE).unactivated().orElseThrow();
                var proposal = new RepositoryCoordinatorReservation.SupersededUnactivated(observed.predecessor(),
                        UUID.randomUUID(), UUID.randomUUID(), LEASE, observed.owner(), observed.preparationSha256(), observed.installation());
                RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, proposal, NONE);
            } else {
                new RepositoryExecutionClaimLedger(c.tx()).takeOver(plan.next().key(), plan.next().command(),
                        plan.reservation().predecessor().epoch()+1, UUID.randomUUID(), LEASE);
            }
            var oldDrains = count(c, "repository_coordinator_drains");
            var oldLocalDrains = count(c, "repository_coordinator_local_drains");
            var progress = r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE);
            assertThat(r.sessions().awaitIdle(Duration.ZERO)).isTrue();
            assertThat(r.sessions().attestLocalDrain(key -> CALLER, NONE)).isEqualTo(reviewed);
            assertThat(progress.detached()).isEqualTo(reviewed && classifyBeforeTransfer ? 1 : 0);
            assertThat(progress.fenced()).isEqualTo(reviewed && !classifyBeforeTransfer ? 1 : 0);
            assertThat(progress.unresolved()).isEqualTo(reviewed ? 0 : 1);
            assertThat(count(c, "repository_coordinator_drains")).isEqualTo(oldDrains);
            assertThat(count(c, "repository_coordinator_local_drains")).isEqualTo(oldLocalDrains);
        }
    }

    @Test void capacityPressureLeavesShutdownIncompleteUntilBytesAreReleased() throws Exception {
        try (var c = context(POSTGRES); var r = resources(c, 1)) {
            var source = RepositorySuccessorInstallIT.plan(c, input(c), LEASE, r.sessions().coordinatorIdentity());
            var plan = RepositorySuccessorInstall.prepare(source.reservation(), source.previous(), LEASE, MODES);
            assertThatThrownBy(() -> r.sessions().activateSuccessor(CALLER, CALLER, plan, NONE))
                    .hasMessageContaining("install is not committed");
            RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, plan, NONE);
            // Real byte-budget pressure; this fixture does not claim to exercise a provider worker.
            try (var held = r.budget().reserve(r.budget().capacity()-r.budget().reservedBytes())) {
                assertThat(RepositorySuccessorShutdown.inspect(c.tx().withTimeouts(TIMEOUTS), r.budget(), CALLER,
                        DocumentSuccessorFingerprint.of(plan), NONE)).isEqualTo(RepositorySuccessorShutdown.State.CAPACITY_UNAVAILABLE);
                assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE).unresolved()).isEqualTo(1);
            }
            assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE).detached()).isEqualTo(1);
            assertThat(r.sessions().awaitIdle(Duration.ZERO)).isTrue();
            assertThat(r.sessions().attestLocalDrain(key -> CALLER, NONE)).isTrue();
        }
    }

    private static DocumentJournaledSessionsIT.Resources resources(Context c, int capacity) {
        return DocumentJournaledSessionsIT.resources(c.tx().withTimeouts(TIMEOUTS), capacity, 1_000_000,
                LEASE, new PayloadBudget(128_000_000));
    }
    private static DocumentPublicationPreparationRecord another(DocumentPublicationPreparationRecord source) {
        var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(source.key().account(), source.key().principal(), command.operationId());
        return new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), source.placements(), LEASE, 0);
    }
    private static RepositorySuccessorInstall.Plan installed(Context c, DocumentJournaledSessionsIT.Resources r,
            DocumentPublicationPreparationRecord source, UUID incarnation) {
        var plan = RepositorySuccessorInstallIT.plan(c, source, LEASE, incarnation);
        RepositorySuccessorInstall.install(c.tx(), r.budget(), CALLER, plan, NONE);
        return plan;
    }
}
