package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Optional;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.capture;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.count;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.activation;
import static org.assertj.core.api.Assertions.*;

/** Real SQL lifecycle checks; historical source setup uses synthetic provider observations. */
@Testcontainers
class RepositoryHistoricalAttemptRetirementIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void boundedMaintenanceDoesNotStarveLaterReadyGeneration(boolean authorityFailure) throws Exception {
        try (var c = context(POSTGRES); var first = historicalInitial(c)) {
            var firstPlan = installedHistoricalSuccessor(c, first);
            try (var second = HistoricalRetirementFixture.sibling(c, first)) {
                var secondPlan = installedHistoricalSuccessor(c, second);
                var budget = new PayloadBudget(256L * 1024 * 1024);
                var attempts = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 2);
                try (var firstSources = capture(c, first); var secondSources = capture(c, second);
                     var firstRoot = firstSources.sources().work(); var firstWorker = firstRoot.fork();
                     var secondRoot = secondSources.sources().work(); var secondWorker = secondRoot.fork()) {
                    try (var one = attempts.beginInstalled(CALLER, firstPlan, first.record());
                         var two = attempts.beginInstalled(CALLER, secondPlan, second.record())) {
                        one.attachSources(firstSources.sources(), firstRoot, NONE);
                        one.openExecution(CALLER, NONE);
                        two.attachSources(secondSources.sources(), secondRoot, NONE);
                        two.openExecution(CALLER, NONE);
                        for (var plan : java.util.List.of(firstPlan, secondPlan)) {
                            var owner = c.tx().inTransaction(em -> {
                                var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), plan.next().command().sha256(),
                                        plan.reservation().predecessor().epoch() + 1, plan.reservation().successorToken());
                                return RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(), plan.next().predecessorGeneration() + 1,
                                        plan.next().seeds().ownerNonce(), Optional.of(claim));
                            });
                            new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, plan.next().command(), NONE);
                        }
                        assertThat(one.retireTerminal(CALLER, Duration.ZERO, NONE)).isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETAINED);
                        assertThat(two.retireTerminal(CALLER, Duration.ZERO, NONE)).isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETAINED);
                    }
                    secondWorker.close();
                    var lookups = new java.util.ArrayList<RepositoryOperationLedger.Key>();
                    var failure = new IllegalStateException("injected cleanup authority failure");
                    java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> authority = key -> {
                        lookups.add(key);
                        if (authorityFailure && key.equals(firstPlan.next().key())) throw failure;
                        return CALLER;
                    };
                    if (authorityFailure) assertThatThrownBy(() -> attempts.retireReady(1, authority, NONE)).isSameAs(failure);
                    else assertThat(attempts.retireReady(1, authority, NONE)).isZero();
                    assertThat(lookups).containsExactly(firstPlan.next().key());
                    assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 2));
                    assertThat(attempts.retireReady(1, authority, NONE)).isEqualTo(1);
                    assertThat(lookups).containsExactly(firstPlan.next().key(), secondPlan.next().key());
                    assertThat(firstSources.history().isReleased()).isFalse();
                    assertThat(secondSources.history().isReleased()).isTrue();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                    firstWorker.close();
                    assertThat(attempts.retireReady(1, ignored -> CALLER, NONE)).isEqualTo(1);
                    assertThat(firstSources.history().isReleased()).isTrue();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
                    assertThat(budget.reservedBytes()).isZero();
                } finally {
                    attempts.close();
                    assertThat(attempts.detachClosed(Duration.ofSeconds(1), ignored -> CALLER, NONE)).isTrue();
                }
            }
        }
    }

    @org.junit.jupiter.api.Test
    void cancellationAfterTerminalProofRetainsDisposalOnlyEntryUntilWorkerExits() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var attempts = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var later = capture(c, rig); var root = later.sources().work(); var worker = root.fork()) {
                try (var attempt = attempts.beginInstalled(CALLER, plan, rig.record())) {
                    attempt.attachSources(later.sources(), root, NONE);
                    attempt.openExecution(CALLER, NONE);
                    var owner = c.tx().inTransaction(em -> {
                        var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), plan.next().command().sha256(),
                                plan.reservation().predecessor().epoch() + 1, plan.reservation().successorToken());
                        return RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(), plan.next().predecessorGeneration() + 1,
                                plan.next().seeds().ownerNonce(), Optional.of(claim));
                    });
                    new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, plan.next().command(), NONE);
                    try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                        var retiring = executor.submit(() -> attempt.retireTerminal(CALLER, Duration.ofSeconds(30), control));
                        try {
                            // Root closure follows terminal proof. The held child prevents disposal
                            // from completing; do not guess timing from control-check counts.
                            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                            boolean rootClosed = false;
                            while (!rootClosed && System.nanoTime() < deadline) {
                                try (var probe = root.fork()) {
                                    Thread.sleep(5);
                                } catch (IllegalStateException closed) {
                                    assertThat(closed).hasMessage("Historical source work has ended");
                                    rootClosed = true;
                                }
                            }
                            assertThat(rootClosed).as("Retirement reached post-proof source drainage").isTrue();
                            cancelled.set(true);
                            assertThatThrownBy(() -> retiring.get(5, java.util.concurrent.TimeUnit.SECONDS))
                                    .hasCauseInstanceOf(RepositoryException.class)
                                    .hasStackTraceContaining("Document read cancelled");
                        } finally { cancelled.set(true); }
                    }
                    assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(1, 1));
                    assertThat(budget.reservedBytes()).isPositive();
                    assertThat(later.history().isReleased()).isFalse();
                    assertThat(count(c, "repository_preparation_capture_drains")).isZero();
                    assertThatThrownBy(() -> attempt.start(Duration.ofMinutes(5), NONE))
                            .isInstanceOf(RepositoryException.class).hasMessageContaining("disposal only");
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                worker.close();
                try (var resumed = attempts.resume(CALLER, plan.next().command()).orElseThrow()) {
                    assertThat(resumed.retireTerminal(CALLER, Duration.ZERO, NONE))
                            .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
                assertThat(budget.reservedBytes()).isZero();
                assertThat(later.history().isReleased()).isTrue();
                assertThat(rig.history().isReleased()).isFalse();
                assertThat(count(c, "repository_preparation_capture_drains")).isEqualTo(1);
            }
            attempts.close();
            assertThat(attempts.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
        }
    }

    @org.junit.jupiter.api.Test
    void retiringOneOperationLeavesAnotherKeyAndSharedHistoricalSourceUsable() throws Exception {
        try (var c = context(POSTGRES); var first = historicalInitial(c)) {
            var firstPlan = installedHistoricalSuccessor(c, first);
            try (var second = HistoricalRetirementFixture.sibling(c, first)) {
                var secondPlan = installedHistoricalSuccessor(c, second);
                var budget = new PayloadBudget(256L * 1024 * 1024);
                var attempts = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 2);
                try (var firstSources = capture(c, first); var secondSources = capture(c, second)) {
                    try (var one = attempts.beginInstalled(CALLER, firstPlan, first.record());
                         var two = attempts.beginInstalled(CALLER, secondPlan, second.record())) {
                        one.attachSources(firstSources.sources(), firstSources.sources().work(), NONE);
                        one.openExecution(CALLER, NONE);
                        two.attachSources(secondSources.sources(), secondSources.sources().work(), NONE);
                        two.openExecution(CALLER, NONE);
                        var owner = c.tx().inTransaction(em -> {
                            var claim = RepositoryExecutionClaimLedger.lockLive(em, firstPlan.next().key(), firstPlan.next().command().sha256(),
                                    firstPlan.reservation().predecessor().epoch() + 1, firstPlan.reservation().successorToken());
                            return RepositoryOperationLedger.lockLiveOwner(em, firstPlan.next().key(), firstPlan.next().predecessorGeneration() + 1,
                                    firstPlan.next().seeds().ownerNonce(), Optional.of(claim));
                        });
                        new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, firstPlan.next().command(), NONE);
                        assertThat(one.retireTerminal(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                        assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(1, 1));
                        assertThat(firstSources.history().isReleased()).isTrue();
                        assertThat(secondSources.history().isReleased()).isFalse();
                        assertThat(budget.reservedBytes()).isPositive();
                        assertThat(two.start(Duration.ofMinutes(5), NONE)).isNotNull();
                        assertThat(two.retireTerminal(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.NOT_PROVEN);
                    }
                    assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                    try (var resumed = attempts.resume(CALLER, secondPlan.next().command()).orElseThrow()) {
                        assertThat(resumed.start(Duration.ofMinutes(5), NONE)).isNotNull();
                    }
                    attempts.close();
                    assertThat(attempts.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                    assertThat(budget.reservedBytes()).isZero();
                    assertThat(first.history().isReleased()).isFalse();
                    assertThat(second.history().isReleased()).isFalse();
                }
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void revokedCredentialCannotProveTerminalButCannotStrandAlreadyProvenCleanup(boolean revokeBeforeProof) throws Exception {
        try (var c = context(POSTGRES)) {
            var binding = new RepositoryCredentialBinding("historical-retirement-test", UUID.randomUUID(), 1);
            var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of(), Optional.of(binding));
            var authorities = new RepositoryCredentialAuthorities(c.tx());
            authorities.register(CALLER, binding, caller.principalName());
            var grants = new RepositoryCreationGrants(c.tx(), new DriveLedger(c.tx()));
            try (var rig = historicalCreationInitial(c, record -> grants.install(CALLER, RepositoryCreationGrants.prepare(
                    caller, record.command(), record.placements(), (System.currentTimeMillis() + 300_000) * 1000)))) {
                var security = ai.protomolt.proto.repo.v1.DocumentSecurity.newBuilder()
                        .addPermissions(ai.protomolt.proto.repo.v1.AccessRule.newBuilder().setIdentityType("public")
                                .setIdentity("public").setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_READ)).build();
                var policy = com.google.protobuf.util.JsonFormat.printer().print(security);
                c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                        .setParameter("policy", policy).setParameter("node", ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(rig.fixture().address()))
                        .executeUpdate(); });
                var plan = installedHistoricalSuccessor(c, rig);
                var budget = new PayloadBudget(256L * 1024 * 1024);
                var attempts = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
                try (var later = capture(c, rig, caller)) {
                    try (var root = later.sources().work(); var worker = root.fork()) {
                        try (var attempt = attempts.beginInstalled(caller, plan, rig.record())) {
                            attempt.attachSources(later.sources(), root, NONE);
                            attempt.openExecution(CALLER, NONE);
                            var owner = c.tx().inTransaction(em -> {
                                var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), plan.next().command().sha256(),
                                        plan.reservation().predecessor().epoch() + 1, plan.reservation().successorToken());
                                return RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(), plan.next().predecessorGeneration() + 1,
                                        plan.next().seeds().ownerNonce(), Optional.of(claim));
                            });
                            new DocumentPublicationRejections(c.tx()).cancel(caller, owner, plan.next().command(), NONE);
                            assertThatThrownBy(() -> attempt.retireFenced(caller, Duration.ZERO, NONE))
                                    .isInstanceOfSatisfying(RepositoryException.class,
                                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
                            if (revokeBeforeProof) authorities.revoke(CALLER, binding, caller.principalName());
                            if (revokeBeforeProof) {
                                assertThatThrownBy(() -> attempt.retireTerminal(CALLER, Duration.ZERO, NONE))
                                        .isInstanceOfSatisfying(RepositoryException.class,
                                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.UNAUTHENTICATED));
                            } else {
                                assertThat(attempt.retireTerminal(CALLER, Duration.ZERO, NONE))
                                        .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETAINED);
                                authorities.revoke(CALLER, binding, caller.principalName());
                            }
                            assertThat(budget.reservedBytes()).isPositive();
                            assertThat(later.history().isReleased()).isFalse();
                        }
                    }
                    assertThatThrownBy(() -> new DocumentPublicationReplay(c.tx()).observe(caller, plan.next().command()))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.UNAUTHENTICATED));
                    if (!revokeBeforeProof) {
                        try (var resumed = attempts.resume(caller, plan.next().command()).orElseThrow()) {
                            assertThat(resumed.retireTerminal(CALLER, Duration.ZERO, NONE))
                                    .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                        }
                        assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
                    } else {
                        assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                    }
                    attempts.close();
                    assertThat(attempts.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                    assertThat(budget.reservedBytes()).isZero();
                    assertThat(later.history().isReleased()).isTrue();
                }
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void realSuccessorReservationFencesOldEntryAndLaterTerminalPreservesModes(boolean laterTerminal) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(5));
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var attempts = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var secondSources = capture(c, rig);
                 var attempt = attempts.beginInstalled(CALLER, second, rig.record())) {
                attempt.attachSources(secondSources.sources(), secondSources.sources().work(), NONE);
                attempt.openExecution(CALLER, NONE);
                c.tx().readOnly(em -> em.createNativeQuery("""
                        SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                        FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        WHERE c.operation_id=:id
                        """).setParameter("id", second.next().key().operationId()).getSingleResult());
                assertThat(attempt.retireFenced(CALLER, Duration.ZERO, NONE))
                        .as("Expiry alone is not permanent fencing")
                        .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.NOT_PROVEN);
                var identity = new RepositoryCoordinatorDrain.Identity(second.next().key(), second.next().command().sha256(),
                        second.reservation().predecessor().epoch() + 1, second.reservation().successorToken(),
                        second.reservation().successorIncarnation());
                var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(identity,
                        UUID.randomUUID(), UUID.randomUUID(), Duration.ofMinutes(5),
                        new RepositoryCoordinatorReservation.OwnerIdentity(second.next().predecessorGeneration() + 1,
                                second.next().seeds().ownerNonce()));
                RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, reservation, NONE);
                if (laterTerminal) {
                    var member = second.next().command().intent().getMembers(0).getMemberId();
                    var changedModes = RepositorySuccessorInstall.prepare(reservation, second.next(), Duration.ofMinutes(5),
                            Map.of(member, DocumentPublicationCandidate.Mode.OPAQUE));
                    assertThat(changedModes.modes()).isNotEqualTo(second.modes());
                    assertThatThrownBy(() -> RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, changedModes, NONE))
                            .hasStackTraceContaining("Successor modes differ from predecessor");
                    var third = RepositorySuccessorInstall.prepare(reservation, second.next(), Duration.ofMinutes(5), second.modes());
                    RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, third, NONE);
                    try (var thirdSources = capture(c, rig)) {
                        var thirdCapture = activation(c.tx(), c, rig, third, thirdSources).activate(CALLER, CALLER, NONE);
                        var owner = c.tx().inTransaction(em -> {
                            var claim = RepositoryExecutionClaimLedger.lockLive(em, third.next().key(), third.next().command().sha256(),
                                    reservation.predecessor().epoch() + 1, reservation.successorToken());
                            return RepositoryOperationLedger.lockLiveOwner(em, third.next().key(),
                                    third.next().predecessorGeneration() + 1, third.next().seeds().ownerNonce(), Optional.of(claim));
                        });
                        new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, third.next().command(), NONE);
                        assertThat(attempt.retireTerminal(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                        assertThat(thirdSources.history().isReleased()).isFalse();
                        assertThat(thirdCapture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                    }
                } else {
                    assertThat(attempt.retireFenced(CALLER, Duration.ZERO, NONE))
                            .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
                assertThat(secondSources.history().isReleased()).isTrue();
                assertThat(rig.history().isReleased()).isFalse();
                assertThat(budget.reservedBytes()).isZero();
            }
            attempts.close();
            assertThat(attempts.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void terminalRetirementRequiresProofAndActualWorkerDrain(boolean holdWorker, boolean loseReply) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var attempts = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var later = capture(c, rig)) {
                DocumentHistoricalAssessmentSources.Work held = null;
                try {
                    try (var attempt = attempts.beginInstalled(CALLER, plan, rig.record())) {
                        var root = later.sources().work();
                        attempt.attachSources(later.sources(), root, NONE);
                        attempt.openExecution(CALLER, NONE);
                        assertThat(attempt.retireTerminal(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.NOT_PROVEN);
                        assertThat(attempt.retireFenced(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.NOT_PROVEN);
                        // Failed proof must leave the live entry executable.
                        attempt.start(Duration.ofMinutes(5), NONE);
                        if (holdWorker) held = root.fork();
                        var owner = c.tx().inTransaction(em -> {
                            var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(),
                                    plan.next().command().sha256(), plan.reservation().predecessor().epoch() + 1,
                                    plan.reservation().successorToken());
                            return RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(),
                                    plan.next().predecessorGeneration() + 1, plan.next().seeds().ownerNonce(), Optional.of(claim));
                        });
                        if (loseReply) {
                            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
                            var fault = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                                if (count(c, "repository_operation_rejection") == 1 && armed.compareAndSet(true, false))
                                    throw new java.sql.SQLException("retirement terminal reply lost", "08006");
                            });
                            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                                    "hibernate.connection.datasource", fault, "hibernate.hbm2ddl.auto", "validate"))) {
                                assertThatThrownBy(() -> new DocumentPublicationRejections(new Tx(emf))
                                        .cancel(CALLER, owner, plan.next().command(), NONE))
                                        .hasStackTraceContaining("retirement terminal reply lost");
                            }
                            assertThat(armed).isFalse();
                            assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, plan.next().command()).state())
                                    .isEqualTo(DocumentPublicationReplay.State.TERMINATED);
                        } else {
                            assertThat(new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, plan.next().command(), NONE).state())
                                    .isEqualTo(DocumentPublicationReplay.State.TERMINATED);
                        }
                        assertThat(attempt.retireTerminal(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(holdWorker ? RepositoryInstalledHistoricalAttempts.Retirement.RETAINED
                                        : RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                        if (holdWorker) {
                            assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(1, 1));
                            assertThat(budget.reservedBytes()).isPositive();
                            assertThat(later.history().isReleased()).isFalse();
                            assertThat(count(c, "repository_preparation_capture_drains")).isZero();
                            assertThatThrownBy(() -> attempt.start(Duration.ofMinutes(5), NONE))
                                    .isInstanceOf(RepositoryException.class).hasMessageContaining("disposal only");
                        } else {
                            assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
                        }
                    }
                } finally { if (held != null) held.close(); }
                if (holdWorker) {
                    assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                    try (var resumed = attempts.resume(CALLER, plan.next().command()).orElseThrow()) {
                        assertThat(resumed.retireTerminal(CALLER, Duration.ZERO, NONE))
                                .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                    }
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
                assertThat(attempts.resume(CALLER, plan.next().command())).isEmpty();
                assertThat(count(c, "repository_preparation_capture_drains")).isEqualTo(1);
                assertThat(later.history().isReleased()).isTrue();
                assertThat(rig.history().isReleased()).isFalse();
                assertThat(budget.reservedBytes()).isZero();
                attempts.close();
                assertThat(attempts.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            }
        }
    }
}
