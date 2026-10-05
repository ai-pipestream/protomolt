package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL recovery; provider ports fail if this ownership-only path attempts I/O. */
@Testcontainers
class DocumentPublicationRecoveryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void shutdownWaitsForSqlCallsAndPreservesUncertainRecovery(boolean recovery) throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var ledger = new RepositoryOperationLedger(c.tx());
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && (!recovery || ledger.find(input.key()).orElseThrow().generation() == 2)
                        && armed.compareAndSet(true, false)) {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) throw new java.sql.SQLException("Shutdown test gate timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new java.sql.SQLException("Shutdown test gate interrupted", interrupted);
                    }
                    throw new java.sql.SQLException("Shutdown test commit acknowledgment lost", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf));
                    var executor = Executors.newSingleThreadExecutor()) {
                var sessions = resources.sessions();
                assertThatThrownBy(() -> sessions.awaitIdle(Duration.ZERO)).isInstanceOf(IllegalStateException.class);
                if (recovery) { pending(sessions, input); expire(c, input); }
                armed.set(true);
                var running = executor.submit(() -> {
                    if (recovery) return sessions.recover(CALLER, input.command(), input.placements(), 1,
                            input.modes(), RepositoryReadControl.NONE);
                    return Optional.of(sessions.execute(CALLER, input.command(), input.placements(), Map.of(), Map.of(),
                            input.modes(), Optional.empty(), (member, occurrence) -> {
                                throw new AssertionError("SQL gate must precede schema resolution");
                            }, RepositoryReadControl.NONE));
                });
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    sessions.close();
                    sessions.close();
                    assertThat(sessions.awaitIdle(Duration.ZERO)).isFalse();
                    assertThat(sessions.awaitIdle(Duration.ofMillis(10))).isFalse();
                    assertThatThrownBy(() -> sessions.awaitIdle(Duration.ofNanos(-1))).isInstanceOf(IllegalArgumentException.class);
                    assertClosed(() -> sessions.execute(CALLER, input.command(), Map.of(), Map.of(), Map.of(),
                            input.modes(), Optional.empty(), (member, occurrence) -> {
                                throw new AssertionError("Closed registry must not resolve schemas");
                            }, RepositoryReadControl.NONE));
                    assertClosed(() -> sessions.recover(CALLER, input.command(), Map.of(), 1, input.modes(), RepositoryReadControl.NONE));
                    assertClosed(() -> sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE));
                    // Observation before session creation also keeps SQL resources alive.
                    assertThat(sessions.retainedSessions()).isEqualTo(recovery ? 1 : 0);
                } finally { release.countDown(); }
                assertThatThrownBy(() -> running.get(10, TimeUnit.SECONDS))
                        .hasStackTraceContaining("Shutdown test commit acknowledgment lost");
                assertThat(sessions.awaitIdle(Duration.ofSeconds(1))).isTrue();
                assertThat(sessions.retainedSessions()).isEqualTo(recovery ? 1 : 0);
                assertThat(sessions.retainedCommandBytes()).isEqualTo(recovery ? input.bytes() : 0);
                assertThat(ledger.find(input.key()).orElseThrow().generation()).isEqualTo(recovery ? 2 : 1);
            }
        }
    }

    private static void assertClosed(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(RepositoryException.class,
                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void uncertainTakeoverKeepsExclusiveTransitionAndExactRetryIdentity(boolean cancel) throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var armed = new AtomicBoolean();
            var cancelled = new AtomicBoolean();
            var ledger = new RepositoryOperationLedger(c.tx());
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && ledger.find(input.key()).orElseThrow().generation() == 2
                        && armed.compareAndSet(true, false)) {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) throw new java.sql.SQLException("Recovery test gate timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new java.sql.SQLException("Recovery test gate interrupted", interrupted);
                    }
                    if (cancel) cancelled.set(true);
                    else throw new java.sql.SQLException("Recovery commit acknowledgment lost", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf));
                    var executor = Executors.newSingleThreadExecutor()) {
                var sessions = resources.sessions();
                pending(sessions, input);
                expire(c, input);
                armed.set(true);
                var control = new RepositoryReadControl() {
                    @Override public long remainingNanos() { return Long.MAX_VALUE; }
                    @Override public boolean isCancelled() { return cancelled.get(); }
                };
                var recovering = executor.submit(() -> sessions.recover(CALLER, input.command(), input.placements(), 1,
                        input.modes(), control));
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    assertThatThrownBy(() -> sessions.execute(CALLER, input.command(), Map.of(), Map.of(), Map.of(), input.modes(),
                            Optional.empty(), (member, occurrence) -> { throw new AssertionError("No schema I/O during recovery"); },
                            RepositoryReadControl.NONE)).isInstanceOfSatisfying(RepositoryException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                    assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), Map.of(), 1, input.modes(), RepositoryReadControl.NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                    assertThatThrownBy(() -> sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                } finally { release.countDown(); }
                if (cancel) assertThatThrownBy(() -> recovering.get(10, TimeUnit.SECONDS))
                        .cause().isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                else assertThatThrownBy(() -> recovering.get(10, TimeUnit.SECONDS)).hasStackTraceContaining("Recovery commit acknowledgment lost");
                cancelled.set(false);
                var stored = ledger.find(input.key()).orElseThrow();
                var token = c.tx().readOnly(em -> em.createNativeQuery("SELECT owner_token FROM repository_operation_owners WHERE operation_id=:id")
                        .setParameter("id", input.command().operationId()).getSingleResult());
                assertThat(stored.generation()).isEqualTo(2);
                assertThat(sessions.retainedSessions()).isEqualTo(1);
                assertThat(sessions.recover(CALLER, input.command(), Map.of(), 1, input.modes(), RepositoryReadControl.NONE)).isEmpty();
                assertThat(ledger.find(input.key())).contains(stored);
                var retriedToken = c.tx().readOnly(em -> em.createNativeQuery("SELECT owner_token FROM repository_operation_owners WHERE operation_id=:id")
                        .setParameter("id", input.command().operationId()).getSingleResult());
                assertThat(retriedToken).isEqualTo(token);
                assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), Map.of(), 2, input.modes(), RepositoryReadControl.NONE))
                        .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
                assertThat(sessions.retainedCommandBytes()).isEqualTo(input.bytes());
            }
        }
    }

    @Test void advancesOnlyAfterExpiryAndReconcilesThirdGenerationAcknowledgmentLoss() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var ledger = new RepositoryOperationLedger(c.tx());
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && ledger.find(input.key()).orElseThrow().generation() == 3
                        && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("Third generation acknowledgment lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf), Duration.ofSeconds(5))) {
                var sessions = resources.sessions();
                expire(c, input);
                assertThat(sessions.recover(CALLER, input.command(), input.placements(), 1, input.modes(), RepositoryReadControl.NONE)).isEmpty();
                var second = ledger.find(input.key()).orElseThrow();
                var secondToken = token(c, input);
                assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), input.placements(), 2, input.modes(), RepositoryReadControl.NONE))
                        .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
                assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), input.placements(), 3, input.modes(), RepositoryReadControl.NONE))
                        .hasMessageContaining("predecessor changed");
                assertThat(ledger.find(input.key())).contains(second);
                assertThat(token(c, input)).isEqualTo(secondToken);
                // A refused advance has not replaced the retained nonce: exact retry still succeeds.
                assertThat(sessions.recover(CALLER, input.command(), Map.of(), 1, input.modes(), RepositoryReadControl.NONE)).isEmpty();
                expire(c, input);
                assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), Map.of(), 2, input.modes(), RepositoryReadControl.NONE))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThat(token(c, input)).isEqualTo(secondToken);
                armed.set(true);
                assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), input.placements(), 2, input.modes(), RepositoryReadControl.NONE))
                        .hasStackTraceContaining("Third generation acknowledgment lost");
                assertThat(armed).isFalse();
                var third = ledger.find(input.key()).orElseThrow();
                var thirdToken = token(c, input);
                assertThat(third.generation()).isEqualTo(3);
                assertThat(thirdToken).isNotEqualTo(secondToken);
                assertThat(sessions.recover(CALLER, input.command(), Map.of(), 2, input.modes(), RepositoryReadControl.NONE)).isEmpty();
                assertThat(ledger.find(input.key())).contains(third);
                assertThat(token(c, input)).isEqualTo(thirdToken);
                assertThat(sessions.retainedSessions()).isEqualTo(1);
                assertThat(sessions.retainedCommandBytes()).isEqualTo(input.bytes());
            }
        }
    }

    @Test void doesNotAdvanceARecoveryNonceThatNeverBecameTheOwner() {
        try (var c = context(POSTGRES); var resources = resources(c.tx())) {
            var input = input(c);
            var sessions = resources.sessions();
            assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), input.placements(), 1, input.modes(), RepositoryReadControl.NONE))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            assertThat(sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE)).isFalse();
            assertThat(sessions.retainedSessions()).isEqualTo(1);
            expire(c, input);
            var ledger = new RepositoryOperationLedger(c.tx());
            var other = ledger.takeOver(input.key(), input.command(), 1, UUID.randomUUID(), Duration.ofSeconds(1));
            expire(c, input);
            assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), input.placements(), 2, input.modes(), RepositoryReadControl.NONE))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            assertThat(token(c, input)).isEqualTo(other.token());
            assertThat(ledger.find(input.key()).orElseThrow().generation()).isEqualTo(2);
            assertThat(sessions.retainedCommandBytes()).isEqualTo(input.bytes());
            assertThat(sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE)).isTrue();
            assertThat(sessions.retainedSessions()).isZero();
            assertThat(sessions.retainedCommandBytes()).isZero();
            assertThat(token(c, input)).isEqualTo(other.token());
        }
    }

    @Test void observationDoesNotGrantOwnershipWhenAnotherHostWinsBeforeTakeover() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var ledger = new RepositoryOperationLedger(c.tx());
            var armed = new AtomicBoolean();
            var commits = new java.util.concurrent.atomic.AtomicInteger();
            var winner = new java.util.concurrent.atomic.AtomicReference<RepositoryOperationLedger.Owner>();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                // Replay commits first; the second commit releases the exact expired-owner observation lock.
                if (armed.get() && commits.incrementAndGet() == 2 && armed.compareAndSet(true, false)) {
                    winner.set(ledger.takeOver(input.key(), input.command(), 2, UUID.randomUUID(), LEASE));
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf), Duration.ofSeconds(1))) {
                var sessions = resources.sessions();
                expire(c, input);
                assertThat(sessions.recover(CALLER, input.command(), input.placements(), 1, input.modes(), RepositoryReadControl.NONE)).isEmpty();
                expire(c, input);
                armed.set(true);
                assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), input.placements(), 2, input.modes(), RepositoryReadControl.NONE))
                        .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
                assertThat(armed).isFalse();
                assertThat(winner.get()).isNotNull();
                assertThat(winner.get().generation()).isEqualTo(3);
                assertThat(token(c, input)).isEqualTo(winner.get().token());
                assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), Map.of(), 2, input.modes(), RepositoryReadControl.NONE))
                        .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
                assertThat(token(c, input)).isEqualTo(winner.get().token());
                assertThat(sessions.retainedSessions()).isEqualTo(1);
            }
        }
    }

    @Test void failedPreparationPreservesPriorModesAndReturnsNewReservations() {
        try (var c = context(POSTGRES); var resources = resources(c.tx())) {
            var input = input(c);
            var sessions = resources.sessions();
            assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), Map.of(), 1, input.modes(), RepositoryReadControl.NONE))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(sessions.retainedSessions()).isZero();
            assertThat(sessions.retainedCommandBytes()).isZero();
            pending(sessions, input);
            assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), Map.of(), 1, input.modes(), RepositoryReadControl.NONE))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(sessions.retainedSessions()).isEqualTo(1);
            var opaque = new java.util.HashMap<>(input.modes());
            opaque.replaceAll((member, mode) -> DocumentPublicationCandidate.Mode.OPAQUE);
            assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), input.placements(), 1, opaque, RepositoryReadControl.NONE))
                    .hasMessageContaining("modes changed");
            assertThat(new RepositoryOperationLedger(c.tx()).find(input.key()).orElseThrow().generation()).isEqualTo(1);
            expire(c, input);
            assertThat(sessions.recover(CALLER, input.command(), input.placements(), 1, input.modes(), RepositoryReadControl.NONE)).isEmpty();
            assertThat(sessions.retainedCommandBytes()).isEqualTo(input.bytes());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void failedSupersessionObservationRetainsCapacityAndRetryIdentity(boolean cancel) {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var armed = new AtomicBoolean();
            var cancelled = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) {
                    if (cancel) cancelled.set(true);
                    else throw new java.sql.SQLException("Supersession observation acknowledgment lost", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf))) {
                var sessions = resources.sessions();
                pending(sessions, input);
                var stored = new RepositoryOperationLedger(c.tx()).find(input.key());
                var ownerToken = token(c, input);
                var control = new RepositoryReadControl() {
                    @Override public long remainingNanos() { return Long.MAX_VALUE; }
                    @Override public boolean isCancelled() { return cancelled.get(); }
                };
                armed.set(true);
                if (cancel) assertThatThrownBy(() -> sessions.retireSuperseded(CALLER, input.command(), control))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                else assertThatThrownBy(() -> sessions.retireSuperseded(CALLER, input.command(), control))
                        .hasStackTraceContaining("Supersession observation acknowledgment lost");
                assertThat(armed).isFalse();
                assertThat(sessions.retainedCommandBytes()).isEqualTo(input.bytes());
                assertThat(sessions.retainedSessions()).isEqualTo(1);
                cancelled.set(false);
                assertThat(sessions.retireSuperseded(CALLER, input.command(), control)).isTrue();
                assertThat(sessions.retainedCommandBytes()).isZero();
                assertThat(sessions.retainedSessions()).isZero();
                assertThat(sessions.retireSuperseded(CALLER, input.command(), control)).isFalse();
                assertThat(new RepositoryOperationLedger(c.tx()).find(input.key())).isEqualTo(stored);
                assertThat(token(c, input)).isEqualTo(ownerToken);
                pending(sessions, input); // Retirement grants neither admission nor takeover.
            }
        }
    }

    @Test void supersessionExcludesSameSessionButDoesNotHoldTheRegistryDuringSql() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var otherCommand = new DocumentPublicationCommand(input.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            var other = new Input(otherCommand, input.placements(), input.modes());
            new RepositoryOperationLedger(c.tx()).admit(other.key(), other.command(), UUID.randomUUID(), LEASE);
            var armed = new AtomicBoolean();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) throw new java.sql.SQLException("Retirement test gate timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new java.sql.SQLException("Retirement test gate interrupted", interrupted);
                    }
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf)); var executor = Executors.newSingleThreadExecutor()) {
                var sessions = resources.sessions();
                pending(sessions, input);
                armed.set(true);
                var retirement = executor.submit(() -> sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE));
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    pending(sessions, input);
                    assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), input.placements(), 1, input.modes(), RepositoryReadControl.NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                    assertThatThrownBy(() -> sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                    pending(sessions, other);
                    assertThat(sessions.retainedCommandBytes()).isEqualTo(input.bytes() + other.bytes());
                } finally { release.countDown(); }
                assertThat(retirement.get(10, TimeUnit.SECONDS)).isTrue();
                assertThat(sessions.retainedSessions()).isEqualTo(1);
                assertThat(sessions.retainedCommandBytes()).isEqualTo(other.bytes());
            }
        }
    }

    @Test void supersessionRequiresDurableProofAndTheSameCommand() {
        try (var c = context(POSTGRES); var resources = resources(c.tx(), Duration.ofSeconds(1))) {
            var input = input(c);
            var ledger = new RepositoryOperationLedger(c.tx());
            var missing = new DocumentPublicationCommand(input.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            var missingSession = new DocumentPublicationSession(c.tx(), CALLER, missing, input.placements(), LEASE);
            assertThat(missingSession.isSuperseded(CALLER, RepositoryReadControl.NONE)).isFalse();
            expire(c, input);
            var sessions = resources.sessions();
            sessions.recover(CALLER, input.command(), input.placements(), 1, input.modes(), RepositoryReadControl.NONE);
            assertThat(sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE)).isFalse();
            expire(c, input);
            assertThat(sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE)).isFalse();
            var owner = ledger.takeOver(input.key(), input.command(), 2, UUID.randomUUID(), LEASE);
            var changed = new DocumentPublicationCommand(input.command().intent().toBuilder()
                    .setMembers(0, input.command().intent().getMembers(0).toBuilder().setMemberId("changed")).build());
            assertThatThrownBy(() -> sessions.retireSuperseded(CALLER, changed, RepositoryReadControl.NONE))
                    .hasMessageContaining("command changed");
            assertThatThrownBy(() -> ledger.isSuperseded(input.key(), changed, 2, UUID.randomUUID()))
                    .isInstanceOf(RepositoryOperationLedger.CommandConflictException.class);
            assertThat(sessions.retireSuperseded(new RepositoryCaller("other", true), input.command(), RepositoryReadControl.NONE)).isFalse();
            assertThat(sessions.retainedCommandBytes()).isEqualTo(input.bytes());
            assertThat(sessions.retireSuperseded(CALLER, input.command(), RepositoryReadControl.NONE)).isTrue();
            assertThat(sessions.retainedCommandBytes()).isZero();
            assertThat(token(c, input)).isEqualTo(owner.token());
        }
    }

    @Test void terminalCancellationEvictsOnlyAfterAuthorizedReplayAndNeverOpensProviders() {
        try (var c = context(POSTGRES); var resources = resources(c.tx())) {
            var input = input(c, LEASE);
            var sessions = resources.sessions();
            pending(sessions, input);
            var snapshot = new RepositoryOperationLedger(c.tx()).find(input.key()).orElseThrow();
            var owner = new RepositoryOperationLedger.Owner(input.key(), snapshot.generation(), token(c, input), snapshot.leaseUntil());
            var receipt = new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, input.command(), RepositoryReadControl.NONE)
                    .rejection().orElseThrow();
            assertThat(sessions.retainedSessions()).isEqualTo(1);
            for (int i = 0; i < 2; i++) {
                assertThatThrownBy(() -> sessions.execute(CALLER, input.command(), Map.of(), Map.of(), Map.of(), Map.of(), Optional.empty(),
                        (member, occurrence) -> { throw new AssertionError("Terminal replay must not resolve a schema"); }, RepositoryReadControl.NONE))
                        .isInstanceOfSatisfying(DocumentPublicationReplay.Terminated.class, failure -> assertThat(failure.receipt()).isEqualTo(receipt));
                assertThat(sessions.retainedSessions()).isZero();
                assertThat(sessions.retainedCommandBytes()).isZero();
            }
            assertThatThrownBy(() -> sessions.recover(CALLER, input.command(), Map.of(), 1, Map.of(), RepositoryReadControl.NONE))
                    .isInstanceOf(DocumentPublicationReplay.Terminated.class);
            assertThat(token(c, input)).isEqualTo(owner.token());
        }
    }

    private record Input(DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements,
            Map<String, DocumentPublicationCandidate.Mode> modes) {
        RepositoryOperationLedger.Key key() { return new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId()); }
        long bytes() { return (long) command.canonical().size() + command.intent().getSerializedSize(); }
    }

    private static Input input(Context c) {
        return input(c, Duration.ofSeconds(1));
    }

    private static Input input(Context c, Duration ownerLease) {
        var seed = prepare(c, 2, true);
        var command = new DocumentPublicationCommand(seed.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var id = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var drive = new DriveLedger(c.tx()).findById(id).orElseThrow();
        var profile = new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow();
        var modes = new java.util.HashMap<String, DocumentPublicationCandidate.Mode>();
        command.intent().getMembersList().forEach(member -> modes.put(member.getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
        new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId()),
                command, UUID.randomUUID(), ownerLease);
        return new Input(command, Map.of(id, DocumentUploadPlan.Placement.sample(drive, "native-test", profile)), Map.copyOf(modes));
    }

    private static void pending(DocumentPublicationSessions sessions, Input input) {
        assertThatThrownBy(() -> sessions.execute(CALLER, input.command(), input.placements(), Map.of(), Map.of(), input.modes(), Optional.empty(),
                (member, occurrence) -> { throw new AssertionError("Pending contender must not resolve schemas"); }, RepositoryReadControl.NONE))
                .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CONFLICT));
    }

    private static void expire(Context c, Input input) {
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02) FROM repository_operation_owners WHERE operation_id=:id")
                .setParameter("id", input.command().operationId()).getSingleResult());
    }

    private static UUID token(Context c, Input input) {
        return c.tx().readOnly(em -> (UUID) em.createNativeQuery("SELECT owner_token FROM repository_operation_owners WHERE operation_id=:id")
                .setParameter("id", input.command().operationId()).getSingleResult());
    }

    private record Resources(DocumentUploadCoordinator uploads, DocumentPublicationSessions sessions) implements AutoCloseable {
        @Override public void close() { uploads.close(); }
    }

    private static Resources resources(Tx tx) {
        return resources(tx, LEASE);
    }

    private static Resources resources(Tx tx, Duration lease) {
        var drives = new DriveLedger(tx);
        var budget = new PayloadBudget(8_000_000);
        var uploads = new DocumentUploadCoordinator(tx, drives, budget,
                (generation, profile) -> { throw new AssertionError("Recovery must not open a byte provider"); },
                2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5)));
        var execution = new DocumentPublicationExecution(tx, drives, new DocumentReadLedger(tx, UUID.randomUUID()), uploads,
                (plan, member, control) -> { throw new AssertionError("Recovery must not read retained bytes"); }, budget,
                new DocumentRevisionAssembly.Limits(1_000_000, 100, 100, 100, 100_000), false);
        return new Resources(uploads, new DocumentPublicationSessions(tx, execution, lease, 2, 4_000_000));
    }
}
