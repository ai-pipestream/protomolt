package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real registration SQL. Missing policy stops execution before fail-fast provider ports can be used. */
@Testcontainers
class DocumentJournaledSessionsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);

    @Test void acceptedCallCannotRegisterAfterDrainSnapshot() throws Exception {
        try (var c = context(POSTGRES); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var input = input(c);
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var firstCheck = new AtomicBoolean(true);
            var control = new RepositoryReadControl() {
                public boolean isCancelled() {
                    if (firstCheck.compareAndSet(true, false)) {
                        entered.countDown();
                        try {
                            if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("release timeout");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt(); throw new AssertionError(interrupted);
                        }
                    }
                    return false;
                }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var r = resources(c.tx(), 1, input.bytes())) {
                var operation = workers.submit(() -> catchThrowable(() -> execute(r.sessions(), input, input.modes(), control)));
                try {
                    assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(r.sessions().drainRegistrations(Duration.ZERO,
                            key -> { throw new AssertionError("No registered identity"); }, NONE))
                            .isEqualTo(new DocumentPublicationSessions.DrainProgress(true, 0, 0));
                    release.countDown();
                    assertThat(operation.get(5, TimeUnit.SECONDS)).isInstanceOf(RepositoryException.class);
                    assertThat(count(c, "repository_execution_claims", input)).isZero();
                    assertThat(r.sessions().retainedSessions()).isZero();
                    assertThat(r.sessions().retainedCommandBytes()).isZero();
                } finally { release.countDown(); }
            }
        }
    }

    @Test void absentClaimAfterRegistrationFailureRemainsUnresolved() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c); var armed = new AtomicBoolean(true);
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!armed.get()) return;
                try (var statement = connection.prepareStatement("SELECT count(*) FROM repository_coordinator_bindings WHERE operation_id=?")) {
                    statement.setObject(1, input.command().operationId());
                    try (var rows = statement.executeQuery()) {
                        rows.next();
                        if (rows.getInt(1) == 1 && armed.compareAndSet(true, false))
                            throw new java.sql.SQLException("registration commit refused", "08006");
                    }
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var r = resources(new Tx(emf), 1, input.bytes())) {
                assertThatThrownBy(() -> execute(r.sessions(), input, input.modes(), NONE))
                        .hasStackTraceContaining("registration commit refused");
                assertThat(count(c, "repository_execution_claims", input)).isZero();
                assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE))
                        .isEqualTo(new DocumentPublicationSessions.DrainProgress(true, 0, 1));
                assertThat(r.sessions().retainedSessions()).isEqualTo(1);
                assertThat(r.sessions().retainedCommandBytes()).isEqualTo(input.bytes());
                assertThat(count(c, "repository_coordinator_drains", input)).isZero();
            }
        }
    }

    @Test void managerDrainsEveryRetainedRegistrationWithoutEvictingItsIdentity() throws Exception {
        try (var c = context(POSTGRES)) {
            var first = input(c);
            var second = new Input(new DocumentPublicationCommand(first.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build()), first.placements(), first.modes());
            try (var r = resources(c.tx(), 2, first.bytes() + second.bytes())) {
                pending(r.sessions(), first); pending(r.sessions(), second);
                var firstIdentity = identity(c, first); var secondIdentity = identity(c, second);
                var result = r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE);
                assertThat(result).isEqualTo(new DocumentPublicationSessions.DrainProgress(true, 2, 0));
                assertThat(r.uploads().providerActivity().accepting()).isFalse();
                assertThat(count(c, "repository_coordinator_drains", first)).isEqualTo(1);
                assertThat(count(c, "repository_coordinator_drains", second)).isEqualTo(1);
                assertThat(r.sessions().retainedSessions()).isEqualTo(2);
                assertThat(r.sessions().retainedCommandBytes()).isEqualTo(first.bytes() + second.bytes());
                assertIdentity(c, first, firstIdentity); assertIdentity(c, second, secondIdentity);
                assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE)).isEqualTo(result);
            }
        }
    }

    @Test void drainRequiresSuppliedPrivateAuthorityAndRetriesARealLostMarkerReply() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c); var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && count(c, "repository_coordinator_drains", input) == 1 && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("drain reply lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var r = resources(new Tx(emf), 1, input.bytes())) {
                pending(r.sessions(), input);
                var original = identity(c, input);
                var scoped = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
                assertThatThrownBy(() -> r.sessions().drainRegistrations(Duration.ZERO, key -> scoped, NONE))
                        .hasMessageContaining("private process authority");
                assertThat(count(c, "repository_coordinator_drains", input)).isZero();
                assertThat(r.uploads().providerActivity().accepting()).isFalse();
                assertThat(r.sessions().retainedSessions()).isEqualTo(1);
                armed.set(true);
                assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE))
                        .isEqualTo(new DocumentPublicationSessions.DrainProgress(true, 1, 0));
                assertThat(count(c, "repository_coordinator_drains", input)).isEqualTo(1);
                assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE))
                        .isEqualTo(new DocumentPublicationSessions.DrainProgress(true, 1, 0));
                assertIdentity(c, input, original);
            }
        }
    }

    @Test void drainCannotSnapshotWhileRegistrationCommitIsStillHeld() throws Exception {
        try (var c = context(POSTGRES); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var input = input(c); var armed = new AtomicBoolean(true);
            var committing = new CountDownLatch(1); var release = new CountDownLatch(1);
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!armed.get()) return;
                try (var statement = connection.prepareStatement("SELECT count(*) FROM repository_coordinator_bindings WHERE operation_id=?")) {
                    statement.setObject(1, input.command().operationId());
                    try (var rows = statement.executeQuery()) {
                        rows.next();
                        if (rows.getInt(1) == 0 || !armed.compareAndSet(true, false)) return;
                    }
                }
                committing.countDown();
                try { if (!release.await(10, TimeUnit.SECONDS)) throw new java.sql.SQLException("release timeout"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.sql.SQLException(interrupted); }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var r = resources(new Tx(emf), 1, input.bytes())) {
                var operation = workers.submit(() -> { pending(r.sessions(), input); return true; });
                try {
                    assertThat(committing.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> { throw new AssertionError("registration is still active"); }, NONE))
                            .isEqualTo(new DocumentPublicationSessions.DrainProgress(false, 0, 0));
                    assertThat(r.uploads().providerActivity().accepting()).isFalse();
                    assertThat(count(c, "repository_coordinator_drains", input)).isZero();
                    release.countDown();
                    assertThat(operation.get(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(r.sessions().drainRegistrations(Duration.ZERO, key -> CALLER, NONE))
                            .isEqualTo(new DocumentPublicationSessions.DrainProgress(true, 1, 0));
                } finally { release.countDown(); }
            }
        }
    }

    @Test void differentManagerCannotResumeAnotherIncarnationsNonterminalOwner() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            try (var original = resources(c.tx(), 1, input.bytes()); var foreign = resources(c.tx(), 1, input.bytes())) {
                pending(original.sessions(), input);
                var identity = identity(c, input);
                var key = new RepositoryOperationLedger.Key(input.command().intent().getAccountId(), "principal", input.command().operationId());
                var claim = new RepositoryExecutionClaimLedger.Claim(key, input.command().sha256(), 1,
                        (UUID) identity[0], (java.time.Instant) identity[1]);
                var lease = c.tx().readOnly(em -> (java.time.Instant) em.createNativeQuery(
                        "SELECT lease_until FROM repository_operation_owners WHERE operation_id=:id")
                        .setParameter("id", key.operationId()).getSingleResult());
                var owner = new RepositoryOperationLedger.Owner(key, 1, (UUID) identity[2], lease, Optional.of(claim));
                assertThatThrownBy(() -> foreign.sessions().resumeStarted(CALLER, input.command(), owner, NONE))
                        .hasMessageContaining("restoring incarnation");
                assertThat(foreign.sessions().retainedSessions()).isZero();
                assertIdentity(c, input, identity);
                assertThat(count(c, "document_part_attempts", input)).isZero();
            }
        }
    }
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void markerRacingRegistrationFailureEvictsOnlyAfterFreshConfirmation(boolean cancelConfirmation) throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c); var armed = new AtomicBoolean(true); var cancelled = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && count(c, "repository_publication_preparations", input) == 1 && armed.compareAndSet(true, false)) {
                    var row = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                            SELECT c.claim_token,p.preparation_bytes FROM repository_execution_claims c
                            JOIN repository_publication_preparations p USING(account_id,principal,operation_id)
                            WHERE c.operation_id=:o AND p.predecessor_generation=0
                            """).setParameter("o", input.command().operationId()).getSingleResult());
                    var key = new RepositoryOperationLedger.Key("account", "principal", input.command().operationId());
                    var record = DocumentPublicationPreparationCodec.decode(com.google.protobuf.ByteString.copyFrom((byte[]) row[1]),
                            key, input.command().sha256());
                    DocumentPublicationAbandonment.abandonRetained(c.tx(), new PayloadBudget(64_000_000), CALLER, (UUID) row[0], record, NONE);
                    cancelled.set(cancelConfirmation);
                    throw new java.sql.SQLException("Registration response lost after abandonment", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf), 1, input.bytes())) {
                var control = new RepositoryReadControl() {
                    public boolean isCancelled() { return cancelled.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                var failure = catchThrowable(() -> execute(resources.sessions(), input, input.modes(), control));
                assertThat(failure).hasStackTraceContaining("Registration response lost after abandonment");
                assertThat(count(c, "repository_publication_abandonments", input)).isEqualTo(1);
                assertThat(resources.sessions().retainedSessions()).isEqualTo(cancelConfirmation ? 1 : 0);
                if (cancelConfirmation) {
                    assertThat(failure.getSuppressed()).anyMatch(e -> e instanceof RepositoryException r
                            && r.code() == RepositoryException.Code.CANCELLED);
                    cancelled.set(false);
                    assertThat(resources.sessions().abandonRetained(CALLER, input.command(), NONE)).isTrue();
                }
                assertThat(resources.sessions().retainedSessions()).isZero();
                assertThat(resources.sessions().retainedCommandBytes()).isZero();
            }
        }
    }

    @Test void expiredRegistrationWithoutMarkerCannotReleaseCapacity() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c); var armed = new AtomicBoolean(true);
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && count(c, "repository_publication_preparations", input) == 1 && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("Registration response lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf), 1, input.bytes(), Duration.ofSeconds(1))) {
                var manager = resources.sessions();
                assertThatThrownBy(() -> execute(manager, input, input.modes(), NONE)).hasStackTraceContaining("Registration response lost");
                c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
                assertThatThrownBy(() -> manager.abandonRetained(CALLER, input.command(), NONE))
                        .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
                assertThat(count(c, "repository_publication_abandonments", input)).isZero();
                assertThat(manager.retainedSessions()).isEqualTo(1);
                assertThat(manager.retainedCommandBytes()).isEqualTo(input.bytes());
            }
        }
    }

    @Test void admittedRegistrationAndUnprivilegedCancellationKeepCapacity() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            try (var resources = resources(c.tx(), 1, input.bytes())) {
                var manager = resources.sessions(); pending(manager, input);
                assertThatThrownBy(() -> manager.abandonRetained(new RepositoryCaller("principal", false,
                        java.util.Set.of("account"), java.util.Set.of()), input.command(), NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
                assertThatThrownBy(() -> manager.abandonRetained(CALLER, input.command(), NONE))
                        .hasStackTraceContaining("cannot be abandoned");
                assertThat(manager.retainedSessions()).isEqualTo(1);
                assertThat(count(c, "repository_publication_abandonments", input)).isZero();
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"repository_publication_preparations", "repository_publication_modes"})
    void lostAbandonmentAcknowledgmentRetainsCapacityUntilConfirmedAfterExpiry(String stopAt) throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c); var registration = new AtomicBoolean(true); var abandonment = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (registration.get() && count(c, stopAt, input) == 1 && registration.compareAndSet(true, false))
                    throw new java.sql.SQLException("Registration response lost", "08006");
                if (abandonment.get() && count(c, "repository_publication_abandonments", input) == 1
                        && abandonment.compareAndSet(true, false)) throw new java.sql.SQLException("Abandonment response lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf), 1, input.bytes(), Duration.ofSeconds(2))) {
                var manager = resources.sessions();
                assertThatThrownBy(() -> execute(manager, input, input.modes(), NONE)).hasStackTraceContaining("Registration response lost");
                assertThat(manager.retainedSessions()).isEqualTo(1);
                abandonment.set(true);
                assertThatThrownBy(() -> manager.abandonRetained(CALLER, input.command(), NONE)).hasStackTraceContaining("Abandonment response lost");
                assertThat(manager.retainedSessions()).isEqualTo(1);
                assertThat(manager.retainedCommandBytes()).isEqualTo(input.bytes());
                assertThat(count(c, "repository_operation_owners", input)).isZero();
                c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(2.1)").getSingleResult());
                assertThat(manager.abandonRetained(CALLER, input.command(), NONE)).isTrue();
                assertThat(manager.retainedSessions()).isZero();
                assertThat(manager.retainedCommandBytes()).isZero();
                assertThatThrownBy(() -> execute(manager, input, input.modes(), NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION))
                        .hasMessage("Publication registration was abandoned");
                assertThat(manager.retainedSessions()).isZero();
                assertThat(manager.abandonRetained(CALLER, input.command(), NONE)).isFalse();
            }
        }
    }

    @Test void deniedScopedRegistrationReleasesCapacityBeforeAnyJournalWrite() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            try (var resources = resources(c.tx(), 1, input.bytes())) {
                var scoped = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
                assertThatThrownBy(() -> resources.sessions().execute(scoped, input.command(), input.placements(),
                        Map.of(), Map.of(), input.modes(), Optional.empty(),
                        (member, occurrence) -> { throw new AssertionError("Denied request must not resolve schemas"); }, NONE))
                        .isInstanceOf(RepositoryException.class);
                assertThat(resources.sessions().retainedSessions()).isZero();
                assertThat(resources.sessions().retainedCommandBytes()).isZero();
                assertThat(count(c, "repository_execution_claims", input)).isZero();
                pending(resources.sessions(), input);
                assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
            }
        }
    }

    @Test void invalidModesReleasePreRegistrationCapacity() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            try (var resources = resources(c.tx(), 1, input.bytes())) {
                var manager = resources.sessions();
                assertThatThrownBy(() -> execute(manager, input, Map.of(), NONE)).isInstanceOf(IllegalArgumentException.class);
                assertThat(manager.retainedSessions()).isZero();
                assertThat(manager.retainedCommandBytes()).isZero();
                assertThat(count(c, "repository_execution_claims", input)).isZero();
                pending(manager, input);
                assertThat(manager.retainedSessions()).isEqualTo(1);
                assertThat(manager.retainedCommandBytes()).isEqualTo(input.bytes());
            }
        }
    }

    @Test void capacityRefusalDoesNotRegisterAnything() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            try (var resources = resources(c.tx(), 1, input.bytes()-1)) {
                assertThatThrownBy(() -> execute(resources.sessions(), input, input.modes(), NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(resources.sessions().retainedSessions()).isZero();
                assertThat(resources.sessions().retainedCommandBytes()).isZero();
                assertThat(count(c, "repository_execution_claims", input)).isZero();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"repository_publication_preparations,false", "repository_publication_modes,false", "repository_operation_owners,false",
            "repository_publication_preparations,true", "repository_publication_modes,true", "repository_operation_owners,true"})
    void uncertainRegistrationRetainsExactIdentityAndCannotUseUnjournaledRecovery(String table, boolean cancel) throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c); var armed = new AtomicBoolean(); var cancelled = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && count(c, table, input) == 1 && armed.compareAndSet(true, false)) {
                    if (cancel) cancelled.set(true);
                    else throw new java.sql.SQLException("Registration response lost", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf), 1, input.bytes())) {
                var manager = resources.sessions();
                var control = new RepositoryReadControl() {
                    public long remainingNanos() { return Long.MAX_VALUE; }
                    public boolean isCancelled() { return cancelled.get(); }
                };
                armed.set(true);
                var failure = catchThrowable(() -> execute(manager, input, input.modes(), control));
                if (cancel) assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                else assertThat(failure).hasStackTraceContaining("Registration response lost");
                assertThat(armed.get()).isFalse();
                assertThat(manager.retainedSessions()).isEqualTo(1);
                assertThat(manager.retainedCommandBytes()).isEqualTo(input.bytes());
                assertThat(resources.budget().reservedBytes()).isZero();
                var original = identity(c, input);
                cancelled.set(false);
                pending(manager, input);
                assertIdentity(c, input, original);
                assertThatThrownBy(() -> manager.recover(CALLER, input.command(), input.placements(), 1, input.modes(), NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                pending(manager, input);
                assertIdentity(c, input, original);
                assertThat(manager.retireSuperseded(CALLER, input.command(), NONE)).isFalse();
                manager.close();
                assertThat(manager.awaitIdle(Duration.ZERO)).isTrue();
                assertThat(manager.retainedSessions()).isEqualTo(1);
                assertThat(manager.retainedCommandBytes()).isEqualTo(input.bytes());
            }
        }
    }

    @Test void concurrentRejectionCannotEvictSessionWithRegistrationInFlight() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c); var armed = new AtomicBoolean();
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && count(c, "repository_publication_preparations", input) == 1 && armed.compareAndSet(true, false)) {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) throw new java.sql.SQLException("Registration gate timed out");
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt(); throw new java.sql.SQLException("Registration gate interrupted", failure);
                    }
                    throw new java.sql.SQLException("Registration response lost", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf), 1, input.bytes());
                    var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var manager = resources.sessions(); armed.set(true);
                var first = workers.submit(() -> catchThrowable(() -> execute(manager, input, input.modes(), NONE)));
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    assertThatThrownBy(() -> execute(manager, input, input.modes(), NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                    assertThat(manager.retainedSessions()).isEqualTo(1);
                    assertThat(manager.retainedCommandBytes()).isEqualTo(input.bytes());
                    assertThatThrownBy(() -> manager.abandonRetained(CALLER, input.command(), NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                    manager.close();
                    assertThat(manager.awaitIdle(Duration.ZERO)).isFalse();
                } finally { release.countDown(); }
                assertThat(first.get(10, TimeUnit.SECONDS)).hasStackTraceContaining("Registration response lost");
                assertThat(manager.awaitIdle(Duration.ofSeconds(1))).isTrue();
                assertThat(manager.retainedSessions()).isEqualTo(1);
                assertThat(manager.retainedCommandBytes()).isEqualTo(input.bytes());
            }
        }
    }

    private static void execute(DocumentPublicationSessions sessions, Input input,
            Map<String, DocumentPublicationCandidate.Mode> modes, RepositoryReadControl control) throws Exception {
        sessions.execute(CALLER, input.command(), input.placements(), Map.of(), Map.of(), modes, Optional.empty(),
                (member, occurrence) -> { throw new AssertionError("Registration must not resolve schemas"); }, control);
    }
    private static void pending(DocumentPublicationSessions sessions, Input input) {
        assertThatThrownBy(() -> execute(sessions, input, input.modes(), NONE))
                .isInstanceOf(DocumentSchemaPolicies.StalePolicy.class).hasMessage("No active schema policy for account");
    }
    private static int count(Context c, String table, Input input) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                .setParameter("id", input.command().operationId()).getSingleResult()).intValue());
    }
    private static Object[] identity(Context c, Input input) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.claim_token,c.lease_until,p.owner_nonce,p.preparation_bytes,b.incarnation
                FROM repository_execution_claims c JOIN repository_publication_preparations p USING(account_id,principal,operation_id)
                JOIN repository_coordinator_bindings b USING(account_id,principal,operation_id)
                WHERE operation_id=:id AND predecessor_generation=0
                """).setParameter("id", input.command().operationId()).getSingleResult());
    }
    private static void assertIdentity(Context c, Input input, Object[] original) {
        var current = identity(c, input);
        for (int i = 0; i < 3; i++) assertThat(current[i]).isEqualTo(original[i]);
        assertThat((byte[]) current[3]).isEqualTo((byte[]) original[3]);
        assertThat(current[4]).isEqualTo(original[4]);
    }
    private record Input(DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements,
            Map<String, DocumentPublicationCandidate.Mode> modes) {
        long bytes() { return (long) command.canonical().size() + command.intent().getSerializedSize(); }
    }
    private static Input input(Context c) {
        var seed = prepare(c, 2, true);
        var command = new DocumentPublicationCommand(seed.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var drive = new DriveLedger(c.tx()).findById(UUID.fromString(command.intent().getMembers(0).getDriveId())).orElseThrow();
        return new Input(command, Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow())),
                Map.of("member-0", DocumentPublicationCandidate.Mode.OPAQUE, "member-1", DocumentPublicationCandidate.Mode.OPAQUE));
    }
    private record Resources(DocumentUploadCoordinator uploads, DocumentPublicationSessions sessions,
            DocumentReadLedger reads, PayloadBudget budget) implements AutoCloseable {
        public void close() throws Exception {
            sessions.close(); uploads.close(); reads.closeForShutdown();
            assertThat(reads.awaitLocalDrain(Duration.ZERO)).isTrue(); reads.attestLocalQuiescence();
            assertThat(budget.reservedBytes()).isZero();
        }
    }
    private static Resources resources(Tx tx, int capacity, long commandBytes) {
        return resources(tx, capacity, commandBytes, LEASE);
    }
    private static Resources resources(Tx tx, int capacity, long commandBytes, Duration lease) {
        var drives = new DriveLedger(tx); var budget = new PayloadBudget(32L * 1024 * 1024);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var uploads = new DocumentUploadCoordinator(tx, drives, budget,
                (generation, profile) -> { throw new AssertionError("Registration must not open a byte provider"); },
                2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5)));
        var execution = new DocumentPublicationExecution(tx, drives, reads, uploads,
                (plan, member, control) -> { throw new AssertionError("Registration must not read retained bytes"); }, budget,
                new DocumentRevisionAssembly.Limits(1_000_000, 100, 100, 100, 100_000), false);
        return new Resources(uploads, DocumentPublicationSessions.journaled(tx, execution, lease, capacity, commandBytes, budget), reads, budget);
    }
}
