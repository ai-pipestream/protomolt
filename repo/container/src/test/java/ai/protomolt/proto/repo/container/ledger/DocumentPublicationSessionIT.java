package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Admission identity over actual SQL; payload/provider observations in the seed are fixtures. */
@Testcontainers
class DocumentPublicationSessionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void recoveryRetainsFreshAttemptAndModeIdentitiesAfterCommittedSqlFailure(boolean cancel) {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var original = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), Duration.ofSeconds(1));
            var oldOwner = original.admit(CALLER, RepositoryReadControl.NONE).orElseThrow();
            c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                    FROM repository_operation_owners WHERE operation_id=:id
                    """).setParameter("id", input.command().operationId()).getSingleResult());
            var armed = new AtomicBoolean();
            var cancelled = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) {
                    if (cancel) cancelled.set(true);
                    else throw new java.sql.SQLException("Recovery acknowledgment lost after commit", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var modes = new java.util.HashMap<String, DocumentPublicationCandidate.Mode>();
                input.command().intent().getMembersList().forEach(member -> modes.put(member.getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
                var expectedModes = Map.copyOf(modes);
                for (long invalid : new long[]{0, -1, Long.MAX_VALUE}) {
                    assertThatThrownBy(() -> DocumentPublicationSession.recovering(new Tx(emf), CALLER,
                            input.command(), input.placements(), LEASE, invalid, modes)).isInstanceOf(IllegalArgumentException.class);
                }
                assertThatThrownBy(() -> DocumentPublicationSession.recovering(new Tx(emf), CALLER,
                        input.command(), input.placements(), LEASE, 1, Map.of())).isInstanceOf(IllegalArgumentException.class);
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command())).orElseThrow().generation()).isEqualTo(1);
                var recovery = DocumentPublicationSession.recovering(new Tx(emf), CALLER, input.command(), input.placements(), LEASE, 1, modes);
                modes.replaceAll((member, mode) -> DocumentPublicationCandidate.Mode.OPAQUE);
                var prepared = recovery.prepared();
                assertThat(prepared.members().getFirst().attempt().orElseThrow().id())
                        .isNotEqualTo(original.prepared().members().getFirst().attempt().orElseThrow().id());
                assertThat(prepared.members().get(1).attempt()).isEmpty();
                var control = new RepositoryReadControl() {
                    @Override public long remainingNanos() { return Long.MAX_VALUE; }
                    @Override public boolean isCancelled() { return cancelled.get(); }
                };
                armed.set(true);
                var failure = catchThrowable(() -> recovery.admit(CALLER, control));
                if (cancel) assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                else assertThat(failure).hasStackTraceContaining("Recovery acknowledgment lost after commit");
                assertThat(armed).isFalse();
                cancelled.set(false);
                var owner = recovery.admit(CALLER, RepositoryReadControl.NONE).orElseThrow();
                assertThat(owner.generation()).isEqualTo(2);
                assertThat(owner.token()).isNotEqualTo(oldOwner.token());
                assertThat(recovery.admit(CALLER, RepositoryReadControl.NONE)).contains(owner);
                assertThat(recovery.prepared()).isSameAs(prepared);
                assertThatThrownBy(() -> new RepositoryOperationLedger(c.tx()).renew(oldOwner, LEASE))
                        .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
                try (var execution = recovery.begin(CALLER, RepositoryReadControl.NONE)) {
                    assertThatThrownBy(() -> execution.bindModes(modes)).hasMessageContaining("modes changed");
                    assertThat(execution.bindModes(expectedModes)).isEqualTo(expectedModes);
                }
            }
        }
    }

    @Test void executionExcludesConcurrentUseAndOldCloseCannotUnlockItsSuccessor() throws Exception {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var session = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
            var first = session.begin(CALLER, RepositoryReadControl.NONE);
            try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                executor.submit(() -> assertThatThrownBy(() -> session.begin(CALLER, RepositoryReadControl.NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CONFLICT)))
                        .get(10, java.util.concurrent.TimeUnit.SECONDS);
            }
            first.close();
            try (var second = session.begin(CALLER, RepositoryReadControl.NONE)) {
                first.close();
                assertThatThrownBy(() -> session.begin(CALLER, RepositoryReadControl.NONE))
                        .isInstanceOf(RepositoryException.class);
            }
            try (var third = session.begin(CALLER, RepositoryReadControl.NONE)) {
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command()))).isEmpty();
            }
        }
    }

    @Test void cancelledAcquisitionReleasesLeaseAndAdmissionModesRemainFixedAcrossRetries() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var session = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
            var checks = new AtomicInteger();
            assertThatThrownBy(() -> session.begin(CALLER, new RepositoryReadControl() {
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
                @Override public boolean isCancelled() { return checks.incrementAndGet() >= 2; }
            })).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            var modes = new java.util.HashMap<String, DocumentPublicationCandidate.Mode>();
            input.command().intent().getMembersList().forEach(member -> modes.put(member.getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
            var expected = Map.copyOf(modes);
            try (var execution = session.begin(CALLER, RepositoryReadControl.NONE)) {
                assertThatThrownBy(() -> execution.bindModes(Map.of())).isInstanceOf(IllegalArgumentException.class);
                var inspected = new java.util.HashMap<>(modes);
                inspected.replaceAll((member, mode) -> DocumentPublicationCandidate.Mode.OPAQUE);
                assertThat(execution.checkModes(inspected)).isEqualTo(inspected);
                // Checking a possible replacement must not bind the current session.
                assertThat(execution.bindModes(modes)).isEqualTo(expected);
            }
            modes.replaceAll((member, mode) -> DocumentPublicationCandidate.Mode.OPAQUE);
            try (var retry = session.begin(CALLER, RepositoryReadControl.NONE)) {
                assertThatThrownBy(() -> retry.bindModes(modes)).hasMessageContaining("modes changed");
                assertThat(retry.bindModes(expected)).isEqualTo(expected);
            }
        }
    }

    @Test void committedAdmissionWithLostAcknowledgmentReusesNonceAndEveryAttemptIdentity() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) throw new java.sql.SQLException("Admission acknowledgment lost after commit", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var session = new DocumentPublicationSession(new Tx(emf), CALLER, input.command(), input.placements(), LEASE);
                var plan = session.prepared();
                var attempt = plan.members().getFirst().attempt().orElseThrow();
                assertThat(plan.members().get(1).attempt()).isEmpty();
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command()))).isEmpty();
                armed.set(true);
                assertThatThrownBy(() -> session.admit(CALLER, RepositoryReadControl.NONE))
                        .hasStackTraceContaining("Admission acknowledgment lost after commit");
                assertThat(armed).isFalse();
                var committedToken = c.tx().readOnly(em -> (UUID) em.createNativeQuery(
                        "SELECT owner_token FROM repository_operation_owners WHERE operation_id=:id")
                        .setParameter("id", input.command().operationId()).getSingleResult());
                var owner = session.admit(CALLER, RepositoryReadControl.NONE).orElseThrow();
                assertThat(owner.token()).isEqualTo(committedToken);
                assertThat(owner.generation()).isEqualTo(1);
                assertThat(session.prepared()).isSameAs(plan);
                assertThat(session.prepared().members().getFirst().attempt().orElseThrow()).isSameAs(attempt);
                assertThat(session.admit(CALLER, RepositoryReadControl.NONE)).contains(owner); // no implicit lease extension
                var contender = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
                assertThat(contender.admit(CALLER, RepositoryReadControl.NONE)).isEmpty(); // never silently takes over
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command())).orElseThrow().generation()).isEqualTo(1);
            }
        }
    }

    @Test void cancellationAfterAdmissionRetainsIdentityForExactRetry() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var session = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
            var plan = session.prepared();
            var checks = new AtomicInteger();
            assertThatThrownBy(() -> session.admit(CALLER, new RepositoryReadControl() {
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
                @Override public boolean isCancelled() { return checks.incrementAndGet() >= 2; }
            })).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command()))).isPresent();
            assertThat(session.admit(CALLER, RepositoryReadControl.NONE)).isPresent();
            assertThat(session.prepared()).isSameAs(plan);
        }
    }

    @Test void callerBindingsAreRecheckedBeforeEveryAdmissionWithoutWritingOtherScope() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var session = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
            var wrongPrincipal = new RepositoryCaller("another", true);
            var wrongAccount = new RepositoryCaller("principal", false, java.util.Set.of("another"), java.util.Set.of());
            for (var caller : java.util.List.of(wrongPrincipal, wrongAccount)) {
                assertThatThrownBy(() -> session.admit(caller, RepositoryReadControl.NONE)).isInstanceOf(RepositoryException.class);
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command()))).isEmpty();
            }
            assertThatThrownBy(() -> new DocumentPublicationSession(c.tx(), wrongAccount, input.command(), input.placements(), LEASE))
                    .isInstanceOf(RepositoryException.class);
            assertThat(session.admit(CALLER, RepositoryReadControl.NONE)).isPresent();
        }
    }

    private record Input(DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements) {}
    private static Input input(Context c) {
        var seed = prepare(c, 2, true);
        var command = new DocumentPublicationCommand(seed.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var id = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var drive = new DriveLedger(c.tx()).findById(id).orElseThrow();
        var profile = new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow();
        return new Input(command, Map.of(id, DocumentUploadPlan.Placement.sample(drive, "native-test", profile)));
    }
    private static RepositoryOperationLedger.Key key(DocumentPublicationCommand command) {
        return new RepositoryOperationLedger.Key(command.intent().getAccountId(), CALLER.principalName(), command.operationId());
    }
}
