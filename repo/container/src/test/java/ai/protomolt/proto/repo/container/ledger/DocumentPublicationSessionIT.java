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

    @Test void preparationCommitFailureCannotLeaveAnInitialClaimWithoutSeeds() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!armed.get()) return;
                try (var query = connection.prepareStatement(
                        "SELECT count(*) FROM repository_publication_preparations WHERE operation_id=?")) {
                    query.setObject(1, input.command().operationId());
                    try (var rows = query.executeQuery()) {
                        rows.next();
                        if (rows.getInt(1) == 1 && armed.compareAndSet(true, false))
                            throw new java.sql.SQLException("Preparation commit refused", "08006");
                    }
                }
            });
            var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(32L * 1024 * 1024);
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var session = DocumentPublicationSession.journaled(new Tx(emf), CALLER, input.command(), input.placements(), LEASE, budget);
                var modes = new java.util.HashMap<String, DocumentPublicationCandidate.Mode>();
                input.command().intent().getMembersList().forEach(member -> modes.put(member.getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
                try (var execution = session.begin(CALLER, RepositoryReadControl.NONE)) { execution.bindModes(modes); }
                armed.set(true);
                assertThatThrownBy(() -> session.admit(CALLER, RepositoryReadControl.NONE))
                        .hasStackTraceContaining("Preparation commit refused");
                assertThat(armed.get()).isFalse();
                assertThat(budget.reservedBytes()).isZero();
                for (String table : java.util.List.of("repository_execution_claims", "repository_publication_preparations",
                        "repository_publication_modes", "repository_operation_owners")) {
                    int count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                            .setParameter("id", input.command().operationId()).getSingleResult()).intValue());
                    assertThat(count).as(table).isZero();
                }
                assertThat(session.admit(CALLER, RepositoryReadControl.NONE).orElseThrow().token())
                        .isEqualTo(session.seeds().ownerNonce());
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {0, 1, 2})
    void journaledAssessmentStartKeepsOriginalCoordinatesAfterAcknowledgmentUncertainty(int fault) {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var armed = new AtomicBoolean(); var cancelled = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM repository_publication_assessment_starts WHERE operation_id=:id")
                        .setParameter("id", input.command().operationId()).getSingleResult()).intValue()) == 1
                        && armed.compareAndSet(true, false)) {
                    if (fault == 1) throw new java.sql.SQLException("Assessment start acknowledgment lost", "08006");
                    if (fault == 2) cancelled.set(true);
                }
            });
            var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(32L * 1024 * 1024);
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var session = DocumentPublicationSession.journaled(new Tx(emf), CALLER, input.command(), input.placements(), LEASE, budget);
                var modes = new java.util.HashMap<String, DocumentPublicationCandidate.Mode>();
                input.command().intent().getMembersList().forEach(member -> modes.put(member.getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
                var control = new RepositoryReadControl() {
                    public long remainingNanos() { return Long.MAX_VALUE; }
                    public boolean isCancelled() { return cancelled.get(); }
                };
                try (var execution = session.begin(CALLER, RepositoryReadControl.NONE)) {
                    execution.bindModes(modes);
                    var owner = session.admit(CALLER, RepositoryReadControl.NONE).orElseThrow();
                    var wrong = new RepositoryOperationLedger.Owner(owner.key(), owner.generation(), UUID.randomUUID(),
                            owner.leaseUntil(), owner.executionClaim());
                    assertThatThrownBy(() -> execution.beginAssessmentStage(CALLER, wrong, Duration.ofHours(1), control))
                            .hasMessageContaining("owner differs");
                    var claim = owner.executionClaim().orElseThrow();
                    var wrongClaim = owner.withClaim(new RepositoryExecutionClaimLedger.Claim(claim.key(), claim.commandSha256(),
                            claim.epoch() + 1, claim.token(), claim.leaseUntil()));
                    assertThatThrownBy(() -> execution.beginAssessmentStage(CALLER, wrongClaim, Duration.ofHours(1), control))
                            .hasMessageContaining("claim differs");
                    cancelled.set(true);
                    assertThatThrownBy(() -> execution.beginAssessmentStage(CALLER, owner, Duration.ofHours(1), control))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    cancelled.set(false);
                    assertThatThrownBy(execution::beginAssessmentStage).hasMessageContaining("requires durable start");
                    assertThat(execution.assessmentStageStarted()).isFalse();
                    armed.set(true);
                    DocumentAssessmentStartJournal.Started returned = null;
                    if (fault == 0) returned = execution.beginAssessmentStage(CALLER, owner, Duration.ofHours(1), control);
                    else {
                        var failure = catchThrowable(() -> execution.beginAssessmentStage(CALLER, owner, Duration.ofHours(1), control));
                        if (fault == 1) assertThat(failure).hasStackTraceContaining("Assessment start acknowledgment lost");
                        else assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,
                                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    }
                    assertThat(armed.get()).isFalse(); cancelled.set(false);
                    assertThat(execution.assessmentStageStarted()).isTrue();
                    var stored = new DocumentAssessmentStartJournal(c.tx(), budget)
                            .load(CALLER, owner, input.command(), RepositoryReadControl.NONE).orElseThrow();
                    if (returned != null) assertThat(stored).isEqualTo(returned);
                    assertThatThrownBy(() -> execution.beginAssessmentStage(CALLER, owner, Duration.ofHours(2), control))
                            .hasMessageContaining("already started");
                    assertThat(new DocumentAssessmentStartJournal(c.tx(), budget)
                            .load(CALLER, owner, input.command(), RepositoryReadControl.NONE)).contains(stored);
                }
                try (var retry = session.begin(CALLER, RepositoryReadControl.NONE)) {
                    assertThat(retry.assessmentStageStarted()).isTrue();
                }
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {0, 1, 2, 3})
    void journaledRegistrationReconcilesEachLostAcknowledgmentWithoutNewIdentities(int stage) {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var table = java.util.List.of("repository_execution_claims", "repository_publication_preparations",
                    "repository_publication_modes", "repository_operation_owners").get(stage);
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                        .setParameter("id", input.command().operationId()).getSingleResult()).intValue()) == 1
                        && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("Registration acknowledgment lost after commit", "08006");
            });
            var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(32L * 1024 * 1024);
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var session = DocumentPublicationSession.journaled(new Tx(emf), CALLER, input.command(), input.placements(), LEASE, budget);
                var modes = new java.util.HashMap<String, DocumentPublicationCandidate.Mode>();
                input.command().intent().getMembersList().forEach(member -> modes.put(member.getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
                try (var execution = session.begin(CALLER, RepositoryReadControl.NONE)) { execution.bindModes(modes); }
                armed.set(true);
                assertThatThrownBy(() -> session.admit(CALLER, RepositoryReadControl.NONE))
                        .hasStackTraceContaining("Registration acknowledgment lost after commit");
                assertThat(armed.get()).isFalse();
                assertThat(budget.reservedBytes()).isZero();
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command())).isPresent()).isEqualTo(stage == 3);
                int preparations = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM repository_publication_preparations WHERE operation_id=:id")
                        .setParameter("id", input.command().operationId()).getSingleResult()).intValue());
                assertThat(preparations)
                        .as("acknowledged or uncertain initial claim always has durable seeds").isEqualTo(1);
                var before = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                        SELECT claim_token,lease_until FROM repository_execution_claims WHERE operation_id=:id
                        """).setParameter("id", input.command().operationId()).getSingleResult());
                var owner = session.admit(CALLER, RepositoryReadControl.NONE).orElseThrow();
                var claim = owner.executionClaim().orElseThrow();
                assertThat(claim.token()).isEqualTo(before[0]).isNotEqualTo(owner.token());
                assertThat(owner.token()).isEqualTo(session.seeds().ownerNonce());
                var after = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                        SELECT claim_token,lease_until FROM repository_execution_claims WHERE operation_id=:id
                        """).setParameter("id", input.command().operationId()).getSingleResult());
                assertThat(after).containsExactly(before);
                assertThat(session.admit(CALLER, RepositoryReadControl.NONE).orElseThrow()).isEqualTo(owner);
                try (var loaded = new DocumentPublicationPreparationJournal(c.tx(), budget)
                        .load(CALLER, claim, 0, RepositoryReadControl.NONE).orElseThrow()) {
                    assertThat(loaded.record().seeds().ownerNonce()).isEqualTo(session.seeds().ownerNonce());
                    assertThat(loaded.record().seeds().attempts()).isEqualTo(session.seeds().attempts());
                    assertThat(loaded.record().seeds().uploadTokens()).isEqualTo(session.seeds().uploadTokens());
                    assertThat(loaded.record().command().sha256()).isEqualTo(input.command().sha256());
                }
                assertThat(new DocumentPublicationModesJournal(c.tx(), budget).load(CALLER, claim, 0, RepositoryReadControl.NONE))
                        .contains(Map.copyOf(modes));
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @Test void durableRegistrationRequiresActualAuthorityAndFixedModesBeforeAnyClaim() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(32L * 1024 * 1024);
            var scoped = new RepositoryCaller(CALLER.principalName(), false,
                    java.util.Set.of(input.command().intent().getAccountId()), java.util.Set.of());
            assertThatThrownBy(() -> DocumentPublicationSession.journaled(c.tx(), scoped, input.command(), input.placements(), LEASE, budget))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            var session = DocumentPublicationSession.journaled(c.tx(), CALLER, input.command(), input.placements(), LEASE, budget);
            assertThatThrownBy(() -> session.admit(CALLER, RepositoryReadControl.NONE)).hasMessageContaining("modes must be fixed");
            long claims = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_execution_claims WHERE operation_id=:id")
                    .setParameter("id", input.command().operationId()).getSingleResult()).longValue());
            assertThat(claims).isZero();
            var modes = new java.util.HashMap<String, DocumentPublicationCandidate.Mode>();
            input.command().intent().getMembersList().forEach(member -> modes.put(member.getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
            try (var execution = session.begin(CALLER, RepositoryReadControl.NONE)) {
                execution.bindModes(modes);
                modes.replaceAll((member, mode) -> DocumentPublicationCandidate.Mode.OPAQUE);
                assertThatThrownBy(() -> execution.bindModes(modes)).hasMessageContaining("modes changed");
            }
            assertThatThrownBy(() -> session.admit(scoped, RepositoryReadControl.NONE)).hasMessageContaining("actual process authority");
            assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command()))).isEmpty();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void assessmentStageUncertaintySurvivesExecutionRetry() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var session = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
            var first = session.begin(CALLER, RepositoryReadControl.NONE);
            assertThat(first.assessmentStageStarted()).isFalse();
            first.beginAssessmentStage();
            assertThat(first.assessmentStageStarted()).isTrue();
            first.close();
            assertThatThrownBy(first::beginAssessmentStage).hasMessageContaining("closed");
            assertThatThrownBy(first::assessmentStageStarted).hasMessageContaining("closed");
            try (var retry = session.begin(CALLER, RepositoryReadControl.NONE)) {
                assertThat(retry.assessmentStageStarted()).isTrue();
                assertThatThrownBy(retry::beginAssessmentStage).hasMessageContaining("already started");
                first.close();
                assertThat(retry.assessmentStageStarted()).isTrue();
            }
        }
    }

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
