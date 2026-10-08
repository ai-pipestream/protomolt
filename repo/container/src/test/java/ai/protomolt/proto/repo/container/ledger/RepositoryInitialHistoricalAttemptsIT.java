package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL lifecycle; source publication provider observations are fixture supplied. */
@Testcontainers
class RepositoryInitialHistoricalAttemptsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final Duration LEASE = Duration.ofMinutes(5);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void ownerAcquiresCapturesAndRetainsPartialFailureForLedgerCleanup(boolean cancel) throws Exception {
        try (var c = context(POSTGRES); var template = historicalInitial(c, LEASE)) {
            var record = fresh(template.record());
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var cancellation = new RepositoryReadControl() {
                public boolean isCancelled() { return reads.outstandingReads() > 0; }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try {
                try (var call = owner.beginInitial(CALLER, record, modes(record), UUID.randomUUID())) {
                    assertThat(reads.outstandingReads()).isZero();
                    if (cancel) {
                        assertThatThrownBy(() -> call.captureSources(reads, cancellation))
                                .isInstanceOfSatisfying(RepositoryException.class,
                                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                        assertThat(reads.outstandingReads()).isEqualTo(1);
                        assertThat(count(c, record, "repository_execution_claims")).isZero();
                        assertThat(reads.releaseDrained(10)).isEqualTo(1);
                        assertThat(reads.outstandingReads()).isZero();
                    }
                    call.captureSources(reads, NONE);
                    int captures = reads.outstandingReads();
                    assertThat(captures).isEqualTo(1);
                    assertThatThrownBy(() -> call.captureSources(reads, NONE)).hasMessageContaining("already attached");
                    assertThat(reads.outstandingReads()).isEqualTo(captures);
                    call.openExecution(CALLER, NONE);
                    call.start(LEASE, NONE);
                }
            } finally {
                owner.close();
                assertThat(owner.detachClosed(Duration.ofSeconds(1), ignored -> CALLER, NONE)).isTrue();
                reads.releaseDrained(10);
                assertThat(reads.outstandingReads()).isZero();
                reads.fence(); reads.attestLocalQuiescence();
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @Test void reservesBeforeCaptureAndRetainsStartAcrossCallsAndSourceClosure() throws Exception {
        try (var c = context(POSTGRES); var template = historicalInitial(c, LEASE)) {
            var record = fresh(template.record());
            var modes = modes(record);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            var incarnation = UUID.randomUUID();
            assertThat(owner.inspectSelected(CALLER, record.command())).isEmpty();
            DocumentHistoricalAssessmentSources.Work held;
            DocumentAssessmentStartJournal.Started started;
            try (var call = owner.beginInitial(CALLER, record, modes, incarnation)) {
                var beforeLookup = owner.drain();
                long retainedBytes = budget.reservedBytes();
                assertThat(owner.inspectSelected(CALLER, record.command())).contains(
                        new RepositoryInstalledHistoricalAttempts.SelectedGeneration(call.identity(), true, false, false));
                assertThat(owner.drain()).isEqualTo(beforeLookup);
                assertThat(budget.reservedBytes()).isEqualTo(retainedBytes);
                assertThatThrownBy(() -> owner.resume(CALLER, record.command())).hasMessageContaining("in use");
                var changed = new DocumentPublicationCommand(record.command().intent().toBuilder()
                        .setMembers(0, record.command().intent().getMembers(0).toBuilder().setMemberId("changed")).build());
                assertThatThrownBy(() -> owner.inspectSelected(CALLER, changed)).hasMessageContaining("identity changed");
                assertThatThrownBy(() -> owner.inspectSelected(new RepositoryCaller("principal", false,
                        Set.of("account"), Set.of()), record.command())).hasMessageContaining("identity changed");
                assertThat(count(c, record, "repository_execution_claims")).isZero();
                assertThatThrownBy(() -> owner.beginInitial(CALLER, fresh(record), modes, UUID.randomUUID()))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
                var history = template.reads().captureHistorical(CALLER, template.fixture().address(), template.fixture().revision());
                var sources = DocumentHistoricalAssessmentSources.open(record.command(), CALLER, List.of(history), NONE);
                var work = sources.work();
                call.attachSources(sources, work, NONE);
                held = work.fork();
                sources.close();
                call.openExecution(CALLER, NONE);
                started = call.start(LEASE, NONE);
                assertThat(owner.inspectSelected(CALLER, record.command())).contains(
                        new RepositoryInstalledHistoricalAttempts.SelectedGeneration(call.identity(), true, true, false));
            }
            try (held) {
                try (var retry = owner.beginInitial(CALLER, record, modes, incarnation)) {
                    retry.openExecution(CALLER, NONE);
                    assertThat(retry.start(LEASE, NONE)).isEqualTo(started);
                }
                assertThat(count(c, record, "repository_publication_assessment_starts")).isEqualTo(1);
                assertThatThrownBy(() -> owner.beginInitial(CALLER, record, modes, UUID.randomUUID()))
                        .hasMessageContaining("identity changed");
                owner.close();
                assertThatThrownBy(() -> owner.inspectSelected(CALLER, record.command()))
                        .hasMessageContaining("admission is closed");
                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isFalse();
                assertThat(owner.drain().unresolved()).isEqualTo(1);
                assertThat(count(c, record, "repository_preparation_capture_drains")).isZero();
            }
            assertThat(owner.detachClosed(Duration.ofSeconds(1), ignored -> CALLER, NONE)).isTrue();
            assertThat(count(c, record, "repository_preparation_capture_drains")).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void uncertainInitialRegistrationResumesSameGeneration(boolean committed) throws Exception {
        try (var c = context(POSTGRES); var template = historicalInitial(c, LEASE)) {
            var record = fresh(template.record());
            var armed = new AtomicBoolean(); var injected = new AtomicBoolean();
            var acknowledged = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (committed && armed.get() && injected.compareAndSet(false, true))
                    throw new java.sql.SQLException("initial owner reply lost", "08006");
            });
            var datasource = DocumentJdbcFaults.beforeCommit(acknowledged, connection -> {
                try (var statement = connection.createStatement(); var rows = statement.executeQuery(
                        "SELECT EXISTS(SELECT 1 FROM repository_preparation_pin_batches WHERE creation_xid=pg_current_xact_id())")) {
                    rows.next();
                    if (!rows.getBoolean(1)) return;
                    armed.set(true);
                    if (!committed && injected.compareAndSet(false, true))
                        throw new java.sql.SQLException("initial owner commit refused", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"))) {
                var budget = new PayloadBudget(256L * 1024 * 1024);
                var owner = new RepositoryInstalledHistoricalAttempts(new Tx(emf), budget, new DriveLedger(c.tx()), 1);
                UUID id;
                try (var call = owner.beginInitial(CALLER, record, modes(record), UUID.randomUUID())) {
                    id = call.identity();
                    var history = template.reads().captureHistorical(CALLER, template.fixture().address(), template.fixture().revision());
                    var sources = DocumentHistoricalAssessmentSources.open(record.command(), CALLER, List.of(history), NONE);
                    call.attachSources(sources, sources.work(), NONE);
                    sources.close();
                    assertThatThrownBy(() -> call.openExecution(CALLER, NONE))
                            .hasStackTraceContaining(committed ? "initial owner reply lost" : "initial owner commit refused");
                }
                assertThat(injected).isTrue();
                assertThat(count(c, record, "repository_preparation_pin_batches")).isEqualTo(committed ? 1 : 0);
                try (var retry = owner.resume(CALLER, record.command()).orElseThrow()) {
                    assertThat(retry.identity()).isEqualTo(id);
                    retry.openExecution(CALLER, NONE);
                    retry.start(LEASE, NONE);
                }
                assertThat(count(c, record, "repository_preparation_pin_batches")).isEqualTo(1);
                assertThat(count(c, record, "repository_publication_assessment_starts")).isEqualTo(1);
                owner.close();
                assertThat(owner.detachClosed(Duration.ofSeconds(1), ignored -> CALLER, NONE)).isTrue();
                assertThat(count(c, record, "repository_preparation_capture_drains")).isEqualTo(1);
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @Test void unusedInitialSlotHasNoSqlEffectsAndCanShutdownWithoutReservation() throws Exception {
        try (var c = context(POSTGRES); var template = historicalInitial(c, LEASE)) {
            var record = fresh(template.record());
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var ignored = owner.beginInitial(CALLER, record, modes(record), UUID.randomUUID())) {
                assertThat(owner.drain().unresolved()).isEqualTo(1);
            }
            owner.close();
            assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            assertThat(count(c, record, "repository_execution_claims")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void reconstructedRetryPreservesIdentityAndByteBounds() throws Exception {
        try (var c = context(POSTGRES); var template = historicalInitial(c, LEASE)) {
            var intent = template.record().command().intent().toBuilder().setOperationId(UUID.randomUUID().toString());
            intent.setMembers(0, intent.getMembers(0).toBuilder().setMemberId("member-" + "a".repeat(100)));
            var command = new DocumentPublicationCommand(intent.build());
            var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
            var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                    template.record().placements(), LEASE, 0);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            var incarnation = UUID.randomUUID();
            UUID id;
            try (var call = owner.beginInitial(CALLER, record, modes(record), incarnation)) {
                id = call.identity();
                long encodedBytes = DocumentPublicationPreparationCodec.encode(record).size()
                        + DocumentPublicationModesJournal.encode(command, modes(record)).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                assertThat(budget.reservedBytes()).isGreaterThanOrEqualTo(encodedBytes);
            }
            var copyCommand = new DocumentPublicationCommand(command.intent());
            var copySeeds = DocumentPublicationSeeds.restore(key, copyCommand, record.seeds().ownerNonce(),
                    record.seeds().attempts(), record.seeds().uploadTokens());
            var copy = new DocumentPublicationPreparationRecord(key, copyCommand, copySeeds, record.placements(), LEASE, 0);
            try (var call = owner.beginInitial(CALLER, copy, modes(copy), incarnation)) {
                assertThat(call.identity()).isEqualTo(id);
            }
            owner.close();
            assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void initialSuccessorStartsWhileOldWorkRemainsAndRetirementPreservesNewRoute() throws Exception {
        try (var c = context(POSTGRES); var template = historicalInitial(c, LEASE)) {
            var minted = fresh(template.record());
            var record = new DocumentPublicationPreparationRecord(minted.key(), minted.command(), minted.seeds(),
                    minted.placements(), Duration.ofSeconds(3), 0);
            var modes = modes(record);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 2);
            var timeouts = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));
            try (var initial = owner.beginInitial(CALLER, record, modes, UUID.randomUUID())) {
                var history = template.reads().captureHistorical(CALLER, template.fixture().address(), template.fixture().revision());
                var sources = DocumentHistoricalAssessmentSources.open(record.command(), CALLER, List.of(history), NONE);
                var work = sources.work();
                initial.attachSources(sources, work, NONE);
                initial.openExecution(CALLER, NONE);
                UUID successorId;
                try (var held = work.fork()) {
                    c.tx().readOnly(em -> em.createNativeQuery("""
                            SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                            FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                            WHERE c.operation_id=:id
                            """).setParameter("id", record.command().operationId()).getSingleResult());
                    var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), timeouts)
                            .inspect(CALLER, record.key(), record.command().sha256(), NONE);
                    assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
                    try (var next = owner.beginSuccessor(CALLER, CALLER, initial.identity(), record.command(), modes,
                            observed, LEASE, timeouts)) {
                        successorId = next.identity();
                        assertThat(next.advancePreparation(CALLER, modes, Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                        assertThat(next.advancePreparation(CALLER, modes, Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                        var nextHistory = template.reads().captureHistorical(CALLER, template.fixture().address(), template.fixture().revision());
                        var nextSources = DocumentHistoricalAssessmentSources.open(record.command(), CALLER, List.of(nextHistory), NONE);
                        next.attachSources(nextSources, nextSources.work(), NONE);
                        next.openExecution(CALLER, NONE);
                        assertThat(next.start(LEASE, NONE)).isNotNull();
                    }
                    assertThatThrownBy(() -> initial.start(LEASE, NONE)).hasMessageContaining("disposal only");
                    assertThat(initial.retireFenced(CALLER, Duration.ZERO, NONE))
                            .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETAINED);
                    assertThat(history.isReleased()).isFalse();
                    assertThat(count(c, record, "repository_preparation_capture_drains")).isZero();
                }
                assertThat(initial.retireFenced(CALLER, Duration.ofSeconds(1), NONE))
                        .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                assertThat(history.isReleased()).isTrue();
                try (var routed = owner.resume(CALLER, record.command()).orElseThrow()) {
                    assertThat(routed.identity()).isEqualTo(successorId);
                }
            }
            owner.close();
            assertThat(owner.detachClosed(Duration.ofSeconds(1), ignored -> CALLER, NONE)).isTrue();
            assertThat(count(c, record, "repository_preparation_capture_drains")).isEqualTo(2);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static DocumentPublicationPreparationRecord fresh(DocumentPublicationPreparationRecord template) {
        var command = new DocumentPublicationCommand(template.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(template.key().account(), template.key().principal(), command.operationId());
        return new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), template.placements(), LEASE, 0);
    }
    private static Map<String, DocumentPublicationCandidate.Mode> modes(DocumentPublicationPreparationRecord record) {
        return Map.of(record.command().intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED);
    }
    private static long count(Context c, DocumentPublicationPreparationRecord record, String table) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                .setParameter("a", record.key().account()).setParameter("p", record.key().principal())
                .setParameter("o", record.key().operationId()).getSingleResult()).longValue());
    }
}
