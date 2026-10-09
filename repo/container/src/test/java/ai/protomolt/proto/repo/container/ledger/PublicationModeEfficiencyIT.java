package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Database work of the scoped publication-mode comparison on real PostgreSQL: one
 * fenced transaction, five statements, exact reservations, and the ordering
 * guarantees under takeover, expiry, cancellation and SQL failure.
 */
@Testcontainers
class PublicationModeEfficiencyIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final Map<String, DocumentPublicationCandidate.Mode> MODES = Map.of(
            "member-0", DocumentPublicationCandidate.Mode.TYPED, "member-1", DocumentPublicationCandidate.Mode.OPAQUE);
    private static final Map<String, DocumentPublicationCandidate.Mode> OTHER = Map.of(
            "member-0", DocumentPublicationCandidate.Mode.OPAQUE, "member-1", DocumentPublicationCandidate.Mode.OPAQUE);
    /** Statement text that only the single-read capture issues: both rows in one read. */
    private static final String ROWS = "p.preparation_bytes,p.preparation_sha256,p.owner_nonce,p.command_sha256,m.owner_nonce";
    private static final String SIZES = "octet_length(p.preparation_bytes), octet_length(m.modes::text)";

    record Journaled(DocumentPublicationPreparationRecord value, RepositoryExecutionClaimLedger.Claim claim,
            RepositoryOperationLedger.Owner owner, long preparationBytes, long modesBytes) {
        RepositoryCaller scoped() { return new RepositoryCaller("principal", false, Set.of(value.key().account()), Set.of()); }
        PayloadBudget exactBudget() { return new PayloadBudget(preparationBytes + modesBytes); }
    }

    @Test void validComparisonUsesOneFencedTransactionAndFiveStatements() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE);
            var budget = fixture.exactBudget();
            var peak = new AtomicLong(-1);
            jdbc.afterExecute(call -> { if (call.touches(ROWS)) peak.set(budget.reservedBytes()); });
            new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(), fixture.owner(),
                    fixture.value().command(), MODES, NONE);
            System.out.println("PUBLICATION_MODE_EFFICIENCY valid " + jdbc.summary());
            assertThat(jdbc.acquisitions()).as("pool acquisitions").isEqualTo(1);
            assertThat(jdbc.commits()).as("commits").isEqualTo(1);
            assertThat(jdbc.rollbacks()).as("rollbacks").isZero();
            var executes = jdbc.executes();
            assertThat(executes).as("statements").hasSize(5);
            assertThat(executes.get(0).touches("fence_repository_execution_claim")).as("claim fence first").isTrue();
            assertThat(executes.get(1).touches("UPDATE repository_operation_owners")).as("owner fence second").isTrue();
            assertThat(executes.get(2).touches("FROM repository_operations")).as("command binding third").isTrue();
            assertThat(executes.get(3).touches(SIZES)).as("sizes before bytes").isTrue();
            assertThat(executes.get(4).touches(ROWS)).as("both rows in one read").isTrue();
            assertThat(peak.get()).as("exact reservation while bytes are held").isEqualTo(fixture.preparationBytes() + fixture.modesBytes());
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void mismatchRollsBackTheFenceWithoutACommit() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE); var budget = fixture.exactBudget();
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(),
                    fixture.owner(), fixture.value().command(), OTHER, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                        assertThat(failure.getMessage()).isEqualTo("Observed publication modes differ from fixed modes");
                    });
            System.out.println("PUBLICATION_MODE_EFFICIENCY mismatch " + jdbc.summary());
            assertThat(jdbc.commits()).isZero();
            assertThat(jdbc.rollbacks()).isEqualTo(1);
            assertThat(jdbc.acquisitions()).isEqualTo(1);
            assertThat(jdbc.executes()).hasSize(5);
            assertThat(budget.reservedBytes()).isZero();
            // The refused comparison changed nothing durable: the next fence still succeeds.
            c.tx().inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, fixture.owner()); });
        }
    }

    @Test void claimOnlyComparisonReadsSizesOnlyAndCommitsOnce() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var value = input(c);
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), UUID.randomUUID(), LEASE);
            var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim)
                    .owner().orElseThrow();
            var budget = new PayloadBudget(1);
            new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(CALLER, owner, value.command(), MODES, NONE);
            System.out.println("PUBLICATION_MODE_EFFICIENCY claim-only " + jdbc.summary());
            assertThat(jdbc.commits()).isEqualTo(1);
            assertThat(jdbc.acquisitions()).isEqualTo(1);
            assertThat(jdbc.executes()).hasSize(4);
            assertThat(jdbc.executes().get(3).touches(SIZES)).isTrue();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "members", "value", "nonce", "preparation", "preparation-and-missing"})
    void absentAndMalformedStoredRowsKeepClassificationAndOrderAndRollBack(String corruption) {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE); var budget = new PayloadBudget(32L * 1024 * 1024);
            // Damage real stored rows deliberately, bypassing the immutability guards, to exercise Java handling.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE repository_publication_modes DISABLE TRIGGER repository_publication_modes_guard").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_operation_owners DISABLE TRIGGER repository_journaled_owner_guard").executeUpdate();
                if (corruption.contains("missing")) em.createNativeQuery("DELETE FROM repository_publication_modes").executeUpdate();
                if (corruption.equals("members")) em.createNativeQuery("UPDATE repository_publication_modes SET modes='{\"wrong\":\"TYPED\"}'::jsonb").executeUpdate();
                if (corruption.equals("value")) em.createNativeQuery("UPDATE repository_publication_modes SET modes='{\"member-0\":1,\"member-1\":\"OPAQUE\"}'::jsonb").executeUpdate();
                if (corruption.equals("nonce")) em.createNativeQuery("UPDATE repository_publication_modes SET owner_nonce=:nonce")
                        .setParameter("nonce", UUID.randomUUID()).executeUpdate();
                if (corruption.startsWith("preparation")) {
                    em.createNativeQuery("ALTER TABLE repository_publication_preparations DISABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
                    em.createNativeQuery("ALTER TABLE repository_publication_preparations DROP CONSTRAINT repository_preparation_digest").executeUpdate();
                    em.createNativeQuery("UPDATE repository_publication_preparations SET preparation_bytes=set_byte(preparation_bytes,0,0)").executeUpdate();
                }
                return null;
            });
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget)
                    .requireObservedModes(CALLER, fixture.owner(), fixture.value().command(), MODES, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                        if (corruption.startsWith("preparation")) {
                            assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS);
                            assertThat(failure.getMessage()).contains("preparation integrity");
                        } else if (corruption.equals("missing")) {
                            assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                            assertThat(failure.getMessage()).isEqualTo("Fixed publication modes are absent");
                        } else {
                            assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS);
                            assertThat(failure.getMessage()).isEqualTo("Private publication modes are invalid");
                        }
                    });
            System.out.println("PUBLICATION_MODE_EFFICIENCY " + corruption + " " + jdbc.summary());
            assertThat(jdbc.commits()).isZero();
            assertThat(jdbc.rollbacks()).isEqualTo(1);
            assertThat(jdbc.executes()).as("both reads complete before any decode refusal").hasSize(5);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void insufficientBudgetForEitherRowReleasesTheOtherReservation() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE);
            for (long capacity : new long[]{fixture.preparationBytes() - 1, fixture.preparationBytes(),
                    fixture.preparationBytes() + fixture.modesBytes() - 1}) {
                jdbc.reset();
                var budget = new PayloadBudget(capacity);
                assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(),
                        fixture.owner(), fixture.value().command(), MODES, NONE))
                        .as("capacity " + capacity).isInstanceOf(PayloadBudget.CapacityExceededException.class);
                assertThat(jdbc.commits()).isZero();
                assertThat(jdbc.rollbacks()).isEqualTo(1);
                assertThat(jdbc.executes()).as("bytes are never fetched without a reservation").hasSize(4);
                assertThat(budget.reservedBytes()).as("capacity " + capacity).isZero();
            }
            new DocumentPublicationModesJournal(jdbc.tx(), fixture.exactBudget()).requireObservedModes(fixture.scoped(),
                    fixture.owner(), fixture.value().command(), MODES, NONE);
        }
    }

    @Test void scopeAndAuthorizationRefusalsTouchNoConnection() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE);
            var journal = new DocumentPublicationModesJournal(jdbc.tx(), fixture.exactBudget());
            var foreign = new RepositoryCaller("principal", false, Set.of("other-account"), Set.of());
            assertThatThrownBy(() -> journal.requireObservedModes(foreign, fixture.owner(), fixture.value().command(), MODES, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            var otherPrincipal = new RepositoryCaller("someone-else", false, Set.of(fixture.value().key().account()), Set.of());
            assertThatThrownBy(() -> journal.requireObservedModes(otherPrincipal, fixture.owner(), fixture.value().command(), MODES, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            var otherCommand = new DocumentPublicationCommand(fixture.value().command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            assertThatThrownBy(() -> journal.requireObservedModes(fixture.scoped(), fixture.owner(), otherCommand, MODES, NONE))
                    .isInstanceOf(IllegalArgumentException.class).hasMessage("Mode command differs from owner");
            var cancelled = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return true; }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            assertThatThrownBy(() -> journal.requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), MODES, cancelled))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            assertThat(jdbc.acquisitions()).isZero();
            assertThat(jdbc.calls()).isEmpty();
        }
    }

    @Test void takeoverWaitsOnTheValidationFenceAndRevokesTheNextTransaction() throws Exception {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool());
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var fixture = journaled(c, Duration.ofSeconds(2), LEASE); var budget = fixture.exactBudget();
            var gate = new Gate(jdbc);
            var validation = workers.submit(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget)
                    .requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), MODES, NONE));
            try {
                assertThat(gate.captured.await(5, TimeUnit.SECONDS)).as("validation holds its fence with both rows read").isTrue();
                c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(2.1)").getSingleResult());
                var next = UUID.randomUUID();
                var takeover = workers.submit(() -> new RepositoryExecutionClaimLedger(c.tx())
                        .takeOver(fixture.value().key(), fixture.value().command(), 1, next, LEASE));
                assertThat(waitingOnClaimLock(c)).as("takeover of the expired claim waits on the validation's row lock").isTrue();
                assertThat(validation.isDone()).as("validation is still inside its transaction").isFalse();
                gate.release.countDown();
                validation.get(5, TimeUnit.SECONDS);
                assertThat(takeover.get(5, TimeUnit.SECONDS).epoch()).isEqualTo(2);
            } finally { gate.release.countDown(); }
            assertThat(jdbc.commits()).as("delivered comparison committed once").isEqualTo(1);
            assertThat(jdbc.rollbacks()).isZero();
            assertThat(budget.reservedBytes()).isZero();
            // The delivered result grants nothing: the caller's next fence sees the takeover.
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, fixture.owner()); }))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(),
                    fixture.owner(), fixture.value().command(), MODES, NONE)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void takeoverBeforeTheFenceRefusesWithoutReadingOrReserving() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, Duration.ofSeconds(1), LEASE); var budget = fixture.exactBudget();
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var next = new RepositoryExecutionClaimLedger(c.tx()).takeOver(fixture.value().key(), fixture.value().command(), 1, UUID.randomUUID(), LEASE);
            assertThat(next.epoch()).isEqualTo(2);
            var peak = new AtomicLong();
            jdbc.afterExecute(call -> peak.accumulateAndGet(budget.reservedBytes(), Math::max));
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(),
                    fixture.owner(), fixture.value().command(), MODES, NONE)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThat(jdbc.executes()).as("the claim fence is the first and only statement").hasSize(1);
            assertThat(jdbc.executes().getFirst().touches("fence_repository_execution_claim")).isTrue();
            assertThat(jdbc.commits()).isZero();
            assertThat(jdbc.rollbacks()).isEqualTo(1);
            assertThat(peak.get()).as("no reservation before the fence").isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void ownerExpiryAfterTheFenceIsCaughtByTheNextFence() throws Exception {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool());
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var fixture = journaled(c, LEASE, Duration.ofSeconds(2)); var budget = fixture.exactBudget();
            var gate = new Gate(jdbc);
            var validation = workers.submit(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget)
                    .requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), MODES, NONE));
            try {
                assertThat(gate.captured.await(5, TimeUnit.SECONDS)).isTrue();
                c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(2.1)").getSingleResult());
                gate.release.countDown();
                validation.get(5, TimeUnit.SECONDS);
            } finally { gate.release.countDown(); }
            assertThat(jdbc.commits()).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, fixture.owner()); }))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(),
                    fixture.owner(), fixture.value().command(), MODES, NONE)).isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"inside", "after-commit"})
    void cancellationIsObservedInsideTheFenceAndAfterCommit(String when) throws Exception {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool());
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var fixture = journaled(c, LEASE, LEASE); var budget = fixture.exactBudget();
            var cancelled = new AtomicBoolean();
            var control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return cancelled.get(); }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            Future<?> validation;
            if (when.equals("inside")) {
                var gate = new Gate(jdbc);
                validation = workers.submit(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget)
                        .requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), MODES, control));
                try {
                    assertThat(gate.captured.await(5, TimeUnit.SECONDS)).isTrue();
                    cancelled.set(true);
                } finally { gate.release.countDown(); }
            } else {
                jdbc.beforeCommit(connection -> cancelled.set(true));
                validation = workers.submit(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget)
                        .requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), MODES, control));
            }
            assertThatThrownBy(() -> validation.get(5, TimeUnit.SECONDS)).isInstanceOfSatisfying(ExecutionException.class,
                    failure -> assertThat(failure.getCause()).isInstanceOfSatisfying(RepositoryException.class,
                            cause -> assertThat(cause.code()).isEqualTo(RepositoryException.Code.CANCELLED)));
            System.out.println("PUBLICATION_MODE_EFFICIENCY cancel-" + when + " " + jdbc.summary());
            assertThat(jdbc.commits()).isEqualTo(when.equals("inside") ? 0 : 1);
            assertThat(jdbc.rollbacks()).isEqualTo(when.equals("inside") ? 1 : 0);
            assertThat(budget.reservedBytes()).isZero();
            c.tx().inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, fixture.owner()); });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"rows-statement", "commit"})
    void sqlFailurePropagatesAndReleasesEveryReservation(String site) {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE); var budget = fixture.exactBudget();
            var reservedAtFailure = new AtomicLong(-1);
            if (site.equals("rows-statement"))
                jdbc.beforeExecute(call -> {
                    if (call.touches(ROWS)) { reservedAtFailure.set(budget.reservedBytes()); throw new SQLException("injected statement failure", "57014"); }
                });
            else
                jdbc.beforeCommit(connection -> { reservedAtFailure.set(budget.reservedBytes()); throw new SQLException("injected commit failure", "40001"); });
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(),
                    fixture.owner(), fixture.value().command(), MODES, NONE))
                    .isNotInstanceOf(RepositoryException.class).hasStackTraceContaining("injected " + site.replace("rows-", "") + " failure");
            System.out.println("PUBLICATION_MODE_EFFICIENCY fail-" + site + " " + jdbc.summary());
            assertThat(reservedAtFailure.get()).as("reservation was live when the failure struck")
                    .isEqualTo(fixture.preparationBytes() + fixture.modesBytes());
            assertThat(jdbc.commits()).isZero();
            assertThat(budget.reservedBytes()).isZero();
            jdbc.reset();
            new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(), fixture.owner(),
                    fixture.value().command(), MODES, NONE);
            assertThat(jdbc.commits()).isEqualTo(1);
        }
    }

    @Test void boundModesComparisonIsOneStatementUnderTheOwnerLock() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE);
            var command = fixture.value().command(); var key = fixture.owner().key(); long generation = fixture.owner().generation();
            jdbc.tx().inTransaction(em -> {
                RepositoryOperationLedger.lockLiveOwner(em, fixture.owner());
                int before = jdbc.executes().size();
                DocumentPublicationModesJournal.requireBoundModes(em, key, command, generation, DocumentPublicationModesJournal.encode(command, MODES));
                assertThat(jdbc.executes().size() - before).as("one statement").isEqualTo(1);
                assertThatThrownBy(() -> DocumentPublicationModesJournal.requireBoundModes(em, key, command, generation,
                        DocumentPublicationModesJournal.encode(command, OTHER)))
                        .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                            assertThat(failure.getMessage()).isEqualTo("Publication modes differ from fixed modes");
                        });
                assertThatThrownBy(() -> DocumentPublicationModesJournal.requireBoundModes(em, key, command, generation + 1,
                        DocumentPublicationModesJournal.encode(command, MODES)))
                        .as("absent binding for another generation").isInstanceOfSatisfying(RepositoryException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS);
                            assertThat(failure.getMessage()).isEqualTo("Terminal publication mode binding is invalid");
                        });
                assertThatThrownBy(() -> DocumentPublicationModesJournal.requireBoundModes(em, key, command, generation,
                        "{\"member-0\":\"TYPED\",\"member-1\":\"OPAQUE\",\"extra\":\"TYPED\"}"))
                        .as("malformed observed binding").isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
                return null;
            });
        }
    }

    @Test void decodeWindowInsideTheFenceIsMeasured() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE); var budget = fixture.exactBudget();
            var rowsDone = new AtomicLong(); var commitStart = new AtomicLong();
            jdbc.afterExecute(call -> { if (call.touches(ROWS)) rowsDone.set(System.nanoTime()); });
            jdbc.beforeCommit(connection -> commitStart.set(System.nanoTime()));
            var journal = new DocumentPublicationModesJournal(jdbc.tx(), budget);
            long[] windows = new long[5];
            for (int i = 0; i < windows.length; i++) {
                journal.requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), MODES, NONE);
                windows[i] = commitStart.get() - rowsDone.get();
                assertThat(windows[i]).isPositive();
            }
            // Bound of the modes decode alone: a one-member-id-per-entry object at the 1 MiB column limit.
            var json = new JsonObject();
            for (int i = 0; i < 9000; i++) json.addProperty("member-" + "x".repeat(90) + i, i % 2 == 0 ? "TYPED" : "OPAQUE");
            String text = json.toString();
            assertThat(text.length()).isLessThanOrEqualTo(DocumentPublicationModesJournal.MAX_BYTES)
                    .isGreaterThan(DocumentPublicationModesJournal.MAX_BYTES * 3 / 4);
            long parseStart = System.nanoTime();
            var parsed = JsonParser.parseString(text).getAsJsonObject();
            long parseNanos = System.nanoTime() - parseStart;
            assertThat(parsed.size()).isEqualTo(json.size());
            System.out.println("PUBLICATION_MODE_EFFICIENCY decode-window-us preparation_bytes=" + fixture.preparationBytes()
                    + " modes_bytes=" + fixture.modesBytes() + " windows=" + java.util.Arrays.toString(
                            java.util.Arrays.stream(windows).map(nanos -> nanos / 1000).toArray())
                    + " max_modes_json_bytes=" + text.length() + " entries=" + json.size() + " parse_us=" + parseNanos / 1000);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    /** Holds the validation inside its transaction right after both rows are read, with every lock still held. */
    private static final class Gate {
        final CountDownLatch captured = new CountDownLatch(1); final CountDownLatch release = new CountDownLatch(1);
        Gate(PublicationModeEfficiencyJdbc jdbc) {
            jdbc.afterExecute(call -> {
                if (!call.touches(ROWS)) return;
                captured.countDown();
                try { if (!release.await(10, TimeUnit.SECONDS)) throw new SQLException("Gate release timed out"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new SQLException(interrupted); }
            });
        }
    }

    private static boolean waitingOnClaimLock(Context c) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < until) {
            boolean waiting = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
                      AND wait_event_type='Lock' AND query LIKE '%repository_execution_claims%FOR UPDATE%'
                    """).getSingleResult()).intValue() > 0);
            if (waiting) return true;
            Thread.sleep(10);
        }
        return false;
    }

    private static Journaled journaled(Context c, Duration claimLease, Duration ownerLease) {
        var value = input(c); var budget = new PayloadBudget(32L * 1024 * 1024);
        var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), UUID.randomUUID(), claimLease);
        new DocumentPublicationPreparationJournal(c.tx(), budget).save(CALLER, claim, value, NONE);
        new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
        var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), ownerLease, claim)
                .owner().orElseThrow();
        assertThat(budget.reservedBytes()).isZero();
        var sizes = (Object[]) c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT octet_length(p.preparation_bytes), octet_length(m.modes::text)
                FROM repository_publication_preparations p JOIN repository_publication_modes m
                USING(account_id,principal,operation_id,predecessor_generation)
                """).getSingleResult());
        return new Journaled(value, claim, owner, ((Number) sizes[0]).longValue(), ((Number) sizes[1]).longValue());
    }

    private static DocumentPublicationPreparationRecord input(Context c) {
        var source = prepare(c, 2, true);
        var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
        UUID id = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var placement = DocumentUploadPlan.Placement.sample(new DriveLedger(c.tx()).findById(id).orElseThrow(), "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
        return new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), Map.of(id, placement), LEASE, 0);
    }
}
