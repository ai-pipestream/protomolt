package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 * transaction fenced before the reads and again after the decode, eight statements,
 * exact reservations, the ordering guarantees under takeover, expiry, cancellation
 * and SQL failure, and the lock window at the 16 MiB preparation bound.
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

    record Journaled(DocumentPublicationPreparationRecord value, Map<String, DocumentPublicationCandidate.Mode> modes,
            RepositoryExecutionClaimLedger.Claim claim,
            RepositoryOperationLedger.Owner owner, long preparationBytes, long modesBytes) {
        RepositoryCaller scoped() { return new RepositoryCaller("principal", false, Set.of(value.key().account()), Set.of()); }
        PayloadBudget exactBudget() { return new PayloadBudget(preparationBytes + modesBytes); }
    }

    @Test void validComparisonUsesOneTransactionFencedBeforeAndAfterTheDecode() {
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
            assertThat(executes).as("statements").hasSize(8);
            assertThat(executes.get(0).touches("fence_repository_execution_claim")).as("claim fence first").isTrue();
            assertThat(executes.get(1).touches("UPDATE repository_operation_owners")).as("owner fence second").isTrue();
            assertThat(executes.get(2).touches("FROM repository_operations")).as("command binding third").isTrue();
            assertThat(executes.get(3).touches(SIZES)).as("sizes before bytes").isTrue();
            assertThat(executes.get(4).touches(ROWS)).as("both rows in one read").isTrue();
            assertThat(executes.get(5).touches("fence_repository_execution_claim")).as("claim fenced again after the decode").isTrue();
            assertThat(executes.get(6).touches("UPDATE repository_operation_owners")).as("owner fenced again").isTrue();
            assertThat(executes.get(7).touches("FROM repository_operations")).as("command binding again").isTrue();
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

    @Test void takeoverWaitsOnTheValidationLocksAndTheSecondFenceRefusesTheExpiredClaim() throws Exception {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool());
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var fixture = journaled(c, Duration.ofSeconds(2), LEASE); var budget = fixture.exactBudget();
            String stamp = ownerStamp(c, fixture);
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
                // The claim expired inside the decode window: the second fence refuses delivery.
                assertThatThrownBy(() -> validation.get(5, TimeUnit.SECONDS)).isInstanceOfSatisfying(ExecutionException.class,
                        failure -> assertThat(failure.getCause()).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class));
                assertThat(takeover.get(5, TimeUnit.SECONDS).epoch()).isEqualTo(2);
            } finally { gate.release.countDown(); }
            assertThat(jdbc.commits()).as("refused comparison commits nothing").isZero();
            assertThat(jdbc.rollbacks()).isEqualTo(1);
            assertThat(jdbc.executes()).as("refused at the second claim fence").hasSize(6);
            assertThat(jdbc.executes().get(5).touches("fence_repository_execution_claim")).isTrue();
            assertThat(budget.reservedBytes()).isZero();
            assertThat(ownerStamp(c, fixture)).as("no durable owner fence stamp").isEqualTo(stamp);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, fixture.owner()); }))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(),
                    fixture.owner(), fixture.value().command(), MODES, NONE)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void takeoverAfterTheValidationCommitsIsRefusedByTheNextFence() throws Exception {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, Duration.ofSeconds(2), LEASE); var budget = fixture.exactBudget();
            new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(), fixture.owner(),
                    fixture.value().command(), MODES, NONE);
            assertThat(jdbc.commits()).isEqualTo(1);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(2.1)").getSingleResult());
            assertThat(new RepositoryExecutionClaimLedger(c.tx()).takeOver(fixture.value().key(), fixture.value().command(), 1,
                    UUID.randomUUID(), LEASE).epoch()).isEqualTo(2);
            // The delivered comparison grants nothing: the caller's next fence sees the takeover.
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, fixture.owner()); }))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
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

    @ParameterizedTest
    @ValueSource(strings = {"owner", "claim"})
    void leaseExpiryDuringTheDecodeWindowIsRefusedBeforeCommit(String lease) throws Exception {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool());
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var fixture = lease.equals("owner") ? journaled(c, LEASE, Duration.ofSeconds(2)) : journaled(c, Duration.ofSeconds(2), LEASE);
            var budget = fixture.exactBudget();
            String stamp = ownerStamp(c, fixture);
            Class<? extends RuntimeException> refusal = lease.equals("owner")
                    ? RepositoryOperationLedger.OwnerFencedException.class : RepositoryExecutionClaimLedger.Fenced.class;
            var gate = new Gate(jdbc);
            var validation = workers.submit(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget)
                    .requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), MODES, NONE));
            try {
                assertThat(gate.captured.await(5, TimeUnit.SECONDS)).isTrue();
                c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(2.1)").getSingleResult());
                gate.release.countDown();
                assertThatThrownBy(() -> validation.get(5, TimeUnit.SECONDS)).isInstanceOfSatisfying(ExecutionException.class,
                        failure -> assertThat(failure.getCause()).isInstanceOf(refusal));
            } finally { gate.release.countDown(); }
            System.out.println("PUBLICATION_MODE_EFFICIENCY expiry-" + lease + " " + jdbc.summary());
            assertThat(jdbc.commits()).as("expired comparison commits nothing").isZero();
            assertThat(jdbc.rollbacks()).isEqualTo(1);
            assertThat(jdbc.executes()).as("refused by the second fence").hasSize(lease.equals("owner") ? 7 : 6);
            assertThat(budget.reservedBytes()).isZero();
            assertThat(ownerStamp(c, fixture)).as("no durable owner fence stamp").isEqualTo(stamp);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, fixture.owner()); }))
                    .isInstanceOf(refusal);
            assertThatThrownBy(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget).requireObservedModes(fixture.scoped(),
                    fixture.owner(), fixture.value().command(), MODES, NONE)).isInstanceOf(refusal);
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
    @ValueSource(strings = {"rows-statement", "refence", "commit"})
    void sqlFailurePropagatesAndReleasesEveryReservation(String site) {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = journaled(c, LEASE, LEASE); var budget = fixture.exactBudget();
            var reservedAtFailure = new AtomicLong(-1);
            if (site.equals("rows-statement"))
                jdbc.beforeExecute(call -> {
                    if (call.touches(ROWS)) { reservedAtFailure.set(budget.reservedBytes()); throw new SQLException("injected statement failure", "57014"); }
                });
            else if (site.equals("refence")) {
                var fences = new AtomicInteger();
                jdbc.beforeExecute(call -> {
                    if (call.touches("fence_repository_execution_claim") && fences.incrementAndGet() == 2) {
                        reservedAtFailure.set(budget.reservedBytes()); throw new SQLException("injected refence failure", "57014");
                    }
                });
            }
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
            var journal = new DocumentPublicationModesJournal(jdbc.tx(), budget);
            long[] windows = new long[5];
            for (int i = 0; i < windows.length; i++) {
                jdbc.reset();
                var marks = new Marks(jdbc, budget);
                journal.requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), MODES, NONE);
                windows[i] = marks.decodeNanos();
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
            System.out.println("PUBLICATION_MODE_EFFICIENCY decode-window-us (rows read to second fence) preparation_bytes=" + fixture.preparationBytes()
                    + " modes_bytes=" + fixture.modesBytes() + " windows=" + Arrays.toString(
                            Arrays.stream(windows).map(nanos -> nanos / 1000).toArray())
                    + " max_modes_json_bytes=" + text.length() + " entries=" + json.size() + " parse_us=" + parseNanos / 1000);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    /**
     * The lock window at the preparation bound: from the first claim fence, which takes the claim
     * and owner row locks, to the return after commit. The first call decodes on a JVM that has not
     * yet decoded a preparation this large when the test runs alone; the later calls are warm.
     */
    @Test void maximumPreparationLockWindowIsMeasured() {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool())) {
            var fixture = maximal(c, prepare(c, MAXIMAL_MEMBERS, true), LEASE, LEASE);
            assertThat(fixture.preparationBytes()).isEqualTo(DocumentPublicationPreparationCodec.MAX_BYTES);
            var budget = fixture.exactBudget();
            var threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            var journal = new DocumentPublicationModesJournal(jdbc.tx(), budget);
            int calls = 6;
            long[] lock = new long[calls], decode = new long[calls], allocated = new long[calls];
            for (int i = 0; i < calls; i++) {
                jdbc.reset();
                var marks = new Marks(jdbc, budget);
                long allocatedBefore = threads.getCurrentThreadAllocatedBytes();
                journal.requireObservedModes(fixture.scoped(), fixture.owner(), fixture.value().command(), fixture.modes(), NONE);
                long end = System.nanoTime();
                allocated[i] = threads.getCurrentThreadAllocatedBytes() - allocatedBefore;
                lock[i] = end - marks.locked.get(); decode[i] = marks.decodeNanos();
                assertThat(jdbc.acquisitions()).isEqualTo(1);
                assertThat(jdbc.commits()).isEqualTo(1);
                assertThat(jdbc.rollbacks()).isZero();
                assertThat(jdbc.executes()).hasSize(8);
                assertThat(marks.reservedAtRows.get()).as("exact reservation while the bytes are held")
                        .isEqualTo(fixture.preparationBytes() + fixture.modesBytes());
                assertThat(budget.reservedBytes()).isZero();
            }
            System.out.println("PUBLICATION_MODE_EFFICIENCY max-preparation preparation_bytes=" + fixture.preparationBytes()
                    + " modes_bytes=" + fixture.modesBytes() + " members=" + fixture.modes().size()
                    + " drives=" + fixture.value().placements().size() + " statements=8 commits=1"
                    + " reserved_bytes=" + (fixture.preparationBytes() + fixture.modesBytes())
                    + " lock_us=" + Arrays.toString(Arrays.stream(lock).map(nanos -> nanos / 1000).toArray())
                    + " decode_us=" + Arrays.toString(Arrays.stream(decode).map(nanos -> nanos / 1000).toArray())
                    + " allocated_mib=" + Arrays.toString(Arrays.stream(allocated).map(bytes -> bytes / (1024 * 1024)).toArray())
                    + " " + host());
        }
    }

    /**
     * Holds a bound-size validation inside its transaction: an operation on another claim completes
     * meanwhile, and a takeover of the held claim waits on its row lock. After release the 16 MiB
     * decode runs, the second fence refuses the claim that expired while held, and the takeover wins.
     */
    @Test void heldMaximumValidationBlocksOnlyItsClaimAndTheTakeoverWaitIsBounded() throws Exception {
        try (var c = context(POSTGRES); var jdbc = new PublicationModeEfficiencyJdbc(c.pool());
             var other = new PublicationModeEfficiencyJdbc(c.pool()); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var source = prepare(c, MAXIMAL_MEMBERS, true);
            var held = maximal(c, source, Duration.ofSeconds(5), LEASE);
            var unrelated = journal(c, ordinary(c, source), held.modes(), LEASE, LEASE);
            var budget = held.exactBudget();
            String stamp = ownerStamp(c, held);
            var gate = new Gate(jdbc);
            var validation = workers.submit(() -> new DocumentPublicationModesJournal(jdbc.tx(), budget)
                    .requireObservedModes(held.scoped(), held.owner(), held.value().command(), held.modes(), NONE));
            long unrelatedNanos, takeoverWait;
            try {
                assertThat(gate.captured.await(10, TimeUnit.SECONDS)).as("held with the 16 MiB rows read").isTrue();
                long started = System.nanoTime();
                new DocumentPublicationModesJournal(other.tx(), unrelated.exactBudget()).requireObservedModes(unrelated.scoped(),
                        unrelated.owner(), unrelated.value().command(), unrelated.modes(), NONE);
                unrelatedNanos = System.nanoTime() - started;
                assertThat(other.commits()).as("the other claim's comparison committed").isEqualTo(1);
                assertThat(validation.isDone()).as("still held").isFalse();
                outlive(c, held);
                var takeoverEnd = new AtomicLong();
                var takeover = workers.submit(() -> {
                    var claim = new RepositoryExecutionClaimLedger(c.tx()).takeOver(held.value().key(), held.value().command(), 1,
                            UUID.randomUUID(), LEASE);
                    takeoverEnd.set(System.nanoTime());
                    return claim;
                });
                assertThat(waitingOnClaimLock(c)).as("takeover waits on the held claim row").isTrue();
                long released = System.nanoTime();
                gate.release.countDown();
                assertThatThrownBy(() -> validation.get(30, TimeUnit.SECONDS)).isInstanceOfSatisfying(ExecutionException.class,
                        failure -> assertThat(failure.getCause()).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class));
                assertThat(takeover.get(30, TimeUnit.SECONDS).epoch()).isEqualTo(2);
                takeoverWait = takeoverEnd.get() - released;
            } finally { gate.release.countDown(); }
            System.out.println("PUBLICATION_MODE_EFFICIENCY max-preparation-held preparation_bytes=" + held.preparationBytes()
                    + " unrelated_preparation_bytes=" + unrelated.preparationBytes() + " unrelated_us=" + unrelatedNanos / 1000
                    + " takeover_after_release_us=" + takeoverWait / 1000 + " " + jdbc.summary() + " " + host());
            assertThat(jdbc.commits()).isZero();
            assertThat(jdbc.rollbacks()).isEqualTo(1);
            assertThat(jdbc.executes()).as("refused at the second claim fence").hasSize(6);
            assertThat(budget.reservedBytes()).isZero();
            assertThat(unrelated.exactBudget().reservedBytes()).isZero();
            assertThat(ownerStamp(c, held)).as("no durable owner fence stamp").isEqualTo(stamp);
        }
    }

    /** Per-call marks: the first claim fence (locks taken), both rows read, and the second claim fence (decode done). */
    private static final class Marks {
        final AtomicLong locked = new AtomicLong(), rows = new AtomicLong(), refence = new AtomicLong(), reservedAtRows = new AtomicLong(-1);
        Marks(PublicationModeEfficiencyJdbc jdbc, PayloadBudget budget) {
            jdbc.beforeExecute(call -> {
                if (!call.touches("fence_repository_execution_claim")) return;
                if (!locked.compareAndSet(0, System.nanoTime())) refence.compareAndSet(0, System.nanoTime());
            });
            jdbc.afterExecute(call -> {
                if (!call.touches(ROWS)) return;
                rows.set(System.nanoTime()); reservedAtRows.set(budget.reservedBytes());
            });
        }
        long decodeNanos() { return refence.get() - rows.get(); }
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

    /** The owner row's fence stamp; a refused comparison must leave it as it was. */
    private static String ownerStamp(Context c, Journaled fixture) {
        return c.tx().readOnly(em -> (String) em.createNativeQuery("""
                SELECT write_fence_xid::text FROM repository_operation_owners
                WHERE account_id=:a AND principal=:p AND operation_id=:o
                """).setParameter("a", fixture.value().key().account()).setParameter("p", fixture.value().key().principal())
                .setParameter("o", fixture.value().key().operationId()).getSingleResult());
    }

    private static Journaled journaled(Context c, Duration claimLease, Duration ownerLease) {
        return journal(c, input(c), MODES, claimLease, ownerLease);
    }

    private static Journaled journal(Context c, DocumentPublicationPreparationRecord value,
            Map<String, DocumentPublicationCandidate.Mode> modes, Duration claimLease, Duration ownerLease) {
        var budget = new PayloadBudget(64L * 1024 * 1024);
        var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), UUID.randomUUID(), claimLease);
        new DocumentPublicationPreparationJournal(c.tx(), budget).save(CALLER, claim, value, NONE);
        new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, modes, NONE);
        var owner = new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), ownerLease, claim)
                .owner().orElseThrow();
        assertThat(budget.reservedBytes()).isZero();
        var sizes = (Object[]) c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT octet_length(p.preparation_bytes), octet_length(m.modes::text)
                FROM repository_publication_preparations p JOIN repository_publication_modes m
                USING(account_id,principal,operation_id,predecessor_generation)
                WHERE p.operation_id=:o
                """).setParameter("o", value.key().operationId()).getSingleResult());
        return new Journaled(value, modes, claim, owner, ((Number) sizes[0]).longValue(), ((Number) sizes[1]).longValue());
    }

    private static DocumentPublicationPreparationRecord input(Context c) {
        return ordinary(c, prepare(c, 2, true));
    }

    /** A fresh operation over the source's members, all on the fixture's one drive. */
    private static DocumentPublicationPreparationRecord ordinary(Context c, Prepared source) {
        var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
        UUID id = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var placement = DocumentUploadPlan.Placement.sample(new DriveLedger(c.tx()).findById(id).orElseThrow(), "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
        return new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), Map.of(id, placement), LEASE, 0);
    }

    private static Map<String, DocumentPublicationCandidate.Mode> modes(DocumentPublicationCommand command) {
        var modes = new TreeMap<String, DocumentPublicationCandidate.Mode>();
        for (int i = 0; i < command.intent().getMembersCount(); i++)
            modes.put(command.intent().getMembers(i).getMemberId(),
                    i % 2 == 0 ? DocumentPublicationCandidate.Mode.TYPED : DocumentPublicationCandidate.Mode.OPAQUE);
        return Map.copyOf(modes);
    }

    /**
     * A canonical preparation of exactly {@code DocumentPublicationPreparationCodec.MAX_BYTES}, built through the
     * production record constructor, codec and journal: one member per ACTIVE drive of the account, each drive's
     * free-form metadata a JSON object padded until the encoded preparation lands on the bound. Drive metadata has
     * no column bound, so the shape is reachable; the codec's 1 MiB per-text bound is why it takes seventeen drives.
     * The drives are read back from PostgreSQL before encoding, so the placements are what a sampler would see.
     */
    private static Journaled maximal(Context c, Prepared source, Duration claimLease, Duration ownerLease) {
        int members = source.command().intent().getMembersCount();
        var profile = new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow();
        var drives = new ArrayList<DriveRecord>();
        var intent = source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString());
        for (int i = 0; i < members; i++) {
            var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
            drive.name = "large-" + i; drive.bucket = "bucket"; drive.prefix = "root"; drive.provider = "test-location";
            drive.driveType = "CUSTOM"; drive.status = "ACTIVE"; drive.metadata = metadata(0);
            drives.add(drive);
            intent.setMembers(i, intent.getMembers(i).toBuilder().setDriveId(drive.driveId.toString()));
        }
        var command = new DocumentPublicationCommand(intent.build());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
        var seeds = DocumentPublicationSeeds.mint(key, command);
        // The encoded size grows byte for byte with the ASCII padding: measure unpadded, then fill the remainder.
        long remaining = DocumentPublicationPreparationCodec.MAX_BYTES
                - DocumentPublicationPreparationCodec.encode(record(key, command, seeds, drives, profile)).size();
        for (var drive : drives) {
            int pad = (int) Math.min(remaining, TEXT_BOUND - metadata(0).length());
            drive.metadata = metadata(pad); remaining -= pad;
        }
        assertThat(remaining).as("the drives absorb the whole bound").isZero();
        var stored = new ArrayList<DriveRecord>();
        for (var drive : drives) {
            String written = drive.metadata;
            new DriveLedger(c.tx()).insert(drive);
            var read = new DriveLedger(c.tx()).findById(drive.driveId).orElseThrow();
            assertThat(read.metadata).as("stored metadata is already in jsonb's canonical text").isEqualTo(written);
            stored.add(read);
        }
        var value = record(key, command, seeds, stored, profile);
        assertThat(DocumentPublicationPreparationCodec.encode(value).size()).isEqualTo(DocumentPublicationPreparationCodec.MAX_BYTES);
        return journal(c, value, modes(command), claimLease, ownerLease);
    }

    private static final int MAXIMAL_MEMBERS = 17;
    /** The codec's per-text bound, {@code DocumentPublicationPreparationCodec.MAX_TEXT_BYTES}. */
    private static final int TEXT_BOUND = 1024 * 1024;

    private static String metadata(int pad) { return "{\"pad\": \"" + "m".repeat(pad) + "\"}"; }

    private static DocumentPublicationPreparationRecord record(RepositoryOperationLedger.Key key, DocumentPublicationCommand command,
            DocumentPublicationSeeds seeds, List<DriveRecord> drives, ManagedBackendLedger.Profile profile) {
        var placements = new HashMap<UUID, DocumentUploadPlan.Placement>();
        for (var drive : drives) placements.put(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "native-test", profile));
        return new DocumentPublicationPreparationRecord(key, command, seeds, placements, LEASE, 0);
    }

    /** Sleeps on the database clock until the claim's lease has passed. */
    private static void outlive(Context c, Journaled fixture) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM lease_until - clock_timestamp())) + 0.1)
                FROM repository_execution_claims WHERE account_id=:a AND principal=:p AND operation_id=:o
                """).setParameter("a", fixture.value().key().account()).setParameter("p", fixture.value().key().principal())
                .setParameter("o", fixture.value().key().operationId()).getSingleResult());
    }

    private static String host() {
        String load;
        try { load = Files.readString(Path.of("/proc/loadavg")).strip(); }
        catch (IOException unavailable) { load = "unavailable"; }
        return "loadavg=\"" + load + "\" cpus=" + Runtime.getRuntime().availableProcessors()
                + " max_heap_mib=" + Runtime.getRuntime().maxMemory() / (1024 * 1024);
    }
}
