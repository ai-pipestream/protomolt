package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.capture;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.count;
import static org.assertj.core.api.Assertions.*;

/** Real SQL recovery; archived source setup uses synthetic provider observations. */
@Testcontainers
class RepositoryHistoricalPreparationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));

    @ParameterizedTest @ValueSource(strings = {"valid", "wrong-modes", "missing-anchor"})
    void coldPreparationLoadsAnchorBeforeInstalling(String scenario) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var fixedModes = modes(rig);
            var observed = expired(c, rig, fixedModes).candidate().orElseThrow();
            var proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(observed.predecessor(),
                    java.util.UUID.randomUUID(), java.util.UUID.randomUUID(), LEASE, observed.owner());
            var requested = scenario.equals("wrong-modes")
                    ? Map.of(fixedModes.keySet().iterator().next(), DocumentPublicationCandidate.Mode.OPAQUE) : fixedModes;
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var preparation = new RepositoryHistoricalAttemptPreparation(c.tx(), budget, TIMEOUTS,
                    proposal, rig.record().command(), requested, LEASE);
            try (preparation) {
                assertThat(preparation.cold()).isTrue();
                assertThatThrownBy(preparation::requireResolvedRetention).hasMessageContaining("unresolved");
                if (scenario.equals("wrong-modes")) {
                    assertThatThrownBy(() -> preparation.advance(CALLER, CALLER, requested, Map.of(), NONE))
                            .hasMessageContaining("modes differ from fixed modes");
                    assertThat(count(c, "repository_coordinator_expirations")).isZero();
                } else {
                    assertThat(preparation.advance(CALLER, CALLER, requested, Map.of(), NONE))
                            .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                    assertThatThrownBy(preparation::requireResolvedRetention).hasMessageContaining("unresolved");
                    if (scenario.equals("missing-anchor")) {
                        c.tx().inTransaction(em -> {
                            em.createNativeQuery("SET LOCAL session_replication_role = replica").executeUpdate();
                            em.createNativeQuery("DELETE FROM repository_preparation_history_sets WHERE operation_id=:o")
                                    .setParameter("o", rig.record().key().operationId()).executeUpdate();
                        });
                        assertThatThrownBy(() -> preparation.advance(CALLER, CALLER, requested, Map.of(), NONE))
                                .isInstanceOf(RepositoryException.class);
                        assertThatThrownBy(preparation::requireResolvedRetention).hasMessageContaining("unresolved");
                    } else {
                        assertThat(preparation.advance(CALLER, CALLER, requested, Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                        assertThat(DocumentPublicationPreparationCodec.encode(preparation.requireResolvedRetention()))
                                .isEqualTo(DocumentPublicationPreparationCodec.encode(rig.record()));
                        var plan = preparation.installedPlan();
                        assertThat(preparation.advance(CALLER, CALLER, requested, Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                        assertThat(preparation.installedPlan()).isSameAs(plan);
                        assertThat(preparation.proposal()).isSameAs(proposal);
                        assertThat(budget.reservedBytes()).isPositive();
                    }
                }
                assertThat(count(c, "repository_successor_installs")).isEqualTo(scenario.equals("valid") ? 1 : 0);
                assertThat(count(c, "repository_historical_activations")).isZero();
                long published = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_revision_commits WHERE operation_id=:o")
                        .setParameter("o", rig.record().key().operationId()).getSingleResult()).longValue());
                assertThat(published).isZero();
            }
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(preparation::requireResolvedRetention).hasMessageContaining("closed");
        }
    }

    @Test void coldRegistryAdmissionIsBoundedAndKeepsRetryIdentityBeforeSql() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var fixedModes = modes(rig);
            var observed = expired(c, rig, fixedModes);
            var command = rig.record().command();
            var tiny = new PayloadBudget(1);
            try (var refused = new RepositoryInstalledHistoricalAttempts(c.tx(), tiny, new DriveLedger(c.tx()), 1)) {
                assertThatThrownBy(() -> refused.beginColdProposed(CALLER, command, fixedModes, observed, LEASE, TIMEOUTS))
                        .isInstanceOf(PayloadBudget.CapacityExceededException.class);
                assertThat(refused.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
                assertThat(tiny.reservedBytes()).isZero();
            }
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try {
                try (var attempt = owner.beginColdProposed(CALLER, command, fixedModes, observed, LEASE, TIMEOUTS)) {
                    assertThatThrownBy(() -> attempt.installedPlan(CALLER, NONE))
                            .hasMessageContaining("installation is not confirmed");
                    assertThatThrownBy(() -> owner.beginColdProposed(CALLER, command, fixedModes, observed, LEASE, TIMEOUTS))
                            .isInstanceOf(RepositoryException.class);
                }
                long retained = budget.reservedBytes();
                assertThat(retained).isPositive();
                var changedModes = Map.of(fixedModes.keySet().iterator().next(), DocumentPublicationCandidate.Mode.OPAQUE);
                assertThatThrownBy(() -> owner.beginColdProposed(CALLER, command, changedModes, observed, LEASE, TIMEOUTS))
                        .hasMessageContaining("retry identity changed");
                assertThatThrownBy(() -> owner.beginColdProposed(CALLER, command, fixedModes, observed, LEASE.plusSeconds(1), TIMEOUTS))
                        .hasMessageContaining("retry identity changed");
                assertThatThrownBy(() -> owner.beginProposed(CALLER, rig.record(), fixedModes, observed, LEASE, TIMEOUTS))
                        .hasMessageContaining("retry identity changed");
                var another = new DocumentPublicationCommand(command.intent().toBuilder()
                        .setOperationId(java.util.UUID.randomUUID().toString()).build());
                assertThatThrownBy(() -> owner.beginColdProposed(CALLER, another, fixedModes, observed, LEASE, TIMEOUTS))
                        .hasMessageContaining("capacity exhausted");
                try (var retry = owner.beginColdProposed(CALLER, new DocumentPublicationCommand(command.intent()),
                        fixedModes, null, LEASE, TIMEOUTS)) {
                    assertThat(budget.reservedBytes()).isEqualTo(retained);
                }
                assertThat(count(c, "repository_coordinator_expirations")).isZero();
                assertThat(count(c, "repository_successor_installs")).isZero();
            } finally {
                owner.close();
                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void coldInstalledGenerationCanPassOriginalAnchorToItsSuccessor() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var fixedModes = modes(rig);
            var observed = expired(c, rig, fixedModes);
            var command = rig.record().command();
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 2);
            try (var oldSources = capture(c, rig)) {
                try {
                    java.util.UUID oldId;
                    try (var old = owner.beginColdProposed(CALLER, command, fixedModes, observed, Duration.ofSeconds(3), TIMEOUTS)) {
                        oldId = old.identity();
                        old.advancePreparation(CALLER, fixedModes, Map.of(), NONE);
                        old.advancePreparation(CALLER, fixedModes, Map.of(), NONE);
                        old.attachSources(oldSources.sources(), oldSources.sources().work(), NONE);
                        old.openExecution(CALLER, NONE);
                    }
                    waitExpired(c, rig);
                    var expiredCold = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                            .inspect(CALLER, rig.record().key(), command.sha256(), NONE);
                    assertThat(expiredCold.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
                    try (var freshSources = capture(c, rig)) {
                        java.util.UUID nextId;
                        try (var next = owner.beginSuccessor(CALLER, CALLER, oldId, command,
                                fixedModes, expiredCold, LEASE, TIMEOUTS)) {
                            nextId = next.identity();
                            assertThat(nextId).isNotEqualTo(oldId);
                            assertThat(next.advancePreparation(CALLER, fixedModes, Map.of(), NONE))
                                    .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                            assertThat(next.advancePreparation(CALLER, fixedModes, Map.of(), NONE))
                                    .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                            next.attachSources(freshSources.sources(), freshSources.sources().work(), NONE);
                            next.openExecution(CALLER, NONE);
                            assertThat(next.start(LEASE, NONE)).isNotNull();
                        }
                        assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 2));
                        assertThat(oldSources.history().isReleased()).isFalse();
                        try (var old = owner.resumeGeneration(CALLER, CALLER, command, oldId).orElseThrow()) {
                            assertThat(old.retireFenced(CALLER, Duration.ZERO, NONE))
                                    .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.RETIRED);
                        }
                        assertThat(oldSources.history().isReleased()).isTrue();
                        try (var next = owner.resume(CALLER, command).orElseThrow()) {
                            assertThat(next.identity()).isEqualTo(nextId);
                            assertThat(next.start(LEASE, NONE)).isNotNull();
                        }
                        assertThat(count(c, "repository_successor_installs")).isEqualTo(2);
                        assertThat(count(c, "repository_historical_activations")).isEqualTo(2);
                        owner.close();
                        assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                        assertThat(freshSources.history().isReleased()).isTrue();
                    }
                } finally {
                    owner.close();
                    assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                }
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void historicalWorkAcceptsDecodedExactCommandButRejectsAnotherOperation() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2)); var work = rig.sources().work()) {
            var original = rig.record().command();
            var decoded = new DocumentPublicationCommand(original.intent());
            assertThat(decoded).isNotSameAs(original);
            assertThat(work.references(decoded, () -> {})).isNotEmpty();
            var other = new DocumentPublicationCommand(original.intent().toBuilder()
                    .setOperationId(java.util.UUID.randomUUID().toString()).build());
            assertThat(other.canonical()).isEqualTo(original.canonical());
            assertThatThrownBy(() -> work.references(other, () -> {})).hasMessageContaining("source command differs");
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void wrongRetainedPreparationOrFixedModesRefusesBeforeReservation(boolean wrongModes) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var modes = modes(rig); var observed = expired(c, rig, modes);
            var original = rig.record();
            var submitted = wrongModes ? original : new DocumentPublicationPreparationRecord(original.key(), original.command(),
                    original.seeds(), original.placements(), original.lease().plusSeconds(1), original.predecessorGeneration());
            var requested = wrongModes ? Map.of(modes.keySet().iterator().next(), DocumentPublicationCandidate.Mode.OPAQUE) : modes;
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var attempt = owner.beginProposed(CALLER, submitted, requested, observed, LEASE, TIMEOUTS)) {
                assertThatThrownBy(() -> attempt.advancePreparation(CALLER, requested, Map.of(), NONE))
                        .hasMessageContaining(wrongModes ? "modes differ from fixed modes" : "retention binding is unavailable");
                assertThat(count(c, "repository_coordinator_expirations")).isZero();
                assertThat(count(c, "repository_successor_installs")).isZero();
            }
            owner.close();
            assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void freshOwnerRecoversExpiredUnactivatedReservationOrInstallation(boolean installed, boolean cold) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var modes = modes(rig); var observed = expired(c, rig, modes);
            var firstBudget = new PayloadBudget(256L * 1024 * 1024);
            var first = new RepositoryInstalledHistoricalAttempts(c.tx(), firstBudget, new DriveLedger(c.tx()), 1);
            try (var attempt = first.beginProposed(CALLER, rig.record(), modes, observed, Duration.ofSeconds(2), TIMEOUTS)) {
                attempt.advancePreparation(CALLER, modes, Map.of(), NONE);
                if (installed) attempt.advancePreparation(CALLER, modes, Map.of(), NONE);
            }
            first.close();
            assertThat(first.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            assertThat(firstBudget.reservedBytes()).isZero();
            waitExpired(c, rig);
            var unactivated = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                    .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
            assertThat(unactivated.status()).isEqualTo(installed
                    ? RepositoryCoordinatorRecoveryDiscovery.Status.INSTALLED_NOT_ACTIVATED
                    : RepositoryCoordinatorRecoveryDiscovery.Status.RESERVED_NOT_INSTALLED);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var next = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var later = capture(c, rig)) {
              try (var attempt = cold
                      ? next.beginColdProposed(CALLER, new DocumentPublicationCommand(rig.record().command().intent()),
                              modes, unactivated, LEASE, TIMEOUTS)
                      : next.beginProposed(CALLER, rig.record(), modes, unactivated, LEASE, TIMEOUTS)) {
                assertThat(attempt.advancePreparation(CALLER, modes, Map.of(), NONE))
                        .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                assertThat(attempt.advancePreparation(CALLER, modes, Map.of(), NONE))
                        .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                attempt.attachSources(later.sources(), later.sources().work(), NONE);
                assertThatThrownBy(() -> attempt.reconcileUnactivated(CALLER, modes, Map.of(), NONE))
                        .hasMessageContaining("Attached historical sources");
                attempt.openExecution(CALLER, NONE);
                assertThat(attempt.start(LEASE, NONE)).isNotNull();
                assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(1);
                assertThat(count(c, "repository_coordinator_supersessions")).isEqualTo(1);
                assertThat(count(c, "repository_successor_installs")).isEqualTo(installed ? 2 : 1);
                assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
              }
              next.close();
              assertThat(next.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            }
            assertThat(budget.reservedBytes()).isZero();
            assertThat(rig.history().isReleased()).isFalse();
        }
    }

    @Test void freshColdRegistriesRecoverAcrossMultipleInstalledSuccessors() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var fixedModes = modes(rig);
            var observed = expired(c, rig, fixedModes);
            var command = rig.record().command();
            long originalGeneration = rig.record().predecessorGeneration();
            for (int generation = 1; generation <= 3; generation++) {
                var budget = new PayloadBudget(256L * 1024 * 1024);
                var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
                try {
                    try (var attempt = owner.beginColdProposed(CALLER, new DocumentPublicationCommand(command.intent()),
                            fixedModes, observed, Duration.ofSeconds(3), TIMEOUTS)) {
                        assertThat(attempt.advancePreparation(CALLER, fixedModes, Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                        assertThat(attempt.advancePreparation(CALLER, fixedModes, Map.of(), NONE))
                                .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                        var plan = attempt.installedPlan(CALLER, NONE);
                        assertThat(plan.next().predecessorGeneration()).isEqualTo(originalGeneration + generation);
                        if (generation == 3) {
                            try (var sources = capture(c, rig)) {
                                attempt.attachSources(sources.sources(), sources.sources().work(), NONE);
                                attempt.openExecution(CALLER, NONE);
                                assertThat(attempt.start(LEASE, NONE)).isNotNull();
                                attempt.close();
                                owner.close();
                                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                                assertThat(sources.history().isReleased()).isTrue();
                            }
                        }
                    }
                } finally {
                    owner.close();
                    assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                }
                assertThat(budget.reservedBytes()).isZero();
                assertThat(count(c, "repository_successor_installs")).isEqualTo(generation);
                if (generation < 3) {
                    waitExpired(c, rig);
                    observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                            .inspect(CALLER, rig.record().key(), command.sha256(), NONE);
                    assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.INSTALLED_NOT_ACTIVATED);
                }
            }
            assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(1);
            assertThat(count(c, "repository_coordinator_supersessions")).isEqualTo(2);
            assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
            assertThat(rig.history().isReleased()).isFalse();
        }
    }

    @ParameterizedTest @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void lostReservationOrInstallationReplyKeepsExactIdentityAcrossCalls(boolean installation, boolean cold) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var modes = modes(rig);
            var observed = expired(c, rig, modes);
            var cancelled = new AtomicBoolean(); var armed = new AtomicBoolean(true);
            var fault = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c, installation ? "repository_successor_installs" : "repository_coordinator_expirations") == 1
                        && armed.compareAndSet(true, false)) {
                    cancelled.set(true); // Prevent immediate confirmation, as with an interrupted client call.
                    throw new java.sql.SQLException("Historical recovery commit reply lost", "08006");
                }
            });
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", fault, "hibernate.hbm2ddl.auto", "validate"))) {
                var budget = new PayloadBudget(256L * 1024 * 1024);
                var owner = new RepositoryInstalledHistoricalAttempts(new Tx(emf), budget, new DriveLedger(c.tx()), 1);
                try (var attempt = cold
                        ? owner.beginColdProposed(CALLER, rig.record().command(), modes, observed, LEASE, TIMEOUTS)
                        : owner.beginProposed(CALLER, rig.record(), modes, observed, LEASE, TIMEOUTS)) {
                    if (installation) assertThat(attempt.advancePreparation(CALLER, modes, Map.of(), control))
                            .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                    assertThatThrownBy(() -> attempt.advancePreparation(CALLER, modes, Map.of(), control))
                            .hasStackTraceContaining("Historical recovery commit reply lost");
                    assertThat(armed).isFalse();
                    try (var later = capture(c, rig); var root = later.sources().work()) {
                        assertThatThrownBy(() -> attempt.attachSources(later.sources(), root, NONE))
                                .hasMessageContaining("installation is not confirmed");
                        try (var stillOwned = root.fork()) { assertThat(later.history().isReleased()).isFalse(); }
                    }
                }
                var exact = identity(c, rig);
                assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 1));
                long retainedBytes = budget.reservedBytes();
                assertThat(retainedBytes).isPositive().isLessThan(DocumentPublicationPreparationCodec.MAX_BYTES);
                cancelled.set(false);
                // A fresh observation sees our reservation/install. The retained entry must win.
                var changedObservation = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                        .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
                var decodedRetention = DocumentPublicationPreparationCodec.decode(DocumentPublicationPreparationCodec.encode(rig.record()),
                        rig.record().key(), rig.record().command().sha256());
                try (var retry = cold
                        ? owner.beginColdProposed(CALLER, new DocumentPublicationCommand(rig.record().command().intent()),
                                modes, changedObservation, LEASE, TIMEOUTS)
                        : owner.beginProposed(CALLER, decodedRetention, modes, changedObservation, LEASE, TIMEOUTS)) {
                    var next = retry.advancePreparation(CALLER, modes, Map.of(), NONE);
                    assertThat(next).isEqualTo(installation ? RepositoryHistoricalAttemptPreparation.Phase.INSTALLED
                            : RepositoryHistoricalAttemptPreparation.Phase.RESERVED);
                    assertThat(identity(c, rig)).containsExactly(exact);
                    if (!installation) assertThat(retry.advancePreparation(CALLER, modes, Map.of(), NONE))
                            .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                }
                if (installation) assertThat(budget.reservedBytes()).isEqualTo(retainedBytes);
                assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(1);
                assertThat(count(c, "repository_successor_installs")).isEqualTo(1);
                try (var later = capture(c, rig)) {
                    DocumentAssessmentStartJournal.Started started;
                    try (var retry = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                        retry.attachSources(later.sources(), later.sources().work(), NONE);
                        retry.openExecution(CALLER, NONE);
                        started = retry.start(LEASE, NONE);
                    }
                    try (var retry = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                        retry.openExecution(CALLER, NONE);
                        assertThat(retry.start(LEASE, NONE)).isEqualTo(started);
                    }
                    assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                    owner.close();
                    assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                    assertThat(later.history().isReleased()).isTrue();
                }
                assertThat(owner.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
                assertThat(budget.reservedBytes()).isZero();
                assertThat(rig.history().isReleased()).isFalse();
            }
        }
    }

    @ParameterizedTest @CsvSource({"0,false", "1,false", "2,false", "0,true", "1,true", "2,true"})
    void shutdownReleasesPreparationAtEveryPhaseWithoutInventingCaptureDrain(int phases, boolean cold) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var modes = modes(rig); var observed = expired(c, rig, modes);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var attempt = cold
                    ? owner.beginColdProposed(CALLER, rig.record().command(), modes, observed, LEASE, TIMEOUTS)
                    : owner.beginProposed(CALLER, rig.record(), modes, observed, LEASE, TIMEOUTS)) {
                for (int i = 0; i < phases; i++) attempt.advancePreparation(CALLER, modes, Map.of(), NONE);
                assertThat(attempt.retireFenced(CALLER, Duration.ZERO, NONE))
                        .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.NOT_PROVEN);
                assertThat(attempt.retireTerminal(CALLER, Duration.ZERO, NONE))
                        .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.NOT_PROVEN);
                assertThat(budget.reservedBytes()).isPositive();
            }
            owner.close();
            assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            assertThat(budget.reservedBytes()).isZero();
            assertThat(count(c, "repository_preparation_capture_drains")).isZero();
            assertThat(count(c, "repository_coordinator_expirations")).isEqualTo(phases == 0 ? 0 : 1);
            assertThat(count(c, "repository_successor_installs")).isEqualTo(phases < 2 ? 0 : 1);
            assertThat(rig.history().isReleased()).isFalse();
        }
    }

    @Test void budgetRefusalAndInvalidInputsCannotReserveSuccessor() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var modes = modes(rig); var observed = expired(c, rig, modes);
            var tiny = new PayloadBudget(1);
            var refused = new RepositoryInstalledHistoricalAttempts(c.tx(), tiny, new DriveLedger(c.tx()), 1);
            assertThatThrownBy(() -> refused.beginProposed(CALLER, rig.record(), modes, observed, LEASE, TIMEOUTS))
                    .isInstanceOf(PayloadBudget.CapacityExceededException.class);
            assertThat(refused.drain()).isEqualTo(new RepositoryInstalledHistoricalAttempts.Drain(0, 0));
            assertThat(tiny.reservedBytes()).isZero();
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var attempt = owner.beginProposed(CALLER, rig.record(), modes, observed, LEASE, TIMEOUTS)) {
                assertThatThrownBy(() -> attempt.advancePreparation(CALLER,
                        Map.of(modes.keySet().iterator().next(), DocumentPublicationCandidate.Mode.OPAQUE), Map.of(), NONE))
                        .hasMessageContaining("modes changed");
                var extra = new ai.protomolt.proto.repo.codec.PartObject(
                        rig.record().command().intent().getMembers(0).getParts(0).getSlot().getPart(), "", new byte[]{1}, "invalid");
                assertThatThrownBy(() -> attempt.advancePreparation(CALLER, modes,
                        Map.of(new DocumentUploadPayloads.Key("extra", 0), extra), NONE))
                        .hasMessageContaining("payload keys differ");
                assertThat(count(c, "repository_coordinator_expirations")).isZero();
                assertThat(count(c, "repository_successor_installs")).isZero();
            }
            owner.close();
            assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 3, 4})
    void sameOwnerSupersedesExpiredClaimAndRetainsUncertainReplacement(int initialPhase) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var modes = modes(rig); var observed = expired(c, rig, modes);
            var cancelled = new AtomicBoolean();
            var loseInitial = new AtomicBoolean(initialPhase == 0 || initialPhase == 3);
            var loseReplacement = new AtomicBoolean(true);
            var fault = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                boolean initial = loseInitial.get() && count(c, initialPhase == 0
                        ? "repository_coordinator_expirations" : "repository_successor_installs") == 1
                        && loseInitial.compareAndSet(true, false);
                boolean replacement = count(c, "repository_coordinator_supersessions") == 1
                        && loseReplacement.compareAndSet(true, false);
                if (initial || replacement) {
                    cancelled.set(true);
                    throw new java.sql.SQLException("Supersession reply lost", "08006");
                }
            });
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", fault, "hibernate.hbm2ddl.auto", "validate"))) {
                var budget = new PayloadBudget(256L * 1024 * 1024);
                var owner = new RepositoryInstalledHistoricalAttempts(new Tx(emf), budget, new DriveLedger(c.tx()), 1);
                try (var attempt = owner.beginProposed(CALLER, rig.record(), modes, observed, Duration.ofSeconds(2), TIMEOUTS)) {
                    if (initialPhase == 0) {
                        assertThatThrownBy(() -> attempt.advancePreparation(CALLER, modes, Map.of(), control))
                                .hasStackTraceContaining("Supersession reply lost");
                    } else {
                        attempt.advancePreparation(CALLER, modes, Map.of(), control);
                        if (initialPhase == 3) assertThatThrownBy(() -> attempt.advancePreparation(CALLER, modes, Map.of(), control))
                                .hasStackTraceContaining("Supersession reply lost");
                        else if (initialPhase == 2) attempt.advancePreparation(CALLER, modes, Map.of(), control);
                    }
                }
                cancelled.set(false); waitExpired(c, rig);
                var oldIdentity = identity(c, rig);
                try (var attempt = owner.resume(CALLER, rig.record().command()).orElseThrow()) {
                    assertThatThrownBy(() -> attempt.reconcileUnactivated(CALLER,
                            Map.of(modes.keySet().iterator().next(), DocumentPublicationCandidate.Mode.OPAQUE), Map.of(), NONE))
                            .hasMessageContaining("modes changed");
                    var extra = new ai.protomolt.proto.repo.codec.PartObject(
                            rig.record().command().intent().getMembers(0).getParts(0).getSlot().getPart(), "", new byte[]{1}, "invalid");
                    assertThatThrownBy(() -> attempt.reconcileUnactivated(CALLER, modes,
                            Map.of(new DocumentUploadPayloads.Key("extra", 0), extra), NONE))
                            .hasMessageContaining("payload keys differ");
                    assertThat(count(c, "repository_coordinator_supersessions")).isZero();
                    assertThatThrownBy(() -> attempt.reconcileUnactivated(CALLER, modes, Map.of(), control))
                            .hasStackTraceContaining("Supersession reply lost");
                    assertThat(loseReplacement).isFalse();
                    var replacement = identity(c, rig);
                    assertThat(replacement[0]).isEqualTo(((Number) oldIdentity[0]).longValue() + 1);
                    var retainedBytes = budget.reservedBytes();
                    assertThatThrownBy(() -> attempt.advancePreparation(CALLER, modes, Map.of(), NONE))
                            .hasMessageContaining("supersession must be confirmed");
                    assertThatThrownBy(() -> attempt.installedPlan(CALLER, NONE))
                            .hasMessageContaining("supersession must be confirmed");
                    try (var later = capture(c, rig); var root = later.sources().work()) {
                        assertThatThrownBy(() -> attempt.attachSources(later.sources(), root, NONE))
                                .hasMessageContaining("supersession must be confirmed");
                        try (var borrowed = root.fork()) { assertThat(later.history().isReleased()).isFalse(); }
                    }
                    // Neither expiry nor the old claim being fenced can dispose the pending current claim.
                    assertThat(attempt.retireFenced(CALLER, Duration.ZERO, NONE))
                            .isEqualTo(RepositoryInstalledHistoricalAttempts.Retirement.NOT_PROVEN);
                    assertThat(budget.reservedBytes()).isEqualTo(retainedBytes);
                    if (initialPhase == 4) {
                        attempt.close();
                        owner.close();
                        assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                        assertThat(budget.reservedBytes()).isZero();
                        assertThat(identity(c, rig)).containsExactly(replacement);
                        assertThat(count(c, "repository_coordinator_supersessions")).isEqualTo(1);
                        assertThat(count(c, "repository_preparation_capture_drains")).isZero();
                        assertThat(count(c, "repository_historical_activations")).isZero();
                        assertThat(rig.history().isReleased()).isFalse();
                        return;
                    }
                    cancelled.set(false);
                    assertThat(attempt.reconcileUnactivated(CALLER, modes, Map.of(), NONE)).isTrue();
                    assertThat(identity(c, rig)).containsExactly(replacement);
                    assertThatThrownBy(() -> attempt.installedPlan(CALLER, NONE))
                            .hasMessageContaining("installation is not confirmed");
                    assertThat(attempt.advancePreparation(CALLER, modes, Map.of(), NONE))
                            .isEqualTo(RepositoryHistoricalAttemptPreparation.Phase.INSTALLED);
                    assertThat(attempt.installedPlan(CALLER, NONE).reservation().successorToken()).isEqualTo(replacement[1]);
                    assertThat(count(c, "repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(count(c, "repository_historical_activations")).isZero();
                }
                owner.close();
                assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void foreignSuccessorCannotReplaceRetainedIdentity(boolean installed) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(2))) {
            var modes = modes(rig); var observed = expired(c, rig, modes);
            var budget = new PayloadBudget(256L * 1024 * 1024);
            var owner = new RepositoryInstalledHistoricalAttempts(c.tx(), budget, new DriveLedger(c.tx()), 1);
            try (var attempt = owner.beginProposed(CALLER, rig.record(), modes, observed, Duration.ofSeconds(2), TIMEOUTS)) {
                attempt.advancePreparation(CALLER, modes, Map.of(), NONE);
                if (installed) attempt.advancePreparation(CALLER, modes, Map.of(), NONE);
                waitExpired(c, rig);
                var source = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                        .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE).unactivated().orElseThrow();
                var foreign = new RepositoryCoordinatorReservation.SupersededUnactivated(source.predecessor(),
                        java.util.UUID.randomUUID(), java.util.UUID.randomUUID(), Duration.ofSeconds(2),
                        source.owner(), source.preparationSha256(), source.installation());
                RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, foreign, NONE);
                waitExpired(c, rig);
                var exact = identity(c, rig); var bytes = budget.reservedBytes();
                assertThatThrownBy(() -> attempt.reconcileUnactivated(CALLER, modes, Map.of(), NONE))
                        .hasMessageContaining("successor differs from retained attempt");
                assertThat(identity(c, rig)).containsExactly(exact);
                assertThat(budget.reservedBytes()).isEqualTo(bytes);
                assertThat(count(c, "repository_coordinator_supersessions")).isEqualTo(1);
            }
            owner.close();
            assertThat(owner.detachClosed(Duration.ZERO, ignored -> CALLER, NONE)).isTrue();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static Map<String, DocumentPublicationCandidate.Mode> modes(Rig rig) {
        return Map.of(rig.record().command().intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED);
    }
    private static RepositoryCoordinatorRecoveryDiscovery.Observation expired(Context c, Rig rig,
            Map<String, DocumentPublicationCandidate.Mode> modes) {
        try (var work = rig.sources().work()) {
            var admission = RepositoryOperationLedger.prepareHistoricalAdmission(rig.record().key(), rig.record().command(),
                    rig.record().seeds().ownerNonce(), Duration.ofSeconds(2), work);
            c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                DocumentPublicationModesJournal.insert(em, rig.claim(), rig.record(), DocumentPublicationModesJournal.encode(rig.record().command(), modes));
                admission.apply(em, rig.claim());
            });
        }
        waitExpired(c, rig);
        var result = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
        assertThat(result.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
        return result;
    }
    private static void waitExpired(Context c, Rig rig) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
    }
    private static Object[] identity(Context c, Rig rig) {
        return (Object[]) c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,r.successor_incarnation,o.owner_generation,o.owner_token,
                  encode(i.preparation_sha256,'hex') FROM repository_execution_claims c
                JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                JOIN repository_coordinator_reservations r ON r.operation_id=c.operation_id AND r.successor_epoch=c.claim_epoch
                LEFT JOIN repository_successor_installs i ON i.operation_id=c.operation_id AND i.successor_epoch=c.claim_epoch
                WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
    }
}
