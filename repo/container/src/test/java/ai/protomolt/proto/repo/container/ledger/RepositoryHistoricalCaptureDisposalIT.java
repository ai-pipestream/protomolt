package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.count;
import static org.assertj.core.api.Assertions.*;

/** Real SQL lifecycle; source fixture's provider observations are synthetic, not provider durability evidence. */
@Testcontainers
class RepositoryHistoricalCaptureDisposalIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void classifierWaitsForActivationTransactionOutcome(boolean commit) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var entered = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var holder = new java.util.concurrent.atomic.AtomicInteger();
            var local = new java.util.concurrent.atomic.AtomicReference<RepositoryHistoricalSuccessorActivation>();
            var captureId = new java.util.concurrent.atomic.AtomicReference<DocumentPreparationCaptureDrain.Identity>();
            var datasource = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement();
                     var rows = statement.executeQuery("SELECT pg_backend_pid(),count(*) FROM repository_historical_activations")) {
                    rows.next();
                    if (rows.getLong(2) != 1) return;
                    holder.set(rows.getInt(1));
                    captureId.set(local.get().tentativeCapture().orElseThrow().identity());
                }
                entered.countDown();
                try {
                    if (!release.await(15, java.util.concurrent.TimeUnit.SECONDS))
                        throw new java.sql.SQLException("activation barrier timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new java.sql.SQLException("activation barrier interrupted", interrupted);
                }
                if (!commit) throw new java.sql.SQLException("controlled activation rollback", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var later = capture(c, rig);
                 var workers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
                local.set(activation(new Tx(emf), c, rig, plan, later));
                var activating = workers.submit(() -> local.get().activate(CALLER, CALLER, NONE));
                try {
                    assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    // The competing SQL observer has its own host budget; the held activation reserves its own scratch.
                    var observerBudget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(64L * 1024 * 1024);
                    var classifying = workers.submit(() -> RepositoryHistoricalCaptureState.classify(c.tx(), observerBudget,
                            plan, rig.record(), captureId.get(), NONE));
                    boolean blocked = false;
                    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    while (!blocked && System.nanoTime() < deadline) {
                        blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                                SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                                  WHERE :holder=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'
                                    AND query LIKE '%repository_execution_claims%')
                                """).setParameter("holder", holder.get()).getSingleResult());
                        if (!blocked) Thread.sleep(10);
                    }
                    if (classifying.isDone())
                        throw new AssertionError("Classifier completed before activation outcome: " + classifying.get());
                    assertThat(blocked).as("classifier waits on actual activation claim lock").isTrue();
                    assertThat(classifying.isDone()).isFalse();
                    release.countDown();
                    if (commit) assertThat(activating.get(10, java.util.concurrent.TimeUnit.SECONDS)).isNotNull();
                    else assertThatThrownBy(() -> activating.get(10, java.util.concurrent.TimeUnit.SECONDS))
                            .hasStackTraceContaining("controlled activation rollback");
                    assertThat(classifying.get(10, java.util.concurrent.TimeUnit.SECONDS))
                            .isEqualTo(commit ? RepositoryHistoricalCaptureState.State.REGISTERED
                                    : RepositoryHistoricalCaptureState.State.ABSENT);
                } finally { release.countDown(); }
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void uncertainActivationDisposesAccordingToCommitWithoutInventingDrainEvidence(boolean committed) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var armed = new AtomicBoolean(true);
            var datasource = committed ? DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c, "repository_historical_activations") == 1 && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("disposal activation lost reply", "08006");
            }) : DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement();
                     var rows = statement.executeQuery("SELECT count(*) FROM repository_historical_activations")) {
                    rows.next();
                    if (rows.getLong(1) == 1 && armed.compareAndSet(true, false))
                        throw new java.sql.SQLException("disposal activation rolled back", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var later = capture(c, rig)) {
                var activation = activation(new Tx(emf), c, rig, plan, later);
                var pins = count(c, "document_read_pins");
                try (var work = later.sources().work()) {
                    assertThatThrownBy(() -> activation.activateAccepted(CALLER, CALLER, NONE, work))
                            .hasStackTraceContaining(committed ? "lost reply" : "rolled back");
                    assertThat(armed).isFalse();
                    assertThat(activation.tentativeCapture()).isPresent();
                    assertThat(count(c, "repository_historical_activations")).isEqualTo(committed ? 1 : 0);
                    assertThat(activation.disposeCapture(CALLER, Duration.ZERO, NONE)).isEmpty();
                    assertThat(count(c, "document_read_pins")).isEqualTo(pins);
                    assertThat(count(c, "repository_preparation_capture_drains")).isZero();
                    assertThat(later.history().isReleased()).isFalse();
                }
                var expected = committed ? RepositoryHistoricalSuccessorActivation.Disposal.REGISTERED
                        : RepositoryHistoricalSuccessorActivation.Disposal.LOCAL_ONLY;
                assertThat(activation.disposeCapture(CALLER, Duration.ZERO, NONE)).contains(expected);
                assertThat(activation.disposeCapture(CALLER, Duration.ZERO, NONE)).contains(expected);
                assertThat(later.history().isReleased()).isTrue();
                assertThat(rig.history().isReleased()).isFalse();
                assertThat(count(c, "repository_preparation_capture_drains")).isEqualTo(committed ? 1 : 0);
                assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(committed ? 2 : 1);
                assertThatThrownBy(() -> activation.activate(CALLER, CALLER, NONE)).hasMessageContaining("is closing");
            }
        }
    }

    @Test void failedConfirmationRetainsResourcesAndDisposalCanRetry() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var fail = new AtomicBoolean();
            var datasource = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (fail.get()) throw new java.sql.SQLException("disposal confirmation unavailable", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var later = capture(c, rig)) {
                var activation = activation(new Tx(emf), c, rig, plan, later);
                activation.activate(CALLER, CALLER, NONE);
                var pins = count(c, "document_read_pins");
                fail.set(true);
                assertThatThrownBy(() -> activation.disposeCapture(CALLER, Duration.ZERO, NONE))
                        .hasStackTraceContaining("disposal confirmation unavailable");
                assertThat(count(c, "document_read_pins")).isEqualTo(pins);
                assertThat(later.history().isReleased()).isFalse();
                assertThat(count(c, "repository_preparation_capture_drains")).isZero();
                fail.set(false);
                assertThat(activation.disposeCapture(CALLER, Duration.ZERO, NONE))
                        .contains(RepositoryHistoricalSuccessorActivation.Disposal.REGISTERED);
            }
        }
    }

    @Test void noCaptureLeavesPreactivationResourcesWithCallerAndRejectsUntrustedDisposal() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            try (var later = capture(c, rig)) {
                var activation = activation(c.tx(), c, rig, plan, later);
                assertThatThrownBy(() -> activation.disposeCapture(new RepositoryCaller("principal", false, java.util.Set.of(plan.next().key().account()), java.util.Set.of()), Duration.ZERO, NONE))
                        .isInstanceOf(RepositoryException.class).hasMessageContaining("private process authority");
                assertThat(activation.disposeCapture(CALLER, Duration.ZERO, NONE))
                        .contains(RepositoryHistoricalSuccessorActivation.Disposal.NO_CAPTURE);
                try (var work = later.sources().work()) { work.authorize(NONE); }
                assertThat(later.history().isReleased()).isFalse();
                assertThatThrownBy(() -> activation.activate(CALLER, CALLER, NONE)).hasMessageContaining("is closing");
            }
        }
    }

    @Test void committedCaptureCanDisposeAfterClaimTakeoverWithoutRenewingSuccessor() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            try (var later = capture(c, rig)) {
                var activation = activation(c.tx(), c, rig, plan, later);
                var captured = activation.activate(CALLER, CALLER, NONE);
                c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
                c.tx().inTransaction(em -> {
                    var token = java.util.UUID.randomUUID();
                    em.createNativeQuery("""
                            UPDATE repository_execution_claims SET claim_epoch=claim_epoch+1,claim_token=:token,
                              lease_until=clock_timestamp()+interval '5 minutes',fence_epoch=:epoch,fence_token=:token
                            WHERE operation_id=:o
                            """).setParameter("token", token).setParameter("epoch", captured.identity().owner().epoch()+1)
                            .setParameter("o", plan.next().key().operationId()).executeUpdate();
                });
                var before = leases(c, rig);
                assertThat(activation.disposeCapture(CALLER, Duration.ZERO, NONE))
                        .contains(RepositoryHistoricalSuccessorActivation.Disposal.REGISTERED);
                assertThat(leases(c, rig)).containsExactly(before);
                assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, captured.identity(), NONE)).isPresent();
            }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"owner", "pin"})
    void inconsistentDurableEvidenceDoesNotReleaseLocalPins(String kind) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            try (var later = capture(c, rig)) {
                var activation = activation(c.tx(), c, rig, plan, later);
                var captured = activation.activate(CALLER, CALLER, NONE);
                String table = kind.equals("owner") ? "repository_preparation_pin_owners" : "repository_preparation_source_pins";
                String trigger = kind.equals("owner") ? "repository_preparation_pin_owner_guard" : "repository_preparation_source_pin_guard";
                c.tx().inTransaction(em -> {
                    // Deliberate administrative corruption in this isolated database, never a supported write path.
                    em.createNativeQuery("ALTER TABLE " + table + " DISABLE TRIGGER " + trigger).executeUpdate();
                    em.createNativeQuery("UPDATE " + table + (kind.equals("owner")
                            ? " SET claim_token=gen_random_uuid()" : " SET publication_revision=publication_revision+1")
                            + " WHERE pins_sha256=:digest")
                            .setParameter("digest", java.util.HexFormat.of().parseHex(captured.identity().pinsSha256())).executeUpdate();
                });
                var pins = count(c, "document_read_pins");
                assertThatThrownBy(() -> activation.disposeCapture(CALLER, Duration.ZERO, NONE))
                        .hasMessageContaining("incomplete or inconsistent");
                assertThat(count(c, "document_read_pins")).isEqualTo(pins);
                assertThat(later.history().isReleased()).isFalse();
                assertThat(count(c, "repository_preparation_capture_drains")).isZero();
            }
        }
    }
}
