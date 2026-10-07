package ai.protomolt.proto.repo.container.ledger;

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

/** Real SQL successor/capture protocol. Source publication has synthetic provider observations. */
@Testcontainers
class RepositoryHistoricalSuccessorActivationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void successorCompletionCannotReplaceUndrainedOriginalEpoch() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var original = DocumentPreparationSourcePins.prepare(rig.record().command(),
                    rig.sources().references(rig.record().command(), () -> {}), () -> {});
            try (var later = capture(c, rig)) {
                var activation = activation(c.tx(), c, rig, plan, later);
                var before = leases(c, rig);
                var completed = activation.activate(CALLER, CALLER, NONE);
                assertThat(completed.identity().owner().epoch()).isEqualTo(2);
                assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(2);
                assertThat(completed.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                assertThat(activation.activate(CALLER, CALLER, NONE)).isSameAs(completed);
                var coverage = DocumentPreparationCaptureCoverage.prepare(rig.record(), NONE);
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); }))
                        .hasMessageContaining("has not drained");
                rig.sources().close(); rig.history().close(); rig.history().release();
                rig.reads().fence(); rig.reads().attestLocalQuiescence();
                var id = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(rig.record().key(),
                        rig.record().command().sha256(), rig.claim().epoch(), rig.claim().token(), rig.coordinator()), 0,
                        HexFormat.of().formatHex(original.digest()));
                assertThat(DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, id, NONE).kind()).isEqualTo("QUIESCED");
                int qualified = c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); });
                assertThat(qualified).isEqualTo(2);
                assertThat(leases(c, rig)).containsExactly(before);
            }
        }
    }

    @Test void missingAtomicCaptureBindingRollsBackExecutionAndCoordinator() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var sha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.next()));
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositorySuccessorExecution.insertExecution(em, plan, sha, RepositorySuccessorInstall.encodeModes(plan), true);
                RepositorySuccessorExecution.insertBinding(em, plan);
            })).hasStackTraceContaining("requires atomic capture binding");
            assertThat(count(c, "repository_successor_executions")).isZero();
            assertThat(count(c, "repository_coordinator_bindings")).isEqualTo(1);
            assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(strings = {"failure", "anchor", "capture", "token"})
    void captureBindingFailureRollsBackAllNewStateAndExactAttemptCanRetry(String fault) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            try (var later = capture(c, rig)) {
                c.tx().inTransaction(em -> {
                    String action = switch (fault) {
                        case "anchor" -> "NEW.retention_sha256:=decode(repeat('00',32),'hex'); RETURN NEW;";
                        case "capture" -> "NEW.pins_sha256:=decode(repeat('00',32),'hex'); RETURN NEW;";
                        case "token" -> "NEW.claim_token:=gen_random_uuid(); RETURN NEW;";
                        default -> "RAISE EXCEPTION 'activation binding fault';";
                    };
                    em.createNativeQuery("CREATE FUNCTION fail_historical_activation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " + action + " END $$").executeUpdate();
                    em.createNativeQuery("CREATE TRIGGER fail_activation BEFORE INSERT ON repository_historical_activations FOR EACH ROW EXECUTE FUNCTION fail_historical_activation()").executeUpdate();
                });
                var activation = activation(c.tx(), c, rig, plan, later);
                String expected = switch (fault) {
                    case "anchor", "capture" -> "differs from retained ancestry or capture";
                    case "token" -> "requires its exact creation execution";
                    default -> "activation binding fault";
                };
                assertThatThrownBy(() -> activation.activate(CALLER, CALLER, NONE)).hasStackTraceContaining(expected);
                assertThat(activation.tentativeCapture()).isPresent();
                assertThat(count(c, "repository_successor_executions")).isZero();
                assertThat(count(c, "repository_coordinator_bindings")).isEqualTo(1);
                assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(1);
                assertThat(count(c, "repository_historical_activations")).isZero();
                c.tx().inTransaction(em -> { em.createNativeQuery("DROP TRIGGER fail_activation ON repository_historical_activations").executeUpdate(); });
                var result = activation.activate(CALLER, CALLER, NONE);
                assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                assertThat(result.complete(CALLER, Duration.ZERO, NONE)).isPresent();
            }
        }
    }

    @Test void lostCommitReplyRetainsExactCaptureForConfirmationAndCleanup() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var armed = new AtomicBoolean(true);
            var datasource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c, "repository_historical_activations")==1 && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("historical activation reply lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var later = capture(c, rig)) {
                var activation = activation(new Tx(emf), c, rig, plan, later);
                var before = leases(c, rig);
                assertThatThrownBy(() -> activation.activate(CALLER, CALLER, NONE)).hasStackTraceContaining("historical activation reply lost");
                assertThat(armed).isFalse();
                var tentative = activation.tentativeCapture().orElseThrow();
                var cold = RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER, plan, rig.record(), NONE).orElseThrow();
                assertThat(cold.captureSha256()).isEqualTo(tentative.identity().pinsSha256());
                assertThat(cold.execution()).isEqualTo(tentative.identity().owner());
                assertThat(activation.activate(CALLER, CALLER, NONE)).isSameAs(tentative);
                assertThat(tentative.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(2);
                assertThat(leases(c, rig)).containsExactly(before);
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void cancellationAroundCommitPreservesExactDurableState(boolean afterCommit) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var cancelled = new AtomicBoolean();
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var datasource = afterCommit ? DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c, "repository_historical_activations")==1) cancelled.set(true);
            }) : DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement();
                     var rows = statement.executeQuery("SELECT count(*) FROM repository_historical_activations")) {
                    rows.next();
                    if (rows.getLong(1)==1) { cancelled.set(true); control.check(); }
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var later = capture(c, rig)) {
                var activation = activation(new Tx(emf), c, rig, plan, later);
                assertThatThrownBy(() -> activation.activate(CALLER, CALLER, control)).hasStackTraceContaining("cancel");
                assertThat(cancelled).isTrue();
                assertThat(count(c, "repository_historical_activations")).isEqualTo(afterCommit ? 1 : 0);
                assertThat(count(c, "repository_successor_executions")).isEqualTo(afterCommit ? 1 : 0);
                assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(afterCommit ? 2 : 1);
                // Retry uses the unfaulted transaction manager; old local captures remain available for cleanup.
                if (afterCommit) {
                    assertThat(activation.activate(CALLER, CALLER, NONE)).isSameAs(activation.tentativeCapture().orElseThrow());
                    assertThat(activation.tentativeCapture().orElseThrow().complete(CALLER, Duration.ZERO, NONE)).isPresent();
                } else {
                    var retry = activation(c.tx(), c, rig, plan, later);
                    assertThat(retry.activate(CALLER, CALLER, NONE).complete(CALLER, Duration.ZERO, NONE)).isPresent();
                }
            }
        }
    }

    static RepositoryHistoricalSuccessorActivation activation(Tx tx, Context c, Rig rig,
            RepositorySuccessorInstall.Plan plan, Captured later) {
        return new RepositoryHistoricalSuccessorActivation(tx, rig.budget(), plan, rig.record(), later.sources(), new DriveLedger(c.tx()));
    }
    static long count(Context c, String table) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table).getSingleResult()).longValue());
    }
    static Object[] leases(Context c, Rig rig) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.lease_until,o.lease_until FROM repository_execution_claims c JOIN repository_operation_owners o
                USING(account_id,principal,operation_id) WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
    }
    record Captured(DocumentReadLedger reads, DocumentReadLedger.PinnedHistory history,
            DocumentHistoricalAssessmentSources sources) implements AutoCloseable {
        public void close() throws Exception {
            sources.close(); history.close(); history.release(); reads.fence(); reads.attestLocalQuiescence();
        }
    }
    static Captured capture(Context c, Rig rig) throws Exception { return capture(c, rig, CALLER); }

    static Captured capture(Context c, Rig rig, RepositoryCaller caller) throws Exception {
        var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
        var history = reads.captureHistorical(caller, rig.fixture().address(), rig.fixture().revision());
        boolean delivered = false;
        try {
            var result = new Captured(reads, history, DocumentHistoricalAssessmentSources.open(rig.record().command(), caller, List.of(history), NONE));
            delivered = true; return result;
        } finally {
            if (!delivered) { history.close(); history.release(); reads.fence(); reads.attestLocalQuiescence(); }
        }
    }
}
