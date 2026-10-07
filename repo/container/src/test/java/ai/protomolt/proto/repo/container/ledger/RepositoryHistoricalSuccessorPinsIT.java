package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.activation;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.capture;
import static org.assertj.core.api.Assertions.*;

/** Capture validation only, with real SQL and synthetic source-publication provider observations. */
@Testcontainers
class RepositoryHistoricalSuccessorPinsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @org.junit.jupiter.api.Test void successorHandleRetainsAcceptedWorkAndStartsOnlyItsOwnGeneration() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var scopes = new DocumentPublicationScopeCalls();
            long reserved = rig.budget().reservedBytes();
            try (var later = capture(c, rig); var accepted = later.sources().work()) {
                var activation = activation(c.tx(), c, rig, plan, later);
                try (var execution = activation.openExecution(CALLER, CALLER, accepted, scopes, NONE)) {
                    assertThatThrownBy(() -> execution.validateAttachment(CALLER, NONE))
                            .isInstanceOf(IllegalStateException.class).hasMessage("Successor attachment is not pending");
                    accepted.close(); later.sources().close(); scopes.close();
                    assertThat(scopes.isIdle()).isFalse();
                    assertThat(activation.tentativeCapture().orElseThrow().complete(CALLER, Duration.ZERO, NONE)).isEmpty();
                    var started = execution.start(CALLER, Duration.ofMinutes(1), NONE);
                    assertThat(execution.start(CALLER, Duration.ofMinutes(1), NONE)).isEqualTo(started);
                    var rows = c.tx().readOnly(em -> em.createNativeQuery("""
                            SELECT predecessor_generation,assessment_id FROM repository_publication_assessment_starts
                            WHERE operation_id=:o
                            """).setParameter("o", plan.next().key().operationId()).getResultList());
                    assertThat(rows).hasSize(1);
                    var row = (Object[]) rows.getFirst();
                    assertThat(((Number) row[0]).longValue()).isEqualTo(plan.next().predecessorGeneration());
                    assertThat(row[1]).isEqualTo(started.assessment());
                    assertThatThrownBy(() -> activation.openExecution(CALLER, CALLER, accepted, scopes, NONE))
                            .isInstanceOf(RepositoryException.class).hasMessage("Publication runtime is closed");
                }
                assertThat(scopes.isIdle()).isTrue();
                assertThat(activation.tentativeCapture().orElseThrow().complete(CALLER, Duration.ZERO, NONE)).isPresent();
                assertThat(rig.budget().reservedBytes()).isEqualTo(reserved);
            }
        }
    }

    @org.junit.jupiter.api.Test void openRetriesExactCaptureAfterLostActivationAcknowledgment() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            var datasource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c, "repository_historical_activations") == 1 && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("execution activation reply lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", java.util.Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var later = capture(c, rig); var accepted = later.sources().work()) {
                var activation = activation(new Tx(emf), c, rig, plan, later);
                var scopes = new DocumentPublicationScopeCalls();
                later.sources().close();
                long reserved = rig.budget().reservedBytes();
                assertThatThrownBy(() -> activation.openExecution(CALLER, CALLER, accepted, scopes, NONE))
                        .hasStackTraceContaining("execution activation reply lost");
                assertThat(armed).isFalse();
                assertThat(scopes.isIdle()).isTrue();
                assertThat(rig.budget().reservedBytes()).isEqualTo(reserved);
                assertThat(count(c, "repository_publication_assessment_starts")).isZero();
                var retained = activation.tentativeCapture().orElseThrow();
                assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                var receipt = RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER,
                        plan, rig.record(), NONE).orElseThrow();
                assertThat(receipt.captureSha256()).isEqualTo(retained.identity().pinsSha256());
                assertThat(receipt.execution()).isEqualTo(retained.identity().owner());
                long publications = count(c, "document_revision_commits");

                try (var execution = activation.openExecution(CALLER, CALLER, accepted, scopes, NONE)) {
                    assertThat(activation.tentativeCapture().orElseThrow()).isSameAs(retained);
                    var started = execution.start(CALLER, Duration.ofMinutes(1), NONE);
                    assertThat(execution.start(CALLER, Duration.ofMinutes(1), NONE)).isEqualTo(started);
                    assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                    assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(2);
                    assertThat(count(c, "repository_publication_assessment_starts")).isEqualTo(1);
                    assertThat(count(c, "document_revision_commits")).isEqualTo(publications);
                }
                assertThat(scopes.isIdle()).isTrue();
                assertThat(rig.budget().reservedBytes()).isEqualTo(reserved);
                assertThat(retained.complete(CALLER, Duration.ZERO, NONE)).isEmpty();
                accepted.close();
                assertThat(retained.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                scopes.close();
            }
        }
    }

    @org.junit.jupiter.api.Test void repeatedStartsSkipImmutableScansButStillLockLivePins() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            var queries = new java.util.ArrayList<String>();
            org.hibernate.resource.jdbc.spi.StatementInspector inspector = sql -> { queries.add(sql); return sql; };
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", java.util.Map.of(
                    "hibernate.connection.datasource", c.pool(), "hibernate.hbm2ddl.auto", "validate",
                    "hibernate.session_factory.statement_inspector", inspector));
                 var later = capture(c, rig); var accepted = later.sources().work()) {
                var activation = activation(new Tx(emf), c, rig, plan, later);
                var scopes = new DocumentPublicationScopeCalls();
                var capture = activation.activateAccepted(CALLER, CALLER, NONE, accepted);
                queries.clear();
                try (var execution = activation.openExecution(CALLER, CALLER, accepted, scopes, NONE)) {
                    assertThat(queries).anyMatch(sql -> sql.contains("actual.root_digest"));
                    assertThat(queries).anyMatch(sql -> sql.contains("repository_preparation_source_pins p")
                            && sql.contains("jsonb_to_recordset"));
                    queries.clear();
                    var started = execution.start(CALLER, Duration.ofMinutes(1), NONE);
                    assertThat(execution.start(CALLER, Duration.ofMinutes(1), NONE)).isEqualTo(started);
                    assertThat(queries).noneMatch(sql -> sql.contains("actual.root_digest"));
                    assertThat(queries).noneMatch(sql -> sql.contains("repository_preparation_source_pins p")
                            && sql.contains("jsonb_to_recordset"));
                    assertThat(queries.stream().filter(sql -> sql.contains("document_read_pins p")
                            && sql.contains("FOR SHARE OF p")).count()).isEqualTo(2);
                }
                assertThat(scopes.isIdle()).isTrue();
                accepted.close();
                assertThat(capture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                scopes.close();
            }
        }
    }

    @org.junit.jupiter.api.Test void freshAttachmentRejectsCorruptedCaptureAndReturnsResources() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            try (var later = capture(c, rig); var accepted = later.sources().work()) {
                var activation = activation(c.tx(), c, rig, plan, later);
                var captured = activation.activateAccepted(CALLER, CALLER, NONE, accepted);
                var scopes = new DocumentPublicationScopeCalls();
                var digest = HexFormat.of().parseHex(captured.identity().pinsSha256());
                UUID original = c.tx().readOnly(em -> (UUID) em.createNativeQuery("""
                        SELECT node_id FROM repository_preparation_source_pins
                        WHERE operation_id=:o AND pins_sha256=:d
                        """).setParameter("o", plan.next().key().operationId()).setParameter("d", digest).getSingleResult());
                long reserved = rig.budget().reservedBytes();
                // Privileged test-only corruption: normal updates are forbidden by the immutable-row trigger.
                java.util.function.Consumer<UUID> replaceNode = node -> c.tx().inTransaction(em -> {
                    em.createNativeQuery("SET LOCAL session_replication_role = replica").executeUpdate();
                    assertThat(em.createNativeQuery("""
                            UPDATE repository_preparation_source_pins SET node_id=:n
                            WHERE operation_id=:o AND pins_sha256=:d
                            """).setParameter("n", node).setParameter("o", plan.next().key().operationId())
                            .setParameter("d", digest).executeUpdate()).isEqualTo(1);
                });
                replaceNode.accept(UUID.randomUUID());
                try {
                    assertThatThrownBy(() -> DocumentHistoricalSuccessorExecution.open(c.tx(), rig.budget(), plan,
                            rig.record(), later.sources(), captured, accepted, CALLER, new DriveLedger(c.tx()), NONE, scopes.enter()))
                            .isInstanceOfSatisfying(RepositoryException.class, e ->
                                    assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS))
                            .hasMessage("Preparation source pin batch differs from its captured identities");
                    assertThat(scopes.isIdle()).isTrue();
                    assertThat(rig.budget().reservedBytes()).isEqualTo(reserved);
                    assertThat(count(c, "repository_publication_assessment_starts")).isZero();
                    assertThat(count(c, "document_assessment_owners")).isZero();
                } finally { replaceNode.accept(original); }
                try (var execution = activation.openExecution(CALLER, CALLER, accepted, scopes, NONE)) {
                    assertThat(execution.start(CALLER, Duration.ofMinutes(1), NONE)).isNotNull();
                }
                assertThat(scopes.isIdle()).isTrue();
                assertThat(rig.budget().reservedBytes()).isEqualTo(reserved);
                accepted.close();
                assertThat(captured.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                scopes.close();
            }
        }
    }

    @org.junit.jupiter.api.Test void bindingRequiresActivationAndKeepsCurrentPreparationSeparateFromOriginalRoots() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig);
            try (var later = capture(c, rig); var work = later.sources().work()) {
                var pins = DocumentPreparationSourcePins.prepare(plan.next().command(),
                        work.references(plan.next().command(), () -> {}), () -> {});
                var identity = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(
                        plan.next().key(), plan.next().command().sha256(), plan.reservation().predecessor().epoch() + 1,
                        plan.reservation().successorToken(), plan.reservation().successorIncarnation()),
                        rig.record().predecessorGeneration(), HexFormat.of().formatHex(pins.digest()));
                var binding = new DocumentHistoricalSuccessorBinding(plan, rig.record(), identity, pins);
                Runnable check = () -> c.tx().inTransaction(em -> {
                    var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), plan.next().command().sha256(),
                            identity.owner().epoch(), identity.owner().token());
                    var owner = RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(),
                            plan.next().predecessorGeneration() + 1, plan.next().seeds().ownerNonce(), java.util.Optional.of(claim));
                    binding.lockRegistration(em, owner);
                    binding.requireCapture(em, owner, () -> {});
                });
                assertThatThrownBy(check::run).isInstanceOf(RepositoryException.class)
                        .hasMessage("Successor historical activation is absent");
                var actual = activation(c.tx(), c, rig, plan, later).activateAccepted(CALLER, CALLER, NONE, work);
                assertThat(actual.identity()).isEqualTo(identity);
                assertThatCode(check::run).doesNotThrowAnyException();
                assertThatThrownBy(() -> new DocumentHistoricalSuccessorBinding(plan, plan.next(), identity, pins))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Successor execution capture differs from installed preparation");
                work.close();
                assertThat(actual.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                assertThatThrownBy(check::run).isInstanceOf(RepositoryException.class)
                        .hasMessage("Successor preparation capture is no longer executable");
            }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"valid", "wrong-transaction", "old-capture", "drained", "old-claim", "wrong-incarnation"})
    void successorRequiresItsOwnLiveUndrainedCapture(String variant) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var original = DocumentPreparationSourcePins.prepare(rig.record().command(),
                    rig.sources().references(rig.record().command(), () -> {}), () -> {});
            var plan = installedHistoricalSuccessor(c, rig);
            try (var later = capture(c, rig); var work = later.sources().work()) {
                var selected = DocumentPreparationSourcePins.prepare(plan.next().command(),
                        work.references(plan.next().command(), () -> {}), () -> {});
                var activation = activation(c.tx(), c, rig, plan, later);
                var committed = activation.activateAccepted(CALLER, CALLER, NONE, work);
                var receipt = RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), CALLER,
                        plan, rig.record(), NONE).orElseThrow();
                assertThat(receipt.captureSha256()).isEqualTo(HexFormat.of().formatHex(selected.digest()));
                if (variant.equals("drained")) {
                    work.close();
                    assertThat(committed.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                }
                var pins = variant.equals("old-capture") ? original : selected;
                var xid = variant.equals("wrong-transaction") ? "1" : receipt.activationTransaction();
                var incarnation = variant.equals("wrong-incarnation") ? UUID.randomUUID() : receipt.execution().incarnation();
                Runnable check = () -> c.tx().inTransaction(em -> {
                    var live = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), plan.next().command().sha256(),
                            receipt.execution().epoch(), receipt.execution().token());
                    DocumentPreparationSourcePins.requireSuccessor(em, rig.record(), pins,
                            variant.equals("old-claim") ? rig.claim() : live, incarnation, xid, () -> {});
                });
                switch (variant) {
                    case "valid" -> assertThatCode(check::run).doesNotThrowAnyException();
                    case "wrong-transaction", "old-capture" -> assertThatThrownBy(check::run)
                            .isInstanceOf(RepositoryException.class).hasMessageContaining("pin batch differs");
                    case "old-claim" -> assertThatThrownBy(check::run).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Successor capture differs from preparation claim");
                    case "drained" -> assertThatThrownBy(check::run).isInstanceOf(RepositoryException.class)
                            .hasMessage("Successor preparation capture is no longer executable");
                    case "wrong-incarnation" -> assertThatThrownBy(check::run).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Coordinator binding differs from the restoring incarnation");
                    default -> throw new AssertionError(variant);
                }
                assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(2);
            }
        }
    }
}
