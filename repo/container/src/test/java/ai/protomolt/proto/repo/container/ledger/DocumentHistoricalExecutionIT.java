package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real registration, SQL capture and retained schemas; initial provider observations are synthetic. */
@Testcontainers
class DocumentHistoricalExecutionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest
    @ValueSource(strings = {"valid", "held", "rollback", "lost-ack", "modes", "owner", "missing-capture", "roots", "closed", "registration-closed"})
    void initialHandleRequiresCompleteRegisteredIdentityAndRetainsSources(String variant) throws Exception {
        try (var c = context(POSTGRES)) {
            var original = DocumentSchemaRetentionFixture.prepare(c);
            new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {}));
            var fixture = new Fixture(original, revision);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), revision);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try {
                var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history)).build());
                var key = new RepositoryOperationLedger.Key("account", CALLER.principalName(), command.operationId());
                var placement = original.prepared().members().getFirst().placement();
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        Map.of(placement.drive().id(), placement), Duration.ofMinutes(5), 0);
                var armed = new java.util.concurrent.atomic.AtomicBoolean();
                var entered = new java.util.concurrent.CountDownLatch(1);
                var finish = new java.util.concurrent.CountDownLatch(1);
                var faulted = new java.util.concurrent.atomic.AtomicBoolean();
                var loseAcknowledgement = new java.util.concurrent.atomic.AtomicBoolean();
                var beforeCommit = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                    if ((variant.equals("rollback") || variant.equals("lost-ack")) && !faulted.get()) {
                        try (var statement = connection.prepareStatement("SELECT count(*) FROM repository_operation_owners WHERE operation_id=?")) {
                            statement.setObject(1, command.operationId());
                            try (var rows = statement.executeQuery()) {
                                rows.next();
                                if (rows.getLong(1) == 1 && faulted.compareAndSet(false, true)) {
                                    if (variant.equals("rollback")) throw new java.sql.SQLException("injected registration rollback");
                                    loseAcknowledgement.set(true);
                                }
                            }
                        }
                    }
                    if (!armed.get()) return;
                    try (var statement = connection.prepareStatement("""
                            SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid()
                              AND relation='document_read_pins'::regclass AND mode='RowShareLock' AND granted)
                            """); var rows = statement.executeQuery()) {
                        rows.next();
                        if (!rows.getBoolean(1) || !armed.compareAndSet(true, false)) return;
                        entered.countDown();
                        try {
                            if (!finish.await(15, java.util.concurrent.TimeUnit.SECONDS)) throw new java.sql.SQLException("mint gate timeout");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt(); throw new java.sql.SQLException(interrupted);
                        }
                    }
                });
                var datasource = DocumentJdbcFaults.afterCommit(beforeCommit, () -> {
                    if (loseAcknowledgement.compareAndSet(true, false)) throw new java.sql.SQLException("injected registration lost acknowledgement");
                });
                try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                            Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                     var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), NONE)) {
                    var scopes = new DocumentPublicationScopeCalls();
                    var registration = DocumentPublicationRegistration.historical(new Tx(emf), budget, record, sources,
                            UUID.randomUUID(), scopes, new DriveLedger(c.tx()), NONE);
                    var modes = Map.of("member", DocumentPublicationCandidate.Mode.TYPED);
                    RepositoryOperationLedger.Owner owner;
                    if (variant.equals("rollback") || variant.equals("lost-ack")) {
                        assertThatThrownBy(() -> registration.admitInitial(CALLER, modes, NONE))
                                .hasStackTraceContaining("injected registration");
                        assertThat(registration.historicalCapture()).isPresent();
                        assertThat(new RepositoryOperationLedger(c.tx()).find(key).isPresent()).isEqualTo(variant.equals("lost-ack"));
                        // Tentative coordinates grant nothing: only the factory's SQL checks can accept them.
                        var identity = registration.drainIdentity();
                        var deadline = java.time.Instant.now().plus(record.lease());
                        var claim = new RepositoryExecutionClaimLedger.Claim(key, command.sha256(), identity.epoch(), identity.token(), deadline);
                        owner = new RepositoryOperationLedger.Owner(key, 1, record.seeds().ownerNonce(), deadline, Optional.of(claim));
                    } else owner = registration.admitInitial(CALLER, modes, NONE).orElseThrow();
                    var candidateOwner = variant.equals("owner") ? new RepositoryOperationLedger.Owner(owner.key(), owner.generation(),
                            UUID.randomUUID(), owner.leaseUntil(), owner.executionClaim()) : owner;
                    var requestedModes = variant.equals("modes") ? Map.of("member", DocumentPublicationCandidate.Mode.OPAQUE) : modes;
                    if (variant.equals("closed")) sources.close();
                    if (variant.equals("registration-closed")) scopes.close();
                    if (variant.equals("missing-capture") || variant.equals("roots")) {
                        c.tx().inTransaction(em -> {
                            em.createNativeQuery("SET LOCAL session_replication_role=replica").executeUpdate();
                            String table = variant.equals("roots") ? "repository_preparation_history_roots" : "repository_preparation_pin_owners";
                            em.createNativeQuery("DELETE FROM " + table + " WHERE operation_id=:o")
                                    .setParameter("o", command.operationId()).executeUpdate();
                        });
                    }
                    long before = budget.reservedBytes();
                    if (variant.equals("valid") || variant.equals("held") || variant.equals("lost-ack")) {
                        var borrowed = sources.references(command, () -> {}).getFirst();
                        DocumentHistoricalExecution accepted;
                        if (variant.equals("held")) {
                            armed.set(true);
                            try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                                var future = executor.submit(() -> registration.historicalExecution(CALLER, candidateOwner, requestedModes, NONE));
                                try {
                                    assertThat(entered.await(15, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                                    scopes.close(); sources.close(); history.close();
                                    assertThat(scopes.awaitIdle(Duration.ZERO)).isFalse();
                                    assertThat(sources.awaitDrained(Duration.ZERO)).isFalse();
                                    assertThat(history.isDrained()).isFalse();
                                } finally { finish.countDown(); }
                                accepted = future.get(15, java.util.concurrent.TimeUnit.SECONDS);
                            }
                        } else accepted = registration.historicalExecution(CALLER, candidateOwner, requestedModes, NONE);
                        try (var execution = accepted) {
                            scopes.close();
                            assertThat(scopes.awaitIdle(Duration.ZERO)).isFalse();
                            sources.close(); history.close();
                            assertThat(sources.awaitDrained(Duration.ZERO)).isFalse();
                            assertThat(history.isDrained()).isFalse();
                            assertThat(borrowed.plan().revision()).isEqualTo(revision);
                            assertThat(budget.reservedBytes()).isGreaterThan(before);
                        }
                        assertThat(sources.awaitDrained(Duration.ofSeconds(1))).isTrue();
                        assertThat(history.isDrained()).isTrue();
                    } else {
                        var failure = catchThrowable(() -> registration.historicalExecution(CALLER, candidateOwner, requestedModes, NONE));
                        switch (variant) {
                            case "modes" -> assertThat(failure).hasMessage("Historical execution modes differ from registration");
                            case "owner" -> assertThat(failure).hasMessage("Journal owner or command differs from registered session");
                            case "missing-capture" -> assertThat(failure).hasMessage("Initial preparation capture is absent");
                            case "roots" -> assertThat(failure).hasMessage("Preparation history projection differs from its canonical command");
                            case "closed" -> assertThat(failure).hasMessage("Historical assessment sources are closed");
                            case "registration-closed" -> assertThat(failure).hasMessage("Publication runtime is closed");
                            case "rollback" -> assertThat(failure).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
                            default -> throw new AssertionError(variant);
                        }
                    }
                    assertThat(scopes.awaitIdle(Duration.ZERO)).isTrue();
                    assertThat(budget.reservedBytes()).isEqualTo(before);
                }
            } finally {
                assertThat(budget.reservedBytes()).isZero();
                release(reads, history);
            }
        }
    }
}
