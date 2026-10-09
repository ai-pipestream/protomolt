package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL and retained descriptors; provider observations are supplied by the publication fixture. */
@Testcontainers
class DocumentHistoricalSourceDrainIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void acceptedResolutionStartsAfterCloseAndOwnsItsDrainPermit(boolean invalidMember) throws Exception {
        try (var c = context(POSTGRES)) {
            var fixture = retained(c);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
            var command = command(fixture, history);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try (var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), RepositoryReadControl.NONE);
                 var work = sources.work()) {
                var borrowed = work.references(command, () -> {}).getFirst();
                sources.close(); history.close();
                assertThatThrownBy(sources::work).hasMessageContaining("closed");
                var member = command.intent().getMembers(0);
                var limits = fixture.original().batch().policy().policy().limits();
                DocumentPublicationCandidate.Resolver ordinary = (m, occurrence) -> {
                    throw new AssertionError("Historical resolution contacted ordinary registry");
                };
                if (invalidMember) {
                    assertThatThrownBy(() -> work.resolve(member.toBuilder().setMemberId("foreign").build(),
                            Optional.empty(), ordinary, limits, budget, RepositoryReadControl.NONE))
                            .hasMessage("Historical assessment member differs");
                    assertThat(sources.awaitDrained(Duration.ZERO)).isFalse();
                    work.close();
                } else {
                    var ready = new CountDownLatch(1); var finish = new CountDownLatch(1);
                    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                        var worker = executor.submit(() -> {
                            try (var resolution = work.resolve(member, Optional.empty(), ordinary, limits, budget, RepositoryReadControl.NONE)) {
                                work.close();
                                ready.countDown();
                                if (!finish.await(5, TimeUnit.SECONDS)) throw new AssertionError("resolver timeout");
                                assertThat(resolution.resolver().container()).isEqualTo(
                                        DocumentSchemaRetentionFixture.definition(ai.protomolt.proto.repo.v1.Document.getDescriptor()));
                                assertThat(borrowed.plan().revision()).isEqualTo(fixture.revision());
                            }
                            return null;
                        });
                        try {
                            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                            assertThat(sources.awaitDrained(Duration.ZERO)).isFalse();
                            assertThat(history.isDrained()).isFalse();
                            assertThat(budget.reservedBytes()).isPositive();
                            assertThatThrownBy(() -> work.references(command, () -> {})).hasMessageContaining("ended");
                        } finally { finish.countDown(); }
                        worker.get(5, TimeUnit.SECONDS);
                    }
                }
                assertThat(sources.awaitDrained(Duration.ofSeconds(1))).isTrue();
                assertThat(history.isDrained()).isTrue();
                assertThatThrownBy(borrowed::plan).isInstanceOf(IllegalStateException.class);
            } finally {
                assertThat(budget.reservedBytes()).isZero();
                release(reads, history);
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void acceptedRegistrationRetainsSourcesThroughCommitAndAcknowledgment(boolean afterCommit) throws Exception {
        try (var c = context(POSTGRES)) {
            var fixture = retained(c);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
            var command = command(fixture, history);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var gate = new CountDownLatch(1);
            var finish = new CountDownLatch(1);
            var armed = new AtomicBoolean();
            var once = new AtomicBoolean();
            try (var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), RepositoryReadControl.NONE)) {
                var borrowed = sources.references(command, () -> {}).getFirst();
                var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
                var placement = fixture.original().prepared().members().getFirst().placement();
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        Map.of(placement.drive().id(), placement), Duration.ofMinutes(5), 0);
                var acknowledged = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                    if (afterCommit && armed.get() && once.compareAndSet(false, true)) hold(gate, finish);
                });
                var datasource = DocumentJdbcFaults.beforeCommit(acknowledged, connection -> {
                    try (var statement = connection.prepareStatement("SELECT count(*) FROM repository_operation_owners WHERE operation_id=?")) {
                        statement.setObject(1, command.operationId());
                        try (var rows = statement.executeQuery()) {
                            rows.next();
                            if (rows.getInt(1) != 1) return;
                            armed.set(true);
                            if (!afterCommit && once.compareAndSet(false, true)) hold(gate, finish);
                        }
                    }
                });
                try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                        Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                     var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    var registration = DocumentPublicationRegistration.historical(new Tx(emf), budget, record, sources,
                            UUID.randomUUID(), new DocumentPublicationScopeCalls(), new DriveLedger(c.tx()), RepositoryReadControl.NONE);
                    var future = executor.submit(() -> registration.admitInitial(CALLER,
                            Map.of("member", DocumentPublicationCandidate.Mode.TYPED), RepositoryReadControl.NONE));
                    try {
                        assertThat(gate.await(15, TimeUnit.SECONDS)).isTrue();
                        sources.close(); history.close();
                        assertThat(sources.awaitDrained(Duration.ZERO)).isFalse();
                        assertThat(history.isDrained()).isFalse();
                        assertThatThrownBy(sources::work).hasMessageContaining("closed");
                        assertThatThrownBy(history::release).hasMessageContaining("drained");
                        assertThat(borrowed.plan().revision()).isEqualTo(fixture.revision());
                        assertThat(new RepositoryOperationLedger(c.tx()).find(key).isPresent()).isEqualTo(afterCommit);
                    } finally { finish.countDown(); }
                    assertThat(future.get(15, TimeUnit.SECONDS)).isPresent();
                    assertThat(sources.awaitDrained(Duration.ofSeconds(1))).isTrue();
                    assertThat(history.isDrained()).isTrue();
                    assertThatThrownBy(borrowed::plan).isInstanceOf(IllegalStateException.class);
                    assertThat(new RepositoryOperationLedger(c.tx()).find(key)).isPresent();
                } finally { finish.countDown(); }
            } finally {
                assertThat(budget.reservedBytes()).isZero();
                release(reads, history);
            }
        }
    }

    @Test void returnedResolutionRetainsSourcesUntilItsOwnCleanupWithoutRegistryAccess() throws Exception {
        try (var c = context(POSTGRES)) {
            var fixture = retained(c);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
            var command = command(fixture, history);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try (var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), RepositoryReadControl.NONE)) {
                var borrowed = sources.references(command, () -> {}).getFirst();
                try (var resolution = sources.resolve(command.intent().getMembers(0), Optional.empty(),
                        (member, occurrence) -> { throw new AssertionError("Historical resolution contacted ordinary registry"); },
                        new DocumentSchemaAdmission.Limits(100, 4_000_000, 100, 4_000_000,
                                32, 16_000_000, 4_000_000), budget, RepositoryReadControl.NONE)) {
                    sources.close(); history.close();
                    assertThat(sources.awaitDrained(Duration.ZERO)).isFalse();
                    assertThat(history.isDrained()).isFalse();
                    assertThat(budget.reservedBytes()).isPositive();
                    assertThat(borrowed.plan().revision()).isEqualTo(fixture.revision());
                    assertThat(resolution.resolver().container()).isEqualTo(
                            DocumentSchemaRetentionFixture.definition(ai.protomolt.proto.repo.v1.Document.getDescriptor()));
                }
                assertThat(sources.awaitDrained(Duration.ofSeconds(1))).isTrue();
                assertThat(history.isDrained()).isTrue();
                assertThatThrownBy(borrowed::plan).isInstanceOf(IllegalStateException.class);
            } finally {
                assertThat(budget.reservedBytes()).isZero();
                release(reads, history);
            }
        }
    }

    private static Fixture retained(Context c) throws Exception {
        var original = DocumentSchemaRetentionFixture.prepare(c);
        new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
        return new Fixture(original, DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {})));
    }

    private static DocumentPublicationCommand command(Fixture fixture, DocumentReadLedger.PinnedHistory history) {
        return new DocumentPublicationCommand(fixture.original().command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history)).build());
    }

    private static void hold(CountDownLatch entered, CountDownLatch finish) throws java.sql.SQLException {
        entered.countDown();
        try {
            if (!finish.await(20, TimeUnit.SECONDS)) throw new java.sql.SQLException("Commit gate timed out");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new java.sql.SQLException("Commit gate interrupted", interrupted);
        }
    }
}
