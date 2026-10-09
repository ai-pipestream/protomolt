package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL execution and decision handoff; byte ports fail if a refused candidate reaches them. */
@Testcontainers
class DocumentPublicationPreconditionExecutionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @Test void executionRecordsRecheckedConflictBeforeProviderWork() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET filename='changed'").executeUpdate(); });
            policy(c.tx());
            try (var resources = resources(c.tx())) {
                assertThatThrownBy(() -> execute(resources, c.tx(), p, RepositoryReadControl.NONE))
                        .isInstanceOfSatisfying(DocumentPublicationReplay.Terminated.class, failure ->
                                assertThat(failure.receipt().getReason()).isEqualTo(
                                        DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_PRECONDITION_NOT_MET));
                assertThat(count(c, "repository_operation_rejection")).isEqualTo(1);
                assertThat(count(c, "repository_operation_success")).isZero();
                assertThat(count(c, "document_events_outbox")).isZero();
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void wrappedTransactionFailureCannotBecomeATerminalRejection(boolean cancel) {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            policy(c.tx());
            var cancelled = new AtomicBoolean();
            var commits = new AtomicInteger();
            var conflict = new DocumentLedger.RevisionConflictException();
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                // Direct execution first replays, then reads active policy. Fail that
                // real policy transaction before commit, before any provider work.
                if (armed.get() && commits.incrementAndGet() == 2 && armed.compareAndSet(true, false)) {
                    cancelled.set(cancel);
                    throw conflict;
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf))) {
                var control = new RepositoryReadControl() {
                    @Override public long remainingNanos() { return Long.MAX_VALUE; }
                    @Override public boolean isCancelled() { return cancelled.get(); }
                };
                armed.set(true);
                assertThatThrownBy(() -> execute(resources, c.tx(), p, control))
                        .isInstanceOf(jakarta.persistence.RollbackException.class).hasRootCause(conflict);
                assertThat(armed).isFalse();
                assertThat(count(c, "repository_operation_rejection")).isZero();
                assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, p.command()).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void conflictSignalAloneCannotRejectMatchingConditions(boolean cancel) {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            policy(c.tx());
            var afterReplay = new AtomicBoolean();
            var firstCommit = new AtomicBoolean();
            var fired = new AtomicBoolean();
            var cancelled = new AtomicBoolean();
            var checks = new AtomicInteger();
            var conflict = new DocumentLedger.RevisionConflictException();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (firstCommit.compareAndSet(true, false)) afterReplay.set(true);
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var resources = resources(new Tx(emf))) {
                var control = new RepositoryReadControl() {
                    @Override public boolean isCancelled() { return cancelled.get(); }
                    @Override public long remainingNanos() {
                        // The first check after real replay is outside executeCandidate;
                        // the second is its policy-read check. Inject a direct signal
                        // there without changing the durable command conditions.
                        if (afterReplay.get() && checks.incrementAndGet() == 2 && fired.compareAndSet(false, true)) {
                            cancelled.set(cancel);
                            throw conflict;
                        }
                        return Long.MAX_VALUE;
                    }
                };
                firstCommit.set(true);
                if (cancel) assertThatThrownBy(() -> execute(resources, c.tx(), p, control))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                else assertThatThrownBy(() -> execute(resources, c.tx(), p, control)).isSameAs(conflict);
                assertThat(fired).isTrue();
                assertThat(checks.get()).as("fresh decision control check after the conflict signal").isGreaterThan(2);
                assertThat(count(c, "repository_operation_rejection")).isZero();
                assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, p.command()).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
            }
        }
    }

    private static void execute(Resources resources, Tx tx, Prepared p, RepositoryReadControl control) throws Exception {
        var driveId = UUID.fromString(p.command().intent().getMembers(0).getDriveId());
        var drive = new DriveLedger(tx).findById(driveId).orElseThrow();
        var placement = DocumentUploadPlan.Placement.sample(drive, "native-test", new ManagedBackendLedger(tx).find("native-test").orElseThrow());
        var prepared = DocumentOperationUploadAdmission.prepare(p.command(), Map.of(driveId, placement), Map.of(), Duration.ofMinutes(5));
        resources.execution().execute(CALLER, p.owner(), prepared, Map.of(), Map.of(),
                Map.of("member-0", DocumentPublicationCandidate.Mode.OPAQUE), Optional.empty(),
                (member, occurrence) -> { throw new AssertionError("Refused execution must not resolve schemas"); }, control);
    }

    private static void policy(Tx tx) {
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).setAnyResolvedSchema(true)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(100).setMaxFragmentBytes(4_000_000)
                        .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20)
                        .setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000)).build(), () -> {});
        new DocumentSchemaPolicies(tx).activate(policy, 0, () -> {});
    }

    private record Resources(DocumentUploadCoordinator uploads, DocumentPublicationExecution execution) implements AutoCloseable {
        @Override public void close() { uploads.close(); }
    }

    private static Resources resources(Tx tx) {
        var drives = new DriveLedger(tx);
        var budget = new PayloadBudget(8_000_000);
        var uploads = new DocumentUploadCoordinator(tx, drives, budget,
                (generation, profile) -> { throw new AssertionError("Refused execution must not open a byte provider"); },
                2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5)));
        var execution = new DocumentPublicationExecution(tx, drives, new DocumentReadLedger(tx, UUID.randomUUID()), uploads,
                (plan, member, control) -> { throw new AssertionError("Refused execution must not read retained bytes"); }, budget,
                new DocumentRevisionAssembly.Limits(1_000_000, 100, 100, 100, 100_000), false);
        return new Resources(uploads, execution);
    }
}
