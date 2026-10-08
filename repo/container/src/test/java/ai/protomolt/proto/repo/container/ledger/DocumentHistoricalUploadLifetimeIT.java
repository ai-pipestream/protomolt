package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL lifetime tests for zero-upload historical revisions; no provider I/O is claimed. */
@Testcontainers
class DocumentHistoricalUploadLifetimeIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @org.junit.jupiter.api.Test void configuredLockTimeoutReleasesTransferChild() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig, Duration.ofMinutes(5));
            try (var later = capture(c, rig); var accepted = later.sources().work()) {
                var activation = activation(c.tx(), c, rig, plan, later);
                var scopes = new DocumentPublicationScopeCalls();
                try (var execution = activation.openExecution(CALLER, CALLER, accepted, scopes, NONE);
                     var uploads = new DocumentUploadCoordinator(c.tx(), new DriveLedger(c.tx()), rig.budget(),
                             (generation, profile) -> { throw new AssertionError("Zero-upload revision resolved a provider"); },
                             2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofMillis(200), Duration.ofSeconds(2)));
                     var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                    long reserved = rig.budget().reservedBytes();
                    var entered = new CountDownLatch(1);
                    var finish = new CountDownLatch(1);
                    var blocking = workers.submit(() -> c.tx().inTransaction(em -> {
                        em.createNativeQuery("""
                                SELECT claim_token FROM repository_execution_claims
                                WHERE operation_id=:o FOR UPDATE
                                """).setParameter("o", plan.next().key().operationId()).getSingleResult();
                        entered.countDown();
                        try {
                            if (!finish.await(15, TimeUnit.SECONDS)) throw new AssertionError("Claim lock gate timed out");
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt(); throw new IllegalStateException(failure);
                        }
                    }));
                    try {
                        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                        var transfer = workers.submit(() -> execution.stageUploads(CALLER, uploads, Map.of(), Map.of(), NONE));
                        assertThatThrownBy(() -> transfer.get(5, TimeUnit.SECONDS))
                                .isInstanceOf(ExecutionException.class).satisfies(failure -> {
                                    Throwable cause = failure;
                                    while (cause != null && !(cause instanceof java.sql.SQLException)) cause = cause.getCause();
                                    assertThat(cause).isInstanceOf(java.sql.SQLException.class);
                                    assertThat(((java.sql.SQLException) cause).getSQLState()).isEqualTo("55P03");
                                });
                        assertThat(rig.budget().reservedBytes()).isEqualTo(reserved);
                        assertThat(uploads.providerActivity().active()).isZero();
                    } finally { finish.countDown(); }
                    blocking.get(5, TimeUnit.SECONDS);
                    execution.close(); accepted.close(); later.sources().close(); scopes.close();
                    assertThat(scopes.isIdle()).isTrue();
                    assertThat(activation.tentativeCapture().orElseThrow().complete(CALLER, Duration.ZERO, NONE)).isPresent();
                }
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void parentCloseCannotReleaseTransferMetadataOrCaptureBeforeTransactionExits(boolean cancel) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var plan = installedHistoricalSuccessor(c, rig, Duration.ofMinutes(5));
            var armed = new AtomicBoolean();
            var cancelled = new AtomicBoolean();
            var entered = new CountDownLatch(1);
            var finish = new CountDownLatch(1);
            var datasource = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!armed.compareAndSet(true, false)) return;
                entered.countDown();
                try {
                    if (!finish.await(20, TimeUnit.SECONDS)) throw new java.sql.SQLException("Transfer transaction gate timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt(); throw new java.sql.SQLException(failure);
                }
            });
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                        Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var later = capture(c, rig); var accepted = later.sources().work()) {
                var activation = activation(new Tx(emf), c, rig, plan, later);
                var scopes = new DocumentPublicationScopeCalls();
                long baseline = rig.budget().reservedBytes();
                try (var execution = activation.openExecution(CALLER, CALLER, accepted, scopes, NONE);
                     var uploads = new DocumentUploadCoordinator(c.tx(), new DriveLedger(c.tx()), rig.budget(),
                             (generation, profile) -> { throw new AssertionError("Zero-upload revision resolved a provider"); },
                             2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(10)));
                     var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                    long parentBytes = rig.budget().reservedBytes();
                    assertThat(parentBytes).isGreaterThan(baseline);
                    armed.set(true);
                    var transfer = workers.submit(() -> execution.stageUploads(CALLER, uploads, Map.of(), Map.of(), control));
                    try {
                        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                        // The child transaction runs independently of the parent's execution monitor.
                        var closing = workers.submit(execution::close);
                        closing.get(2, TimeUnit.SECONDS);
                        accepted.close(); later.sources().close(); scopes.close();
                        assertThat(scopes.isIdle()).isFalse();
                        assertThat(rig.budget().reservedBytes()).isGreaterThanOrEqualTo(parentBytes);
                        assertThat(activation.tentativeCapture().orElseThrow().complete(CALLER, Duration.ZERO, NONE)).isEmpty();
                        assertThat(transfer.isDone()).isFalse();
                        cancelled.set(cancel);
                    } finally { finish.countDown(); }
                    if (cancel) {
                        assertThatThrownBy(() -> transfer.get(10, TimeUnit.SECONDS))
                                .isInstanceOf(ExecutionException.class)
                                .satisfies(failure -> assertThat(failure.getCause())
                                        .isInstanceOfSatisfying(RepositoryException.class,
                                                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CANCELLED)));
                    } else assertThat(transfer.get(10, TimeUnit.SECONDS).members()).isEmpty();
                    assertThat(scopes.isIdle()).isTrue();
                    assertThat(rig.budget().reservedBytes()).isEqualTo(baseline);
                    assertThat(uploads.providerActivity().active()).isZero();
                    assertThat(activation.tentativeCapture().orElseThrow().complete(CALLER, Duration.ZERO, NONE)).isPresent();
                } finally { finish.countDown(); }
            }
        }
    }
}
