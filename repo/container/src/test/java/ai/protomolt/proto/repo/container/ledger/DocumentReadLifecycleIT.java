package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** SQL/lifecycle races; provider-worker drain is separately exercised with real S3 in the upload suite. */
@Testcontainers
class DocumentReadLifecycleIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("reader", true);

    @Test void tickKeepsAdmissionOpenAndShutdownPreservesInterruptedBatchUse() throws Exception {
        try (var c = context(POSTGRES); var reader = idleReader()) {
            var published = publish(c, prepare(c, 1), Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var lifecycle = new DocumentReadLifecycle(ledger, reader, 2);
            ledger.captureHistorical(ADMIN, published.getAddress(), revision).close();
            assertThat(lifecycle.tick()).isEqualTo(1);
            var read = ledger.captureHistorical(ADMIN, published.getAddress(), revision);
            var use = read.use();
            assertThat(lifecycle.shutdownStep(Duration.ZERO)).isFalse();
            assertThatThrownBy(() -> ledger.captureHistorical(ADMIN, published.getAddress(), revision)).hasMessageContaining("closed");
            assertThatThrownBy(read::use).hasMessageContaining("closed");
            assertThatThrownBy(() -> {
                Thread.currentThread().interrupt();
                try { lifecycle.shutdownStep(Duration.ofSeconds(1)); }
                finally { Thread.interrupted(); }
            }).isInstanceOf(InterruptedException.class);
            assertThat(count(c, "document_read_pins")).isEqualTo(2);
            assertThatThrownBy(ledger::attestLocalQuiescence).hasMessageContaining("local lifetimes");
            use.close();
            assertThat(lifecycle.shutdownStep(Duration.ofSeconds(1))).isFalse();
            assertThat(lifecycle.shutdownStep(Duration.ZERO)).isTrue();
            assertThat(ledger.outstandingReads()).isZero();
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void failedSqlCleanupKeepsShutdownRetryable() throws Exception {
        try (var c = context(POSTGRES); var reader = idleReader()) {
            var published = publish(c, prepare(c, 1), Fault.NONE, em -> {}).getMembers(0);
            var ledger = new DocumentReadLedger(c.tx().withTimeouts(
                    new SqlTimeouts(Duration.ofMillis(100), Duration.ofSeconds(2))), UUID.randomUUID());
            ledger.captureHistorical(ADMIN, published.getAddress(), UUID.fromString(published.getRevisionId()));
            var lifecycle = new DocumentReadLifecycle(ledger, reader, 2);
            try (var blocker = c.emf().createEntityManager()) {
                blocker.getTransaction().begin();
                try {
                    blocker.createNativeQuery("LOCK TABLE document_read_pins IN ACCESS EXCLUSIVE MODE").executeUpdate();
                    assertThatThrownBy(() -> lifecycle.shutdownStep(Duration.ZERO)).hasStackTraceContaining("lock timeout");
                    assertThat(ledger.outstandingReads()).isEqualTo(1);
                } finally { blocker.getTransaction().rollback(); }
            }
            assertThat(count(c, "document_read_pins")).isEqualTo(2);
            assertThat(lifecycle.shutdownStep(Duration.ZERO)).isFalse();
            assertThat(lifecycle.shutdownStep(Duration.ZERO)).isTrue();
            assertThat(ledger.outstandingReads()).isZero();
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void captureReturningAfterShutdownFenceIsClosedAndDrainedBeforeQuiescence() throws Exception {
        try (var c = context(POSTGRES); var reader = idleReader(); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var published = publish(c, prepare(c, 1), Fault.NONE, em -> {}).getMembers(0);
            var armed = new AtomicBoolean();
            var committed = new CountDownLatch(1); var deliver = new CountDownLatch(1);
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (!armed.compareAndSet(true, false)) return;
                committed.countDown();
                try {
                    if (!deliver.await(10, TimeUnit.SECONDS)) throw new java.sql.SQLException("Capture response gate expired");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new java.sql.SQLException("Capture response interrupted", interrupted);
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    java.util.Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var ledger = new DocumentReadLedger(new Tx(emf), UUID.randomUUID());
                var lifecycle = new DocumentReadLifecycle(ledger, reader, 2);
                armed.set(true);
                var capture = tasks.submit(() -> ledger.captureHistorical(ADMIN, published.getAddress(), UUID.fromString(published.getRevisionId())));
                try {
                    assertThat(committed.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(lifecycle.shutdownStep(Duration.ZERO)).isFalse();
                    assertThatThrownBy(ledger::attestLocalQuiescence).hasMessageContaining("local lifetimes");
                    assertThat(count(c, "document_read_pins")).isEqualTo(2);
                } finally { deliver.countDown(); }
                var read = capture.get(5, TimeUnit.SECONDS);
                assertThatThrownBy(read::use).hasMessageContaining("closed");
                assertThat(read.isDrained()).isTrue();
                assertThat(lifecycle.shutdownStep(Duration.ZERO)).isFalse();
                assertThat(lifecycle.shutdownStep(Duration.ZERO)).isTrue();
                assertThat(ledger.outstandingReads()).isZero();
            }
        }
    }

    private static DocumentPartReader idleReader() {
        return new DocumentPartReader((generation, profile) -> {
            throw new AssertionError("These SQL tests must not perform provider reads");
        });
    }
}
