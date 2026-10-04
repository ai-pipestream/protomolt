package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Actual PostgreSQL pin failures; fixture publications do not imply provider execution. */
@Testcontainers
class DocumentReadReleaseIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("reader", true);

    @Test void capacityIncludesClosedPlansUntilActualDrainAndSqlRelease() throws Exception {
        try (var c = context(POSTGRES)) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID(), 1);
            var read = ledger.captureHistorical(ADMIN, published.getAddress(), revision);
            var use = read.use();
            assertThat(ledger.outstandingReads()).isEqualTo(1);
            read.close();
            assertThat(ledger.releaseDrained(1)).isZero();
            assertThat(count(c, "document_read_pins")).isEqualTo(2);
            assertCapacity(() -> ledger.captureHistorical(ADMIN, published.getAddress(), revision));
            use.close();
            assertThat(read.awaitDrained(Duration.ZERO)).isTrue();
            assertCapacity(() -> ledger.captureHistorical(ADMIN, published.getAddress(), revision));
            assertThat(ledger.releaseDrained(1)).isEqualTo(1);
            assertThat(ledger.outstandingReads()).isZero();
            read.release(); // repeated completion does not return the capacity twice
            assertThat(ledger.outstandingReads()).isZero();
            var next = ledger.captureHistorical(ADMIN, published.getAddress(), revision);
            next.close(); next.release();
            ledger.fence(); ledger.attestLocalQuiescence();
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void sqlTimeoutRetainsEveryFailedHandleAndRetryReleasesBoundedBatches() {
        try (var c = context(POSTGRES)) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var bounded = c.tx().withTimeouts(new SqlTimeouts(Duration.ofMillis(100), Duration.ofSeconds(2)));
            var ledger = new DocumentReadLedger(bounded, UUID.randomUUID(), 2);
            // Drop caller references after close; the ledger must own retry handles.
            ledger.captureHistorical(ADMIN, published.getAddress(), revision).close();
            ledger.captureHistorical(ADMIN, published.getAddress(), revision).close();
            try (var blocker = c.emf().createEntityManager()) {
                blocker.getTransaction().begin();
                try {
                    blocker.createNativeQuery("LOCK TABLE document_read_pins IN ACCESS EXCLUSIVE MODE").executeUpdate();
                    assertThatThrownBy(() -> ledger.releaseDrained(2))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("handles retained for retry")
                            .satisfies(failure -> {
                                assertThat(failure.getCause()).isNotNull();
                                assertThat(failure.getSuppressed()).hasSize(1);
                            });
                    assertThat(ledger.outstandingReads()).isEqualTo(2);
                    assertCapacity(() -> ledger.captureHistorical(ADMIN, published.getAddress(), revision));
                } finally { blocker.getTransaction().rollback(); }
            }
            assertThat(count(c, "document_read_pins")).isEqualTo(4);
            assertThat(ledger.releaseDrained(1)).isEqualTo(1);
            assertThat(ledger.outstandingReads()).isEqualTo(1);
            assertThat(count(c, "document_read_pins")).isEqualTo(2);
            assertThat(ledger.releaseDrained(1)).isEqualTo(1);
            assertThat(ledger.releaseDrained(1)).isZero();
            assertThat(ledger.outstandingReads()).isZero();
            ledger.fence(); ledger.attestLocalQuiescence();
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void deniedCaptureReturnsCapacityWithoutCreatingPins() {
        try (var c = context(POSTGRES)) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID(), 1);
            var denied = new RepositoryCaller("denied", false);
            for (int i = 0; i < 3; i++) {
                assertThatThrownBy(() -> ledger.captureHistorical(denied, published.getAddress(), revision))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
                assertThat(ledger.outstandingReads()).isZero();
            }
            ledger.captureHistorical(ADMIN, published.getAddress(), revision).close();
            assertThat(ledger.releaseDrained(1)).isEqualTo(1);
            ledger.fence(); ledger.attestLocalQuiescence();
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void concurrentCaptureCannotOversubscribeOneSlot() throws Exception {
        try (var c = context(POSTGRES); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID(), 1);
            var start = new CountDownLatch(1);
            java.util.concurrent.Callable<Boolean> capture = () -> {
                start.await();
                try { ledger.captureHistorical(ADMIN, published.getAddress(), revision).close(); return true; }
                catch (RepositoryException failure) {
                    assertThat(failure.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED);
                    return false;
                }
            };
            var first = workers.submit(capture); var second = workers.submit(capture);
            start.countDown();
            assertThat(java.util.List.of(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(ledger.outstandingReads()).isEqualTo(1);
            assertThat(count(c, "document_read_pins")).isEqualTo(2);
            var releaseStart = new CountDownLatch(1);
            java.util.concurrent.Callable<Integer> release = () -> {
                releaseStart.await(); return ledger.releaseDrained(1);
            };
            var firstRelease = workers.submit(release); var secondRelease = workers.submit(release);
            releaseStart.countDown();
            assertThat(firstRelease.get(5, TimeUnit.SECONDS) + secondRelease.get(5, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(ledger.outstandingReads()).isZero();
            ledger.fence(); ledger.attestLocalQuiescence();
        }
    }

    private static void assertCapacity(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(RepositoryException.class,
                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
    }
}
