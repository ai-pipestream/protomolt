package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentHistoricalSourceLifetimeTest {
    @Test void acceptedChildOutlivesParentWithoutReopeningAdmission() throws Exception {
        var releases = new AtomicInteger();
        var lifetime = new DocumentHistoricalSourceLifetime(releases::incrementAndGet);
        var parent = lifetime.enter();
        lifetime.close();
        var child = parent.fork();
        parent.close();
        assertThatThrownBy(parent::fork).hasMessageContaining("ended");
        assertThatThrownBy(lifetime::enter).hasMessageContaining("closed");
        child.requireActive();
        assertThat(lifetime.awaitDrained(Duration.ZERO)).isFalse();
        assertThat(releases).hasValue(0);
        child.close();
        assertThat(lifetime.awaitDrained(Duration.ZERO)).isTrue();
        assertThat(releases).hasValue(1);
    }

    @Test void concurrentParentCloseAndForkCannotLoseAnAcceptedChild() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int iteration = 0; iteration < 100; iteration++) {
                var releases = new AtomicInteger();
                var lifetime = new DocumentHistoricalSourceLifetime(releases::incrementAndGet);
                var parent = lifetime.enter();
                lifetime.close();
                var start = new CountDownLatch(1);
                var closing = executor.submit(() -> { start.await(); parent.close(); return null; });
                var forking = executor.submit(() -> {
                    start.await();
                    try { return parent.fork(); }
                    catch (IllegalStateException ended) {
                        assertThat(ended).hasMessage("Historical source work has ended");
                        return null;
                    }
                });
                start.countDown();
                closing.get(5, TimeUnit.SECONDS);
                var child = forking.get(5, TimeUnit.SECONDS);
                if (child != null) {
                    assertThat(releases).hasValue(0);
                    child.requireActive(); child.close();
                }
                assertThat(lifetime.awaitDrained(Duration.ZERO)).isTrue();
                assertThat(releases).hasValue(1);
            }
        }
    }

    @Test void closeRefusesNewWorkAndWaitsForActualWorkerExitDespiteFutureCancellation() throws Exception {
        var releases = new AtomicInteger();
        var lifetime = new DocumentHistoricalSourceLifetime(releases::incrementAndGet);
        var accepted = lifetime.enter();
        var entered = new CountDownLatch(1); var finish = new CountDownLatch(1); var exited = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = executor.submit(() -> {
                try (accepted) {
                    entered.countDown();
                    // This models an accepted call whose actual worker ignores interruption until completion.
                    boolean interrupted = false;
                    while (true) {
                        try { if (!finish.await(5, TimeUnit.SECONDS)) throw new AssertionError("worker timeout"); break; }
                        catch (InterruptedException ignoredUntilExit) { interrupted = true; }
                    }
                    accepted.requireActive();
                    if (interrupted) Thread.currentThread().interrupt();
                } finally { exited.countDown(); }
            });
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                lifetime.close();
                assertThat(future.cancel(true)).isTrue();
                assertThat(future.isDone()).isTrue();
                assertThatThrownBy(lifetime::enter).hasMessageContaining("closed");
                assertThat(lifetime.awaitDrained(Duration.ZERO)).isFalse();
                assertThat(releases).hasValue(0);
            } finally { finish.countDown(); }
            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(lifetime.awaitDrained(Duration.ofSeconds(1))).isTrue();
            assertThat(releases).hasValue(1);
            lifetime.close(); accepted.close();
            assertThat(releases).hasValue(1);
            assertThatThrownBy(accepted::requireActive).hasMessageContaining("ended");
        }
    }

    @Test void cleanupFailureCannotBecomeDrainProofAndCloseCanRetry() throws Exception {
        var attempts = new AtomicInteger();
        var failure = new IllegalStateException("release failed");
        var lifetime = new DocumentHistoricalSourceLifetime(() -> { if (attempts.incrementAndGet() == 1) throw failure; });
        assertThatThrownBy(lifetime::close).isSameAs(failure);
        assertThatThrownBy(() -> lifetime.awaitDrained(Duration.ZERO)).hasCause(failure);
        lifetime.close();
        assertThat(lifetime.awaitDrained(Duration.ZERO)).isTrue();
        assertThat(attempts).hasValue(2);
    }

    @Test void admissionMustCloseBeforeDrainCanBeObserved() {
        var lifetime = new DocumentHistoricalSourceLifetime(() -> {});
        assertThatThrownBy(() -> lifetime.awaitDrained(Duration.ZERO)).hasMessageContaining("Close source admission");
    }

    @Test void failingComponentCleanupStillAttemptsAllClosersBeforeEndingWork() throws Exception {
        var order = new java.util.ArrayList<String>();
        var lifetime = new DocumentHistoricalSourceLifetime(() -> order.add("release sources"));
        var work = lifetime.enter();
        lifetime.close();
        var first = new IllegalStateException("composite close");
        var second = new IllegalArgumentException("loader close");
        assertThatThrownBy(() -> DocumentHistoricalAssessmentSources.closeAll(java.util.List.<AutoCloseable>of(
                () -> { order.add("composite"); throw first; },
                () -> { order.add("loader"); throw second; },
                () -> { assertThat(lifetime.awaitDrained(Duration.ZERO)).isFalse(); order.add("last loader"); },
                work)))
                .isSameAs(first).satisfies(failure -> assertThat(failure.getSuppressed()).containsExactly(second));
        assertThat(order).containsExactly("composite", "loader", "last loader", "release sources");
        assertThat(lifetime.awaitDrained(Duration.ZERO)).isTrue();
    }
}
