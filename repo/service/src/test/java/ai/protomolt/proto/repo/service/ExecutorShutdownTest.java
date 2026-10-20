package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Closing the transport releases dependencies only after its executor has stopped. A refused
 * timeout must not shut anything down, a handler that ignores interruption must leave the
 * executor reported as still running, and an interrupted closer must keep its interrupt.
 */
class ExecutorShutdownTest {

    @Test
    void nonPositiveTimeoutsAreRefusedBeforeTheExecutorIsTouched() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            for (Duration timeout : new Duration[] {Duration.ZERO, Duration.ofNanos(-1)}) {
                assertThatThrownBy(() -> ExecutorShutdown.stop(executor, timeout))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Shutdown timeout must be positive");
            }
            assertThat(executor.isShutdown()).isFalse();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void anIdleExecutorStopsWithinTheTimeout() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> { }).get();
        ExecutorShutdown.stop(executor, Duration.ofSeconds(5));
        assertThat(executor.isTerminated()).isTrue();
    }

    @Test
    void aHandlerThatHonoursInterruptionIsStoppedByTheSecondPhase() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        var started = new CountDownLatch(1);
        var interrupted = new AtomicBoolean();
        executor.submit(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        started.await();
        ExecutorShutdown.stop(executor, Duration.ofMillis(100));
        assertThat(executor.isTerminated()).isTrue();
        assertThat(interrupted).isTrue();
    }

    @Test
    void aHandlerThatIgnoresInterruptionLeavesTheExecutorOwnedAndReported() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        executor.submit(() -> {
            started.countDown();
            while (true) {
                try {
                    release.await();
                    return;
                } catch (InterruptedException ignored) {
                    // Simulates a handler that does not stop on interrupt.
                }
            }
        });
        started.await();
        try {
            assertThatThrownBy(() -> ExecutorShutdown.stop(executor, Duration.ofMillis(50)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Repository transport executor did not terminate; retry close after handlers stop");
            assertThat(executor.isShutdown()).isTrue();
            assertThat(executor.isTerminated()).isFalse();
        } finally {
            release.countDown();
        }
        ExecutorShutdown.stop(executor, Duration.ofSeconds(5));
        assertThat(executor.isTerminated()).isTrue();
    }

    @Test
    void anInterruptedCloserForcesShutdownAndKeepsItsInterrupt() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        var started = new CountDownLatch(1);
        executor.submit(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        started.await();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> ExecutorShutdown.stop(executor, Duration.ofSeconds(30)))
                    .isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
}
