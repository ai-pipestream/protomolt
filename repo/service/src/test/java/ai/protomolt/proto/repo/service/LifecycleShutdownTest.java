package ai.protomolt.proto.repo.service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class LifecycleShutdownTest {
    @Test void timeoutRetainsResourcesUntilUncooperativeWorkerActuallyStops() throws Exception {
        var entered = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var released = new AtomicBoolean();
        var observedPrematureRelease = new AtomicBoolean();
        Thread worker = Thread.ofVirtual().name("blocked-provider").start(() -> {
            entered.countDown();
            while (finish.getCount() != 0) {
                try { finish.await(); }
                catch (InterruptedException ignored) { /* Model a provider that does not stop on interruption. */ }
            }
            observedPrematureRelease.set(released.get());
        });
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> LifecycleShutdown.stopBeforeRelease(List.of(worker), Duration.ofMillis(50), () -> released.set(true)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("resources retained");
            assertThat(released).isFalse();
            assertThat(worker.isAlive()).isTrue();
        } finally { finish.countDown(); worker.join(5000); }
        assertThat(worker.isAlive()).isFalse();
        assertThat(observedPrematureRelease).isFalse();
        LifecycleShutdown.stopBeforeRelease(List.of(worker), Duration.ofSeconds(1), () -> released.set(true));
        assertThat(released).isTrue();
    }

    @Test void interruptedCallerRetainsResourcesAndInterruptFlag() throws Exception {
        var finish = new CountDownLatch(1);
        var released = new AtomicBoolean();
        Thread worker = Thread.ofVirtual().start(() -> {
            while (finish.getCount() != 0) {
                try { finish.await(); }
                catch (InterruptedException ignored) { /* Deliberately uncooperative provider. */ }
            }
        });
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> LifecycleShutdown.stopBeforeRelease(List.of(worker), Duration.ofSeconds(1), () -> released.set(true)))
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(released).isFalse();
        } finally { Thread.interrupted(); finish.countDown(); worker.join(5000); }
        assertThat(worker.isAlive()).isFalse();
    }
}
