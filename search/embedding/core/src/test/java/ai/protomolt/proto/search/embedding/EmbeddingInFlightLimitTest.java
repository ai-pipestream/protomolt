package ai.protomolt.proto.search.embedding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class EmbeddingInFlightLimitTest {
    @Test
    void activeConfigurationConflictsAndIdleReconfigurationSucceeds() throws Exception {
        var limit = new EmbeddingInFlightLimit();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var active = executor.submit(() -> limit.execute(1, () -> {
                entered.countDown();
                await(release);
                return 1;
            }));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> limit.execute(2, () -> 2))
                        .hasMessageContaining("Conflicting process-wide");
            } finally {
                release.countDown();
            }
            assertThat(active.get(5, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(limit.execute(2, () -> 2)).isEqualTo(2);
        }
    }

    @Test
    void admissionTimeoutAndOperationFailureReleaseAllAccounting() throws Exception {
        var limit = new EmbeddingInFlightLimit(Duration.ofMillis(50));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var active = executor.submit(() -> limit.execute(1, () -> {
                entered.countDown();
                await(release);
                return 1;
            }));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> limit.execute(1, () -> 2))
                        .hasCauseInstanceOf(TimeoutException.class);
            } finally {
                release.countDown();
            }
            active.get(5, TimeUnit.SECONDS);
            var failure = new IllegalArgumentException("test failure");
            assertThatThrownBy(() -> limit.execute(2, () -> { throw failure; })).isSameAs(failure);
            assertThat(limit.execute(3, () -> 3)).isEqualTo(3);
        }
    }

    @Test
    void perCallAdmissionTimeoutOverridesTheDefaultWithoutConflicting() throws Exception {
        var limit = new EmbeddingInFlightLimit(Duration.ofSeconds(30));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var active = executor.submit(() -> limit.execute(1, () -> {
                entered.countDown();
                await(release);
                return 1;
            }));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                long started = System.nanoTime();
                assertThatThrownBy(() -> limit.execute(1, Duration.ofMillis(50), () -> 2))
                        .hasCauseInstanceOf(TimeoutException.class);
                assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started)).isLessThan(5);
            } finally {
                release.countDown();
            }
            assertThat(active.get(5, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(limit.execute(1, Duration.ofSeconds(1), () -> 3)).isEqualTo(3);
        }
    }

    @Test
    void admissionTimeoutSecondsParsesDefaultsAndRefusesNonPositive() {
        assertThat(EmbeddingInFlightLimit.admissionTimeoutSeconds(null, "knob"))
                .isEqualTo(EmbeddingInFlightLimit.DEFAULT_ADMISSION_TIMEOUT);
        assertThat(EmbeddingInFlightLimit.admissionTimeoutSeconds(" 90 ", "knob")).isEqualTo(Duration.ofSeconds(90));
        for (String bad : new String[] {"0", "-5", "later"}) {
            assertThatThrownBy(() -> EmbeddingInFlightLimit.admissionTimeoutSeconds(bad, "knob"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("knob")
                    .hasMessageContaining(bad);
        }
        var limit = new EmbeddingInFlightLimit();
        assertThatThrownBy(() -> limit.execute(1, Duration.ZERO, () -> 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void admissionTimeoutSecondsRefusesAWaitThatOverflowsNanosecondsNamingTheKnob() {
        long largest = EmbeddingInFlightLimit.MAX_ADMISSION_TIMEOUT_SECONDS;
        assertThat(EmbeddingInFlightLimit.admissionTimeoutSeconds(Long.toString(largest), "knob").toNanos())
                .isEqualTo(TimeUnit.SECONDS.toNanos(largest));
        for (String bad : new String[] {Long.toString(largest + 1), Long.toString(Long.MAX_VALUE)}) {
            assertThatThrownBy(() -> EmbeddingInFlightLimit.admissionTimeoutSeconds(bad, "knob"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("knob")
                    .hasMessageContaining(bad);
        }
        var limit = new EmbeddingInFlightLimit();
        assertThatThrownBy(() -> limit.execute(1, Duration.ofSeconds(Long.MAX_VALUE), () -> 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasCauseInstanceOf(ArithmeticException.class)
                .hasMessageContaining("too long");
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("fixture timed out");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }
}
