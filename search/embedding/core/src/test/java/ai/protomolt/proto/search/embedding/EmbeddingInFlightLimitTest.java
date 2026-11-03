package ai.protomolt.proto.search.embedding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
                        .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
            } finally {
                release.countDown();
            }
            active.get(5, TimeUnit.SECONDS);
            var failure = new IllegalArgumentException("test failure");
            assertThatThrownBy(() -> limit.execute(2, () -> { throw failure; })).isSameAs(failure);
            assertThat(limit.execute(3, () -> 3)).isEqualTo(3);
        }
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
