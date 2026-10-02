package ai.protomolt.proto.jobs.service;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowRunsTimeBoundsTest {
    private static WorkflowRunsConfig config(Duration lease, Duration poll, long backoff) {
        return new WorkflowRunsConfig("worker", 1, lease, poll, backoff, 3, 4, null, "events", null, null);
    }

    @Test
    void retryBackoffCannotOverflowItsMillisecondRepresentation() {
        long largest = Long.MAX_VALUE / 1000 / (1L << 20);
        assertThat(config(Duration.ofSeconds(30), Duration.ofMillis(50), largest).backoffBaseSeconds())
                .isEqualTo(largest);
        for (long invalid : new long[] {largest + 1, Long.MAX_VALUE}) {
            assertThatThrownBy(() -> config(Duration.ofSeconds(30), Duration.ofMillis(50), invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("backoffBaseSeconds");
        }
    }

    @Test
    void durationsMustRemainPositiveAndRepresentableInMilliseconds() {
        for (Duration invalid : new Duration[] {Duration.ofNanos(1), Duration.ofSeconds(Long.MAX_VALUE)}) {
            assertThatThrownBy(() -> config(invalid, Duration.ofMillis(50), 1))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("leaseDuration");
            assertThatThrownBy(() -> config(Duration.ofSeconds(30), invalid, 1))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pollInterval");
        }
        assertThat(config(Duration.ofMillis(1), Duration.ofMillis(1), 0).leaseDuration()).isEqualTo(Duration.ofMillis(1));
    }
}
