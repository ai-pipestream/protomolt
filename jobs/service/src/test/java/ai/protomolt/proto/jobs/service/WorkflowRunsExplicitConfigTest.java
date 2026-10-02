package ai.protomolt.proto.jobs.service;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowRunsExplicitConfigTest {
    private static WorkflowRunsConfig config(int workers, Duration lease, Duration poll,
            long backoff, int attempts, int concurrency, String topic) {
        return new WorkflowRunsConfig("worker", workers, lease, poll, backoff, attempts,
                concurrency, null, topic, null, null);
    }

    @Test
    void zeroBackoffMeansImmediateRetry() {
        assertThat(config(1, Duration.ofSeconds(30), Duration.ofMillis(50), 0, 3, 4, "events")
                .backoffBaseSeconds()).isZero();
    }

    @Test
    void invalidCountsAndBackoffAreRejected() {
        assertThatThrownBy(() -> config(0, Duration.ofSeconds(30), Duration.ofMillis(50), 1, 3, 4, "events"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("workerCount");
        assertThatThrownBy(() -> config(1, Duration.ofSeconds(30), Duration.ofMillis(50), -1, 3, 4, "events"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("backoffBaseSeconds");
        assertThatThrownBy(() -> config(1, Duration.ofSeconds(30), Duration.ofMillis(50), 1, 0, 4, "events"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxAttemptsDefault");
        assertThatThrownBy(() -> config(1, Duration.ofSeconds(30), Duration.ofMillis(50), 1, 3, 0, "events"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxConcurrentPerTarget");
    }

    @Test
    void missingOrInvalidDurationsAreRejected() {
        for (Duration duration : new Duration[] {null, Duration.ZERO, Duration.ofMillis(-1)}) {
            assertThatThrownBy(() -> config(1, duration, Duration.ofMillis(50), 1, 3, 4, "events"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("leaseDuration");
            assertThatThrownBy(() -> config(1, Duration.ofSeconds(30), duration, 1, 3, 4, "events"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pollInterval");
        }
    }

    @Test
    void missingEventsTopicDoesNotSelectAnotherTopic() {
        for (String topic : new String[] {null, "", " "}) {
            assertThatThrownBy(() -> config(1, Duration.ofSeconds(30), Duration.ofMillis(50), 1, 3, 4, topic))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("eventsTopic");
        }
    }
}
