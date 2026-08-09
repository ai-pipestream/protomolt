package ai.pipestream.proto.jobs.service;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The worker/relay/consumer configuration record: the required (and
 * non-blank) workerId, the request-topic/broker pair rule, and the defaults
 * every non-positive or absent knob falls back to.
 */
class ChainJobsConfigTest {

    private static ChainJobsConfig config(String workerId, int workerCount, Duration lease,
            Duration poll, long backoffBase, int maxAttempts, int maxConcurrent,
            String requestTopic, String eventsTopic, String bootstrap) {
        return new ChainJobsConfig(workerId, workerCount, lease, poll, backoffBase, maxAttempts,
                maxConcurrent, requestTopic, eventsTopic, bootstrap, null);
    }

    /** A fully-defaulted config: every knob non-positive or absent. */
    private static ChainJobsConfig defaulted() {
        return config("worker-1", 0, null, null, 0, 0, 0, null, null, null);
    }

    @Test
    void aNullOrBlankWorkerIdIsRejected() {
        assertThatThrownBy(() -> config(null, 1, Duration.ofMinutes(1),
                Duration.ofMillis(10), 1, 1, 1, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("workerId");
        assertThatThrownBy(() -> config("   ", 1, Duration.ofMinutes(1),
                Duration.ofMillis(10), 1, 1, 1, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workerId must not be blank");
    }

    @Test
    void nonPositiveKnobsFallBackToTheDefaults() {
        ChainJobsConfig config = defaulted();
        assertThat(config.workerCount()).isEqualTo(ChainJobsConfig.DEFAULT_WORKER_COUNT);
        assertThat(config.leaseDuration()).isEqualTo(ChainJobsConfig.DEFAULT_LEASE_DURATION);
        assertThat(config.pollInterval()).isEqualTo(ChainJobsConfig.DEFAULT_POLL_INTERVAL);
        assertThat(config.backoffBaseSeconds())
                .isEqualTo(ChainJobsConfig.DEFAULT_BACKOFF_BASE_SECONDS);
        assertThat(config.maxAttemptsDefault()).isEqualTo(ChainJobsConfig.DEFAULT_MAX_ATTEMPTS);
        assertThat(config.maxConcurrentPerTarget())
                .isEqualTo(ChainJobsConfig.DEFAULT_MAX_CONCURRENT_PER_TARGET);
        assertThat(config.eventsTopic()).isEqualTo(ChainJobsConfig.DEFAULT_EVENTS_TOPIC);
        assertThat(config.requestTopic()).isNull();
        assertThat(config.kafkaBootstrapServers()).isNull();
    }

    @Test
    void zeroAndNegativeDurationsFallBackToo() {
        ChainJobsConfig zeroed = config("worker-1", -3, Duration.ZERO, Duration.ZERO,
                -1, -2, -8, null, " ", null);
        assertThat(zeroed.leaseDuration()).isEqualTo(ChainJobsConfig.DEFAULT_LEASE_DURATION);
        assertThat(zeroed.pollInterval()).isEqualTo(ChainJobsConfig.DEFAULT_POLL_INTERVAL);
        assertThat(zeroed.eventsTopic()).isEqualTo(ChainJobsConfig.DEFAULT_EVENTS_TOPIC);

        ChainJobsConfig negative = config("worker-1", 1, Duration.ofSeconds(-5),
                Duration.ofMillis(-1), 1, 1, 1, null, null, null);
        assertThat(negative.leaseDuration()).isEqualTo(ChainJobsConfig.DEFAULT_LEASE_DURATION);
        assertThat(negative.pollInterval()).isEqualTo(ChainJobsConfig.DEFAULT_POLL_INTERVAL);
    }

    @Test
    void explicitValuesAreKept() {
        ChainJobsConfig config = config("worker-9", 7, Duration.ofMinutes(2),
                Duration.ofSeconds(1), 9, 5, 3, null, "my-events", null);
        assertThat(config.workerId()).isEqualTo("worker-9");
        assertThat(config.workerCount()).isEqualTo(7);
        assertThat(config.leaseDuration()).isEqualTo(Duration.ofMinutes(2));
        assertThat(config.pollInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.backoffBaseSeconds()).isEqualTo(9);
        assertThat(config.maxAttemptsDefault()).isEqualTo(5);
        assertThat(config.maxConcurrentPerTarget()).isEqualTo(3);
        assertThat(config.eventsTopic()).isEqualTo("my-events");
    }

    @Test
    void aRequestTopicWithoutABrokerIsAConfigurationError() {
        assertThatThrownBy(() -> config("worker-1", 1, Duration.ofMinutes(1),
                Duration.ofMillis(10), 1, 1, 1, "chain-job-requests", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requestTopic")
                .hasMessageContaining("bootstrap");
        // A blank bootstrap is no bootstrap at all.
        assertThatThrownBy(() -> config("worker-1", 1, Duration.ofMinutes(1),
                Duration.ofMillis(10), 1, 1, 1, "chain-job-requests", null, "  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aRequestTopicWithABrokerIsAccepted() {
        ChainJobsConfig config = config("worker-1", 1, Duration.ofMinutes(1),
                Duration.ofMillis(10), 1, 1, 1, "chain-job-requests", null,
                "localhost:9092");
        assertThat(config.requestTopic()).isEqualTo("chain-job-requests");
        assertThat(config.kafkaBootstrapServers()).isEqualTo("localhost:9092");
    }

    @Test
    void theSchemaRegistryUrlPassesThroughUntouched() {
        ChainJobsConfig without = defaulted();
        assertThat(without.schemaRegistryUrl()).isNull();
        ChainJobsConfig with = new ChainJobsConfig("worker-1", 1, Duration.ofMinutes(1),
                Duration.ofMillis(10), 1, 1, 1, null, null, null, "http://registry:8081");
        assertThat(with.schemaRegistryUrl()).isEqualTo("http://registry:8081");
    }
}
