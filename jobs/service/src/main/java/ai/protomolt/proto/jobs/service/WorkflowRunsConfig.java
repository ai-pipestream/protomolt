package ai.protomolt.proto.jobs.service;

import java.time.Duration;
import java.util.Objects;

/**
 * The workflow-runs worker/relay/consumer configuration. A plain value object —
 * no framework binding; the serve wiring builds it from its flags.
 * <p>
 * {@code workerId} is the only identity the fleet has (it stamps lease_owner
 * on claimed rows), so it is required — a fleet of anonymous workers cannot
 * be debugged from the row. {@code kafkaBootstrapServers} and
 * {@code requestTopic} are a pair: a request topic with no broker is a
 * configuration error, not a silent no-op.
 *
 * @param workerId this worker's identity (the lease owner); required
 * @param workerCount how many claim-execute loops to run (virtual threads)
 * @param leaseDuration how long a claimed job stays RUNNING before the lease
 *        sweeper may requeue it — size it above p99 segment latency, LLM
 *        steps are minutes, not milliseconds
 * @param pollInterval idle backoff for the worker loops
 * @param backoffBaseSeconds the base of the retry backoff
 *        ({@code base * 2^(attempt-1)} seconds)
 * @param maxAttemptsDefault the retry ceiling stamped on new jobs
 * @param maxConcurrentPerTarget the per-target concurrency cap (one OpenVINO
 *        box per model: an uncapped worker fleet DDoSes the inference tier)
 * @param requestTopic the WorkflowRunRequest topic to consume, or null to run
 *        without broker-native submission
 * @param eventsTopic the WorkflowRunEvent topic the relay publishes to
 * @param kafkaBootstrapServers the Kafka bootstrap servers, or null when no
 *        broker is involved (verb-only submission, no relay)
 * @param schemaRegistryUrl a Confluent-compatible schema registry the serde
 *        resolves schema ids from, or null for registry-free framing
 */
public record WorkflowRunsConfig(
        String workerId,
        int workerCount,
        Duration leaseDuration,
        Duration pollInterval,
        long backoffBaseSeconds,
        int maxAttemptsDefault,
        int maxConcurrentPerTarget,
        String requestTopic,
        String eventsTopic,
        String kafkaBootstrapServers,
        String schemaRegistryUrl) {

    /** Default worker loop count. */
    public static final int DEFAULT_WORKER_COUNT = 2;
    /** Default claim lease: five minutes (LLM-step scale). */
    public static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(5);
    /** Default idle backoff for the worker loops. */
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(500);
    /** Default retry backoff base. */
    public static final long DEFAULT_BACKOFF_BASE_SECONDS = 5;
    /** Default retry ceiling for new jobs. */
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    /** Default per-target concurrency cap. */
    public static final int DEFAULT_MAX_CONCURRENT_PER_TARGET = 8;
    /** Default lifecycle-events topic. */
    public static final String DEFAULT_EVENTS_TOPIC = "workflow-run-events";
    /** The request-topic consumer's group id. */
    public static final String CONSUMER_GROUP = "protomolt-jobs-worker";

    public WorkflowRunsConfig {
        Objects.requireNonNull(workerId, "workerId");
        if (workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank: it stamps "
                    + "lease_owner on claimed rows — an anonymous worker fleet cannot "
                    + "be debugged from the row");
        }
        if (workerCount <= 0) {
            workerCount = DEFAULT_WORKER_COUNT;
        }
        if (leaseDuration == null || leaseDuration.isNegative() || leaseDuration.isZero()) {
            leaseDuration = DEFAULT_LEASE_DURATION;
        }
        if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
            pollInterval = DEFAULT_POLL_INTERVAL;
        }
        if (backoffBaseSeconds <= 0) {
            backoffBaseSeconds = DEFAULT_BACKOFF_BASE_SECONDS;
        }
        if (maxAttemptsDefault <= 0) {
            maxAttemptsDefault = DEFAULT_MAX_ATTEMPTS;
        }
        if (maxConcurrentPerTarget <= 0) {
            maxConcurrentPerTarget = DEFAULT_MAX_CONCURRENT_PER_TARGET;
        }
        if (eventsTopic == null || eventsTopic.isBlank()) {
            eventsTopic = DEFAULT_EVENTS_TOPIC;
        }
        if (requestTopic != null && (kafkaBootstrapServers == null
                || kafkaBootstrapServers.isBlank())) {
            throw new IllegalArgumentException("requestTopic is set but no Kafka bootstrap "
                    + "servers are configured — broker-native submission needs a broker");
        }
    }
}
