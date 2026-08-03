package ai.pipestream.proto.jobs.service.worker;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainRunner;
import ai.pipestream.proto.jobs.service.ChainJobSubmitter;
import ai.pipestream.proto.jobs.service.ChainJobsConfig;
import ai.pipestream.proto.jobs.service.TestChains;
import ai.pipestream.proto.jobs.service.events.ChainJobEventFactory;
import ai.pipestream.proto.jobs.service.store.ChainJobEventRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryChainJobStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The worker's execute loop end to end against live in-process gRPC
 * services and the in-memory store: the complete cycle, the park/resume
 * cycle over an external step, the retry → dead-letter path, the
 * verifier-rejection fail-loud path, gate-skip checkpointing, and the
 * checkpoint-observer store hiccup (a retryable CHAIN-kind failure — the
 * step's response may not have persisted, so the job requeues instead of
 * settling).
 */
class ChainJobWorkerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static TestChains chains;
    static String inProcessName;
    static ChainRunner runner;
    static ActionContext context;

    InMemoryChainJobStore store;
    ChainJobWorker worker;
    ChainJobSubmitter submitter;

    @BeforeAll
    static void start() {
        chains = new TestChains();
        inProcessName = chains.startInProcess();
        runner = chains.inProcessRunner(inProcessName);
        context = ActionContext.create();
    }

    @AfterAll
    static void stop() {
        chains.stop();
    }

    @BeforeEach
    void fresh() {
        store = new InMemoryChainJobStore();
        ChainJobsConfig config = new ChainJobsConfig("test-worker", 1,
                Duration.ofSeconds(30), Duration.ofMillis(50), 1, 2, 4,
                null, "chain-job-events", null, null);
        worker = new ChainJobWorker(store, context, null, runner, config);
        submitter = new ChainJobSubmitter(store, null, config.maxAttemptsDefault());
    }

    private static JsonNode input(String text, boolean fail) {
        try {
            return MAPPER.readTree(
                    "{\"text\": \"" + text + "\", \"fail\": " + fail + "}");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private UUID submit(com.fasterxml.jackson.databind.node.ObjectNode chain, JsonNode input) {
        ChainJobSubmitter.Outcome outcome = submitter.submit(chain, null, input, null, context);
        assertThat(outcome.ok()).as("submit failed: %s", outcome.error()).isTrue();
        return UUID.fromString(outcome.jobId());
    }

    private static List<String> eventTypes(InMemoryChainJobStore store) {
        return store.events().stream().map(event -> event.eventType).toList();
    }

    @Test
    void aTwoStepChainRunsToCompletionWithPerStepCheckpoints() throws Exception {
        UUID jobId = submit(chains.twoStepChain("in-process", null), input("hi", false));

        assertThat(worker.workOnce()).isTrue();
        assertThat(worker.workOnce()).isFalse();

        ChainJobRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(ChainJobRecord.STATUS_COMPLETED);
        assertThat(job.attempt).isEqualTo(1);
        assertThat(job.leaseOwner).isNull();
        assertThat(job.completedAt).isNotNull();
        assertThat(job.verdict).isEqualTo("2 steps, output jobs.test.Embedding");

        JsonNode checkpoints = MAPPER.readTree(job.checkpoints);
        assertThat(checkpoints).hasSize(2);
        assertThat(checkpoints.get(0).get("name").asText()).isEqualTo("tokenize");
        assertThat(checkpoints.get(0).get("skipped").asBoolean()).isFalse();
        assertThat(checkpoints.get(0).get("response").get("ids")).hasSize(2);
        assertThat(checkpoints.get(1).get("name").asText()).isEqualTo("embed");

        JsonNode result = MAPPER.readTree(job.result);
        assertThat(result.get("sourceText").asText()).isEqualTo("hi");
        assertThat(result.get("vector")).hasSize(2);

        assertThat(eventTypes(store)).containsExactly(
                ChainJobEventRecord.TYPE_ACCEPTED,
                ChainJobEventRecord.TYPE_STEP_CHECKPOINT,
                ChainJobEventRecord.TYPE_STEP_CHECKPOINT,
                ChainJobEventRecord.TYPE_COMPLETED);
    }

    @Test
    void anExternalStepParksTheJobAndCompleteStepResumesIt() throws Exception {
        UUID jobId = submit(chains.threeStepChain("in-process"), input("hi", false));

        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord parked = store.get(jobId).orElseThrow();
        assertThat(parked.status).isEqualTo(ChainJobRecord.STATUS_WAITING);
        assertThat(parked.outstandingStep).isEqualTo("review");
        assertThat(MAPPER.readTree(parked.checkpoints)).hasSize(1);
        assertThat(eventTypes(store)).containsExactly(
                ChainJobEventRecord.TYPE_ACCEPTED,
                ChainJobEventRecord.TYPE_STEP_CHECKPOINT,
                ChainJobEventRecord.TYPE_WAITING);

        // The human-in-the-loop lane supplies the review's response.
        ObjectMapper mapper = MAPPER;
        var entry = mapper.createObjectNode();
        entry.put("name", "review");
        entry.put("skipped", false);
        entry.putObject("response").put("notes", "looks good");
        assertThat(store.completeParkedStep(jobId, "review", entry.toString(),
                ChainJobEventFactory.stepCheckpoint(parked, "review")))
                .isInstanceOf(ai.pipestream.proto.jobs.service.store.ParkedCompletion.Completed.class);

        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord done = store.get(jobId).orElseThrow();
        assertThat(done.status).isEqualTo(ChainJobRecord.STATUS_COMPLETED);
        JsonNode checkpoints = MAPPER.readTree(done.checkpoints);
        assertThat(checkpoints).hasSize(3);
        assertThat(checkpoints.get(1).get("name").asText()).isEqualTo("review");
        assertThat(checkpoints.get(1).get("response").get("notes").asText())
                .isEqualTo("looks good");
        JsonNode result = MAPPER.readTree(done.result);
        assertThat(result.get("notes").asText()).isEqualTo("looks good");
        assertThat(done.verdict).isEqualTo("3 steps, output jobs.test.Embedding");
    }

    @Test
    void aRetryableFailureRequeuesWithBackoffAndThenDeadLetters() throws Exception {
        UUID jobId = submit(chains.twoStepChain("in-process", null), input("hi", true));

        // Attempt 1: UNAVAILABLE is retryable; attempt (1) < max (2) → requeue.
        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord requeued = store.get(jobId).orElseThrow();
        assertThat(requeued.status).isEqualTo(ChainJobRecord.STATUS_QUEUED);
        assertThat(requeued.attempt).isEqualTo(1);
        assertThat(requeued.leaseOwner).isNull();
        assertThat(requeued.runAfter).isAfter(java.time.Instant.now());

        // The backoff gate: not claimable until run_after passes.
        assertThat(worker.workOnce()).isFalse();
        Thread.sleep(1100);

        // Attempt 2: retryable but exhausted → DEAD with the error verbatim.
        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord dead = store.get(jobId).orElseThrow();
        assertThat(dead.status).isEqualTo(ChainJobRecord.STATUS_DEAD);
        assertThat(dead.attempt).isEqualTo(2);
        assertThat(dead.error).contains("GRPC").contains("UNAVAILABLE")
                .contains("model loading");
        assertThat(dead.completedAt).isNotNull();
        assertThat(eventTypes(store)).contains(ChainJobEventRecord.TYPE_DEAD);
    }

    @Test
    void aStoredChainThatDoesNotVerifyFailsLoud() {
        // Inserted directly (submit would refuse it): the worker re-verifies
        // the snapshotted definition and fails the job with the findings.
        ChainJobRecord record = new ChainJobRecord();
        record.jobId = UUID.randomUUID();
        record.chainName = "embed-text";
        record.chainDefinition = chains.brokenChain("in-process").toString();
        record.input = "{\"text\": \"hi\"}";
        record.maxAttempts = 3;
        record.runAfter = java.time.Instant.now();
        store.insert(record, ChainJobEventFactory.accepted(record));

        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord failed = store.get(record.jobId).orElseThrow();
        assertThat(failed.status).isEqualTo(ChainJobRecord.STATUS_FAILED);
        assertThat(failed.error).contains("does not verify").contains("typo");
        assertThat(eventTypes(store)).containsExactly(
                ChainJobEventRecord.TYPE_ACCEPTED, ChainJobEventRecord.TYPE_FAILED);
    }

    @Test
    void aGateSkippedStepCheckpointsAsSkippedAndTheChainCompletes() throws Exception {
        UUID jobId = submit(chains.twoStepChain("in-process", "!input.fail"),
                input("hi", true));

        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(ChainJobRecord.STATUS_COMPLETED);
        JsonNode checkpoints = MAPPER.readTree(job.checkpoints);
        assertThat(checkpoints).hasSize(2);
        assertThat(checkpoints.get(1).get("name").asText()).isEqualTo("embed");
        assertThat(checkpoints.get(1).get("skipped").asBoolean()).isTrue();
        assertThat(checkpoints.get(1).has("response")).isFalse();
        // The skipped step binds defaults: an empty vector, not an error.
        assertThat(MAPPER.readTree(job.result).get("vector")).isEmpty();
    }

    @Test
    void aCheckpointStoreHiccupRequeuesInsteadOfFailingTheWork() throws Exception {
        UUID jobId = submit(chains.twoStepChain("in-process", null), input("hi", false));
        store.failNextCheckpoint = true;

        // The checkpoint write fails inside the runner's observer: a
        // CHAIN-kind failure whose cause is the store — retryable, because
        // the step's response may not have persisted.
        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord requeued = store.get(jobId).orElseThrow();
        assertThat(requeued.status).isEqualTo(ChainJobRecord.STATUS_QUEUED);
        assertThat(requeued.attempt).isEqualTo(1);
        assertThat(eventTypes(store)).containsExactly(ChainJobEventRecord.TYPE_ACCEPTED);

        Thread.sleep(1100);
        // The next attempt re-executes the un-checkpointed step and completes.
        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord done = store.get(jobId).orElseThrow();
        assertThat(done.status).isEqualTo(ChainJobRecord.STATUS_COMPLETED);
        assertThat(done.attempt).isEqualTo(2);
        assertThat(MAPPER.readTree(done.checkpoints)).hasSize(2);
    }
}
