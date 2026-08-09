package ai.pipestream.proto.jobs.service.worker;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainRunner;
import ai.pipestream.proto.jobs.service.ChainJobsConfig;
import ai.pipestream.proto.jobs.service.TestChains;
import ai.pipestream.proto.jobs.service.events.ChainJobEventFactory;
import ai.pipestream.proto.jobs.service.store.ChainJobEventRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryChainJobStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The worker's fail-loud paths for a row whose snapshotted state is corrupt:
 * unreadable or unverifiable chain JSON, an input that no longer parses
 * against the chain's inputType, and a stored checkpoint prefix that does not
 * match the definition. Every one lands FAILED with a CHAIN/MAPPING detail
 * and a FAILED event — deterministic corruption is never retried. The rows
 * are inserted directly (submit would refuse most of them), and none of them
 * reaches a gRPC call.
 */
class ChainJobWorkerCorruptionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static TestChains chains;
    static String inProcessName;
    static ChainRunner runner;
    static ActionContext context;

    InMemoryChainJobStore store;
    ChainJobWorker worker;

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
        worker = new ChainJobWorker(store, context, null, runner,
                new ChainJobsConfig("test-worker", 1, Duration.ofSeconds(30),
                        Duration.ofMillis(50), 1, 3, 4, null, "chain-job-events", null, null));
    }

    /** Insert a QUEUED row straight into the store and return its id. */
    private UUID insert(String chainDefinition, String input, String checkpoints) {
        ChainJobRecord record = new ChainJobRecord();
        record.jobId = UUID.randomUUID();
        record.chainName = "embed-text";
        record.chainDefinition = chainDefinition;
        record.input = input;
        record.checkpoints = checkpoints;
        record.maxAttempts = 3;
        record.runAfter = Instant.now();
        store.insert(record, ChainJobEventFactory.accepted(record));
        return record.jobId;
    }

    /** Claim the row, let the worker settle it, and answer the stored row. */
    private ChainJobRecord runOnce(UUID jobId) {
        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(ChainJobRecord.STATUS_FAILED);
        assertThat(job.completedAt).isNotNull();
        assertThat(store.events().stream().map(e -> e.eventType))
                .containsExactly(ChainJobEventRecord.TYPE_ACCEPTED,
                        ChainJobEventRecord.TYPE_FAILED);
        return job;
    }

    @Test
    void anUnreadableChainDefinitionFailsLoud() {
        UUID jobId = insert("{not json", "{\"text\": \"hi\"}", "[]");
        assertThat(runOnce(jobId).error)
                .startsWith("CHAIN: stored chain definition is not readable JSON:");
    }

    @Test
    void aNonObjectChainDefinitionFailsLoud() {
        UUID jobId = insert("[1, 2, 3]", "{\"text\": \"hi\"}", "[]");
        assertThat(runOnce(jobId).error)
                .isEqualTo("CHAIN: stored chain definition is not a JSON object");
    }

    @Test
    void aChainThatDoesNotParseFailsLoudWithTheStep() {
        ObjectNode broken = chains.twoStepChain("in-process", null);
        ((ObjectNode) broken.get("steps").get(1)).remove("method");
        UUID jobId = insert(broken.toString(), "{\"text\": \"hi\"}", "[]");
        ChainJobRecord job = runOnce(jobId);
        assertThat(job.error).startsWith("CHAIN: stored chain does not parse (step 'embed')")
                .contains("'target' and 'method'");
    }

    @Test
    void anInputThatNoLongerParsesFailsLoud() {
        // An object for a string field: unknown fields and scalar coercions
        // are tolerated by design, a nested object is not.
        UUID jobId = insert(chains.twoStepChain("in-process", null).toString(),
                "{\"text\": {\"nested\": true}}", "[]");
        assertThat(runOnce(jobId).error).startsWith(
                "MAPPING: stored input is not valid proto3 JSON for jobs.test.Text:");
    }

    @Test
    void checkpointsThatAreNotAnArrayFailLoud() {
        UUID jobId = insert(chains.twoStepChain("in-process", null).toString(),
                "{\"text\": \"hi\"}", "{}");
        assertThat(runOnce(jobId).error)
                .isEqualTo("CHAIN: stored checkpoints are not an array");
    }

    @Test
    void checkpointsOutnumberingTheStepsFailLoud() {
        String checkpoints = "[{\"name\": \"a\"}, {\"name\": \"b\"}, {\"name\": \"c\"}]";
        UUID jobId = insert(chains.twoStepChain("in-process", null).toString(),
                "{\"text\": \"hi\"}", checkpoints);
        assertThat(runOnce(jobId).error).isEqualTo(
                "CHAIN: stored checkpoints (3) outnumber the chain's steps (2)");
    }

    @Test
    void aCheckpointNamingTheWrongStepFailsLoud() {
        // Entry 0 claims to be 'embed' but the chain's step 0 is 'tokenize'.
        String checkpoints = "[{\"name\": \"embed\", \"skipped\": false,"
                + " \"response\": {\"values\": [1.0]}}]";
        UUID jobId = insert(chains.twoStepChain("in-process", null).toString(),
                "{\"text\": \"hi\"}", checkpoints);
        assertThat(runOnce(jobId).error).isEqualTo(
                "CHAIN: checkpoint 0 belongs to step 'embed' but the chain's step 0 is"
                        + " 'tokenize'; the chain definition changed under the job");
    }

    @Test
    void aCheckpointWithAnUndecodableResponseFailsLoud() {
        String checkpoints = "[{\"name\": \"tokenize\", \"skipped\": false,"
                + " \"response\": {\"ids\": \"not-an-array\"}}]";
        UUID jobId = insert(chains.twoStepChain("in-process", null).toString(),
                "{\"text\": \"hi\"}", checkpoints);
        assertThat(runOnce(jobId).error).startsWith(
                "CHAIN: checkpoint 'tokenize' response is not valid proto3 JSON for"
                        + " jobs.test.Tokens:");
    }

    @Test
    void aSkippedCheckpointResumesWithoutAResponse() throws Exception {
        // A gate-skipped checkpoint entry (no response) is legitimate: the
        // rebuild binds it and the next step runs.
        String checkpoints = "[{\"name\": \"tokenize\", \"skipped\": true}]";
        UUID jobId = insert(chains.twoStepChain("in-process", null).toString(),
                "{\"text\": \"hi\"}", checkpoints);

        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(ChainJobRecord.STATUS_COMPLETED);
        assertThat(MAPPER.readTree(job.checkpoints)).hasSize(2);
    }
}
