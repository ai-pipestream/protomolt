package ai.pipestream.proto.jobs.service.worker;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.jobs.service.TestWorkflows;
import ai.pipestream.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
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
 * unreadable or unverifiable workflow JSON, an input that no longer parses
 * against the workflow's inputType, and a stored checkpoint prefix that does not
 * match the definition. Every one lands FAILED with a WORKFLOW/MAPPING detail
 * and a FAILED event — deterministic corruption is never retried. The rows
 * are inserted directly (submit would refuse most of them), and none of them
 * reaches a gRPC call.
 */
class WorkflowRunWorkerCorruptionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static TestWorkflows workflows;
    static String inProcessName;
    static WorkflowRunner runner;
    static ActionContext context;

    InMemoryWorkflowRunStore store;
    WorkflowRunWorker worker;

    @BeforeAll
    static void start() {
        workflows = new TestWorkflows();
        inProcessName = workflows.startInProcess();
        runner = workflows.inProcessRunner(inProcessName);
        context = ActionContext.create();
    }

    @AfterAll
    static void stop() {
        workflows.stop();
    }

    @BeforeEach
    void fresh() {
        store = new InMemoryWorkflowRunStore();
        worker = new WorkflowRunWorker(store, context, null, runner,
                new WorkflowRunsConfig("test-worker", 1, Duration.ofSeconds(30),
                        Duration.ofMillis(50), 1, 3, 4, null, "workflow-run-events", null, null));
    }

    /** Insert a QUEUED row straight into the store and return its id. */
    private UUID insert(String workflowDefinition, String input, String checkpoints) {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = UUID.randomUUID();
        record.workflowName = "embed-text";
        record.workflowDefinition = workflowDefinition;
        record.input = input;
        record.checkpoints = checkpoints;
        record.maxAttempts = 3;
        record.runAfter = Instant.now();
        store.insert(record, WorkflowRunEventFactory.accepted(record));
        return record.jobId;
    }

    /** Claim the row, let the worker settle it, and answer the stored row. */
    private WorkflowRunRecord runOnce(UUID jobId) {
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
        assertThat(job.completedAt).isNotNull();
        assertThat(store.events().stream().map(e -> e.eventType))
                .containsExactly(WorkflowRunEventRecord.TYPE_ACCEPTED,
                        WorkflowRunEventRecord.TYPE_FAILED);
        return job;
    }

    @Test
    void anUnreadableCompiledWorkflowFailsLoud() {
        UUID jobId = insert("{not json", "{\"text\": \"hi\"}", "[]");
        assertThat(runOnce(jobId).error)
                .startsWith("WORKFLOW: stored workflow definition is not readable JSON:");
    }

    @Test
    void aNonObjectCompiledWorkflowFailsLoud() {
        UUID jobId = insert("[1, 2, 3]", "{\"text\": \"hi\"}", "[]");
        assertThat(runOnce(jobId).error)
                .isEqualTo("WORKFLOW: stored workflow definition is not a JSON object");
    }

    @Test
    void aWorkflowThatDoesNotParseFailsLoudWithTheStep() {
        ObjectNode broken = workflows.twoStepWorkflow("in-process", null);
        ((ObjectNode) broken.get("steps").get(1)).remove("method");
        UUID jobId = insert(broken.toString(), "{\"text\": \"hi\"}", "[]");
        WorkflowRunRecord job = runOnce(jobId);
        assertThat(job.error).startsWith("WORKFLOW: stored workflow does not parse (step 'embed')")
                .contains("'target' and 'method'");
    }

    @Test
    void anInputThatNoLongerParsesFailsLoud() {
        // An object for a string field: unknown fields and scalar coercions
        // are tolerated by design, a nested object is not.
        UUID jobId = insert(workflows.twoStepWorkflow("in-process", null).toString(),
                "{\"text\": {\"nested\": true}}", "[]");
        assertThat(runOnce(jobId).error).startsWith(
                "MAPPING: stored input is not valid proto3 JSON for jobs.test.Text:");
    }

    @Test
    void checkpointsThatAreNotAnArrayFailLoud() {
        UUID jobId = insert(workflows.twoStepWorkflow("in-process", null).toString(),
                "{\"text\": \"hi\"}", "{}");
        assertThat(runOnce(jobId).error)
                .isEqualTo("WORKFLOW: stored checkpoints are not an array");
    }

    @Test
    void checkpointsOutnumberingTheStepsFailLoud() {
        String checkpoints = "[{\"name\": \"a\"}, {\"name\": \"b\"}, {\"name\": \"c\"}]";
        UUID jobId = insert(workflows.twoStepWorkflow("in-process", null).toString(),
                "{\"text\": \"hi\"}", checkpoints);
        assertThat(runOnce(jobId).error).isEqualTo(
                "WORKFLOW: stored checkpoints (3) outnumber the workflow's steps (2)");
    }

    @Test
    void aCheckpointNamingTheWrongStepFailsLoud() {
        // Entry 0 claims to be 'embed' but the workflow's step 0 is 'tokenize'.
        String checkpoints = "[{\"name\": \"embed\", \"skipped\": false,"
                + " \"response\": {\"values\": [1.0]}}]";
        UUID jobId = insert(workflows.twoStepWorkflow("in-process", null).toString(),
                "{\"text\": \"hi\"}", checkpoints);
        assertThat(runOnce(jobId).error).isEqualTo(
                "WORKFLOW: checkpoint 0 belongs to step 'embed' but the workflow's step 0 is"
                        + " 'tokenize'; the workflow definition changed under the job");
    }

    @Test
    void aCheckpointWithAnUndecodableResponseFailsLoud() {
        String checkpoints = "[{\"name\": \"tokenize\", \"skipped\": false,"
                + " \"response\": {\"ids\": \"not-an-array\"}}]";
        UUID jobId = insert(workflows.twoStepWorkflow("in-process", null).toString(),
                "{\"text\": \"hi\"}", checkpoints);
        assertThat(runOnce(jobId).error).startsWith(
                "WORKFLOW: checkpoint 'tokenize' response is not valid proto3 JSON for"
                        + " jobs.test.Tokens:");
    }

    @Test
    void aSkippedCheckpointResumesWithoutAResponse() throws Exception {
        // A gate-skipped checkpoint entry (no response) is legitimate: the
        // rebuild binds it and the next step runs.
        String checkpoints = "[{\"name\": \"tokenize\", \"skipped\": true}]";
        UUID jobId = insert(workflows.twoStepWorkflow("in-process", null).toString(),
                "{\"text\": \"hi\"}", checkpoints);

        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(MAPPER.readTree(job.checkpoints)).hasSize(2);
    }
}
