package ai.pipestream.proto.jobs.service.worker;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.jobs.service.WorkflowRunSubmitter;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.jobs.service.ValidatingWorkflows;
import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The worker's VALIDATION verdict against declared response rules (the
 * {@link ValidatingWorkflows} fixture): a step whose {@code validate} flag is set
 * has its response checked against the rules the output type declares — a
 * rejection is a verdict (FAILED with the violations, no retry), not a
 * retryable error.
 */
class WorkflowRunWorkerValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    ValidatingWorkflows workflows;
    InMemoryWorkflowRunStore store;
    WorkflowRunWorker worker;
    WorkflowRunSubmitter submitter;
    ActionContext context;

    @BeforeEach
    void fresh() {
        workflows = new ValidatingWorkflows();
        store = new InMemoryWorkflowRunStore();
        context = ActionContext.create();
        WorkflowRunsConfig config = new WorkflowRunsConfig("test-worker", 1,
                Duration.ofSeconds(30), Duration.ofMillis(50), 1, 3, 4,
                null, "workflow-run-events", null, null);
        submitter = new WorkflowRunSubmitter(store, null, config.maxAttemptsDefault());
    }

    @AfterEach
    void stop() {
        workflows.stop();
    }

    /** A worker whose channels reach the fixture's in-process server. */
    private WorkflowRunWorker workerTo(String inProcessName) {
        return new WorkflowRunWorker(store, context, null,
                new WorkflowRunner(step -> InProcessChannelBuilder.forName(inProcessName).build()),
                new WorkflowRunsConfig("test-worker", 1, Duration.ofSeconds(30),
                        Duration.ofMillis(50), 1, 3, 4, null, "workflow-run-events", null, null));
    }

    private UUID submit() {
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(
                workflows.validatingTokenizeWorkflow("in-process"), null,
                jsonInput(), null, context);
        assertThat(outcome.ok()).as("submit failed: %s", outcome.error()).isTrue();
        return UUID.fromString(outcome.jobId());
    }

    private static com.fasterxml.jackson.databind.JsonNode jsonInput() {
        try {
            return MAPPER.readTree("{\"text\": \"hi\"}");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void aResponsePassingItsDeclaredRulesCompletes() throws Exception {
        String name = workflows.startTokenizer("hello");
        worker = workerTo(name);
        UUID jobId = submit();

        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(MAPPER.readTree(job.result).get("tag").asText()).isEqualTo("hello");
        assertThat(eventTypes()).containsExactly(
                WorkflowRunEventRecord.TYPE_ACCEPTED,
                WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT,
                WorkflowRunEventRecord.TYPE_COMPLETED);
    }

    @Test
    void aResponseFailingItsDeclaredRulesIsAVerdictNotARetry() throws Exception {
        String name = workflows.startTokenizer("no");
        worker = workerTo(name);
        UUID jobId = submit();

        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord job = store.get(jobId).orElseThrow();
        // VALIDATION is a verdict: FAILED with the violations, no requeue.
        assertThat(job.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
        assertThat(job.error).contains("VALIDATION").contains("response failed validation")
                .contains("tag");
        assertThat(job.attempt).isEqualTo(1);
        assertThat(job.completedAt).isNotNull();
        assertThat(eventTypes()).containsExactly(
                WorkflowRunEventRecord.TYPE_ACCEPTED, WorkflowRunEventRecord.TYPE_FAILED);

        // A verdict never requeues: nothing is claimable.
        assertThat(worker.workOnce()).isFalse();
    }

    private List<String> eventTypes() {
        return store.events().stream().map(event -> event.eventType).toList();
    }
}
