package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.jobs.service.TestWorkflows;
import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.pipestream.proto.jobs.service.worker.WorkflowRunWorker;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The four workflow-runs verbs: envelope shapes against the in-memory store
 * (submit/get/list/complete-step, including the park → complete → resume
 * flow), the validation failures, and the null-store "unavailable" answer
 * every verb gives when jobs are not configured on the server.
 */
class WorkflowRunActionsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static TestWorkflows workflows;
    static WorkflowRunner runner;
    static ActionContext context;

    InMemoryWorkflowRunStore store;
    WorkflowRunWorker worker;
    SubmitWorkflowAction submit;
    GetJobAction getJob;
    ListJobsAction listJobs;
    CompleteStepAction completeStep;

    @BeforeAll
    static void start() {
        workflows = new TestWorkflows();
        String name = workflows.startInProcess();
        runner = workflows.inProcessRunner(name);
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
        submit = new SubmitWorkflowAction(store, null, 3);
        getJob = new GetJobAction(store);
        listJobs = new ListJobsAction(store);
        completeStep = new CompleteStepAction(store);
    }

    private static ObjectNode envelope(String json) {
        try {
            return (ObjectNode) MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private ObjectNode submitTwoStep(String text, boolean fail) throws Exception {
        ObjectNode request = MAPPER.createObjectNode();
        request.set("workflow", workflows.twoStepWorkflow("in-process", null));
        request.putObject("input").put("text", text).put("fail", fail);
        return submit.execute(request, context);
    }

    @Test
    void aNullStoreAnswersUnavailableFromEveryVerb() {
        SubmitWorkflowAction noSubmit = new SubmitWorkflowAction(null, null, 3);
        GetJobAction noGet = new GetJobAction(null);
        ListJobsAction noList = new ListJobsAction(null);
        CompleteStepAction noComplete = new CompleteStepAction(null);
        ObjectNode request = MAPPER.createObjectNode();
        assertThatThrownBy(() -> noSubmit.execute(request, context))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("unavailable");
                    assertThat(e.getMessage()).isEqualTo(ActionSupport.UNAVAILABLE_MESSAGE);
                });
        assertThatThrownBy(() -> noGet.execute(request, context))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unavailable"));
        assertThatThrownBy(() -> noList.execute(request, context))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unavailable"));
        assertThatThrownBy(() -> noComplete.execute(request, context))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unavailable"));
    }

    @Test
    void submitQueuesAndIsIdempotentOnJobId() throws Exception {
        ObjectNode request = MAPPER.createObjectNode();
        request.set("workflow", workflows.twoStepWorkflow("in-process", null));
        request.putObject("input").put("text", "hi");
        String jobId = UUID.randomUUID().toString();
        request.put("jobId", jobId);

        ObjectNode first = submit.execute(request, context);
        assertThat(first.get("ok").asBoolean()).isTrue();
        assertThat(first.get("jobId").asText()).isEqualTo(jobId);
        assertThat(first.get("status").asText()).isEqualTo("QUEUED");
        assertThat(store.events()).hasSize(1);

        // The resubmit returns the existing row and writes nothing.
        ObjectNode second = submit.execute(request, context);
        assertThat(second.get("ok").asBoolean()).isTrue();
        assertThat(second.get("jobId").asText()).isEqualTo(jobId);
        assertThat(store.events()).hasSize(1);
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().workflowName)
                .isEqualTo("embed-text");
    }

    @Test
    void submitRejectsAnUnverifiableWorkflowAndAnUnresolvableName() throws Exception {
        ObjectNode broken = MAPPER.createObjectNode();
        broken.set("workflow", workflows.brokenWorkflow("in-process"));
        broken.putObject("input").put("text", "hi");
        ObjectNode result = submit.execute(broken, context);
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("failedStep").asText()).isEqualTo("embed");
        assertThat(result.get("error").asText()).contains("does not verify");
        assertThat(store.list(null, null, 10, 0)).isEmpty();

        // workflowName with no repository mounted.
        ObjectNode named = MAPPER.createObjectNode();
        named.put("workflowName", "court-decoration");
        named.putObject("input").put("text", "hi");
        ObjectNode refused = submit.execute(named, context);
        assertThat(refused.get("ok").asBoolean()).isFalse();
        assertThat(refused.get("error").asText()).contains("No workflow repository is mounted");
    }

    @Test
    void getJobAnswersTheFullRowAndMissesCleanly() throws Exception {
        String jobId = submitTwoStep("hi", false).get("jobId").asText();

        ObjectNode found = getJob.execute(envelope("{\"jobId\": \"" + jobId + "\"}"), context);
        assertThat(found.get("ok").asBoolean()).isTrue();
        JsonNode job = found.get("job");
        assertThat(job.get("jobId").asText()).isEqualTo(jobId);
        assertThat(job.get("workflowName").asText()).isEqualTo("embed-text");
        assertThat(job.get("status").asText()).isEqualTo("QUEUED");
        assertThat(job.get("attempt").asInt()).isZero();
        assertThat(job.get("input").get("text").asText()).isEqualTo("hi");
        assertThat(job.get("checkpoints")).isEmpty();
        assertThat(job.has("result")).isFalse();
        assertThat(job.get("createdAt").asText()).isNotBlank();

        ObjectNode missing = getJob.execute(
                envelope("{\"jobId\": \"" + UUID.randomUUID() + "\"}"), context);
        assertThat(missing.get("ok").asBoolean()).isFalse();
        assertThat(missing.get("error").asText()).contains("no workflow run");

        assertThatThrownBy(() -> getJob.execute(envelope("{\"jobId\": \"not-a-uuid\"}"), context))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void listJobsFiltersPagesAndValidatesStatus() throws Exception {
        submitTwoStep("one", false);
        submitTwoStep("two", false);

        ObjectNode all = listJobs.execute(MAPPER.createObjectNode(), context);
        assertThat(all.get("ok").asBoolean()).isTrue();
        assertThat(all.get("jobs")).hasSize(2);
        JsonNode summary = all.get("jobs").get(0);
        // Summaries carry no input/checkpoints/result — get-job owns those.
        assertThat(summary.has("input")).isFalse();
        assertThat(summary.has("checkpoints")).isFalse();
        assertThat(summary.has("result")).isFalse();
        assertThat(summary.get("status").asText()).isEqualTo("QUEUED");

        ObjectNode filtered = listJobs.execute(
                envelope("{\"status\": \"JOB_STATUS_COMPLETED\"}"), context);
        assertThat(filtered.get("jobs")).isEmpty();

        // The limit clamps to the ceiling instead of failing.
        ObjectNode clamped = listJobs.execute(envelope("{\"limit\": 100000}"), context);
        assertThat(clamped.get("ok").asBoolean()).isTrue();

        assertThatThrownBy(() -> listJobs.execute(
                envelope("{\"status\": \"SLEEPING\"}"), context))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void completeStepResumesAParkedJobAndIsIdempotent() throws Exception {
        // Park a three-step job on its external review step.
        ObjectNode request = MAPPER.createObjectNode();
        request.set("workflow", workflows.threeStepWorkflow("in-process"));
        request.putObject("input").put("text", "hi");
        String jobId = submit.execute(request, context).get("jobId").asText();
        assertThat(worker.workOnce()).isTrue();
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);

        // Wrong step name: fail fast with the state, nothing mutates.
        ObjectNode wrong = completeStep.execute(envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"embed\","
                        + " \"response\": {}}"), context);
        assertThat(wrong.get("ok").asBoolean()).isFalse();
        assertThat(wrong.get("status").asText()).isEqualTo("WAITING");
        assertThat(wrong.get("outstandingStep").asText()).isEqualTo("review");

        // A malformed response is the caller's error; the job stays parked.
        assertThatThrownBy(() -> completeStep.execute(envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"ok\", \"score\": \"not-a-number\"}}"),
                context))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);

        // The valid completion: checkpointed and requeued.
        ObjectNode done = completeStep.execute(envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"ship it\"}}"), context);
        assertThat(done.get("ok").asBoolean()).isTrue();
        assertThat(done.get("status").asText()).isEqualTo("QUEUED");
        WorkflowRunRecord job = store.get(UUID.fromString(jobId)).orElseThrow();
        JsonNode checkpoints = MAPPER.readTree(job.checkpoints);
        assertThat(checkpoints).hasSize(2);
        assertThat(checkpoints.get(1).get("response").get("notes").asText())
                .isEqualTo("ship it");
        assertThat(store.events().stream().map(e -> e.eventType))
                .contains(WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT);

        // The redelivery answers the current status and changes nothing.
        ObjectNode again = completeStep.execute(envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"ship it\"}}"), context);
        assertThat(again.get("ok").asBoolean()).isTrue();
        assertThat(again.get("status").asText()).isEqualTo("QUEUED");
        assertThat(MAPPER.readTree(
                store.get(UUID.fromString(jobId)).orElseThrow().checkpoints)).hasSize(2);

        // And the resumed job completes with the review's notes.
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord finished = store.get(UUID.fromString(jobId)).orElseThrow();
        assertThat(finished.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(MAPPER.readTree(finished.result).get("notes").asText())
                .isEqualTo("ship it");
    }

    @Test
    void completeStepOnANonWaitingJobAnswersWrongState() throws Exception {
        String jobId = submitTwoStep("hi", false).get("jobId").asText();
        ObjectNode result = completeStep.execute(envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {}}"), context);
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("status").asText()).isEqualTo("QUEUED");
        assertThat(result.get("error").asText()).contains("not waiting on step 'review'");
    }
}
