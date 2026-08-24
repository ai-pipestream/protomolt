package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.jobs.service.TestWorkflows;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.worker.WorkflowRunWorker;
import ai.pipestream.proto.workflow.WorkflowRunner;
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
        return dispatch(submit, request);
    }

    @Test
    void aNullStoreAnswersUnavailableFromEveryVerb() {
        SubmitWorkflowAction noSubmit = new SubmitWorkflowAction(null, null, 3);
        GetJobAction noGet = new GetJobAction(null);
        ListJobsAction noList = new ListJobsAction(null);
        CompleteStepAction noComplete = new CompleteStepAction(null);
        // Requests the contract accepts, so each verb is reached and answers for itself:
        // the catalog checks the request before dispatch, and an unavailable node has
        // nothing to say about a request that was never valid.
        ObjectNode request = MAPPER.createObjectNode();
        request.put("workflowName", "embed-text");
        request.putObject("input");
        assertThatThrownBy(() -> dispatch(noSubmit, request))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("unavailable");
                    assertThat(e.getMessage()).isEqualTo(ActionSupport.UNAVAILABLE_MESSAGE);
                });
        ObjectNode byId = MAPPER.createObjectNode();
        byId.put("jobId", java.util.UUID.randomUUID().toString());
        assertThatThrownBy(() -> dispatch(noGet, byId))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unavailable"));
        assertThatThrownBy(() -> dispatch(noList, MAPPER.createObjectNode()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unavailable"));
        ObjectNode step = byId.deepCopy();
        step.put("stepName", "review");
        step.putObject("response");
        assertThatThrownBy(() -> dispatch(noComplete, step))
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

        ObjectNode first = dispatch(submit, request);
        assertThat(first.get("ok").asBoolean()).isTrue();
        assertThat(first.get("jobId").asText()).isEqualTo(jobId);
        assertThat(first.get("status").asText()).isEqualTo("QUEUED");
        assertThat(store.events()).hasSize(1);

        // The resubmit returns the existing row and writes nothing.
        ObjectNode second = dispatch(submit, request);
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
        ObjectNode result = dispatch(submit, broken);
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("failedStep").asText()).isEqualTo("embed");
        assertThat(result.get("error").asText()).contains("does not verify");
        assertThat(store.list(null, null, 10, 0)).isEmpty();

        // workflowName with no repository mounted.
        ObjectNode named = MAPPER.createObjectNode();
        named.put("workflowName", "court-decoration");
        named.putObject("input").put("text", "hi");
        ObjectNode refused = dispatch(submit, named);
        assertThat(refused.get("ok").asBoolean()).isFalse();
        assertThat(refused.get("error").asText()).contains("No workflow repository is mounted");
    }

    @Test
    void getJobAnswersTheFullRowAndMissesCleanly() throws Exception {
        String jobId = submitTwoStep("hi", false).get("jobId").asText();

        ObjectNode found = dispatch(getJob, envelope("{\"jobId\": \"" + jobId + "\"}"));
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

        ObjectNode missing = dispatch(getJob, envelope("{\"jobId\": \"" + UUID.randomUUID() + "\"}"));
        assertThat(missing.get("ok").asBoolean()).isFalse();
        assertThat(missing.get("error").asText()).contains("no workflow run");

        assertThatThrownBy(() -> dispatch(getJob, envelope("{\"jobId\": \"not-a-uuid\"}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void listJobsFiltersPagesAndValidatesStatus() throws Exception {
        submitTwoStep("one", false);
        submitTwoStep("two", false);

        ObjectNode all = dispatch(listJobs, MAPPER.createObjectNode());
        assertThat(all.get("ok").asBoolean()).isTrue();
        assertThat(all.get("jobs")).hasSize(2);
        JsonNode summary = all.get("jobs").get(0);
        // Summaries carry no input/checkpoints/result — get-job owns those. input and
        // result carry presence, so they are absent; checkpoints is repeated and has none.
        assertThat(summary.has("input")).isFalse();
        assertThat(summary.get("checkpoints")).isEmpty();
        assertThat(summary.has("result")).isFalse();
        assertThat(summary.get("status").asText()).isEqualTo("QUEUED");

        ObjectNode filtered = dispatch(listJobs, envelope("{\"status\": \"JOB_STATUS_COMPLETED\"}"));
        assertThat(filtered.get("jobs")).isEmpty();

        // The limit clamps to the ceiling instead of failing.
        ObjectNode clamped = dispatch(listJobs, envelope("{\"limit\": 100000}"));
        assertThat(clamped.get("ok").asBoolean()).isTrue();

        assertThatThrownBy(() -> dispatch(listJobs, envelope("{\"status\": \"SLEEPING\"}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void completeStepResumesAParkedJobAndIsIdempotent() throws Exception {
        // Park a three-step job on its external review step.
        ObjectNode request = MAPPER.createObjectNode();
        request.set("workflow", workflows.threeStepWorkflow("in-process"));
        request.putObject("input").put("text", "hi");
        String jobId = dispatch(submit, request).get("jobId").asText();
        assertThat(worker.workOnce()).isTrue();
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);

        // Wrong step name: fail fast with the state, nothing mutates.
        ObjectNode wrong = dispatch(completeStep, envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"embed\","
                        + " \"response\": {}}"));
        assertThat(wrong.get("ok").asBoolean()).isFalse();
        assertThat(wrong.get("status").asText()).isEqualTo("WAITING");
        assertThat(wrong.get("outstandingStep").asText()).isEqualTo("review");

        // A malformed response is the caller's error; the job stays parked.
        assertThatThrownBy(() -> dispatch(completeStep, envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"ok\", \"score\": \"not-a-number\"}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);

        // The valid completion: checkpointed and requeued.
        ObjectNode done = dispatch(completeStep, envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"ship it\"}}"));
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
        ObjectNode again = dispatch(completeStep, envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"ship it\"}}"));
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
        ObjectNode result = dispatch(completeStep, envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {}}"));
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("status").asText()).isEqualTo("QUEUED");
        assertThat(result.get("error").asText()).contains("not waiting on step 'review'");
    }

    /**
     * Dispatches the way every surface does: through a catalog holding the verb, which is
     * where the request contract is checked before the verb runs.
     */
    private static ObjectNode dispatch(ProtoAction verb, ObjectNode input)
            throws ActionException {
        return ActionCatalog.defaults(ActionContext.create())
                .replace(verb).execute(verb.name(), input);
    }

}
