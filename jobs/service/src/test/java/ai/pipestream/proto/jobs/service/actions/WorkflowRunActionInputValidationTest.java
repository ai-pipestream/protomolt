package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.jobs.service.TestWorkflows;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The four verbs' envelope validation against the in-memory store: missing,
 * mistyped, and out-of-range fields fail with {@code invalid-input} before
 * anything touches the store, and the filters that are accepted actually
 * reach the query.
 */
class WorkflowRunActionInputValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static TestWorkflows workflows;
    static ActionContext context;

    InMemoryWorkflowRunStore store;
    SubmitWorkflowAction submit;
    GetJobAction getJob;
    ListJobsAction listJobs;
    CompleteStepAction completeStep;

    @BeforeAll
    static void compileFixture() {
        workflows = new TestWorkflows();
        context = ActionContext.create();
    }

    @BeforeEach
    void fresh() {
        store = new InMemoryWorkflowRunStore();
        submit = new SubmitWorkflowAction(store, null, 3);
        getJob = new GetJobAction(store);
        listJobs = new ListJobsAction(store);
        completeStep = new CompleteStepAction(store);
    }

    /**
     * Dispatches through a catalog, which is where the envelope is checked against the
     * verb's request message. Calling a verb's execute directly is below that boundary and
     * would test the handler rather than the contract.
     */
    private ObjectNode dispatch(String verb, ObjectNode input) throws ActionException {
        return ActionCatalog.defaults(context)
                .replace(submit).replace(getJob).replace(listJobs).replace(completeStep)
                .execute(verb, input);
    }

    private static ObjectNode envelope(String json) {
        try {
            return (ObjectNode) MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void assertInvalidInput(Throwable thrown, String messagePart) {
        assertThat(thrown).isInstanceOfSatisfying(ActionException.class, e -> {
            assertThat(e.code()).isEqualTo("invalid-input");
            assertThat(e.getMessage()).contains(messagePart);
        });
    }

    @Test
    void getJobRequiresAStringJobId() {
        assertInvalidInput(catchThrowable(() -> dispatch("get-job", envelope("{}"))),
                "jobId field is required");
        assertInvalidInput(catchThrowable(() -> dispatch("get-job", envelope("{\"jobId\": 42}"))),
                "jobId value must be a valid UUID");
    }

    @Test
    void listJobsValidatesLimitOffsetAndStatus() throws Exception {
        assertInvalidInput(
                catchThrowable(() -> dispatch("list-jobs", envelope("{\"limit\": 0}"))),
                "'limit' must be >= 1");
        assertInvalidInput(
                catchThrowable(() -> dispatch("list-jobs", envelope("{\"limit\": \"50\"}"))),
                "'limit' must be an integer");
        assertInvalidInput(
                catchThrowable(() -> dispatch("list-jobs", envelope("{\"offset\": -1}"))),
                "offset must be >= 0");
        assertInvalidInput(
                catchThrowable(() -> dispatch("list-jobs", envelope("{\"workflowName\": 7}"))),
                "'workflowName' must be a string");
        // The rejection message lists the valid statuses.
        Throwable badStatus = catchThrowable(
                () -> dispatch("list-jobs", envelope("{\"status\": \"ARCHIVED\"}")));
        assertInvalidInput(badStatus, "Invalid enum value");
        assertThat(((ActionException) badStatus).getMessage())
                .contains("JobStatus").contains("ARCHIVED").contains("ARCHIVED");
    }

    @Test
    void listJobsFiltersByWorkflowName() throws Exception {
        ObjectNode request = MAPPER.createObjectNode();
        request.set("workflow", workflows.twoStepWorkflow("in-process", null));
        request.putObject("input").put("text", "hi");
        dispatch("submit-workflow", request);

        ObjectNode matching = dispatch("list-jobs", envelope("{\"workflowName\": \"embed-text\"}"));
        assertThat(matching.get("jobs")).hasSize(1);
        assertThat(matching.get("jobs").get(0).get("workflowName").asText())
                .isEqualTo("embed-text");

        ObjectNode other = dispatch("list-jobs", envelope("{\"workflowName\": \"court-decoration\"}"));
        assertThat(other.get("ok").asBoolean()).isTrue();
        assertThat(other.get("jobs")).isEmpty();
    }

    @Test
    void submitWorkflowValidatesTheEnvelopeShapes() {
        // A workflow that is not an object.
        ObjectNode textualWorkflow = MAPPER.createObjectNode();
        textualWorkflow.put("workflow", "not-an-object");
        textualWorkflow.putObject("input").put("text", "hi");
        assertInvalidInput(catchThrowable(() -> dispatch("submit-workflow", textualWorkflow)),
                "Expect message object");

        // A workflowName that is not a string.
        ObjectNode numericName = MAPPER.createObjectNode();
        numericName.put("workflowName", 7);
        numericName.putObject("input").put("text", "hi");
        assertInvalidInput(catchThrowable(() -> dispatch("submit-workflow", numericName)),
                "Field 'workflowName' must be a string");

        // A jobId that is not a string.
        ObjectNode numericJobId = MAPPER.createObjectNode();
        numericJobId.set("workflow", workflows.twoStepWorkflow("in-process", null));
        numericJobId.putObject("input").put("text", "hi");
        numericJobId.put("jobId", 42);
        assertInvalidInput(catchThrowable(() -> dispatch("submit-workflow", numericJobId)),
                "jobId value must be a valid UUID");

        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void completeStepRequiresAllThreeFields() {
        assertInvalidInput(catchThrowable(() -> dispatch("complete-step", envelope("{}"))),
                "jobId field is required");
        assertInvalidInput(catchThrowable(() -> dispatch("complete-step", envelope("{\"jobId\": \"" + java.util.UUID.randomUUID() + "\"}"))),
                "stepName field is required");
        assertInvalidInput(catchThrowable(() -> dispatch("complete-step", envelope(
                "{\"jobId\": \"" + java.util.UUID.randomUUID() + "\","
                        + " \"stepName\": \"review\"}"))),
                "Missing required object field 'response'");
        assertInvalidInput(catchThrowable(() -> dispatch("complete-step", envelope(
                "{\"jobId\": \"" + java.util.UUID.randomUUID() + "\","
                        + " \"stepName\": \"review\", \"response\": \"nope\"}"))),
                "Expect a map object");
        assertInvalidInput(catchThrowable(() -> dispatch("complete-step", envelope(
                "{\"jobId\": \"not-a-uuid\", \"stepName\": \"review\","
                        + " \"response\": {}}"))),
                "jobId value must be a valid UUID");
    }

    @Test
    void completeStepMissesCleanlyOnAnUnknownJob() throws Exception {
        java.util.UUID jobId = java.util.UUID.randomUUID();
        ObjectNode result = dispatch("complete-step", envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {}}"));
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("error").asText()).isEqualTo("no workflow run " + jobId);
    }

    @Test
    void completeStepRejectsAStepTheWorkflowDoesNotDeclare() throws Exception {
        // Park a job on a step, then corrupt the outstanding step to one the
        // snapshotted definition does not declare: the wrong-state gate passes
        // (the row IS waiting on it) and the definition lookup fails loud.
        ObjectNode request = MAPPER.createObjectNode();
        request.set("workflow", workflows.threeStepWorkflow("in-process"));
        request.putObject("input").put("text", "hi");
        String jobId = dispatch("submit-workflow", request).get("jobId").asText();
        java.util.UUID id = java.util.UUID.fromString(jobId);
        ai.pipestream.proto.jobs.service.store.WorkflowRunRecord job = store.get(id).orElseThrow();
        store.markWaiting(id, "ghost", job.checkpoints,
                ai.pipestream.proto.jobs.service.events.WorkflowRunEventFactory.waiting(job, "ghost"));

        Throwable thrown = catchThrowable(() -> dispatch("complete-step", envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"ghost\","
                        + " \"response\": {}}")));
        assertInvalidInput(thrown, "the workflow has no step named 'ghost'");
    }
}
