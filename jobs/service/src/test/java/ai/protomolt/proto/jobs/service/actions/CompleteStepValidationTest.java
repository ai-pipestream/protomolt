package ai.protomolt.proto.jobs.service.actions;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.jobs.service.ValidatingWorkflows;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * complete-step's declared-validation verdict (the {@link ValidatingWorkflows}
 * fixture): a parked external step with {@code validate: true} checks the
 * supplied response against the step's output rules — a rejection FAILS the
 * job with the violations (a verdict, not an error), an acceptance
 * checkpoints and requeues. The single-step workflow parks immediately, so no
 * gRPC service is involved until the resume.
 */
class CompleteStepValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    ValidatingWorkflows workflows;
    InMemoryWorkflowRunStore store;
    WorkflowRunWorker worker;
    SubmitWorkflowAction submit;
    CompleteStepAction completeStep;
    ActionContext context;

    @BeforeEach
    void fresh() {
        workflows = new ValidatingWorkflows();
        store = new InMemoryWorkflowRunStore();
        context = ActionContext.create();
        // The resumed segment runs no served step (the only step is the
        // external review), so any channel factory does — in-process here.
        worker = new WorkflowRunWorker(store, context, null,
                new WorkflowRunner(step -> InProcessChannelBuilder.forName("unused").build()),
                new WorkflowRunsConfig("test-worker", 1, Duration.ofSeconds(30),
                        Duration.ofMillis(50), 1, 3, 4, null, "workflow-run-events", null, null));
        submit = new SubmitWorkflowAction(store, null, 3);
        completeStep = new CompleteStepAction(store);
    }

    private static ObjectNode envelope(String json) {
        try {
            return (ObjectNode) MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Submit the external-review workflow and park it on the review step. */
    private String parkOnReview() throws Exception {
        ObjectNode request = MAPPER.createObjectNode();
        request.set("workflow", workflows.externalReviewWorkflow("in-process"));
        request.putObject("input").put("text", "hi");
        String jobId = dispatch(submit, request).get("jobId").asText();
        assertThat(worker.workOnce()).isTrue();
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);
        return jobId;
    }

    @Test
    void aResponseFailingTheDeclaredRulesFailsTheJobAsAVerdict() throws Exception {
        String jobId = parkOnReview();

        // "no" trips the declared min_len 3 rule on Review.notes.
        ObjectNode rejected = dispatch(completeStep, envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"no\"}}"));
        assertThat(rejected.get("ok").asBoolean()).isFalse();
        assertThat(rejected.get("status").asText()).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
        assertThat(rejected.get("error").asText()).contains("notes");

        // The verdict lands on the row: FAILED with the violations, a FAILED
        // event, and no checkpoint appended.
        WorkflowRunRecord job = store.get(UUID.fromString(jobId)).orElseThrow();
        assertThat(job.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
        assertThat(job.error)
                .contains("VALIDATION: complete-step response failed validation")
                .contains("notes");
        assertThat(job.completedAt).isNotNull();
        assertThat(MAPPER.readTree(job.checkpoints)).isEmpty();
        assertThat(store.events().stream().map(e -> e.eventType))
                .contains(WorkflowRunEventRecord.TYPE_FAILED);
    }

    @Test
    void aResponsePassingTheDeclaredRulesResumesTheJob() throws Exception {
        String jobId = parkOnReview();

        ObjectNode accepted = dispatch(completeStep, envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"ship it\"}}"));
        assertThat(accepted.get("ok").asBoolean()).isTrue();
        assertThat(accepted.get("status").asText()).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);

        // The resumed job maps the review into the output and completes.
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord done = store.get(UUID.fromString(jobId)).orElseThrow();
        assertThat(done.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(MAPPER.readTree(done.result).get("notes").asText()).isEqualTo("ship it");
    }

    @Test
    void aDifferentResponseCannotReuseACompletedStepEvenAfterTheJobFinishes() throws Exception {
        String jobId = parkOnReview();
        ObjectNode request = envelope("{\"jobId\": \"" + jobId
                + "\", \"stepName\": \"review\", \"response\": {\"notes\": \"ship it\"}}");
        assertThat(dispatch(completeStep, request).path("ok").asBoolean()).isTrue();
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord before = store.get(UUID.fromString(jobId)).orElseThrow();
        String checkpoints = before.checkpoints;
        int events = store.events().size();

        assertThat(dispatch(new CompleteStepAction(store), request).path("ok").asBoolean()).isTrue();
        request.withObject("response").put("notes", "different answer");
        ObjectNode conflict = dispatch(new CompleteStepAction(store), request);
        assertThat(conflict.path("ok").asBoolean()).isFalse();
        assertThat(conflict.path("error").asText()).contains("conflict");
        // Even an invalid competing result cannot overwrite a successful completion.
        request.withObject("response").put("notes", "no");
        assertThat(dispatch(completeStep, request).path("ok").asBoolean()).isFalse();
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status).isEqualTo("COMPLETED");
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().checkpoints).isEqualTo(checkpoints);
        assertThat(store.events()).hasSize(events);
    }

    @Test
    void anIdenticalRetryMatchesALegacyCheckpointWithoutJsonNormalization() throws Exception {
        String jobId = parkOnReview();
        var job = store.get(UUID.fromString(jobId)).orElseThrow();
        // Before this change checkpoints retained the caller's proto3 JSON,
        // including quoted numbers and explicit default values.
        String entry = "{\"name\":\"review\",\"skipped\":false,"
                + "\"response\":{\"review_count\":\"0\",\"notes\":\"ship it\"}}";
        store.completeParkedStep(job.jobId, "review", entry,
                ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory.stepCheckpoint(job, "review"));
        int events = store.events().size();
        ObjectNode request = envelope("{\"jobId\":\"" + jobId
                + "\",\"stepName\":\"review\",\"response\":{\"notes\":\"ship it\",\"review_count\":\"0\"}}");
        assertThat(dispatch(new CompleteStepAction(store), request).path("ok").asBoolean()).isTrue();
        assertThat(store.events()).hasSize(events);
        assertThat(store.get(job.jobId).orElseThrow().checkpoints).contains("review_count");
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
