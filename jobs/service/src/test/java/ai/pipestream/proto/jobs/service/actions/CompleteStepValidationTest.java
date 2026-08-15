package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.jobs.service.ValidatingWorkflows;
import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.pipestream.proto.jobs.service.worker.WorkflowRunWorker;
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
        String jobId = submit.execute(request, context).get("jobId").asText();
        assertThat(worker.workOnce()).isTrue();
        assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);
        return jobId;
    }

    @Test
    void aResponseFailingTheDeclaredRulesFailsTheJobAsAVerdict() throws Exception {
        String jobId = parkOnReview();

        // "no" trips the declared min_len 3 rule on Review.notes.
        ObjectNode rejected = completeStep.execute(envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"no\"}}"), context);
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

        ObjectNode accepted = completeStep.execute(envelope(
                "{\"jobId\": \"" + jobId + "\", \"stepName\": \"review\","
                        + " \"response\": {\"notes\": \"ship it\"}}"), context);
        assertThat(accepted.get("ok").asBoolean()).isTrue();
        assertThat(accepted.get("status").asText()).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);

        // The resumed job maps the review into the output and completes.
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord done = store.get(UUID.fromString(jobId)).orElseThrow();
        assertThat(done.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(MAPPER.readTree(done.result).get("notes").asText()).isEqualTo("ship it");
    }
}
