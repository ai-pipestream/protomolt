package ai.pipestream.proto.jobs.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.workflow.WorkflowRepository;
import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The submit path directly (no verb envelope): workflow resolution (inline,
 * stored name, missing repository), the parse/verify/input-validation
 * failures that come back as verdicts, the jobId idempotency key, and the
 * row/event the happy path persists. Submission never executes anything, so
 * no gRPC server is involved.
 */
class WorkflowRunSubmitterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static TestWorkflows workflows;
    static ActionContext context;

    InMemoryWorkflowRunStore store;
    WorkflowRunSubmitter submitter;

    @BeforeAll
    static void compileFixture() {
        workflows = new TestWorkflows();
        context = ActionContext.create();
    }

    @BeforeEach
    void fresh() {
        store = new InMemoryWorkflowRunStore();
        submitter = new WorkflowRunSubmitter(store, null, 3);
    }

    private static JsonNode json(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void anInlineWorkflowQueuesTheRowWithItsDeclaredName() {
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(
                workflows.twoStepWorkflow("in-process", null), null, json("{\"text\": \"hi\"}"),
                null, context);

        assertThat(outcome.ok()).as("error: %s", outcome.error()).isTrue();
        assertThat(outcome.jobId()).isNotBlank();
        assertThat(outcome.status()).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(outcome.error()).isNull();

        WorkflowRunRecord row = store.get(UUID.fromString(outcome.jobId())).orElseThrow();
        assertThat(row.workflowName).isEqualTo("embed-text");
        assertThat(row.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(row.maxAttempts).isEqualTo(3);
        assertThat(row.attempt).isZero();
        assertThat(row.runAfter).isNotNull();
        assertThat(row.checkpoints).isEqualTo("[]");
        assertThat(row.input).contains("\"text\":\"hi\"");
        assertThat(row.workflowDefinition).contains("tokenize");

        // The ACCEPTED event commits with the row.
        assertThat(store.events()).hasSize(1);
        assertThat(store.events().get(0).eventType)
                .isEqualTo(WorkflowRunEventRecord.TYPE_ACCEPTED);
        assertThat(store.events().get(0).kafkaKey).isEqualTo(outcome.jobId());
    }

    @Test
    void aClientJobIdIsTheIdempotencyKey() {
        String jobId = UUID.randomUUID().toString();
        ObjectNode workflow = workflows.twoStepWorkflow("in-process", null);
        WorkflowRunSubmitter.Outcome first = submitter.submit(workflow, null,
                json("{\"text\": \"hi\"}"), jobId, context);
        assertThat(first.ok()).isTrue();
        assertThat(first.jobId()).isEqualTo(jobId);

        // A resubmit (even with extra whitespace around the id) returns the
        // existing row and writes nothing.
        WorkflowRunSubmitter.Outcome second = submitter.submit(workflow, null,
                json("{\"text\": \"hi\"}"), " " + jobId + " ", context);
        assertThat(second.ok()).isTrue();
        assertThat(second.jobId()).isEqualTo(jobId);
        assertThat(store.events()).hasSize(1);
    }

    @Test
    void aBlankJobIdMintsOne() {
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(
                workflows.twoStepWorkflow("in-process", null), null, json("{\"text\": \"hi\"}"),
                "   ", context);
        assertThat(outcome.ok()).isTrue();
        assertThat(UUID.fromString(outcome.jobId())).isNotNull();
    }

    @Test
    void aNonUuidJobIdFails() {
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(
                workflows.twoStepWorkflow("in-process", null), null, json("{\"text\": \"hi\"}"),
                "not-a-uuid", context);
        assertThat(outcome.ok()).isFalse();
        assertThat(outcome.error()).contains("'jobId' must be a uuid").contains("not-a-uuid");
        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void aMissingWorkflowOrInputFailsCleanly() {
        // Neither inline workflow nor workflow name.
        WorkflowRunSubmitter.Outcome nothing = submitter.submit(null, null,
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(nothing.ok()).isFalse();
        assertThat(nothing.error()).contains("'workflow' (or 'workflowName') and 'input'");
        assertThat(nothing.failedStep()).isEmpty();

        // A blank workflow name is no workflow name.
        WorkflowRunSubmitter.Outcome blank = submitter.submit(null, "  ",
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(blank.ok()).isFalse();
        assertThat(blank.error()).contains("'workflow' (or 'workflowName') and 'input'");

        // A workflow without its input object.
        WorkflowRunSubmitter.Outcome noInput = submitter.submit(
                workflows.twoStepWorkflow("in-process", null), null, null, null, context);
        assertThat(noInput.ok()).isFalse();
        assertThat(noInput.error()).contains("'workflow' (or 'workflowName') and 'input'");

        // A scalar input is not an input object.
        WorkflowRunSubmitter.Outcome scalarInput = submitter.submit(
                workflows.twoStepWorkflow("in-process", null), null, json("\"just a string\""),
                null, context);
        assertThat(scalarInput.ok()).isFalse();
        assertThat(scalarInput.error()).contains("'workflow' (or 'workflowName') and 'input'");

        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void aStoredNameNeedsAMountedRepositoryAndAKnownName() {
        // No repository at all.
        WorkflowRunSubmitter.Outcome noRepo = submitter.submit(null, "embed-text",
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(noRepo.ok()).isFalse();
        assertThat(noRepo.error()).contains("No workflow repository is mounted");

        // A repository that does not know the name.
        WorkflowRunSubmitter withRepo = new WorkflowRunSubmitter(store,
                (WorkflowRepository) name -> Optional.empty(), 3);
        WorkflowRunSubmitter.Outcome unknown = withRepo.submit(null, "court-decoration",
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(unknown.ok()).isFalse();
        assertThat(unknown.error()).contains("No stored workflow named 'court-decoration'");

        // A repository that resolves it — under a name that is not the
        // workflow's declared name, so the row proves which one wins.
        ObjectNode stored = workflows.twoStepWorkflow("in-process", null);
        WorkflowRunSubmitter resolving = new WorkflowRunSubmitter(store,
                (WorkflowRepository) name -> "court-decoration".equals(name)
                        ? Optional.of(stored) : Optional.empty(), 5);
        WorkflowRunSubmitter.Outcome resolved = resolving.submit(null, "court-decoration",
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(resolved.ok()).as("error: %s", resolved.error()).isTrue();
        WorkflowRunRecord row = store.get(UUID.fromString(resolved.jobId())).orElseThrow();
        // The stored name (not the workflow's declared name) stamps the row.
        assertThat(row.workflowName).isEqualTo("court-decoration");
        assertThat(row.maxAttempts).isEqualTo(5);
    }

    @Test
    void aWorkflowThatDoesNotParseFailsWithTheStepName() {
        ObjectNode broken = workflows.twoStepWorkflow("in-process", null);
        ((ObjectNode) broken.get("steps").get(1)).remove("method");
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(broken, null,
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(outcome.ok()).isFalse();
        assertThat(outcome.failedStep()).isEqualTo("embed");
        assertThat(outcome.error()).contains("'target' and 'method'");
        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void anInputThatIsNotProto3JsonForTheInputTypeFails() {
        // An object for a string field: the parser tolerates unknown fields
        // and coerces scalars (ignoringUnknownFields, protobuf leniency), but
        // never a JSON object where a string belongs.
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(
                workflows.twoStepWorkflow("in-process", null), null,
                json("{\"text\": {\"nested\": true}}"), null, context);
        assertThat(outcome.ok()).isFalse();
        assertThat(outcome.error()).contains("'input' is not valid proto3 JSON")
                .contains("jobs.test.Text");
        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void anUnnamedInlineWorkflowIsStampedInline() {
        ObjectNode workflow = workflows.twoStepWorkflow("in-process", null);
        workflow.remove("name");
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(workflow, null,
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(outcome.ok()).as("error: %s", outcome.error()).isTrue();
        assertThat(store.get(UUID.fromString(outcome.jobId())).orElseThrow().workflowName)
                .isEqualTo("inline");
    }
}
