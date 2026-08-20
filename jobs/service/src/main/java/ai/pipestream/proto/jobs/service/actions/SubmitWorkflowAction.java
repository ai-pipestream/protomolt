package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.workflow.WorkflowRepository;
import ai.pipestream.proto.jobs.service.WorkflowRunSubmitter;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@code submit-workflow} verb: accept a workflow run for asynchronous
 * execution — the async sibling of {@code run-workflow}, same definition, same
 * serial semantics, different coupling. The workflow (inline object or stored
 * name) is parsed, verified, and its input validated before anything is
 * persisted; the job row (status QUEUED) and its ACCEPTED event insert in
 * one transaction. {@code jobId} is the idempotency key: resubmitting an
 * existing id returns the existing row untouched.
 * <p>
 * A null store means workflow runs are not configured on this server; every
 * call then answers {@code unavailable}.
 */
public final class SubmitWorkflowAction implements ProtoAction {

    private final WorkflowRunStore store;
    private final WorkflowRunSubmitter submitter;

    /**
     * @param store the jobs store, or null when jobs are not configured
     * @param repository resolves stored workflow names, or null for inline-only
     * @param maxAttemptsDefault the retry ceiling stamped on new jobs
     */
    public SubmitWorkflowAction(WorkflowRunStore store, WorkflowRepository repository,
            int maxAttemptsDefault) {
        this.store = store;
        this.submitter = store == null
                ? null
                : new WorkflowRunSubmitter(store, repository, maxAttemptsDefault);
    }

    @Override
    public String name() {
        return "submit-workflow";
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
    }

    @Override
    public String description() {
        return "Submits a workflow for asynchronous execution as a durable job: the "
                + "definition (inline 'workflow' or stored 'workflowName') is verified, the "
                + "input validated against the workflow's inputType, and the job queued — "
                + "workers execute it with the same serial semantics as run-workflow, "
                + "checkpointing every step. 'jobId' is the idempotency key (a uuid is "
                + "minted when absent). Returns the job id and status; lifecycle events "
                + "are published to the workflow-run-events topic.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = baseSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("workflow")
                .put("type", "object")
                .put("description", "The workflow definition: a schema declaring every "
                        + "step's service, an inputType, and serial steps whose requests "
                        + "are mapped from 'input' plus prior steps' responses (by step "
                        + "name). Steps: {name, target, method, tls?, when?, rules?, "
                        + "celRules?, validate?, deadlineMs?, completion?}; completion "
                        + "'external' parks the job until complete-step supplies the "
                        + "response.");
        properties.putObject("workflowName")
                .put("type", "string")
                .put("description", "A stored workflow to run instead of an inline 'workflow' — "
                        + "registered via the registry's workflows endpoint.");
        properties.putObject("input")
                .put("type", "object")
                .put("description", "The workflow input, as proto3 JSON of the workflow's "
                        + "inputType.");
        properties.putObject("jobId")
                .put("type", "string")
                .put("description", "Client-generated uuid; the idempotency key. Minted "
                        + "when absent.");
        schema.putArray("required").add("input");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ActionSupport.requireStore(store);
        ObjectNode workflow = ActionSupport.optionalObject(input, "workflow");
        String workflowName = ActionSupport.optionalString(input, "workflowName");
        String jobId = ActionSupport.optionalString(input, "jobId");
        JsonNode inputNode = input.get("input");
        WorkflowRunSubmitter.Outcome outcome =
                submitter.submit(workflow, workflowName, inputNode, jobId, context);
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.put("ok", outcome.ok());
        if (outcome.ok()) {
            result.put("jobId", outcome.jobId());
            result.put("status", outcome.status());
        } else {
            if (outcome.failedStep() != null && !outcome.failedStep().isEmpty()) {
                result.put("failedStep", outcome.failedStep());
            }
            result.put("error", outcome.error());
        }
        return result;
    }

    private static ObjectNode baseSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }
}
