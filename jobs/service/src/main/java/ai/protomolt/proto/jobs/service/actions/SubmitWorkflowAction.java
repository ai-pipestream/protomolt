package ai.protomolt.proto.jobs.service.actions;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.workflow.WorkflowRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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
    public Descriptor requestType() {
        return CatalogContract.request("SubmitWorkflowRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("SubmitWorkflowResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // Availability first: a node with no job store cannot serve any request, so
        // saying that is more use than listing fields on a verb that cannot run.
        ActionSupport.requireStore(store);
        // The workflow and its input are structures: their shape is the caller's workflow
        // definition, which this contract does not describe, so they stay documents.
        ObjectNode workflow = Fields.has(input, "workflow")
                ? Fields.json(input, "workflow")
                : null;
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(
                workflow,
                Fields.string(input, "workflowName"),
                Fields.has(input, "input") ? Fields.json(input, "input") : null,
                Fields.string(input, "jobId"),
                context);
        Reply result = Reply.of(responseType()).set("ok", outcome.ok());
        if (outcome.ok()) {
            return result
                    .set("jobId", outcome.jobId())
                    .set("status", outcome.status())
                    .build();
        }
        return result
                .set("failedStep", outcome.failedStep() == null ? "" : outcome.failedStep())
                .set("error", outcome.error())
                .build();
    }

    private static ObjectNode baseSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }
}
