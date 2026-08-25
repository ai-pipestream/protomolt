package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.http.json.MalformedProtobufJsonException;
import ai.pipestream.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.pipestream.proto.jobs.service.store.ParkedCompletion;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import ai.pipestream.proto.workflow.CompiledWorkflow;
import ai.pipestream.proto.workflow.WorkflowJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.Optional;
import java.util.UUID;

/**
 * The {@code complete-step} verb: supply the response for a parked
 * external-completion step — the human-in-the-loop lane. The job's state is
 * gated first (only a WAITING job parked on exactly this step accepts a
 * response; a response for an already-checkpointed step is an idempotent
 * redelivery), then the response is parsed against the step's output type
 * and, when the step declares {@code validate}, checked against its declared
 * rules — a rejection fails the job with the violations (a verdict, not an
 * error). Accepted, the checkpoint appends and the job requeues in one
 * transaction; the worker fleet runs the next segment.
 * <p>
 * A null store means workflow runs are not configured on this server; every
 * call then answers {@code unavailable}.
 */
public final class CompleteStepAction implements ProtoAction {

    private final WorkflowRunStore store;

    /**
     * @param store the jobs store, or null when jobs are not configured
     */
    public CompleteStepAction(WorkflowRunStore store) {
        this.store = store;
    }

    @Override
    public String name() {
        return "complete-step";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Completes a workflow run's parked external step: validates the supplied "
                + "response against the step's output type (and its declared validation "
                + "rules — a rejection fails the job as a verdict), checkpoints it, and "
                + "requeues the job for its next segment. Idempotent: redelivering a "
                + "completed step answers the current status.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("CompleteStepRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("CompleteStepResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // Availability first: a node with no job store cannot serve any request, so
        // saying that is more use than listing fields on a verb that cannot run.
        ActionSupport.requireStore(store);
        UUID jobId = ActionSupport.jobId(Fields.string(input, "jobId"));
        String stepName = Fields.string(input, "stepName");
        // The step's response is a structure: its shape is the step's own output type,
        // which this contract does not describe.
        ObjectNode response = Fields.json(input, "response");
        Optional<WorkflowRunRecord> found = store.get(jobId);
        if (found.isEmpty()) {
            return Reply.of(responseType())
                    .set("ok", false)
                    .set("error", "no workflow run " + jobId)
                    .build();
        }
        WorkflowRunRecord job = found.get();

        // The wrong-state gate comes first — fail fast without mutating
        // anything. The store re-gates under the row lock when the entry is
        // appended, so a race between the two answers the same way.
        if (!WorkflowRunRecord.STATUS_WAITING.equals(job.status)
                || !stepName.equals(job.outstandingStep)) {
            if (alreadyCheckpointed(job, stepName)) {
                return ok(job.status);
            }
            return wrongState(job.status, job.outstandingStep, stepName, jobId);
        }

        // Build the checkpoint entry: the response parsed against the step's
        // output type from the job's snapshotted definition. A parse failure
        // is the caller's error; the job is untouched.
        CompiledWorkflow.Step step = resolveStep(job, stepName, context);
        DynamicMessage parsed;
        try {
            parsed = context.transcoder().fromJsonDynamic(response.toString(),
                    step.method().getOutputType());
        } catch (MalformedProtobufJsonException e) {
            throw ActionSupport.invalidInput("'response' is not valid proto3 JSON for "
                    + step.method().getOutputType().getFullName() + ": " + e.getMessage());
        }
        if (step.validate()) {
            ValidationResult validation = ProtoValidator
                    .forMessageType(step.method().getOutputType())
                    .validate(parsed);
            if (!validation.valid()) {
                String violations = violations(validation);
                String detail = "VALIDATION: complete-step response failed validation: "
                        + violations;
                store.markFailed(jobId, detail,
                        WorkflowRunEventFactory.failed(job, stepName, detail));
                return Reply.of(responseType())
                        .set("ok", false)
                        .set("status", WorkflowRunRecord.STATUS_FAILED)
                        .set("error", violations)
                        .build();
            }
        }

        ObjectNode entry = JsonNodeFactory.instance.objectNode();
        entry.put("name", stepName);
        entry.put("skipped", false);
        entry.set("response", response);
        ParkedCompletion completion = store.completeParkedStep(jobId, stepName,
                entry.toString(), WorkflowRunEventFactory.stepCheckpoint(job, stepName));
        if (completion instanceof ParkedCompletion.Completed) {
            return ok(WorkflowRunRecord.STATUS_QUEUED);
        }
        if (completion instanceof ParkedCompletion.AlreadyDone alreadyDone) {
            return ok(alreadyDone.currentStatus());
        }
        ParkedCompletion.WrongState wrong = (ParkedCompletion.WrongState) completion;
        return wrongState(wrong.currentStatus(), wrong.outstandingStep(), stepName, jobId);
    }

    /** The step's definition from the job's snapshotted workflow. */
    private CompiledWorkflow.Step resolveStep(WorkflowRunRecord job, String stepName,
            ActionContext context) throws ActionException {
        JsonNode tree;
        try {
            tree = context.objectMapper().readTree(job.workflowDefinition);
        } catch (Exception e) {
            throw new ActionException("job-corrupt",
                    "the job's stored workflow definition is not readable: " + e.getMessage());
        }
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse((ObjectNode) tree, context);
        } catch (WorkflowJson.WorkflowParseException | ClassCastException e) {
            throw new ActionException("job-corrupt",
                    "the job's stored workflow definition does not parse: " + e.getMessage());
        }
        for (CompiledWorkflow.Step step : definition.steps()) {
            if (step.name().equals(stepName)) {
                return step;
            }
        }
        throw ActionSupport.invalidInput("the workflow has no step named '" + stepName + "'");
    }

    private static boolean alreadyCheckpointed(WorkflowRunRecord job, String stepName) {
        try {
            JsonNode tree = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readTree(job.checkpoints == null ? "[]" : job.checkpoints);
            if (tree instanceof ArrayNode array) {
                for (JsonNode entry : array) {
                    if (stepName.equals(entry.path("name").asText())) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            // Unreadable checkpoints are the worker's WORKFLOW failure to
            // report; the gate simply cannot confirm idempotency.
            return false;
        }
        return false;
    }

    private Message ok(String status) {
        return Reply.of(responseType()).set("ok", true).set("status", status).build();
    }

    private Message wrongState(String status, String outstanding, String stepName,
            UUID jobId) {
        return Reply.of(responseType())
                .set("ok", false)
                .set("status", status)
                .set("outstandingStep", outstanding == null ? "" : outstanding)
                .set("error", "job " + jobId + " is " + status
                        + (outstanding == null ? "" : ", parked on step '" + outstanding + "'")
                        + "; it is not waiting on step '" + stepName + "'")
                .build();
    }

    private static String violations(ValidationResult result) {
        StringBuilder out = new StringBuilder();
        for (ValidationResult.Violation violation : result.violations()) {
            if (!out.isEmpty()) {
                out.append("; ");
            }
            out.append(violation.path()).append(": ").append(violation.message());
        }
        return out.toString();
    }
}
