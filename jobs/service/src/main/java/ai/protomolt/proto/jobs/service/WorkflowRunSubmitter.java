package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowJson;
import ai.protomolt.proto.workflow.WorkflowRepository;
import ai.protomolt.proto.workflow.WorkflowVerifier;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.UUID;

/**
 * The submit path shared by the {@code submit-workflow} verb and the
 * request-topic consumer: resolve the workflow (inline object or stored name),
 * parse it, verify it, validate the input parses as the workflow's inputType,
 * then insert the QUEUED row and its ACCEPTED event in one transaction.
 * <p>
 * Submission never executes anything — the worker fleet picks the row up.
 * Validation failures come back as an {@link Outcome} with {@code ok ==
 * false} (the verb answers its caller; the consumer writes a FAILED row —
 * nothing is ever silently dropped). Store failures propagate: fail loud.
 */
public final class WorkflowRunSubmitter {

    /**
     * The submit verdict.
     *
     * @param ok true when the job row exists (newly inserted or idempotent
     *        resubmit)
     * @param jobId the job id (set when {@code ok})
     * @param status the stored row's status
     * @param failedStep the step a parse/verify failure names ("" for
     *        workflow-level failures)
     * @param error the failure detail (null when {@code ok})
     */
    public record Outcome(boolean ok, String jobId, String status, String failedStep,
                          String error) {
    }

    private final WorkflowRunStore store;
    private final WorkflowRepository repository;
    private final int maxAttemptsDefault;

    /**
     * @param store the jobs store (required)
     * @param repository resolves stored workflow names, or null for inline-only
     * @param maxAttemptsDefault the retry ceiling stamped on new jobs
     */
    public WorkflowRunSubmitter(WorkflowRunStore store, WorkflowRepository repository,
            int maxAttemptsDefault) {
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.repository = repository;
        this.maxAttemptsDefault = maxAttemptsDefault;
    }

    /**
     * Submit one job.
     *
     * @param inlineWorkflow an inline workflow definition object, or null
     * @param workflowName a stored workflow name, or null
     * @param input the workflow input object (proto3 JSON of the inputType)
     * @param jobIdOrNull the client-generated job uuid, or null/blank to mint
     * @param context type resolution and JSON machinery
     * @return the submit verdict
     */
    public Outcome submit(ObjectNode inlineWorkflow, String workflowName, JsonNode input,
            String jobIdOrNull, ActionContext context) {
        JsonNode workflowNode = inlineWorkflow;
        if (workflowNode == null && workflowName != null && !workflowName.isBlank()) {
            if (repository == null) {
                return fail("", "No workflow repository is mounted; submit with an inline "
                        + "'workflow' or start a server with a registry");
            }
            workflowNode = repository.workflow(workflowName).orElse(null);
            if (workflowNode == null) {
                return fail("", "No stored workflow named '" + workflowName + "'");
            }
        }
        if (!(workflowNode instanceof ObjectNode workflow) || !(input instanceof ObjectNode)) {
            return fail("", "'workflow' (or 'workflowName') and 'input' objects are required");
        }
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(workflow, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            return fail(e.step, e.getMessage());
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(definition);
        if (!findings.isEmpty()) {
            WorkflowVerifier.Finding first = findings.getFirst();
            return fail(first.step(), "workflow does not verify (" + findings.size() + " finding"
                    + (findings.size() == 1 ? "" : "s") + "); first: [" + first.kind() + "] "
                    + first.error());
        }
        try {
            context.transcoder().fromJsonDynamic(input.toString(), definition.inputType());
        } catch (MalformedProtobufJsonException e) {
            return fail("", "'input' is not valid proto3 JSON for "
                    + definition.inputType().getFullName() + ": " + e.getMessage());
        }
        UUID jobId;
        if (jobIdOrNull == null || jobIdOrNull.isBlank()) {
            jobId = UUID.randomUUID();
        } else {
            try {
                jobId = UUID.fromString(jobIdOrNull.trim());
            } catch (IllegalArgumentException e) {
                return fail("", "'jobId' must be a uuid; got '" + jobIdOrNull + "'");
            }
        }
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = jobId;
        record.workflowName = workflowName != null && !workflowName.isBlank()
                ? workflowName
                : definition.name() == null ? "inline" : definition.name();
        record.workflowDefinition = workflow.toString();
        record.input = input.toString();
        record.status = WorkflowRunRecord.STATUS_QUEUED;
        record.maxAttempts = maxAttemptsDefault;
        record.runAfter = java.time.Instant.now();
        WorkflowRunStore.InsertOutcome outcome =
                store.insert(record, WorkflowRunEventFactory.accepted(record));
        return new Outcome(true, outcome.job().jobId.toString(), outcome.job().status, "", null);
    }

    private static Outcome fail(String step, String error) {
        return new Outcome(false, null, null, step == null ? "" : step, error);
    }
}
