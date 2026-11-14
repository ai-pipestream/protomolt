package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.pipeline.PipelineChecker;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowJson;
import ai.protomolt.proto.workflow.WorkflowRepository;
import ai.protomolt.proto.workflow.WorkflowVerifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * The submit path shared by the {@code submit-workflow} verb and the
 * request-topic consumer: resolve the workflow (inline object or stored name),
 * parse it, verify it, parse the input as the workflow's inputType, and enforce
 * declared input rules when the workflow opts into contract validation,
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
     * @param conflict true when the job id belongs to a different submission
     * @param jobId the job id (set when {@code ok})
     * @param status the stored row's status
     * @param failedStep the step a parse/verify failure names ("" for
     *        workflow-level failures)
     * @param error the failure detail (null when {@code ok})
     */
    public record Outcome(boolean ok, boolean conflict, String jobId, String status,
                          String failedStep, String error) {
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
        if (!(workflowNode instanceof ObjectNode workflow) || !(input instanceof ObjectNode inputObj)) {
            return fail("", "'workflow' (or 'workflowName') and 'input' objects are required");
        }
        if (PipelineRunCodec.isPipelineNode(workflow)) {
            return submitInlinePipeline(workflow, workflowName, inputObj, jobIdOrNull, context);
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
            var parsedInput = context.transcoder().fromJsonDynamic(input.toString(), definition.inputType());
            if (definition.validateContract()) {
                var validation = ProtoValidator.forMessageType(definition.inputType())
                        .validate(parsedInput);
                if (!validation.valid()) {
                    return fail("", "'input' failed declared validation rules: "
                            + validation.violations());
                }
            }
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
        if (outcome.conflict()) {
            return new Outcome(false, true, jobId.toString(), outcome.job().status, "",
                    "jobId '" + jobId + "' already belongs to a different workflow submission");
        }
        return new Outcome(true, false, outcome.job().jobId.toString(),
                outcome.job().status, "", null);
    }

    /**
     * Submits a statically verified {@link Pipeline} and binary input message for durable
     * asynchronous execution. Snapshots the pipeline bytes, transitive descriptor closure,
     * and canonical binary input onto the job row so workers and {@code complete-step}
     * replay with exact binary protobuf fidelity across restarts.
     *
     * @param pipeline the compiled pipeline contract
     * @param files the descriptor set backing {@code pipeline}
     * @param input the binary input message matching {@code pipeline.input_type}
     * @param jobIdOrNull the client-generated job UUID (submit idempotency key), or null/blank to mint
     * @return the submit verdict
     */
    public Outcome submitPipeline(Pipeline pipeline, List<FileDescriptor> files,
                                  Message input, String jobIdOrNull) {
        return submitPipelineNamed(pipeline, files, input, null, jobIdOrNull);
    }

    private Outcome submitInlinePipeline(ObjectNode workflow, String workflowName,
                                         ObjectNode inputObj, String jobIdOrNull,
                                         ActionContext context) {
        PipelineRunCodec.PartialSubmission partial;
        try {
            partial = PipelineRunCodec.decodeSubmissionEnvelope(workflow,
                    context == null ? null : context.registry());
        } catch (PipelineDefinitionException e) {
            return fail(e.step(), e.getMessage());
        }
        DynamicMessage parsedInput;
        try {
            parsedInput = partial.transcoder().fromJsonDynamic(
                    inputObj.toString(), partial.inputType());
        } catch (MalformedProtobufJsonException e) {
            return fail("", "'input' is not valid proto3 JSON for "
                    + partial.inputType().getFullName() + ": " + e.getMessage());
        }
        return submitPipelineNamed(partial.pipeline(), partial.files(), parsedInput,
                workflowName, jobIdOrNull);
    }

    private Outcome submitPipelineNamed(Pipeline pipeline, List<FileDescriptor> files,
                                        Message input, String explicitName,
                                        String jobIdOrNull) {
        if (pipeline == null || files == null || input == null) {
            return fail("", "'pipeline', 'files', and 'input' are required");
        }
        List<PipelineChecker.Finding> findings = new PipelineChecker().verify(pipeline, files);
        if (!findings.isEmpty()) {
            PipelineChecker.Finding first = findings.getFirst();
            return fail(first.step(), "pipeline does not verify (" + findings.size()
                    + " finding" + (findings.size() == 1 ? "" : "s") + "); first: ["
                    + first.kind() + "] " + first.error());
        }
        DescriptorRegistry registry = PipelineRunCodec.buildRegistry(files, null);
        Descriptor inputType = registry.findDescriptorByFullName(pipeline.getInputType());
        if (!PipelineRunCodec.matchesDescriptor(input.getDescriptorForType(), inputType)) {
            return fail("", "pipeline input must be " + inputType.getFullName() + "; got "
                    + input.getDescriptorForType().getFullName());
        }
        DynamicMessage canonicalInput;
        try {
            canonicalInput = DynamicMessage.parseFrom(
                    inputType, PipelineRunCodec.canonicalBytes(input));
        } catch (InvalidProtocolBufferException e) {
            return fail("", "pipeline input cannot be decoded as "
                    + inputType.getFullName() + ": " + e.getMessage());
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
        ObjectNode definitionEnvelope = PipelineRunCodec.encodeDefinition(
                pipeline, files, canonicalInput);
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = jobId;
        record.workflowName = explicitName != null && !explicitName.isBlank()
                ? explicitName
                : pipeline.getName();
        record.workflowDefinition = definitionEnvelope.toString();
        record.input = PipelineRunCodec.renderInputJson(canonicalInput, files);
        record.status = WorkflowRunRecord.STATUS_QUEUED;
        record.maxAttempts = maxAttemptsDefault;
        record.runAfter = Instant.now();
        WorkflowRunStore.InsertOutcome outcome =
                store.insert(record, WorkflowRunEventFactory.accepted(record));
        if (outcome.conflict()) {
            return new Outcome(false, true, jobId.toString(), outcome.job().status, "",
                    "jobId '" + jobId + "' already belongs to a different workflow submission");
        }
        return new Outcome(true, false, outcome.job().jobId.toString(),
                outcome.job().status, "", null);
    }

    private static Outcome fail(String step, String error) {
        return new Outcome(false, false, null, null, step == null ? "" : step, error);
    }
}
