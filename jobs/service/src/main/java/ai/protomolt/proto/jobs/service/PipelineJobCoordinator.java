package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.ParkedCompletion;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreException;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.PipelineExecutor.Result;
import ai.protomolt.proto.pipeline.PipelineExecutor.StepOutcome;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoint;
import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoints;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Coordinates durable {@link Pipeline} execution on top of {@link WorkflowRunStore}:
 * binary-faithful job submission, checkpoint reconstruction, parked-step inspection,
 * and external-completion settlement with intent and schema validation.
 */
public final class PipelineJobCoordinator {


    /**
     * Reconstructed durable snapshot of a submitted pipeline run.
     *
     * @param pipeline verified compiled pipeline contract
     * @param files reconstructed root descriptor set matching {@code pipeline.descriptor_fingerprint}
     * @param input binary-faithful pipeline input message
     * @param pipelineFingerprint canonical pipeline and descriptor-closure SHA-256 identity
     * @param inputFingerprint canonical binary input SHA-256 identity
     */
    public record Definition(Pipeline pipeline, List<FileDescriptor> files,
                             DynamicMessage input, String pipelineFingerprint,
                             String inputFingerprint) {
        public Definition {
            Objects.requireNonNull(pipeline, "pipeline");
            files = List.copyOf(files);
            Objects.requireNonNull(input, "input");
            Objects.requireNonNull(pipelineFingerprint, "pipelineFingerprint");
            Objects.requireNonNull(inputFingerprint, "inputFingerprint");
        }
    }

    /**
     * Durable parked external-completion state of a {@code WAITING} pipeline run.
     *
     * @param jobId durable job UUID
     * @param stepName parked external step name
     * @param pipelineFingerprint canonical pipeline and descriptor-closure identity
     * @param inputFingerprint canonical binary input identity
     * @param cardinality expected response cardinality ({@code ONE} or {@code MANY})
     * @param checkpoints binary-faithful checkpoint prefix committed before parking
     */
    public record ParkedStep(UUID jobId, String stepName, String pipelineFingerprint,
                             String inputFingerprint, EdgeCardinality cardinality,
                             List<Checkpoint> checkpoints) {
        public ParkedStep {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(stepName, "stepName");
            Objects.requireNonNull(pipelineFingerprint, "pipelineFingerprint");
            Objects.requireNonNull(inputFingerprint, "inputFingerprint");
            Objects.requireNonNull(cardinality, "cardinality");
            checkpoints = List.copyOf(checkpoints);
        }
    }

    /** Raised when a stored pipeline definition or checkpoint envelope is corrupt or fails static checks. */
    public static final class PipelineDefinitionException extends RuntimeException {
        private final String step;

        /**
         * @param step attributed step name, or empty for pipeline-level failures
         * @param message diagnostic detail
         */
        public PipelineDefinitionException(String step, String message) {
            super(message);
            this.step = step == null ? "" : step;
        }

        /**
         * @param step attributed step name, or empty for pipeline-level failures
         * @param message diagnostic detail
         * @param cause underlying decode or validation failure
         */
        public PipelineDefinitionException(String step, String message, Throwable cause) {
            super(message, cause);
            this.step = step == null ? "" : step;
        }

        /** Returns the failing step name, or {@code ""} for pipeline-level failures. */
        public String step() {
            return step;
        }
    }

    private final WorkflowRunStore store;
    private final WorkflowRunSubmitter submitter;

    /**
     * Creates a coordinator using {@link WorkflowRunsConfig#DEFAULT_MAX_ATTEMPTS}.
     *
     * @param store durable workflow-run store
     */
    public PipelineJobCoordinator(WorkflowRunStore store) {
        this(store, WorkflowRunsConfig.DEFAULT_MAX_ATTEMPTS);
    }

    /**
     * Creates a coordinator with an explicit default retry ceiling.
     *
     * @param store durable workflow-run store
     * @param maxAttemptsDefault retry ceiling stamped on newly submitted pipeline jobs
     */
    public PipelineJobCoordinator(WorkflowRunStore store, int maxAttemptsDefault) {
        this.store = Objects.requireNonNull(store, "store");
        if (maxAttemptsDefault <= 0) {
            throw new IllegalArgumentException("maxAttemptsDefault must be positive");
        }
        this.submitter = new WorkflowRunSubmitter(store, null, maxAttemptsDefault);
    }

    /** Returns {@code true} when {@code job} is a pipeline run: it stores a pipeline definition. */
    public static boolean isPipelineJob(WorkflowRunRecord job) {
        return job != null && job.pipelineDefinition != null;
    }

    /** Reconstructs and statically verifies the durable pipeline snapshot stored on {@code job}. */
    public static Definition readDefinition(WorkflowRunRecord job) {
        return PipelineRunCodec.readDefinition(job);
    }

    /** Reconstructs the binary-faithful checkpoint prefix stored on {@code job}. */
    public static List<Checkpoint> readCheckpoints(WorkflowRunRecord job, Definition definition) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(definition, "definition");
        return PipelineCheckpointCodec.readCheckpoints(job, definition);
    }

    /** Serializes an ordered checkpoint prefix as a {@code PipelineRunCheckpoints}. */
    public static byte[] writeCheckpoints(List<Checkpoint> checkpoints) {
        Objects.requireNonNull(checkpoints, "checkpoints");
        return PipelineCheckpointCodec.writeCheckpoints(checkpoints).toByteArray();
    }

    /** Serializes a completed pipeline result as a {@code PipelineRunResult}. */
    public static byte[] writeResult(Result result, Definition definition) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(definition, "definition");
        return PipelineCheckpointCodec.writeResult(result, definition).toByteArray();
    }

    /**
     * The JSON view of a pipeline run for display: its input, one entry per checkpoint and,
     * once complete, its result. Output only; nothing reads it back.
     *
     * @param input the run's input message
     * @param checkpoints one entry per executed step
     * @param result the final output, or null until the run completes
     */
    public record RunView(ObjectNode input, ArrayNode checkpoints, ObjectNode result) {
    }

    /** Renders {@code job}, a pipeline run, for the get-job and list-jobs replies. */
    public static RunView view(WorkflowRunRecord job) {
        Objects.requireNonNull(job, "job");
        PipelineRunView.Documents documents = PipelineRunView.render(job);
        return new RunView(documents.input(), documents.checkpoints(), documents.result());
    }

    /**
     * Submits a verified {@link Pipeline} and binary input message for durable execution.
     * Resubmitting the same {@code jobId} with identical pipeline, descriptors, and binary input
     * is idempotent; altering any of them returns a conflict verdict without mutating the job.
     */
    public WorkflowRunSubmitter.Outcome submit(Pipeline pipeline, List<FileDescriptor> files,
                                               Message input, String jobIdOrNull) {
        return submitter.submitPipeline(pipeline, files, input, jobIdOrNull);
    }

    /**
     * Reads the durable parked external step and committed binary checkpoint prefix when
     * {@code jobId} exists and is currently {@code WAITING}.
     */
    public Optional<ParkedStep> parkedStep(UUID jobId) {
        Objects.requireNonNull(jobId, "jobId");
        Optional<WorkflowRunRecord> found = store.get(jobId);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        WorkflowRunRecord job = found.get();
        if (!WorkflowRunRecord.STATUS_WAITING.equals(job.status)
                || job.outstandingStep == null) {
            return Optional.empty();
        }
        Definition definition = readDefinition(job);
        PipelineStep step = findStep(definition.pipeline(), job.outstandingStep);
        if (step == null || !step.hasGrpcCall()) {
            throw new PipelineDefinitionException(job.outstandingStep,
                    "parked step '" + job.outstandingStep + "' is absent from stored pipeline");
        }
        List<Checkpoint> checkpoints = readCheckpoints(job, definition);
        return Optional.of(new ParkedStep(
                jobId,
                job.outstandingStep,
                definition.pipelineFingerprint(),
                definition.inputFingerprint(),
                step.getGrpcCall().getOutputCardinality(),
                checkpoints));
    }

    /** Reads the committed binary-faithful checkpoints of an existing pipeline job. */
    public List<Checkpoint> checkpoints(UUID jobId) {
        Objects.requireNonNull(jobId, "jobId");
        WorkflowRunRecord job = store.get(jobId)
                .orElseThrow(() -> WorkflowRunStoreException.notFound(jobId));
        Definition definition = readDefinition(job);
        return readCheckpoints(job, definition);
    }

    /**
     * Supplies binary-faithful responses for a parked external pipeline step, validating
     * cardinality, descriptor identity, and declared {@code validate_response} constraints
     * before committing the checkpoint and requeuing the job in one store transaction.
     */
    public ParkedCompletion completeStep(UUID jobId, String stepName,
                                         List<? extends Message> responses) {
        return completeStep(jobId, stepName, null, null, responses);
    }

    /**
     * Supplies binary-faithful responses for a parked external pipeline step while optionally
     * binding completion to expected pipeline and input fingerprints. A fingerprint mismatch
     * or altered response payload after completion is refused as {@link ParkedCompletion.Conflict}
     * without mutating the stored job.
     */
    public ParkedCompletion completeStep(UUID jobId, String stepName,
                                         String expectedPipelineFingerprint,
                                         String expectedInputFingerprint,
                                         List<? extends Message> responses) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(stepName, "stepName");
        Objects.requireNonNull(responses, "responses");
        if (stepName.isBlank()) {
            throw new IllegalArgumentException("stepName must not be blank");
        }
        WorkflowRunRecord job = store.get(jobId)
                .orElseThrow(() -> WorkflowRunStoreException.notFound(jobId));
        Definition definition = readDefinition(job);

        if ((expectedPipelineFingerprint != null
                        && !expectedPipelineFingerprint.equals(definition.pipelineFingerprint()))
                || (expectedInputFingerprint != null
                        && !expectedInputFingerprint.equals(definition.inputFingerprint()))) {
            return new ParkedCompletion.Conflict(job.status);
        }

        boolean parkedHere = WorkflowRunRecord.STATUS_WAITING.equals(job.status)
                && stepName.equals(job.outstandingStep);
        if (!parkedHere && !hasExternalCheckpoint(job, definition.pipeline(), stepName)) {
            return new ParkedCompletion.WrongState(job.status, job.outstandingStep);
        }

        PipelineStep step = requireExternalStep(definition.pipeline(), stepName);
        MethodDescriptor method = PipelineRunCodec.resolveMethod(
                definition.files(), step.getGrpcCall().getMethod());
        Descriptor outputType = method.getOutputType();
        EdgeCardinality cardinality = step.getGrpcCall().getOutputCardinality();
        List<DynamicMessage> canonicalResponses = canonicalizeResponses(
                responses, cardinality, definition.pipeline().getMaxStreamMessages(),
                outputType, stepName);
        String validationError = step.getGrpcCall().getValidateResponse()
                ? validateResponses(outputType, canonicalResponses)
                : null;

        Checkpoint candidate = new Checkpoint(
                definition.pipelineFingerprint(),
                definition.inputFingerprint(),
                new StepOutcome(stepName, false, 0, canonicalResponses.size()),
                cardinality,
                canonicalResponses,
                List.of());
        return store.completeParkedPipelineStep(
                jobId,
                stepName,
                PipelineCheckpointCodec.toProto(candidate),
                validationError == null
                        ? WorkflowRunEventFactory.stepCheckpoint(job, stepName)
                        : WorkflowRunEventFactory.failed(job, stepName, validationError),
                validationError);
    }

    /**
     * Parses proto3 JSON responses for {@code stepName} against a pipeline job's snapshotted
     * output descriptor (used by the {@code complete-step} action verb) and settles the step.
     */
    public ParkedCompletion completeStepJson(UUID jobId, String stepName, ObjectNode response,
                                             DescriptorRegistry extraRegistry)
            throws MalformedProtobufJsonException {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(stepName, "stepName");
        Objects.requireNonNull(response, "response");
        WorkflowRunRecord job = store.get(jobId)
                .orElseThrow(() -> WorkflowRunStoreException.notFound(jobId));
        Definition definition = readDefinition(job);

        boolean parkedHere = WorkflowRunRecord.STATUS_WAITING.equals(job.status)
                && stepName.equals(job.outstandingStep);
        if (!parkedHere && !hasExternalCheckpoint(job, definition.pipeline(), stepName)) {
            return new ParkedCompletion.WrongState(job.status, job.outstandingStep);
        }

        PipelineStep step = requireExternalStep(definition.pipeline(), stepName);
        MethodDescriptor method = PipelineRunCodec.resolveMethod(
                definition.files(), step.getGrpcCall().getMethod());
        List<DynamicMessage> parsed = PipelineExternalCompletionParser.parseResponses(
                response,
                step.getGrpcCall().getOutputCardinality(),
                method.getOutputType(),
                definition.pipeline().getMaxStreamMessages(),
                stepName,
                definition.files(),
                extraRegistry);
        return completeStep(jobId, stepName, parsed);
    }

    private static PipelineStep requireExternalStep(Pipeline pipeline, String stepName) {
        PipelineStep step = findStep(pipeline, stepName);
        if (step == null) {
            throw new IllegalArgumentException(
                    "the pipeline has no step named '" + stepName + "'");
        }
        if (!step.hasGrpcCall()
                || step.getGrpcCall().getCompletion() != StepCompletion.STEP_COMPLETION_EXTERNAL) {
            throw new IllegalArgumentException(
                    "pipeline step '" + stepName + "' is not an external-completion step");
        }
        return step;
    }

    private static List<DynamicMessage> canonicalizeResponses(List<? extends Message> responses,
                                                              EdgeCardinality cardinality,
                                                              int limit,
                                                              Descriptor outputType,
                                                              String stepName) {
        if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE && responses.size() != 1) {
            throw new IllegalArgumentException("external pipeline step '" + stepName
                    + "' expects exactly 1 response; got " + responses.size());
        }
        if (responses.size() > limit) {
            throw new IllegalArgumentException("external pipeline step '" + stepName
                    + "' responses (" + responses.size()
                    + ") exceed max_stream_messages " + limit);
        }
        List<DynamicMessage> canonical = new ArrayList<>(responses.size());
        for (Message response : responses) {
            Objects.requireNonNull(response, "response element");
            if (!PipelineRunCodec.matchesDescriptor(response.getDescriptorForType(), outputType)) {
                throw new IllegalArgumentException("external pipeline step '" + stepName
                        + "' expects " + outputType.getFullName() + "; got "
                        + response.getDescriptorForType().getFullName());
            }
            try {
                canonical.add(DynamicMessage.parseFrom(
                        outputType, MessageFingerprints.deterministicBytes(response)));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("external pipeline step '" + stepName
                        + "' response cannot be decoded as " + outputType.getFullName(), e);
            }
        }
        return canonical;
    }

    private static String validateResponses(Descriptor outputType,
                                            List<DynamicMessage> canonicalResponses) {
        ProtoValidator validator = ProtoValidator.forMessageType(outputType);
        StringBuilder violations = new StringBuilder();
        for (DynamicMessage response : canonicalResponses) {
            ValidationResult validation = validator.validate(response);
            if (!validation.valid()) {
                for (ValidationResult.Violation violation : validation.violations()) {
                    if (!violations.isEmpty()) {
                        violations.append("; ");
                    }
                    violations.append(violation.path())
                            .append(": ")
                            .append(violation.message());
                }
            }
        }
        return violations.isEmpty()
                ? null
                : "VALIDATION: complete-step response failed validation: " + violations;
    }

    private static PipelineStep findStep(Pipeline pipeline, String stepName) {
        for (PipelineStep step : pipeline.getStepsList()) {
            if (step.getName().equals(stepName)) {
                return step;
            }
        }
        return null;
    }

    private static boolean hasExternalCheckpoint(WorkflowRunRecord job, Pipeline pipeline,
                                                 String stepName) {
        PipelineStep step = findStep(pipeline, stepName);
        if (step == null
                || !step.hasGrpcCall()
                || step.getGrpcCall().getCompletion() != StepCompletion.STEP_COMPLETION_EXTERNAL) {
            return false;
        }
        for (PipelineRunCheckpoint checkpoint : storedCheckpoints(job, stepName)) {
            if (stepName.equals(checkpoint.getOutcome().getStep())
                    && !checkpoint.getOutcome().getSkipped()) {
                return true;
            }
        }
        return false;
    }

    private static List<PipelineRunCheckpoint> storedCheckpoints(WorkflowRunRecord job,
                                                                 String stepName) {
        if (job.pipelineCheckpoints == null) {
            return List.of();
        }
        try {
            return PipelineRunCheckpoints.parseFrom(job.pipelineCheckpoints)
                    .getCheckpointsList();
        } catch (InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException(stepName,
                    "stored checkpoints are not a readable PipelineRunCheckpoints: "
                            + e.getMessage(), e);
        }
    }
}
