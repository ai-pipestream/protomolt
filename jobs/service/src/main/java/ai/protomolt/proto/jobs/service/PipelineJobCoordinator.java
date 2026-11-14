package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.ParkedCompletion;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreException;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.PipelineExecutor.Result;
import ai.protomolt.proto.pipeline.PipelineExecutor.StepOutcome;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final ObjectMapper JSON = new ObjectMapper();

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

    /** Returns {@code true} when {@code job} carries a durable pipeline definition envelope. */
    public static boolean isPipelineJob(WorkflowRunRecord job) {
        return job != null && PipelineRunCodec.isPipelineDefinition(job.workflowDefinition);
    }

    /** Reconstructs and statically verifies the durable pipeline snapshot stored on {@code job}. */
    public static Definition readDefinition(WorkflowRunRecord job) {
        return PipelineRunCodec.readDefinition(job);
    }

    /** Reconstructs the binary-faithful checkpoint prefix stored on {@code job}. */
    public static List<Checkpoint> readCheckpoints(WorkflowRunRecord job, Definition definition) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(definition, "definition");
        return PipelineRunCodec.readCheckpoints(job, definition);
    }

    /** Encodes an ordered checkpoint prefix for persistence on {@code workflow_run.checkpoints}. */
    public static String writeCheckpoints(List<Checkpoint> checkpoints,
                                          List<FileDescriptor> files) {
        Objects.requireNonNull(checkpoints, "checkpoints");
        Objects.requireNonNull(files, "files");
        return PipelineRunCodec.writeCheckpoints(checkpoints, files);
    }

    /** Encodes a completed pipeline result as a JSON object for {@code workflow_run.result}. */
    public static String writeResult(Result result, List<FileDescriptor> files) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(files, "files");
        return PipelineRunCodec.writeResult(result, files);
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

        PipelineStep step = findStep(definition.pipeline(), stepName);
        if (step == null) {
            throw new IllegalArgumentException(
                    "the pipeline has no step named '" + stepName + "'");
        }
        if (!step.hasGrpcCall()
                || step.getGrpcCall().getCompletion() != StepCompletion.STEP_COMPLETION_EXTERNAL) {
            throw new IllegalArgumentException(
                    "pipeline step '" + stepName + "' is not an external-completion step");
        }

        MethodDescriptor method = PipelineRunCodec.resolveMethod(
                definition.files(), step.getGrpcCall().getMethod());
        Descriptor outputType = method.getOutputType();
        EdgeCardinality cardinality = step.getGrpcCall().getOutputCardinality();
        if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE && responses.size() != 1) {
            throw new IllegalArgumentException("external pipeline step '" + stepName
                    + "' expects exactly 1 response; got " + responses.size());
        }
        int limit = definition.pipeline().getMaxStreamMessages();
        if (responses.size() > limit) {
            throw new IllegalArgumentException("external pipeline step '" + stepName
                    + "' responses (" + responses.size()
                    + ") exceed max_stream_messages " + limit);
        }

        List<DynamicMessage> canonicalResponses = new ArrayList<>(responses.size());
        for (Message response : responses) {
            Objects.requireNonNull(response, "response element");
            if (!PipelineRunCodec.matchesDescriptor(response.getDescriptorForType(), outputType)) {
                throw new IllegalArgumentException("external pipeline step '" + stepName
                        + "' expects " + outputType.getFullName() + "; got "
                        + response.getDescriptorForType().getFullName());
            }
            try {
                canonicalResponses.add(DynamicMessage.parseFrom(
                        outputType, PipelineRunCodec.canonicalBytes(response)));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("external pipeline step '" + stepName
                        + "' response cannot be decoded as " + outputType.getFullName(), e);
            }
        }

        String validationError = null;
        if (step.getGrpcCall().getValidateResponse()) {
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
            if (!violations.isEmpty()) {
                validationError = "VALIDATION: complete-step response failed validation: "
                        + violations;
            }
        }

        Checkpoint candidate = new Checkpoint(
                definition.pipelineFingerprint(),
                definition.inputFingerprint(),
                new StepOutcome(stepName, false, 0, canonicalResponses.size()),
                cardinality,
                canonicalResponses,
                List.of());
        String entryJson = PipelineRunCodec.writeCheckpointEntry(candidate, definition.files());
        return store.completeParkedStep(
                jobId,
                stepName,
                entryJson,
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

        PipelineStep step = findStep(definition.pipeline(), stepName);
        if (step == null) {
            throw new IllegalArgumentException(
                    "the pipeline has no step named '" + stepName + "'");
        }
        if (!step.hasGrpcCall()
                || step.getGrpcCall().getCompletion() != StepCompletion.STEP_COMPLETION_EXTERNAL) {
            throw new IllegalArgumentException(
                    "pipeline step '" + stepName + "' is not an external-completion step");
        }
        MethodDescriptor method = PipelineRunCodec.resolveMethod(
                definition.files(), step.getGrpcCall().getMethod());
        List<DynamicMessage> parsed = PipelineRunCodec.parseExternalJsonResponses(
                response,
                step.getGrpcCall().getOutputCardinality(),
                method.getOutputType(),
                definition.pipeline().getMaxStreamMessages(),
                stepName,
                definition.files(),
                extraRegistry);
        return completeStep(jobId, stepName, parsed);
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
        try {
            JsonNode tree = JSON.readTree(job.checkpoints == null ? "[]" : job.checkpoints);
            if (tree instanceof ArrayNode array) {
                for (JsonNode entry : array) {
                    if (stepName.equals(entry.path("name").asText())
                            && !entry.path("skipped").asBoolean(false)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }
}
