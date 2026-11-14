package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.CelMappingRule;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.inference.structured.StructuredGenerationException;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.inference.v1.GenerateStructuredRequest;
import ai.protomolt.proto.mapper.cel.CelProtoMapper;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.pipeline.v1.StructuredStep;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.shapes.MessageScope;
import ai.protomolt.proto.shapes.ScopedProtoMapper;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Executes a statically checked {@link Pipeline} in process. The engine owns typed dataflow,
 * cardinality transitions, mapping, projection, validation, deadlines, and bounded fan-out;
 * a host-owned {@link PipelineTransport} owns endpoint resolution, credentials, and the live
 * gRPC calls. All four gRPC streaming shapes are finite inside one run and every materialized
 * stream is bounded by {@code pipeline.max_stream_messages}.
 */
public final class PipelineExecutor {

    /** Why a pipeline run stopped. */
    public enum FailureKind {
        PREFLIGHT, GATE, MAPPING, PROJECTION, VALIDATION, DEADLINE,
        GRPC, STRUCTURED, FAN_OUT, EXTERNAL, PIPELINE
    }

    /** A named, classified execution failure suitable for job retry policy. */
    public static final class PipelineExecutionException extends Exception {
        private final String step;
        private final FailureKind kind;
        private final Status.Code grpcCode;

        PipelineExecutionException(String step, FailureKind kind, Status.Code grpcCode,
                                   String message, Throwable cause) {
            super(message, cause);
            this.step = step;
            this.kind = kind;
            this.grpcCode = grpcCode;
        }

        public String step() {
            return step;
        }

        public FailureKind kind() {
            return kind;
        }

        public Status.Code grpcCode() {
            return grpcCode;
        }
    }

    /** One serial step's observable cardinality and skip outcome. */
    public record StepOutcome(String name, boolean skipped, int requestCount,
                              int responseCount) {
    }

    /** One stable fan-out branch outcome, including bounded failure evidence. */
    public record BranchOutcome(String id, int index, boolean succeeded, FailureKind kind,
                                String error) {
    }

    /** The final binding: one message or an ordered bounded stream. */
    public record Result(EdgeCardinality cardinality, List<DynamicMessage> messages,
                         List<StepOutcome> steps, List<BranchOutcome> branches) {
        public Result {
            messages = List.copyOf(messages);
            steps = List.copyOf(steps);
            branches = List.copyOf(branches);
            if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE
                    && messages.size() != 1) {
                throw new IllegalArgumentException("a ONE result contains exactly one message");
            }
        }

        /** Returns the single result, rejecting a stream result. */
        public DynamicMessage message() {
            if (cardinality != EdgeCardinality.EDGE_CARDINALITY_ONE) {
                throw new IllegalStateException("pipeline result is a stream");
            }
            return messages.getFirst();
        }
    }

    /**
     * Durable coordinator identity bound to one segment execution so live step calls carry
     * stable operation identity ({@code jobId/stepName} or {@code jobId/step#index}).
     *
     * @param jobId durable job identifier, or empty for in-process runs
     * @param attempt 1-based durable claim attempt, or {@code 0} for in-process runs
     */
    public record ExecutionIdentity(String jobId, int attempt) {
        /** Identity used by ordinary in-process runs with no coordinator. */
        public static final ExecutionIdentity IN_PROCESS = new ExecutionIdentity("", 0);

        public ExecutionIdentity {
            Objects.requireNonNull(jobId, "jobId");
            if (attempt < 0) {
                throw new IllegalArgumentException("attempt must not be negative");
            }
        }
    }

    /**
     * Typed step execution state for replay across suspended segments and worker restarts.
     * Persistence, claim fencing, and completion authorization belong to the job coordinator.
     *
     * @param pipelineFingerprint canonical pipeline and descriptor-closure identity
     * @param inputFingerprint canonical binary input identity
     * @param outcome step cardinality and skip outcome
     * @param cardinality output binding cardinality ({@code ONE} or {@code MANY})
     * @param messages binary-faithful output messages (default instance when a {@code ONE} step skips)
     * @param branches ordered fan-out branch evidence when the step executed fan-out
     */
    public record Checkpoint(String pipelineFingerprint, String inputFingerprint,
                             StepOutcome outcome, EdgeCardinality cardinality,
                             List<DynamicMessage> messages, List<BranchOutcome> branches) {
        public Checkpoint {
            Objects.requireNonNull(pipelineFingerprint, "pipelineFingerprint");
            Objects.requireNonNull(inputFingerprint, "inputFingerprint");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(cardinality, "cardinality");
            messages = List.copyOf(messages);
            branches = List.copyOf(branches);
        }
    }

    /** Outcome of executing one resumable pipeline segment. */
    public sealed interface Segment {
        /** The pipeline ran to completion and produced its final binding. */
        record Completed(Result result, List<Checkpoint> checkpoints) implements Segment {
            public Completed {
                Objects.requireNonNull(result, "result");
                checkpoints = List.copyOf(checkpoints);
            }
        }

        /**
         * Execution reached an unskipped {@code STEP_COMPLETION_EXTERNAL} step and yielded
         * without invoking transport.
         */
        record Parked(String step, String pipelineFingerprint, String inputFingerprint,
                      EdgeCardinality cardinality, List<Checkpoint> checkpoints)
                implements Segment {
            public Parked {
                Objects.requireNonNull(step, "step");
                Objects.requireNonNull(pipelineFingerprint, "pipelineFingerprint");
                Objects.requireNonNull(inputFingerprint, "inputFingerprint");
                Objects.requireNonNull(cardinality, "cardinality");
                checkpoints = List.copyOf(checkpoints);
            }

            /**
             * Builds a candidate external-completion checkpoint bound to this parked step's
             * pipeline and input fingerprints. Type, cardinality, and declared validation rules
             * are enforced on replay and by the durable coordinator.
             */
            public Checkpoint completion(List<DynamicMessage> responses) {
                Objects.requireNonNull(responses, "responses");
                return new Checkpoint(pipelineFingerprint, inputFingerprint,
                        new StepOutcome(step, false, 0, responses.size()), cardinality,
                        responses, List.of());
            }
        }
    }

    private record Binding(Descriptor type, EdgeCardinality cardinality,
                           List<DynamicMessage> values) {
        Binding {
            values = List.copyOf(values);
            if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE
                    && values.size() != 1) {
                throw new IllegalArgumentException("a ONE binding contains exactly one value");
            }
        }

        DynamicMessage one() {
            return values.getFirst();
        }
    }

    private final PipelineTransport transport;
    private final StructuredGenerator generator;

    public PipelineExecutor(PipelineTransport transport) {
        this(transport, null);
    }

    public PipelineExecutor(PipelineTransport transport, StructuredGenerator generator) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.generator = generator;
    }

    /**
     * Canonical SHA-256 identity binding a {@link Pipeline} and the transitive closure of
     * its {@link FileDescriptor} set.
     */
    public static String pipelineFingerprint(Pipeline pipeline, List<FileDescriptor> files) {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(files, "files");
        return ServiceProfileValidation.sha256(canonicalBytes(pipeline))
                + ":" + descriptorIdentity(files);
    }

    /** Canonical SHA-256 identity of a binary protobuf input message. */
    public static String inputFingerprint(Message input) {
        Objects.requireNonNull(input, "input");
        return ServiceProfileValidation.sha256(canonicalBytes(input));
    }

    /** Checks and executes one pipeline against the exact supplied descriptor set. */
    public Result run(Pipeline pipeline, List<FileDescriptor> files, DynamicMessage input)
            throws PipelineExecutionException {
        return switch (runSegment(pipeline, files, input, List.of(),
                ExecutionIdentity.IN_PROCESS, checkpoint -> { })) {
            case Segment.Completed completed -> completed.result();
            case Segment.Parked parked -> throw failure(parked.step(), FailureKind.EXTERNAL,
                    "external-completion step requires a durable job coordinator", null);
        };
    }

    /**
     * Replays an ordered checkpoint prefix and executes until completion or an external-completion
     * suspension with {@link ExecutionIdentity#IN_PROCESS}.
     */
    public Segment runSegment(Pipeline pipeline, List<FileDescriptor> files, DynamicMessage input,
                              List<Checkpoint> prior, Consumer<Checkpoint> onCheckpoint)
            throws PipelineExecutionException {
        return runSegment(pipeline, files, input, prior, ExecutionIdentity.IN_PROCESS,
                onCheckpoint);
    }

    /**
     * Replays an ordered checkpoint prefix and executes until completion or an unskipped
     * {@code STEP_COMPLETION_EXTERNAL} step. Each newly executed step notifies {@code onCheckpoint}
     * before the next step begins; an observer runtime exception halts execution immediately
     * with {@link FailureKind#PIPELINE} and preserves the original exception as {@link Throwable#getCause()}.
     */
    public Segment runSegment(Pipeline pipeline, List<FileDescriptor> files, DynamicMessage input,
                              List<Checkpoint> prior, ExecutionIdentity identity,
                              Consumer<Checkpoint> onCheckpoint)
            throws PipelineExecutionException {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(files, "files");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(prior, "prior");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(onCheckpoint, "onCheckpoint");
        prior = List.copyOf(prior);
        List<PipelineChecker.Finding> findings = new PipelineChecker().verify(pipeline, files);
        if (!findings.isEmpty()) {
            throw failure("", FailureKind.PREFLIGHT,
                    "pipeline failed static checking: " + findingsSummary(findings), null);
        }

        DescriptorRegistry registry = DescriptorRegistry.create(false);
        registerTransitiveFiles(files, registry);
        Descriptor inputType = registry.findDescriptorByFullName(pipeline.getInputType());
        if (!input.getDescriptorForType().getFullName().equals(inputType.getFullName())
                || !DescriptorFingerprints.fingerprintOf(input.getDescriptorForType())
                        .equals(DescriptorFingerprints.fingerprintOf(inputType))) {
            throw failure("", FailureKind.PREFLIGHT,
                    "pipeline input must be " + inputType.getFullName() + "; got "
                            + input.getDescriptorForType().getFullName(), null);
        }
        Map<String, ServiceDependency> dependencies = new LinkedHashMap<>();
        pipeline.getDependenciesList().forEach(dependency ->
                dependencies.put(dependency.getAlias(), dependency));
        Map<String, Binding> scope = new LinkedHashMap<>();
        scope.put("input", one(input));
        List<StepOutcome> outcomes = new ArrayList<>();
        List<BranchOutcome> branches = new ArrayList<>();
        long deadlineNanos = StepDeadlines.pipelineDeadline(pipeline);
        Binding last = scope.get("input");

        String currentPipelineFingerprint = pipelineFingerprint(pipeline, files);
        String currentInputFingerprint = inputFingerprint(input);
        if (prior.size() > pipeline.getStepsCount()) {
            throw failure("", FailureKind.PIPELINE,
                    "checkpoint prefix exceeds pipeline steps", null);
        }
        List<Checkpoint> checkpoints = new ArrayList<>(prior);
        int index = 0;
        for (PipelineStep step : pipeline.getStepsList()) {
            if (index < prior.size()) {
                Checkpoint saved = prior.get(index++);
                last = restore(step, saved, currentPipelineFingerprint, currentInputFingerprint,
                        scope, registry, files, pipeline.getMaxStreamMessages());
                scope.put(step.getName(), last);
                outcomes.add(saved.outcome());
                branches.addAll(saved.branches());
                continue;
            }
            int branchStart = branches.size();
            StepDeadlines.requireTime(step.getName(), deadlineNanos);
            if (step.hasUnnest()) {
                last = runUnnest(step, scope, pipeline.getMaxStreamMessages());
            } else if (step.hasCollect()) {
                last = runCollect(step, scope, registry);
            } else if (step.hasStructured()) {
                last = runStructured(step, scope, registry, deadlineNanos,
                        pipeline.getMaxStreamMessages(), branches);
            } else {
                if (step.getGrpcCall().getCompletion() == StepCompletion.STEP_COMPLETION_EXTERNAL
                        && (step.getWhen().isBlank() || evaluateGate(step, scope,
                                DescriptorSets.resolveMethod(files, step.getGrpcCall().getMethod())
                                        .getInputType()))) {
                    return new Segment.Parked(step.getName(), currentPipelineFingerprint,
                            currentInputFingerprint, step.getGrpcCall().getOutputCardinality(),
                            checkpoints);
                }
                last = runGrpc(step, scope, registry, files, dependencies, identity,
                        deadlineNanos, pipeline.getMaxStreamMessages(), outcomes, branches);
            }
            scope.put(step.getName(), last);
            if (!step.hasGrpcCall()) {
                outcomes.add(new StepOutcome(step.getName(), false, 0, last.values().size()));
            }
            Checkpoint checkpoint = new Checkpoint(currentPipelineFingerprint,
                    currentInputFingerprint, outcomes.getLast(), last.cardinality(),
                    last.values(), branches.subList(branchStart, branches.size()));
            try {
                onCheckpoint.accept(checkpoint);
            } catch (RuntimeException e) {
                throw failure(step.getName(), FailureKind.PIPELINE,
                        "checkpoint observer failed; persistence may be uncertain", e);
            }
            checkpoints.add(checkpoint);
        }

        if (pipeline.hasOutput()) {
            Descriptor outputType = registry.findDescriptorByFullName(
                    pipeline.getOutput().getType());
            DynamicMessage output = map(outputType, singleValues(scope),
                    pipeline.getOutput().getRulesList(),
                    pipeline.getOutput().getCelRulesList(), registry, "output");
            last = one(output);
        }
        return new Segment.Completed(
                new Result(last.cardinality(), last.values(), outcomes, branches),
                checkpoints);
    }

    private static void registerTransitiveFiles(List<FileDescriptor> files,
                                                DescriptorRegistry registry) {
        List<FileDescriptor> pending = new ArrayList<>(files);
        Map<String, FileDescriptor> seen = new LinkedHashMap<>();
        for (int i = 0; i < pending.size(); i++) {
            FileDescriptor file = pending.get(i);
            if (seen.putIfAbsent(file.getName(), file) == null) {
                registry.registerFile(file);
                pending.addAll(file.getDependencies());
            }
        }
    }

    private static String descriptorIdentity(List<FileDescriptor> files) {
        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        List<FileDescriptor> pending = new ArrayList<>(files);
        for (int i = 0; i < pending.size(); i++) {
            FileDescriptor file = pending.get(i);
            FileDescriptor existing = closure.putIfAbsent(file.getName(), file);
            if (existing == null) {
                pending.addAll(file.getDependencies());
            } else if (!existing.toProto().equals(file.toProto())) {
                throw new IllegalArgumentException("conflicting descriptor file: "
                        + file.getName());
            }
        }
        return DescriptorFingerprints.fingerprint(List.copyOf(closure.values()));
    }

    private static Binding restore(PipelineStep step, Checkpoint saved,
                                   String pipelineFingerprint, String inputFingerprint,
                                   Map<String, Binding> scope, DescriptorRegistry registry,
                                   List<FileDescriptor> files, int limit)
            throws PipelineExecutionException {
        if (!saved.pipelineFingerprint().equals(pipelineFingerprint)
                || !saved.inputFingerprint().equals(inputFingerprint)
                || !saved.outcome().name().equals(step.getName())) {
            throw failure(step.getName(), FailureKind.PIPELINE,
                    "checkpoint identity does not match the pipeline, input and ordered step",
                    null);
        }
        Descriptor type;
        EdgeCardinality cardinality = EdgeCardinality.EDGE_CARDINALITY_ONE;
        MethodDescriptor method = null;
        if (step.hasGrpcCall()) {
            method = DescriptorSets.resolveMethod(files, step.getGrpcCall().getMethod());
            type = step.getGrpcCall().hasFanOut()
                    ? registry.findDescriptorByFullName(
                            step.getGrpcCall().getFanOut().getCollectType())
                    : method.getOutputType();
            cardinality = step.getGrpcCall().getOutputCardinality();
        } else if (step.hasStructured()) {
            type = registry.findDescriptorByFullName(step.getStructured().hasFanOut()
                    ? step.getStructured().getFanOut().getCollectType()
                    : step.getStructured().getSpec().getTargetType());
        } else if (step.hasCollect()) {
            type = registry.findDescriptorByFullName(step.getCollect().getCollectType());
        } else {
            type = StreamPaths.repeatedField(
                    scope.get(step.getUnnest().getSource()).type(),
                    step.getUnnest().getPath()).getMessageType();
            cardinality = EdgeCardinality.EDGE_CARDINALITY_MANY;
        }
        boolean externalStep = step.hasGrpcCall()
                && step.getGrpcCall().getCompletion() == StepCompletion.STEP_COMPLETION_EXTERNAL;
        if (saved.cardinality() != cardinality
                || saved.messages().size() > limit
                || (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE
                        && saved.messages().size() != 1)
                || saved.outcome().requestCount() < 0
                || saved.outcome().responseCount() < 0
                || ((!step.hasGrpcCall() || externalStep) && saved.outcome().requestCount() != 0)
                || (!saved.outcome().skipped()
                        && saved.outcome().responseCount() != saved.messages().size())
                || (saved.outcome().skipped()
                        && (!step.hasGrpcCall() || step.getWhen().isBlank()
                                || saved.outcome().requestCount() != 0
                                || saved.outcome().responseCount() != 0))) {
            throw failure(step.getName(), FailureKind.PIPELINE,
                    "invalid checkpoint cardinality or outcome", null);
        }
        if (saved.outcome().skipped()
                && cardinality == EdgeCardinality.EDGE_CARDINALITY_MANY
                && !saved.messages().isEmpty()) {
            throw failure(step.getName(), FailureKind.PIPELINE,
                    "skipped stream checkpoint must be empty", null);
        }
        FanOutSpec fanOut = step.hasGrpcCall() && step.getGrpcCall().hasFanOut()
                ? step.getGrpcCall().getFanOut()
                : step.hasStructured() && step.getStructured().hasFanOut()
                        ? step.getStructured().getFanOut() : null;
        if ((fanOut == null || saved.outcome().skipped()) && !saved.branches().isEmpty()) {
            throw failure(step.getName(), FailureKind.PIPELINE,
                    "checkpoint has branch evidence for a step without executed fan-out", null);
        }
        if (fanOut != null && !saved.outcome().skipped()) {
            if (saved.branches().size() > fanOut.getMaxItems()
                    || saved.branches().size() > limit) {
                throw failure(step.getName(), FailureKind.PIPELINE,
                        "checkpoint branch evidence exceeds fan-out bounds", null);
            }
            for (int i = 0; i < saved.branches().size(); i++) {
                BranchOutcome branch = saved.branches().get(i);
                if (branch.index() != i
                        || !(step.getName() + "#" + i).equals(branch.id())
                        || branch.error() == null
                        || (branch.succeeded()
                                && (branch.kind() != null || !branch.error().isEmpty()))
                        || (!branch.succeeded()
                                && (branch.kind() == null || branch.error().isBlank()
                                        || fanOut.getFailurePolicy()
                                                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST))) {
                    throw failure(step.getName(), FailureKind.PIPELINE,
                            "invalid checkpoint branch evidence", null);
                }
            }
        }
        List<DynamicMessage> values = new ArrayList<>(saved.messages().size());
        for (DynamicMessage message : saved.messages()) {
            if (!message.getDescriptorForType().getFullName().equals(type.getFullName())
                    || !DescriptorFingerprints.fingerprintOf(message.getDescriptorForType())
                            .equals(DescriptorFingerprints.fingerprintOf(type))) {
                throw failure(step.getName(), FailureKind.PIPELINE,
                        "checkpoint response descriptor does not match " + type.getFullName(),
                        null);
            }
            if (saved.outcome().skipped()
                    && !message.equals(DynamicMessage.getDefaultInstance(
                            message.getDescriptorForType()))) {
                throw failure(step.getName(), FailureKind.PIPELINE,
                        "skipped checkpoint contains a response", null);
            }
            try {
                DynamicMessage restored = DynamicMessage.parseFrom(type, canonicalBytes(message));
                if (!saved.outcome().skipped()
                        && step.hasGrpcCall()
                        && step.getGrpcCall().getValidateResponse()
                        && !step.getGrpcCall().hasFanOut()) {
                    validate(restored, step.getName(), "checkpoint response");
                }
                values.add(restored);
            } catch (InvalidProtocolBufferException e) {
                throw failure(step.getName(), FailureKind.PIPELINE,
                        "checkpoint response cannot be decoded", e);
            }
        }
        if (fanOut != null && !saved.outcome().skipped()) {
            long successes = saved.branches().stream().filter(BranchOutcome::succeeded).count();
            FieldDescriptor collectedField = type.findFieldByName(fanOut.getCollectInto());
            int collected = values.getFirst().getRepeatedFieldCount(collectedField);
            if (step.hasGrpcCall() && step.getGrpcCall().getValidateResponse()) {
                for (int i = 0; i < collected; i++) {
                    validate(dynamic((Message) values.getFirst()
                                    .getRepeatedField(collectedField, i)),
                            step.getName(), "checkpoint branch response");
                }
            }
            if (successes != collected
                    || (step.hasGrpcCall()
                            && saved.outcome().requestCount() != saved.branches().size())) {
                throw failure(step.getName(), FailureKind.PIPELINE,
                        "checkpoint branches do not match the collected responses", null);
            }
        }
        if (step.hasCollect()) {
            scope.remove(step.getCollect().getSource());
        } else if (method != null) {
            consumeRequestStream(step.getGrpcCall(), method, scope);
        }
        return new Binding(type, cardinality, values);
    }

    private Binding runGrpc(PipelineStep step, Map<String, Binding> scope,
                            DescriptorRegistry registry, List<FileDescriptor> files,
                            Map<String, ServiceDependency> dependencies,
                            ExecutionIdentity identity,
                            long pipelineDeadlineNanos, int streamLimit,
                            List<StepOutcome> outcomes, List<BranchOutcome> branches)
            throws PipelineExecutionException {
        GrpcCallStep call = step.getGrpcCall();
        MethodDescriptor method = DescriptorSets.resolveMethod(files, call.getMethod());
        long stepDeadlineNanos = StepDeadlines.stepDeadline(step, pipelineDeadlineNanos);

        if (!step.getWhen().isBlank() && !evaluateGate(step, scope, method.getInputType())) {
            consumeRequestStream(call, method, scope);
            Binding skipped = call.hasFanOut()
                    ? one(DynamicMessage.getDefaultInstance(registry.findDescriptorByFullName(
                            call.getFanOut().getCollectType())))
                    : method.isServerStreaming()
                            ? many(method.getOutputType(), List.of())
                            : one(DynamicMessage.getDefaultInstance(method.getOutputType()));
            scope.put(step.getName(), skipped);
            outcomes.add(new StepOutcome(step.getName(), true, 0, 0));
            return skipped;
        }
        if (call.getCompletion() == StepCompletion.STEP_COMPLETION_EXTERNAL) {
            throw failure(step.getName(), FailureKind.EXTERNAL,
                    "external-completion step requires a durable job coordinator", null);
        }

        List<DynamicMessage> produced = mapEdge(call.getEdge(), scope, registry,
                step.getName());
        if (call.hasFanOut()) {
            Binding result = runFanOutGrpc(step, produced.getFirst(), method,
                    dependencies.get(step.getDependency()), registry, identity,
                    stepDeadlineNanos, streamLimit, branches);
            scope.put(step.getName(), result);
            outcomes.add(new StepOutcome(step.getName(), false,
                    fanOutItems(produced.getFirst(), call.getFanOut()).size(), 1));
            return result;
        }

        List<DynamicMessage> requests = deliver(produced, call.getEdge(), registry,
                step.getName());
        long callMillis = StepDeadlines.remainingMillis(step.getName(), stepDeadlineNanos);
        PipelineTransport.CallContext callContext = new PipelineTransport.CallContext(
                identity.jobId(), step.getName(), "", identity.attempt());
        List<DynamicMessage> responses;
        try {
            responses = transport.invoke(callContext, dependencies.get(step.getDependency()),
                    method, requests, callMillis, streamLimit + 1);
        } catch (StatusRuntimeException e) {
            throw failure(step.getName(), FailureKind.GRPC, e.getStatus().getCode(),
                    "gRPC " + e.getStatus().getCode() + ": "
                            + e.getStatus().getDescription(), e);
        } catch (RuntimeException e) {
            throw failure(step.getName(), FailureKind.GRPC, Status.Code.UNKNOWN,
                    "gRPC transport failed: " + e.getMessage(), e);
        }
        verifyResponseCount(step.getName(), method, responses, streamLimit);
        if (call.getValidateResponse()) {
            for (DynamicMessage response : responses) {
                validate(response, step.getName(), "response");
            }
        }
        consumeRequestStream(call, method, scope);
        Binding binding = method.isServerStreaming()
                ? many(method.getOutputType(), responses)
                : one(responses.getFirst());
        scope.put(step.getName(), binding);
        outcomes.add(new StepOutcome(step.getName(), false, requests.size(),
                responses.size()));
        return binding;
    }

    private Binding runStructured(PipelineStep step, Map<String, Binding> scope,
                                  DescriptorRegistry registry, long deadlineNanos,
                                  int streamLimit, List<BranchOutcome> branches)
            throws PipelineExecutionException {
        if (generator == null) {
            throw failure(step.getName(), FailureKind.STRUCTURED,
                    "structured step needs a StructuredGenerator", null);
        }
        long stepDeadlineNanos = StepDeadlines.stepDeadline(step, deadlineNanos);
        StructuredStep structured = step.getStructured();
        Descriptor target = registry.findDescriptorByFullName(
                structured.getSpec().getTargetType());
        if (!structured.hasEdge()) {
            return one(generate(step.getName(), structured, target, null,
                    stepDeadlineNanos));
        }
        List<DynamicMessage> produced = mapEdge(structured.getEdge(), scope, registry,
                step.getName());
        if (!structured.hasFanOut()) {
            DynamicMessage grounding = deliver(produced, structured.getEdge(), registry,
                    step.getName()).getFirst();
            return one(generate(step.getName(), structured, target, grounding,
                    stepDeadlineNanos));
        }
        return runFanOutStructured(step, produced.getFirst(), target, registry, streamLimit,
                stepDeadlineNanos, branches);
    }

    private Binding runUnnest(PipelineStep step, Map<String, Binding> scope, int limit)
            throws PipelineExecutionException {
        Binding source = scope.get(step.getUnnest().getSource());
        FieldDescriptor field = StreamPaths.repeatedField(source.type(),
                step.getUnnest().getPath());
        List<?> raw = StreamPaths.repeatedValues(source.one(), step.getUnnest().getPath());
        if (raw.size() > limit) {
            throw failure(step.getName(), FailureKind.PIPELINE,
                    "unnest produced " + raw.size() + " messages, over max_stream_messages "
                            + limit, null);
        }
        List<DynamicMessage> values = raw.stream()
                .map(value -> dynamic((Message) value)).toList();
        return many(field.getMessageType(), values);
    }

    private Binding runCollect(PipelineStep step, Map<String, Binding> scope,
                               DescriptorRegistry registry) throws PipelineExecutionException {
        Binding source = scope.remove(step.getCollect().getSource());
        Descriptor type = registry.findDescriptorByFullName(step.getCollect().getCollectType());
        FieldDescriptor field = type.findFieldByName(step.getCollect().getCollectInto());
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        source.values().forEach(value -> builder.addRepeatedField(field, value));
        return one(builder.build());
    }

    private Binding runFanOutGrpc(PipelineStep step, DynamicMessage holder,
                                  MethodDescriptor method, ServiceDependency dependency,
                                  DescriptorRegistry registry, ExecutionIdentity identity,
                                  long deadlineNanos, int streamLimit,
                                  List<BranchOutcome> branches)
            throws PipelineExecutionException {
        FanOutSpec fanOut = step.getGrpcCall().getFanOut();
        List<DynamicMessage> items = prepareFanOut(step.getName(), holder,
                step.getGrpcCall().getEdge(), fanOut, registry);
        if (items.size() > streamLimit) {
            throw failure(step.getName(), FailureKind.FAN_OUT,
                    "fan-out exceeds max_stream_messages " + streamLimit, null);
        }
        List<DynamicMessage> outputs = FanOutBranches.runIndexed(step.getName(), items, fanOut,
                (branchIndex, item) -> {
                    long callMillis = StepDeadlines.remainingMillis(step.getName(), deadlineNanos);
                    PipelineTransport.CallContext callContext = new PipelineTransport.CallContext(
                            identity.jobId(), step.getName(),
                            step.getName() + "#" + branchIndex, identity.attempt());
                    List<DynamicMessage> responses;
                    try {
                        responses = transport.invoke(callContext, dependency, method,
                                List.of(item), callMillis, 2);
                    } catch (StatusRuntimeException e) {
                        throw failure(step.getName(), FailureKind.GRPC,
                                e.getStatus().getCode(), "gRPC "
                                        + e.getStatus().getCode() + ": "
                                        + e.getStatus().getDescription(), e);
                    }
                    verifyResponseCount(step.getName(), method, responses, streamLimit);
                    if (step.getGrpcCall().getValidateResponse()) {
                        validate(responses.getFirst(), step.getName(), "branch response");
                    }
                    return responses.getFirst();
                }, deadlineNanos, branches);
        return one(collect(fanOut, outputs, registry));
    }

    private Binding runFanOutStructured(PipelineStep step, DynamicMessage holder,
                                        Descriptor target, DescriptorRegistry registry,
                                        int streamLimit, long deadlineNanos,
                                        List<BranchOutcome> branches)
            throws PipelineExecutionException {
        StructuredStep structured = step.getStructured();
        FanOutSpec fanOut = structured.getFanOut();
        List<DynamicMessage> items = prepareFanOut(step.getName(), holder,
                structured.getEdge(), fanOut, registry);
        if (items.size() > streamLimit) {
            throw failure(step.getName(), FailureKind.FAN_OUT,
                    "fan-out exceeds max_stream_messages " + streamLimit, null);
        }
        List<DynamicMessage> outputs = FanOutBranches.run(step.getName(), items, fanOut,
                grounding -> generate(step.getName(), structured, target, grounding,
                        deadlineNanos), deadlineNanos, branches);
        return one(collect(fanOut, outputs, registry));
    }

    private static List<DynamicMessage> mapEdge(TypedEdge edge,
                                                Map<String, Binding> scope,
                                                DescriptorRegistry registry,
                                                String step)
            throws PipelineExecutionException {
        Binding stream = null;
        for (String source : edge.getSourcesList()) {
            Binding binding = scope.get(source);
            if (binding.cardinality() == EdgeCardinality.EDGE_CARDINALITY_MANY) {
                stream = binding;
            }
        }
        Descriptor produceType = registry.findDescriptorByFullName(edge.getProduceType());
        List<DynamicMessage> produced = new ArrayList<>();
        int count = stream == null ? 1 : stream.values().size();
        for (int i = 0; i < count; i++) {
            Map<String, DynamicMessage> values = new LinkedHashMap<>();
            for (String source : edge.getSourcesList()) {
                Binding binding = scope.get(source);
                values.put(source, binding.cardinality()
                        == EdgeCardinality.EDGE_CARDINALITY_MANY
                        ? binding.values().get(i) : binding.one());
            }
            produced.add(map(produceType, values, edge.getRulesList(),
                    edge.getCelRulesList(), registry, step));
        }
        return produced;
    }

    private static List<DynamicMessage> deliver(List<DynamicMessage> produced,
                                                TypedEdge edge,
                                                DescriptorRegistry registry,
                                                String step)
            throws PipelineExecutionException {
        List<DynamicMessage> delivered = new ArrayList<>(produced.size());
        for (DynamicMessage value : produced) {
            DynamicMessage next = edge.getProjectTo().isEmpty()
                    ? value : project(value, edge.getProjectTo(), registry, step);
            if (edge.getValidate()) {
                validate(next, step, "edge value");
            }
            delivered.add(next);
        }
        return delivered;
    }

    private static List<DynamicMessage> prepareFanOut(String step, DynamicMessage holder,
                                                      TypedEdge edge, FanOutSpec fanOut,
                                                      DescriptorRegistry registry)
            throws PipelineExecutionException {
        List<DynamicMessage> raw = fanOutItems(holder, fanOut);
        if (raw.size() > fanOut.getMaxItems()) {
            throw failure(step, FailureKind.FAN_OUT,
                    "fan-out produced " + raw.size() + " items, over max_items "
                            + fanOut.getMaxItems(), null);
        }
        return deliver(raw, edge, registry, step);
    }

    private static List<DynamicMessage> fanOutItems(DynamicMessage holder,
                                                    FanOutSpec fanOut) {
        return StreamPaths.repeatedValues(holder, fanOut.getItems()).stream()
                .map(value -> dynamic((Message) value)).toList();
    }

    private static DynamicMessage collect(FanOutSpec fanOut,
                                          List<DynamicMessage> outputs,
                                          DescriptorRegistry registry) {
        Descriptor type = registry.findDescriptorByFullName(fanOut.getCollectType());
        FieldDescriptor field = type.findFieldByName(fanOut.getCollectInto());
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        outputs.forEach(output -> builder.addRepeatedField(field, output));
        return builder.build();
    }

    private DynamicMessage generate(String step, StructuredStep structured,
                                    Descriptor target, DynamicMessage grounding,
                                    long deadlineNanos)
            throws PipelineExecutionException {
        GenerateStructuredRequest.Builder request = GenerateStructuredRequest.newBuilder()
                .setTargetType(target.getFullName())
                .setModel(structured.getSpec().getModel())
                .setMaxAttempts(structured.getSpec().getMaxAttempts());
        if (grounding != null) {
            request.setGrounding(Any.pack(grounding));
        }
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        Future<ai.protomolt.proto.inference.v1.GenerateStructuredResponse> future =
                executor.submit(() -> generator.generate(request.build(), target));
        try {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                future.cancel(true);
                throw failure(step, FailureKind.DEADLINE,
                        "pipeline step deadline exhausted before structured generation",
                        null);
            }
            var response = future.get(remaining, TimeUnit.NANOSECONDS);
            return DynamicMessage.parseFrom(target, response.getMessage().getValue());
        } catch (InvalidProtocolBufferException e) {
            throw failure(step, FailureKind.STRUCTURED,
                    "structured result does not contain " + target.getFullName(), e);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw failure(step, FailureKind.DEADLINE,
                    "pipeline step deadline exhausted during structured generation", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failure(step, FailureKind.DEADLINE,
                    "interrupted during structured generation", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StructuredGenerationException structuredFailure) {
                throw failure(step, FailureKind.STRUCTURED,
                        "structured generation failed: " + structuredFailure.getMessage(),
                        structuredFailure);
            }
            throw failure(step, FailureKind.STRUCTURED,
                    "structured generation failed: " + cause.getMessage(), cause);
        } finally {
            executor.shutdownNow();
        }
    }

    private static DynamicMessage map(Descriptor type,
                                      Map<String, DynamicMessage> values,
                                      List<String> rules,
                                      List<CelMappingRule> celRules,
                                      DescriptorRegistry registry,
                                      String where)
            throws PipelineExecutionException {
        ScopedProtoMapper mapper = new ScopedProtoMapper(registry);
        MessageScope.Builder messageScope = MessageScope.builder();
        values.forEach(messageScope::add);
        DynamicMessage.Builder target = DynamicMessage.newBuilder(type);
        try {
            mapper.map(messageScope.build(), target, rules);
            if (!celRules.isEmpty()) {
                new CelProtoMapper(mapper.fieldMapper(), evaluator(values, type), "target",
                        new LinkedHashMap<>(values)).map(target,
                        CelMappingRules.toMapperRules(celRules));
            }
            return target.build();
        } catch (Exception e) {
            throw failure(where, FailureKind.MAPPING,
                    "mapping failed: " + e.getMessage(), e);
        }
    }

    private static DynamicMessage project(DynamicMessage source, String targetType,
                                          DescriptorRegistry registry, String step)
            throws PipelineExecutionException {
        Descriptor target = registry.findDescriptorByFullName(targetType);
        try {
            Message projected = MessageProjection.forTarget(target, registry)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "projection target declares no sources"))
                    .project(source);
            return dynamic(projected);
        } catch (Exception e) {
            throw failure(step, FailureKind.PROJECTION,
                    "edge projection failed: " + e.getMessage(), e);
        }
    }

    private static boolean evaluateGate(PipelineStep step, Map<String, Binding> scope,
                                        Descriptor target) throws PipelineExecutionException {
        Map<String, DynamicMessage> values = singleValues(scope);
        try {
            return evaluator(values, target).evaluateBooleanOrFail(step.getWhen(),
                    new LinkedHashMap<>(values));
        } catch (RuntimeException e) {
            throw failure(step.getName(), FailureKind.GATE,
                    "gate failed: " + e.getMessage(), e);
        }
    }

    private static CelEvaluator evaluator(Map<String, DynamicMessage> values,
                                          Descriptor target) {
        CelEnvironmentFactory factory = CelEnvironmentFactory.builder();
        values.forEach((name, value) ->
                factory.addMessageVar(name, value.getDescriptorForType()));
        factory.addMessageVar("target", target);
        return new CelEvaluator(factory.build());
    }

    private static Map<String, DynamicMessage> singleValues(Map<String, Binding> scope) {
        Map<String, DynamicMessage> values = new LinkedHashMap<>();
        scope.forEach((name, binding) -> {
            if (binding.cardinality() == EdgeCardinality.EDGE_CARDINALITY_ONE) {
                values.put(name, binding.one());
            }
        });
        return values;
    }

    private static void validate(DynamicMessage value, String step, String label)
            throws PipelineExecutionException {
        ValidationResult result = ProtoValidator.forMessageType(
                value.getDescriptorForType()).validate(value);
        if (!result.valid()) {
            throw failure(step, FailureKind.VALIDATION,
                    label + " failed validation: " + validationSummary(result), null);
        }
    }

    private static void verifyResponseCount(String step, MethodDescriptor method,
                                            List<DynamicMessage> responses, int limit)
            throws PipelineExecutionException {
        if (responses.size() > limit) {
            throw failure(step, FailureKind.PIPELINE,
                    "response stream exceeded max_stream_messages " + limit, null);
        }
        if (!method.isServerStreaming() && responses.size() != 1) {
            throw failure(step, FailureKind.GRPC,
                    "non-server-streaming method returned " + responses.size()
                            + " responses; expected exactly one", null);
        }
    }

    private static void consumeRequestStream(GrpcCallStep call, MethodDescriptor method,
                                             Map<String, Binding> scope) {
        if (!method.isClientStreaming() || !call.hasEdge()) {
            return;
        }
        call.getEdge().getSourcesList().stream()
                .filter(source -> {
                    Binding binding = scope.get(source);
                    return binding != null && binding.cardinality()
                            == EdgeCardinality.EDGE_CARDINALITY_MANY;
                })
                .findFirst().ifPresent(scope::remove);
    }

    private static Binding one(DynamicMessage value) {
        return new Binding(value.getDescriptorForType(),
                EdgeCardinality.EDGE_CARDINALITY_ONE, List.of(value));
    }

    private static Binding many(Descriptor type, List<DynamicMessage> values) {
        return new Binding(type, EdgeCardinality.EDGE_CARDINALITY_MANY, values);
    }

    private static DynamicMessage dynamic(Message message) {
        if (message instanceof DynamicMessage dynamic) {
            return dynamic;
        }
        try {
            return DynamicMessage.parseFrom(message.getDescriptorForType(),
                    message.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("message cannot be converted to its descriptor", e);
        }
    }

    static byte[] canonicalBytes(Message message) {
        byte[] bytes = new byte[message.getSerializedSize()];
        CodedOutputStream out = CodedOutputStream.newInstance(bytes);
        out.useDeterministicSerialization();
        try {
            message.writeTo(out);
            out.checkNoSpaceLeft();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory protobuf serialization failed", e);
        }
        return bytes;
    }

    private static String findingsSummary(List<PipelineChecker.Finding> findings) {
        return findings.stream().map(finding -> (finding.step().isEmpty()
                        ? "pipeline" : finding.step()) + "/" + finding.kind() + ": "
                        + finding.error()).reduce((left, right) -> left + "; " + right)
                .orElse("");
    }

    private static String validationSummary(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + ": " + violation.message())
                .reduce((left, right) -> left + "; " + right).orElse("");
    }

    /** Builds a classified failure that carries no gRPC status code. */
    static PipelineExecutionException failure(String step, FailureKind kind,
                                              String message, Throwable cause) {
        return failure(step, kind, null, message, cause);
    }

    /** Builds a classified failure, with the gRPC status code when one applies. */
    static PipelineExecutionException failure(String step, FailureKind kind,
                                              Status.Code grpcCode,
                                              String message, Throwable cause) {
        return new PipelineExecutionException(step, kind, grpcCode, message, cause);
    }
}
