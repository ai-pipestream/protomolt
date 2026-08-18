package ai.pipestream.proto.workflow;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.grpc.policy.OutboundChannelPolicyException;
import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import ai.pipestream.proto.inference.structured.StructuredGenerationException;
import ai.pipestream.proto.inference.structured.StructuredGenerator;
import ai.pipestream.proto.inference.v1.GenerateStructuredRequest;
import ai.pipestream.proto.inference.v1.GenerateStructuredResponse;
import ai.pipestream.proto.shapes.MessageScope;
import ai.pipestream.proto.shapes.ScopedProtoMapper;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Executes a workflow: serial, fail-fast, deadline-bounded. Each step's request is built from
 * the scope (the workflow {@code input} plus every prior step's response) with scoped text
 * rules and CEL (expressions also see {@code target}, the progressive request); a false
 * {@code when} gate skips the step, binding its name to the output type's default instance
 * so later references stay well-defined; {@code validate} runs the response's declared
 * rules before proceeding. Nothing persists between calls — a workflow execution lives inside one
 * invocation, by design.
 */
public final class WorkflowRunner {

    /** Receives bounded in-memory step fixtures while a run is executing. */
    public interface ExecutionObserver {

        /** Called after request mapping and immediately before the live gRPC call. */
        default void stepStarted(CompiledWorkflow.Step step, DynamicMessage request,
                                 Instant startedAt) {
        }

        /** Called for a successful or gate-skipped step. Skipped steps have null fixtures. */
        default void stepCompleted(CompiledWorkflow.Step step, DynamicMessage request,
                                   DynamicMessage response, boolean skipped,
                                   Instant startedAt, Instant completedAt) {
        }

        /** Called immediately before the structured-generation coordinator runs. */
        default void structuredStepStarted(CompiledWorkflow.Step step,
                                           GenerateStructuredRequest request,
                                           Instant startedAt) {
        }

        /**
         * Called for a successful structured-generation step. {@code output} is the
         * unpacked, validated typed message bound into the workflow scope;
         * {@code structuredResponse} is the coordinator's full envelope (attempt
         * history, fingerprints, provider provenance) so a recorder can persist
         * attempt evidence. The envelope's raw attempt texts must never be persisted.
         */
        default void structuredStepCompleted(CompiledWorkflow.Step step,
                                             GenerateStructuredRequest request,
                                             Message output,
                                             GenerateStructuredResponse structuredResponse,
                                             Instant startedAt, Instant completedAt) {
        }

        /**
         * Called after a typed edge produced its value and before the step executes:
         * {@code produced} is the mapped, pre-projection message (the fan-out items
         * holder); {@code validationPassed} is the verdict of the edge's declared
         * validation (true when the edge declares none); {@code itemCount} is the
         * fan-out cardinality, 0 without fan-out. A {@code false} verdict means the
         * step was rejected before any invocation.
         */
        default void edgeEvaluated(CompiledWorkflow.Step step, DynamicMessage produced,
                                   boolean validationPassed, int sourceCount,
                                   int itemCount) {
        }

        /**
         * Called once per fan-out branch, in stable index order after the fan-out
         * settles. {@code response} is the branch output, null on failure;
         * {@code failureSummary} is the bounded failure detail, null on success.
         */
        default void branchCompleted(CompiledWorkflow.Step step, String branchId,
                                     int branchIndex, Message response,
                                     String failureSummary) {
        }
    }

    private static final ExecutionObserver NO_OBSERVER = new ExecutionObserver() {
    };

    /** Opens the channel a step calls through; a test seam and a TLS/policy hook. */
    public interface ChannelFactory {
        ManagedChannel open(CompiledWorkflow.Step step);

        /** Validates a step target before the workflow performs its first network operation. */
        default void validateTarget(CompiledWorkflow.Step step) {
        }

        /** Validates the effective per-call deadline against the host policy. */
        default void validateDeadline(long deadlineMillis) {
        }
    }

    /** How the workflow ended per step: executed or gate-skipped. */
    public record StepOutcome(String name, boolean skipped) {
    }

    public record Result(Message output, List<StepOutcome> steps) {
    }

    /**
     * One persisted step response: the unit a workflow run checkpoints and resumes from.
     * {@code skipped} steps carry a null {@code response} (the output type's default
     * instance is bound on replay, exactly as a live skip does).
     */
    public record Checkpoint(String name, boolean skipped, Message response) {
    }

    /**
     * The outcome of one execution segment: either the workflow ran to completion, or it
     * parked on an {@code external}-completion step. Both carry the full checkpoint
     * prefix (replayed plus newly executed) so the caller persists one list.
     */
    public sealed interface Segment {

        /** The workflow completed; {@code result} is the composed output. */
        record Completed(Result result, List<Checkpoint> checkpoints) implements Segment {
        }

        /** The workflow parked on {@code step}, awaiting {@code complete-step}. */
        record Parked(String step, List<Checkpoint> checkpoints) implements Segment {
        }
    }

    /** Why a step failed; the jobs worker's retry decision rides on this. */
    public enum FailureKind {
        /** The 'when' gate could not be evaluated. Not retryable (deterministic). */
        GATE,
        /** Request/output mapping failed. Not retryable (deterministic). */
        MAPPING,
        /** The step's gRPC call returned a status; retryable per the exception's grpcCode. */
        GRPC,
        /** A structured-generation step failed in the coordinator or a provider. */
        STRUCTURED,
        /** A typed edge could not map, project, or bound the step input. Not
         *  retryable (deterministic). */
        EDGE,
        /** The response failed its declared validation rules. A verdict, not an error. */
        VALIDATION,
        /** The workflow (segment) deadline was exhausted. Retryable: resume continues. */
        DEADLINE,
        /** Synchronous execution reached an external-completion step. Not retryable. */
        EXTERNAL,
        /** Workflow-level corruption (e.g. checkpoints no longer match the definition). */
        WORKFLOW
    }

    /** A step failed: gRPC status, gate/mapping evaluation, or response validation. */
    public static final class WorkflowExecutionException extends Exception {
        private final String step;
        private final FailureKind kind;
        private final io.grpc.Status.Code grpcCode;

        public WorkflowExecutionException(String step, FailureKind kind,
                                       io.grpc.Status.Code grpcCode,
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

        /** The gRPC status code when {@link #kind()} is {@link FailureKind#GRPC}. */
        public io.grpc.Status.Code grpcCode() {
            return grpcCode;
        }
    }

    private final ChannelFactory channels;
    private final StructuredGenerator generator;
    // The workflow budget's clock. Injectable so the deadline arithmetic is
    // testable without racing the wall clock (a parallel build's scheduler
    // noise once flaked a 200ms-budget test); production always ticks real
    // nanoTime.
    private final java.util.function.LongSupplier nanoClock;

    public WorkflowRunner() {
        this(OutboundChannelPolicy.defaults());
    }

    /** Creates a runner using a host-configured shared outbound channel policy. */
    public WorkflowRunner(OutboundChannelPolicy policy) {
        this(policyFactory(Objects.requireNonNull(policy, "channel policy")));
    }

    /** Creates a policy-backed runner that can execute structured steps. */
    public WorkflowRunner(OutboundChannelPolicy policy, StructuredGenerator generator) {
        this(policyFactory(Objects.requireNonNull(policy, "channel policy")), generator);
    }

    private static ChannelFactory policyFactory(OutboundChannelPolicy policy) {
        return new ChannelFactory() {
            @Override
            public ManagedChannel open(CompiledWorkflow.Step step) {
                return policy.open(step.target(), step.tls(), canonical -> {
                    ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(canonical);
                    if (!step.tls()) {
                        builder.usePlaintext();
                    }
                    return builder.build();
                });
            }

            @Override
            public void validateTarget(CompiledWorkflow.Step step) {
                policy.validateTarget(step.target(), step.tls());
            }

            @Override
            public void validateDeadline(long deadlineMillis) {
                policy.validateDeadline(deadlineMillis);
            }
        };
    }

    public WorkflowRunner(ChannelFactory channels) {
        this(channels, null);
    }

    /**
     * Creates a runner that can also execute structured-generation steps through
     * {@code generator}. A structured step on a runner built without a generator
     * fails fast, naming the step.
     */
    public WorkflowRunner(ChannelFactory channels, StructuredGenerator generator) {
        this(channels, generator, System::nanoTime);
    }

    /** Visible for tests: a runner whose workflow budget ticks {@code nanoClock}. */
    WorkflowRunner(ChannelFactory channels, StructuredGenerator generator,
            java.util.function.LongSupplier nanoClock) {
        this.channels = channels;
        this.generator = generator;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    /**
     * Synchronous execution: the whole workflow inside one invocation. Rejects
     * external-completion steps — parking needs a job; submit the workflow with
     * {@code submit-workflow} instead.
     */
    public Result run(CompiledWorkflow workflow, DynamicMessage input)
            throws WorkflowExecutionException {
        return run(workflow, input, NO_OBSERVER);
    }

    /** Runs a workflow while reporting the exact request and response fixtures per step. */
    public Result run(CompiledWorkflow workflow, DynamicMessage input, ExecutionObserver observer)
            throws WorkflowExecutionException {
        Segment segment = runSegment(workflow, input, List.of(), checkpoint -> { }, observer);
        if (segment instanceof Segment.Completed completed) {
            return completed.result();
        }
        Segment.Parked parked = (Segment.Parked) segment;
        throw new WorkflowExecutionException(parked.step(), FailureKind.EXTERNAL, null,
                "step declares completion='external'; synchronous run-workflow cannot park "
                        + "- submit the workflow as a job (submit-workflow) so complete-step "
                        + "can supply the response", null);
    }

    /**
     * Executes one segment of a workflow with no prior checkpoints and no observer.
     *
     * @see #runSegment(CompiledWorkflow, DynamicMessage, List, Consumer)
     */
    public Segment runSegment(CompiledWorkflow workflow, DynamicMessage input,
                              List<Checkpoint> prior) throws WorkflowExecutionException {
        return runSegment(workflow, input, prior, checkpoint -> { }, NO_OBSERVER);
    }

    /**
     * Executes one segment of a workflow: replays the checkpoint prefix, then runs steps
     * until the workflow completes or an external-completion step parks it. The workflow
     * deadline bounds this segment, not the job's wall-clock life — a workflow parked for
     * hours of human review must not hold a 30-second budget across the park.
     *
     * @param prior the persisted checkpoint prefix; entry i must name step i of the
     *        workflow, so a workflow definition edited under a live job fails loud instead of
     *        resuming against a shifted scope
     * @param onCheckpoint called with each newly executed (or gate-skipped) step's
     *        checkpoint as it lands — the jobs worker's per-step persistence hook.
     *        Replayed checkpoints are not re-reported. A throwing observer fails the
     *        segment as {@link FailureKind#WORKFLOW}: a checkpoint that did not persist
     *        must not be treated as done.
     */
    public Segment runSegment(CompiledWorkflow workflow, DynamicMessage input,
                              List<Checkpoint> prior, Consumer<Checkpoint> onCheckpoint)
            throws WorkflowExecutionException {
        return runSegment(workflow, input, prior, onCheckpoint, NO_OBSERVER);
    }

    /** Executes a segment with both durable-checkpoint and evidence observers. */
    public Segment runSegment(CompiledWorkflow workflow, DynamicMessage input,
                              List<Checkpoint> prior, Consumer<Checkpoint> onCheckpoint,
                              ExecutionObserver observer)
            throws WorkflowExecutionException {
        Objects.requireNonNull(observer, "observer");
        long deadlineNanos = nanoClock.getAsLong()
                + TimeUnit.MILLISECONDS.toNanos(workflow.deadlineMs());
        DescriptorRegistry registry = DescriptorRegistry.create();
        for (FileDescriptor file : workflow.files()) {
            registry.registerFile(file);
        }
        ScopedProtoMapper mapper = new ScopedProtoMapper(registry);

        Map<String, Message> values = new LinkedHashMap<>();
        values.put("input", input);
        List<StepOutcome> outcomes = new ArrayList<>();
        List<Checkpoint> checkpoints = new ArrayList<>(prior);
        Map<String, ManagedChannel> open = new LinkedHashMap<>();
        try {
            Message last = input;
            int index = 0;
            for (CompiledWorkflow.Step step : workflow.steps()) {
                if (index < prior.size()) {
                    Checkpoint done = prior.get(index);
                    if (!done.name().equals(step.name())) {
                        throw new WorkflowExecutionException(step.name(), FailureKind.WORKFLOW, null,
                                "checkpoint " + index + " belongs to step '" + done.name()
                                        + "' but the workflow's step " + index + " is '"
                                        + step.name() + "'; the workflow definition changed "
                                        + "under the job", null);
                    }
                    Message replayed = done.skipped()
                            ? DynamicMessage.getDefaultInstance(outputTypeOf(step))
                            : done.response();
                    values.put(step.name(), replayed);
                    outcomes.add(new StepOutcome(step.name(), done.skipped()));
                    last = replayed;
                    index++;
                    continue;
                }
                if (step.external()) {
                    if (step.edge() != null) {
                        throw new WorkflowExecutionException(step.name(), FailureKind.WORKFLOW,
                                null, "external-completion steps do not carry edges; "
                                        + "the completion lane owns their request", null);
                    }
                    return new Segment.Parked(step.name(), List.copyOf(checkpoints));
                }
                if (step.structured() == null && step.when() != null
                        && !step.when().isBlank()) {
                    CelEvaluator gateEvaluator = evaluator(values,
                            step.method().getInputType());
                    boolean go;
                    try {
                        go = gateEvaluator.evaluateBooleanOrFail(step.when(),
                                Map.copyOf(values));
                    } catch (Exception e) {
                        throw new WorkflowExecutionException(step.name(), FailureKind.GATE, null,
                                "gate failed: " + e.getMessage(), e);
                    }
                    if (!go) {
                        // A skipped step still binds its name: the well-defined default
                        // instance of its output type. Later rules, gates, and the output
                        // mapping see deterministic empty values - the same scope the
                        // verifier checked - instead of an undeclared reference.
                        values.put(step.name(), DynamicMessage
                                .getDefaultInstance(outputTypeOf(step)));
                        outcomes.add(new StepOutcome(step.name(), true));
                        Instant skippedAt = Instant.now();
                        observer.stepCompleted(step, null, null, true, skippedAt, skippedAt);
                        record(checkpoints, new Checkpoint(step.name(), true, null),
                                onCheckpoint, step.name());
                        index++;
                        continue;
                    }
                }
                if (step.edge() != null) {
                    last = runEdgeStep(step, values, mapper, registry, open, outcomes,
                            checkpoints, onCheckpoint, observer, deadlineNanos);
                    index++;
                    continue;
                }
                if (step.structured() != null) {
                    Message generated = runStructured(step, values, outcomes, checkpoints,
                            onCheckpoint, observer, deadlineNanos);
                    last = generated;
                    index++;
                    continue;
                }
                DynamicMessage request = buildMessage(mapper,
                        evaluator(values, step.method().getInputType()), values,
                        step.method().getInputType(), step.rules(), step.celRules(),
                        step.name());
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(
                        deadlineNanos - nanoClock.getAsLong());
                if (remainingMs <= 0) {
                    throw new WorkflowExecutionException(step.name(), FailureKind.DEADLINE, null,
                            "workflow deadline exhausted before the step ran", null);
                }
                long callMs = step.deadlineMs() > 0
                        ? Math.min(step.deadlineMs(), remainingMs)
                        : remainingMs;
                try {
                    channels.validateTarget(step);
                    channels.validateDeadline(callMs);
                } catch (IllegalArgumentException e) {
                    throw new WorkflowExecutionException(step.name(), FailureKind.WORKFLOW, null,
                            "outbound channel policy rejected step: " + e.getMessage(), e);
                }
                ManagedChannel channel;
                try {
                    channel = open.computeIfAbsent(
                            step.target() + (step.tls() ? "+tls" : ""),
                            key -> channels.open(step));
                } catch (OutboundChannelPolicyException e) {
                    throw new WorkflowExecutionException(step.name(), FailureKind.GRPC,
                            io.grpc.Status.Code.RESOURCE_EXHAUSTED,
                            "outbound channel policy rejected step: " + e.getMessage(), e);
                }
                DynamicMessage response;
                Instant stepStarted = Instant.now();
                observer.stepStarted(step, request, stepStarted);
                try {
                    response = DynamicGrpcCalls.call(channel, step.method(), request,
                            CallOptions.DEFAULT.withDeadlineAfter(callMs, TimeUnit.MILLISECONDS),
                            new Metadata(), 1).get(0);
                } catch (StatusRuntimeException e) {
                    throw new WorkflowExecutionException(step.name(), FailureKind.GRPC,
                            e.getStatus().getCode(),
                            "gRPC " + e.getStatus().getCode() + " from " + step.target()
                                    + ": " + e.getStatus().getDescription(), e);
                }
                if (step.validate()) {
                    ValidationResult result = ProtoValidator
                            .forMessageType(step.method().getOutputType())
                            .validate(response);
                    if (!result.valid()) {
                        throw new WorkflowExecutionException(step.name(), FailureKind.VALIDATION,
                                null, "response failed validation: " + summary(result), null);
                    }
                }
                values.put(step.name(), response);
                outcomes.add(new StepOutcome(step.name(), false));
                observer.stepCompleted(step, request, response, false, stepStarted, Instant.now());
                record(checkpoints, new Checkpoint(step.name(), false, response),
                        onCheckpoint, step.name());
                last = response;
                index++;
            }
            Message output = workflow.output() == null
                    ? last
                    : buildMessage(mapper, evaluator(values, workflow.output().type()), values,
                            workflow.output().type(), workflow.output().rules(),
                            workflow.output().celRules(), "output");
            return new Segment.Completed(new Result(output, List.copyOf(outcomes)),
                    List.copyOf(checkpoints));
        } finally {
            for (ManagedChannel channel : open.values()) {
                channel.shutdown();
            }
        }
    }

    /**
     * Executes one structured-generation step: the coordinator fills the step's target
     * type with the step's model, and the unpacked, validated message binds into the
     * scope exactly like a gRPC step's response. A coordinator failure fails the workflow
     * with the step named, like a gRPC failure.
     *
     * @return the unpacked typed output, bound as the step's scope response
     */
    private Message runStructured(CompiledWorkflow.Step step, Map<String, Message> values,
                                  List<StepOutcome> outcomes, List<Checkpoint> checkpoints,
                                  Consumer<Checkpoint> onCheckpoint,
                                  ExecutionObserver observer, long deadlineNanos)
            throws WorkflowExecutionException {
        GenerateStructuredRequest request = structuredRequest(step, null);
        Instant stepStarted = Instant.now();
        observer.structuredStepStarted(step, request, stepStarted);
        GenerateStructuredResponse generated = invokeCoordinator(step, request,
                deadlineNanos);
        Message output = unpackStructured(step, generated);
        values.put(step.name(), output);
        outcomes.add(new StepOutcome(step.name(), false));
        observer.structuredStepCompleted(step, request, output, generated, stepStarted,
                Instant.now());
        record(checkpoints, new Checkpoint(step.name(), false, output), onCheckpoint,
                step.name());
        return output;
    }

    /** The step's structured-generation request, with grounding when supplied. */
    private static GenerateStructuredRequest structuredRequest(CompiledWorkflow.Step step,
                                                               Any grounding) {
        GenerateStructuredRequest.Builder request = GenerateStructuredRequest.newBuilder()
                .setTargetType(step.structured().targetType().getFullName())
                .setModel(step.structured().model())
                .setMaxAttempts(step.structured().maxAttempts());
        if (grounding != null) {
            request.setGrounding(grounding);
        }
        return request.build();
    }

    /** Runs the coordinator for one structured request, failing the workflow on rejection. */
    private GenerateStructuredResponse invokeCoordinator(CompiledWorkflow.Step step,
                                                         GenerateStructuredRequest request,
                                                         long deadlineNanos)
            throws WorkflowExecutionException {
        if (generator == null) {
            throw new WorkflowExecutionException(step.name(), FailureKind.STRUCTURED, null,
                    "structured step '" + step.name() + "' needs a StructuredGenerator; "
                            + "this runner was built without one", null);
        }
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - nanoClock.getAsLong());
        if (remainingMs <= 0) {
            throw new WorkflowExecutionException(step.name(), FailureKind.DEADLINE, null,
                    "workflow deadline exhausted before the step ran", null);
        }
        try {
            return generator.generate(request, step.structured().targetType());
        } catch (StructuredGenerationException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.STRUCTURED, null,
                    "structured generation failed: " + e.getMessage(), e);
        }
    }

    /** The coordinator's packed output as the step's typed message. */
    private static Message unpackStructured(CompiledWorkflow.Step step,
                                            GenerateStructuredResponse generated)
            throws WorkflowExecutionException {
        try {
            return DynamicMessage.parseFrom(step.structured().targetType(),
                    generated.getMessage().getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.WORKFLOW, null,
                    "the coordinator's packed output does not parse as the step's "
                            + "target type", e);
        }
    }

    /**
     * Executes one edge-carrying step: map the edge's declared sources into the
     * produced type, then either deliver the (projected, validated) value to one
     * invocation or fan out over its items. The produced message and the verdict are
     * reported to the observer before anything executes, so a pre-invocation
     * rejection still leaves complete evidence.
     *
     * @return the message bound into the scope under the step's name
     */
    private Message runEdgeStep(CompiledWorkflow.Step step, Map<String, Message> values,
                                ScopedProtoMapper mapper, DescriptorRegistry registry,
                                Map<String, ManagedChannel> open,
                                List<StepOutcome> outcomes, List<Checkpoint> checkpoints,
                                Consumer<Checkpoint> onCheckpoint,
                                ExecutionObserver observer, long deadlineNanos)
            throws WorkflowExecutionException {
        CompiledWorkflow.EdgeSpec edge = step.edge();
        Map<String, Message> restricted = new LinkedHashMap<>();
        for (String source : edge.sources()) {
            Message value = values.get(source);
            if (value == null) {
                throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                        "edge source '" + source + "' is not 'input' or a prior step",
                        null);
            }
            restricted.put(source, value);
        }
        DynamicMessage produced = buildMessage(mapper,
                evaluator(restricted, edge.produceType()), restricted, edge.produceType(),
                edge.rules(), edge.celRules(), step.name());
        if (step.fanOut() != null) {
            return runFanOut(step, produced, restricted.size(), values, registry, open,
                    outcomes, checkpoints, onCheckpoint, observer, deadlineNanos);
        }

        Message delivered = produced;
        if (edge.projectTo() != null) {
            delivered = projectValue(step, edge.projectTo(), produced, registry);
        }
        if (edge.validate()) {
            ValidationResult result = ProtoValidator
                    .forMessageType(delivered.getDescriptorForType()).validate(delivered);
            if (!result.valid()) {
                observer.edgeEvaluated(step, produced, false, restricted.size(), 0);
                throw new WorkflowExecutionException(step.name(), FailureKind.VALIDATION, null,
                        "edge value failed validation before the step executed: "
                                + summary(result), null);
            }
        }
        observer.edgeEvaluated(step, produced, true, restricted.size(), 0);

        if (step.structured() != null) {
            GenerateStructuredRequest request = structuredRequest(step, Any.pack(delivered));
            Instant stepStarted = Instant.now();
            observer.structuredStepStarted(step, request, stepStarted);
            GenerateStructuredResponse generated = invokeCoordinator(step, request,
                    deadlineNanos);
            Message output = unpackStructured(step, generated);
            values.put(step.name(), output);
            outcomes.add(new StepOutcome(step.name(), false));
            observer.structuredStepCompleted(step, request, output, generated, stepStarted,
                    Instant.now());
            record(checkpoints, new Checkpoint(step.name(), false, output), onCheckpoint,
                    step.name());
            return output;
        }

        long callMs = remainingMs(step, deadlineNanos);
        ManagedChannel channel = openChannel(step, open, callMs);
        DynamicMessage request = (DynamicMessage) delivered;
        Instant stepStarted = Instant.now();
        observer.stepStarted(step, request, stepStarted);
        DynamicMessage response = call(step, channel, request, callMs);
        validateResponse(step, response);
        values.put(step.name(), response);
        outcomes.add(new StepOutcome(step.name(), false));
        observer.stepCompleted(step, request, response, false, stepStarted, Instant.now());
        record(checkpoints, new Checkpoint(step.name(), false, response), onCheckpoint,
                step.name());
        return response;
    }

    /**
     * Executes one fanned-out step: every item of the produced message's items field
     * runs one branch on a virtual thread, bounded by the semaphore; outcomes land in
     * index-addressed slots, so evidence and the collect are deterministic regardless
     * of completion order. FAIL_FAST cancels remaining branches at the first failure
     * and fails the step; CONTINUE collects the survivors.
     *
     * @return the collected message bound into the scope under the step's name
     */
    private Message runFanOut(CompiledWorkflow.Step step, DynamicMessage produced,
                              int sourceCount, Map<String, Message> values,
                              DescriptorRegistry registry,
                              Map<String, ManagedChannel> open,
                              List<StepOutcome> outcomes, List<Checkpoint> checkpoints,
                              Consumer<Checkpoint> onCheckpoint,
                              ExecutionObserver observer, long deadlineNanos)
            throws WorkflowExecutionException {
        CompiledWorkflow.EdgeSpec edge = step.edge();
        CompiledWorkflow.FanOutSpec fanOut = step.fanOut();
        List<Message> items;
        try {
            items = EdgeFlow.items(produced, fanOut.items());
        } catch (IllegalArgumentException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                    "fan-out items path rejected: " + e.getMessage(), e);
        }
        if (items.size() > fanOut.maxItems()) {
            throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                    "fan-out produced " + items.size() + " items, over the max_items "
                            + "cap of " + fanOut.maxItems(), null);
        }

        // Project and validate every item up front, before any invocation: an invalid
        // item is a failed branch that never executes, and the all-items verdict is
        // the step's deterministic validation evidence.
        BranchOutcome[] slots = new BranchOutcome[items.size()];
        Message[] branchValues = new Message[items.size()];
        boolean allValid = true;
        for (int i = 0; i < items.size(); i++) {
            Message value = items.get(i);
            if (edge.projectTo() != null) {
                try {
                    value = projectValue(step, edge.projectTo(), value, registry);
                } catch (WorkflowExecutionException e) {
                    slots[i] = new BranchOutcome(null, e.getMessage(), FailureKind.EDGE,
                            null);
                    allValid = false;
                    continue;
                }
            }
            if (edge.validate()) {
                ValidationResult result = ProtoValidator
                        .forMessageType(value.getDescriptorForType()).validate(value);
                if (!result.valid()) {
                    slots[i] = new BranchOutcome(null,
                            "branch value failed validation: " + summary(result),
                            FailureKind.VALIDATION, null);
                    allValid = false;
                    continue;
                }
            }
            branchValues[i] = value;
        }
        observer.edgeEvaluated(step, produced, allValid, sourceCount, items.size());

        // gRPC branches share one channel, opened before any task starts: the channel
        // map is confined to this thread.
        ManagedChannel channel = null;
        long callMs = -1;
        if (step.structured() == null) {
            callMs = remainingMs(step, deadlineNanos);
            channel = openChannel(step, open, callMs);
        }
        final long branchCallMs = callMs;
        Instant stepStarted = Instant.now();
        if (step.structured() == null) {
            observer.stepStarted(step, produced, stepStarted);
        }

        java.util.concurrent.Semaphore permits =
                new java.util.concurrent.Semaphore(fanOut.maxConcurrency());
        java.util.concurrent.atomic.AtomicBoolean abandoned =
                new java.util.concurrent.atomic.AtomicBoolean();
        try (var executor = java.util.concurrent.Executors
                .newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>(items.size());
            for (int i = 0; i < items.size(); i++) {
                int index = i;
                if (slots[index] != null) {
                    // The item never reached an invocation: rejected up front. Under
                    // FAIL_FAST every later branch is abandoned.
                    if (fanOut.failurePolicy()
                            == CompiledWorkflow.BranchFailurePolicy.FAIL_FAST) {
                        break;
                    }
                    continue;
                }
                ManagedChannel branchChannel = channel;
                futures.add(executor.submit(() -> {
                    if (abandoned.get()) {
                        return;
                    }
                    try {
                        permits.acquire();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    try {
                        if (abandoned.get()) {
                            return;
                        }
                        slots[index] = executeBranch(step, branchValues[index], index,
                                branchChannel, branchCallMs, deadlineNanos);
                    } catch (BranchFailure failure) {
                        slots[index] = failure.outcome();
                        if (fanOut.failurePolicy()
                                == CompiledWorkflow.BranchFailurePolicy.FAIL_FAST) {
                            abandoned.set(true);
                        }
                    } finally {
                        permits.release();
                    }
                }));
            }
            for (java.util.concurrent.Future<?> future : futures) {
                try {
                    if (abandoned.get()) {
                        future.cancel(true);
                    } else {
                        future.get();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new WorkflowExecutionException(step.name(), FailureKind.WORKFLOW, null,
                            "interrupted while fanning out", e);
                } catch (java.util.concurrent.ExecutionException
                         | java.util.concurrent.CancellationException e) {
                    // Branch bodies record their own outcomes; a cancelled branch was
                    // abandoned and keeps its null slot.
                }
            }
        }

        List<Message> outputs = new ArrayList<>(items.size());
        BranchOutcome firstFailure = null;
        int firstFailureIndex = -1;
        for (int i = 0; i < slots.length; i++) {
            String branchId = step.name() + "#" + i;
            BranchOutcome outcome = slots[i];
            if (outcome == null) {
                observer.branchCompleted(step, branchId, i, null,
                        "abandoned after the first branch failure");
                continue;
            }
            observer.branchCompleted(step, branchId, i, outcome.response(),
                    outcome.summary());
            if (outcome.response() != null) {
                outputs.add(outcome.response());
            } else if (firstFailure == null) {
                firstFailure = outcome;
                firstFailureIndex = i;
            }
        }
        if (firstFailure != null && fanOut.failurePolicy()
                == CompiledWorkflow.BranchFailurePolicy.FAIL_FAST) {
            throw new WorkflowExecutionException(step.name(), firstFailure.kind(),
                    firstFailure.grpcCode(),
                    "branch '" + step.name() + "#" + firstFailureIndex + "' failed: "
                            + firstFailure.summary(), null);
        }

        DynamicMessage collected;
        try {
            collected = EdgeFlow.collect(fanOut.collectType(), fanOut.collectInto(),
                    outputs);
        } catch (IllegalArgumentException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                    "fan-out collect rejected: " + e.getMessage(), e);
        }
        values.put(step.name(), collected);
        outcomes.add(new StepOutcome(step.name(), false));
        observer.stepCompleted(step, produced, collected, false, stepStarted,
                Instant.now());
        record(checkpoints, new Checkpoint(step.name(), false, collected), onCheckpoint,
                step.name());
        return collected;
    }

    /** The verdict of one fan-out branch; exactly one of response/summary is null. */
    private record BranchOutcome(Message response, String summary, FailureKind kind,
                                 io.grpc.Status.Code grpcCode) {

        static BranchOutcome success(Message response) {
            return new BranchOutcome(response, null, null, null);
        }
    }

    /** A branch failure carrying its recorded outcome. */
    private static final class BranchFailure extends Exception {
        private final BranchOutcome outcome;

        BranchFailure(BranchOutcome outcome) {
            super(outcome.summary());
            this.outcome = outcome;
        }

        BranchOutcome outcome() {
            return outcome;
        }
    }

    /**
     * Executes one fan-out branch on its prepared (projected, validated) value. Every
     * failure is a {@link BranchFailure}; the failure policy decides what it does to
     * the step.
     */
    private BranchOutcome executeBranch(CompiledWorkflow.Step step, Message value,
                                        int index, ManagedChannel channel, long callMs,
                                        long deadlineNanos) throws BranchFailure {
        if (step.structured() != null) {
            GenerateStructuredRequest request = structuredRequest(step, Any.pack(value));
            GenerateStructuredResponse generated;
            try {
                generated = invokeCoordinator(step, request, deadlineNanos);
            } catch (WorkflowExecutionException e) {
                throw new BranchFailure(new BranchOutcome(null,
                        e.kind() == FailureKind.STRUCTURED
                                ? "structured generation failed at branch '"
                                        + step.name() + "#" + index + "'"
                                : e.getMessage(),
                        e.kind(), e.grpcCode()));
            }
            try {
                return BranchOutcome.success(unpackStructured(step, generated));
            } catch (WorkflowExecutionException e) {
                throw new BranchFailure(new BranchOutcome(null, e.getMessage(),
                        FailureKind.WORKFLOW, null));
            }
        }
        DynamicMessage response;
        try {
            response = call(step, channel, (DynamicMessage) value, callMs);
        } catch (WorkflowExecutionException e) {
            throw new BranchFailure(new BranchOutcome(null, e.getMessage(), e.kind(),
                    e.grpcCode()));
        }
        if (step.validate()) {
            ValidationResult result = ProtoValidator
                    .forMessageType(step.method().getOutputType()).validate(response);
            if (!result.valid()) {
                throw new BranchFailure(new BranchOutcome(null,
                        "branch response failed validation: " + summary(result),
                        FailureKind.VALIDATION, null));
            }
        }
        return BranchOutcome.success(response);
    }

    /** Projects one edge value to its consumer-visible form. */
    private static Message projectValue(CompiledWorkflow.Step step,
                                        com.google.protobuf.Descriptors.Descriptor projectTo,
                                        Message value, DescriptorRegistry registry)
            throws WorkflowExecutionException {
        try {
            return ai.pipestream.proto.projection.MessageProjection
                    .forTarget(projectTo, registry)
                    .orElseThrow(() -> new ai.pipestream.proto.projection
                            .ProjectionException("projection target "
                            + projectTo.getFullName() + " declares no projection sources"))
                    .project(value);
        } catch (ai.pipestream.proto.projection.ProjectionException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                    "edge projection failed: " + e.getMessage(), e);
        }
    }

    /** The per-call budget: the step's own deadline nested in the remaining workflow budget. */
    private long remainingMs(CompiledWorkflow.Step step, long deadlineNanos)
            throws WorkflowExecutionException {
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(
                deadlineNanos - nanoClock.getAsLong());
        if (remainingMs <= 0) {
            throw new WorkflowExecutionException(step.name(), FailureKind.DEADLINE, null,
                    "workflow deadline exhausted before the step ran", null);
        }
        return step.deadlineMs() > 0
                ? Math.min(step.deadlineMs(), remainingMs)
                : remainingMs;
    }

    /** Opens (or reuses) the step's channel after the outbound policy accepts the call. */
    private ManagedChannel openChannel(CompiledWorkflow.Step step,
                                       Map<String, ManagedChannel> open, long callMs)
            throws WorkflowExecutionException {
        try {
            channels.validateTarget(step);
            channels.validateDeadline(callMs);
        } catch (IllegalArgumentException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.WORKFLOW, null,
                    "outbound channel policy rejected step: " + e.getMessage(), e);
        }
        try {
            return open.computeIfAbsent(step.target() + (step.tls() ? "+tls" : ""),
                    key -> channels.open(step));
        } catch (OutboundChannelPolicyException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.GRPC,
                    io.grpc.Status.Code.RESOURCE_EXHAUSTED,
                    "outbound channel policy rejected step: " + e.getMessage(), e);
        }
    }

    /** One unary call with the step's budget. */
    private static DynamicMessage call(CompiledWorkflow.Step step, ManagedChannel channel,
                                       DynamicMessage request, long callMs)
            throws WorkflowExecutionException {
        try {
            return DynamicGrpcCalls.call(channel, step.method(), request,
                    CallOptions.DEFAULT.withDeadlineAfter(callMs, TimeUnit.MILLISECONDS),
                    new Metadata(), 1).get(0);
        } catch (StatusRuntimeException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.GRPC,
                    e.getStatus().getCode(),
                    "gRPC " + e.getStatus().getCode() + " from " + step.target()
                            + ": " + e.getStatus().getDescription(), e);
        }
    }

    /** Runs the response's declared validation rules when the step asks for them. */
    private static void validateResponse(CompiledWorkflow.Step step, DynamicMessage response)
            throws WorkflowExecutionException {
        if (step.validate()) {
            ValidationResult result = ProtoValidator
                    .forMessageType(step.method().getOutputType())
                    .validate(response);
            if (!result.valid()) {
                throw new WorkflowExecutionException(step.name(), FailureKind.VALIDATION,
                        null, "response failed validation: " + summary(result), null);
            }
        }
    }

    /** The type a step's response binds under its name, gRPC, structured, or collected. */
    private static com.google.protobuf.Descriptors.Descriptor outputTypeOf(
            CompiledWorkflow.Step step) {
        if (step.fanOut() != null) {
            return step.fanOut().collectType();
        }
        return step.structured() != null
                ? step.structured().targetType()
                : step.method().getOutputType();
    }

    /** Appends a checkpoint and notifies the observer; an observer failure fails the segment. */
    private static void record(List<Checkpoint> checkpoints, Checkpoint checkpoint,
                               Consumer<Checkpoint> onCheckpoint, String step)
            throws WorkflowExecutionException {
        checkpoints.add(checkpoint);
        try {
            onCheckpoint.accept(checkpoint);
        } catch (RuntimeException e) {
            throw new WorkflowExecutionException(step, FailureKind.WORKFLOW, null,
                    "checkpoint observer failed; the step's response may not have "
                            + "persisted: " + e.getMessage(), e);
        }
    }

    private static DynamicMessage buildMessage(ScopedProtoMapper mapper,
                                               CelEvaluator evaluator,
                                               Map<String, Message> values,
                                               com.google.protobuf.Descriptors.Descriptor type,
                                               List<String> rules,
                                               List<ai.pipestream.proto.cel.CelMappingRule> celRules,
                                               String where) throws WorkflowExecutionException {
        MessageScope.Builder scope = MessageScope.builder();
        values.forEach(scope::add);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        try {
            mapper.map(scope.build(), builder, rules);
            if (!celRules.isEmpty()) {
                new CelProtoMapper(mapper.fieldMapper(), evaluator, "target",
                        Map.copyOf(values)).map(builder, celRules);
            }
        } catch (Exception e) {
            throw new WorkflowExecutionException(where, FailureKind.MAPPING, null,
                    "mapping failed: " + e.getMessage(), e);
        }
        return builder.build();
    }

    /** A CEL environment over exactly the values in scope (plus 'target' for mappings). */
    private static CelEvaluator evaluator(Map<String, Message> values,
                                          com.google.protobuf.Descriptors.Descriptor targetType) {
        CelEnvironmentFactory factory = CelEnvironmentFactory.builder();
        values.forEach((name, message) ->
                factory.addMessageVar(name, message.getDescriptorForType()));
        factory.addMessageVar("target", targetType);
        return new CelEvaluator(factory.build());
    }

    private static String summary(ValidationResult result) {
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
