package ai.pipestream.proto.chain;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.grpc.policy.OutboundChannelPolicyException;
import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import ai.pipestream.proto.shapes.MessageScope;
import ai.pipestream.proto.shapes.ScopedProtoMapper;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Executes a chain: serial, fail-fast, deadline-bounded. Each step's request is built from
 * the scope (the chain {@code input} plus every prior step's response) with scoped text
 * rules and CEL (expressions also see {@code target}, the progressive request); a false
 * {@code when} gate skips the step, binding its name to the output type's default instance
 * so later references stay well-defined; {@code validate} runs the response's declared
 * rules before proceeding. Nothing persists between calls — a chain execution lives inside one
 * invocation, by design.
 */
public final class ChainRunner {

    /** Opens the channel a step calls through; a test seam and a TLS/policy hook. */
    public interface ChannelFactory {
        ManagedChannel open(ChainDefinition.Step step);

        /** Validates a step target before the chain performs its first network operation. */
        default void validateTarget(ChainDefinition.Step step) {
        }

        /** Validates the effective per-call deadline against the host policy. */
        default void validateDeadline(long deadlineMillis) {
        }
    }

    /** How the chain ended per step: executed or gate-skipped. */
    public record StepOutcome(String name, boolean skipped) {
    }

    public record Result(Message output, List<StepOutcome> steps) {
    }

    /**
     * One persisted step response: the unit a chain job checkpoints and resumes from.
     * {@code skipped} steps carry a null {@code response} (the output type's default
     * instance is bound on replay, exactly as a live skip does).
     */
    public record Checkpoint(String name, boolean skipped, Message response) {
    }

    /**
     * The outcome of one execution segment: either the chain ran to completion, or it
     * parked on an {@code external}-completion step. Both carry the full checkpoint
     * prefix (replayed plus newly executed) so the caller persists one list.
     */
    public sealed interface Segment {

        /** The chain completed; {@code result} is the composed output. */
        record Completed(Result result, List<Checkpoint> checkpoints) implements Segment {
        }

        /** The chain parked on {@code step}, awaiting {@code complete-step}. */
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
        /** The response failed its declared validation rules. A verdict, not an error. */
        VALIDATION,
        /** The chain (segment) deadline was exhausted. Retryable: resume continues. */
        DEADLINE,
        /** Synchronous execution reached an external-completion step. Not retryable. */
        EXTERNAL,
        /** Chain-level corruption (e.g. checkpoints no longer match the definition). */
        CHAIN
    }

    /** A step failed: gRPC status, gate/mapping evaluation, or response validation. */
    public static final class ChainExecutionException extends Exception {
        private final String step;
        private final FailureKind kind;
        private final io.grpc.Status.Code grpcCode;

        public ChainExecutionException(String step, FailureKind kind,
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

    public ChainRunner() {
        this(OutboundChannelPolicy.defaults());
    }

    /** Creates a runner using a host-configured shared outbound channel policy. */
    public ChainRunner(OutboundChannelPolicy policy) {
        this(policyFactory(Objects.requireNonNull(policy, "channel policy")));
    }

    private static ChannelFactory policyFactory(OutboundChannelPolicy policy) {
        return new ChannelFactory() {
            @Override
            public ManagedChannel open(ChainDefinition.Step step) {
                return policy.open(step.target(), step.tls(), canonical -> {
                    ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(canonical);
                    if (!step.tls()) {
                        builder.usePlaintext();
                    }
                    return builder.build();
                });
            }

            @Override
            public void validateTarget(ChainDefinition.Step step) {
                policy.validateTarget(step.target(), step.tls());
            }

            @Override
            public void validateDeadline(long deadlineMillis) {
                policy.validateDeadline(deadlineMillis);
            }
        };
    }

    public ChainRunner(ChannelFactory channels) {
        this.channels = channels;
    }

    /**
     * Synchronous execution: the whole chain inside one invocation. Rejects
     * external-completion steps — parking needs a job; submit the chain with
     * {@code submit-chain} instead.
     */
    public Result run(ChainDefinition chain, DynamicMessage input)
            throws ChainExecutionException {
        Segment segment = runSegment(chain, input, List.of());
        if (segment instanceof Segment.Completed completed) {
            return completed.result();
        }
        Segment.Parked parked = (Segment.Parked) segment;
        throw new ChainExecutionException(parked.step(), FailureKind.EXTERNAL, null,
                "step declares completion='external'; synchronous run-chain cannot park "
                        + "- submit the chain as a job (submit-chain) so complete-step "
                        + "can supply the response", null);
    }

    /**
     * Executes one segment of a chain with no prior checkpoints and no observer.
     *
     * @see #runSegment(ChainDefinition, DynamicMessage, List, Consumer)
     */
    public Segment runSegment(ChainDefinition chain, DynamicMessage input,
                              List<Checkpoint> prior) throws ChainExecutionException {
        return runSegment(chain, input, prior, checkpoint -> { });
    }

    /**
     * Executes one segment of a chain: replays the checkpoint prefix, then runs steps
     * until the chain completes or an external-completion step parks it. The chain
     * deadline bounds this segment, not the job's wall-clock life — a chain parked for
     * hours of human review must not hold a 30-second budget across the park.
     *
     * @param prior the persisted checkpoint prefix; entry i must name step i of the
     *        chain, so a chain definition edited under a live job fails loud instead of
     *        resuming against a shifted scope
     * @param onCheckpoint called with each newly executed (or gate-skipped) step's
     *        checkpoint as it lands — the jobs worker's per-step persistence hook.
     *        Replayed checkpoints are not re-reported. A throwing observer fails the
     *        segment as {@link FailureKind#CHAIN}: a checkpoint that did not persist
     *        must not be treated as done.
     */
    public Segment runSegment(ChainDefinition chain, DynamicMessage input,
                              List<Checkpoint> prior, Consumer<Checkpoint> onCheckpoint)
            throws ChainExecutionException {
        long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(chain.deadlineMs());
        DescriptorRegistry registry = DescriptorRegistry.create();
        for (FileDescriptor file : chain.files()) {
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
            for (ChainDefinition.Step step : chain.steps()) {
                if (index < prior.size()) {
                    Checkpoint done = prior.get(index);
                    if (!done.name().equals(step.name())) {
                        throw new ChainExecutionException(step.name(), FailureKind.CHAIN, null,
                                "checkpoint " + index + " belongs to step '" + done.name()
                                        + "' but the chain's step " + index + " is '"
                                        + step.name() + "'; the chain definition changed "
                                        + "under the job", null);
                    }
                    Message replayed = done.skipped()
                            ? DynamicMessage.getDefaultInstance(step.method().getOutputType())
                            : done.response();
                    values.put(step.name(), replayed);
                    outcomes.add(new StepOutcome(step.name(), done.skipped()));
                    last = replayed;
                    index++;
                    continue;
                }
                if (step.external()) {
                    return new Segment.Parked(step.name(), List.copyOf(checkpoints));
                }
                CelEvaluator evaluator = evaluator(values, step.method().getInputType());
                if (step.when() != null && !step.when().isBlank()) {
                    boolean go;
                    try {
                        go = evaluator.evaluateBooleanOrFail(step.when(), Map.copyOf(values));
                    } catch (Exception e) {
                        throw new ChainExecutionException(step.name(), FailureKind.GATE, null,
                                "gate failed: " + e.getMessage(), e);
                    }
                    if (!go) {
                        // A skipped step still binds its name: the well-defined default
                        // instance of its output type. Later rules, gates, and the output
                        // mapping see deterministic empty values - the same scope the
                        // verifier checked - instead of an undeclared reference.
                        values.put(step.name(), DynamicMessage
                                .getDefaultInstance(step.method().getOutputType()));
                        outcomes.add(new StepOutcome(step.name(), true));
                        record(checkpoints, new Checkpoint(step.name(), true, null),
                                onCheckpoint, step.name());
                        index++;
                        continue;
                    }
                }
                DynamicMessage request = buildMessage(mapper, evaluator, values,
                        step.method().getInputType(), step.rules(), step.celRules(),
                        step.name());
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(
                        deadlineNanos - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new ChainExecutionException(step.name(), FailureKind.DEADLINE, null,
                            "chain deadline exhausted before the step ran", null);
                }
                long callMs = step.deadlineMs() > 0
                        ? Math.min(step.deadlineMs(), remainingMs)
                        : remainingMs;
                try {
                    channels.validateTarget(step);
                    channels.validateDeadline(callMs);
                } catch (IllegalArgumentException e) {
                    throw new ChainExecutionException(step.name(), FailureKind.CHAIN, null,
                            "outbound channel policy rejected step: " + e.getMessage(), e);
                }
                ManagedChannel channel;
                try {
                    channel = open.computeIfAbsent(
                            step.target() + (step.tls() ? "+tls" : ""),
                            key -> channels.open(step));
                } catch (OutboundChannelPolicyException e) {
                    throw new ChainExecutionException(step.name(), FailureKind.GRPC,
                            io.grpc.Status.Code.RESOURCE_EXHAUSTED,
                            "outbound channel policy rejected step: " + e.getMessage(), e);
                }
                DynamicMessage response;
                try {
                    response = DynamicGrpcCalls.call(channel, step.method(), request,
                            CallOptions.DEFAULT.withDeadlineAfter(callMs, TimeUnit.MILLISECONDS),
                            new Metadata(), 1).get(0);
                } catch (StatusRuntimeException e) {
                    throw new ChainExecutionException(step.name(), FailureKind.GRPC,
                            e.getStatus().getCode(),
                            "gRPC " + e.getStatus().getCode() + " from " + step.target()
                                    + ": " + e.getStatus().getDescription(), e);
                }
                if (step.validate()) {
                    ValidationResult result = ProtoValidator
                            .forMessageType(step.method().getOutputType())
                            .validate(response);
                    if (!result.valid()) {
                        throw new ChainExecutionException(step.name(), FailureKind.VALIDATION,
                                null, "response failed validation: " + summary(result), null);
                    }
                }
                values.put(step.name(), response);
                outcomes.add(new StepOutcome(step.name(), false));
                record(checkpoints, new Checkpoint(step.name(), false, response),
                        onCheckpoint, step.name());
                last = response;
                index++;
            }
            Message output = chain.output() == null
                    ? last
                    : buildMessage(mapper, evaluator(values, chain.output().type()), values,
                            chain.output().type(), chain.output().rules(),
                            chain.output().celRules(), "output");
            return new Segment.Completed(new Result(output, List.copyOf(outcomes)),
                    List.copyOf(checkpoints));
        } finally {
            for (ManagedChannel channel : open.values()) {
                channel.shutdown();
            }
        }
    }

    /** Appends a checkpoint and notifies the observer; an observer failure fails the segment. */
    private static void record(List<Checkpoint> checkpoints, Checkpoint checkpoint,
                               Consumer<Checkpoint> onCheckpoint, String step)
            throws ChainExecutionException {
        checkpoints.add(checkpoint);
        try {
            onCheckpoint.accept(checkpoint);
        } catch (RuntimeException e) {
            throw new ChainExecutionException(step, FailureKind.CHAIN, null,
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
                                               String where) throws ChainExecutionException {
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
            throw new ChainExecutionException(where, FailureKind.MAPPING, null,
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
