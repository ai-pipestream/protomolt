package ai.pipestream.proto.chain;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
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
import java.util.concurrent.TimeUnit;

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
    }

    /** How the chain ended per step: executed or gate-skipped. */
    public record StepOutcome(String name, boolean skipped) {
    }

    public record Result(Message output, List<StepOutcome> steps) {
    }

    /** A step failed: gRPC status, gate/mapping evaluation, or response validation. */
    public static final class ChainExecutionException extends Exception {
        private final String step;

        public ChainExecutionException(String step, String message, Throwable cause) {
            super(message, cause);
            this.step = step;
        }

        public String step() {
            return step;
        }
    }

    private final ChannelFactory channels;

    public ChainRunner() {
        this(step -> {
            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(step.target());
            if (!step.tls()) {
                builder.usePlaintext();
            }
            return builder.build();
        });
    }

    public ChainRunner(ChannelFactory channels) {
        this.channels = channels;
    }

    public Result run(ChainDefinition chain, DynamicMessage input)
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
        Map<String, ManagedChannel> open = new LinkedHashMap<>();
        try {
            Message last = input;
            for (ChainDefinition.Step step : chain.steps()) {
                CelEvaluator evaluator = evaluator(values, step.method().getInputType());
                if (step.when() != null && !step.when().isBlank()) {
                    boolean go;
                    try {
                        go = evaluator.evaluateBooleanOrFail(step.when(), Map.copyOf(values));
                    } catch (Exception e) {
                        throw new ChainExecutionException(step.name(),
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
                        continue;
                    }
                }
                DynamicMessage request = buildMessage(mapper, evaluator, values,
                        step.method().getInputType(), step.rules(), step.celRules(),
                        step.name());
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(
                        deadlineNanos - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new ChainExecutionException(step.name(),
                            "chain deadline exhausted before the step ran", null);
                }
                long callMs = step.deadlineMs() > 0
                        ? Math.min(step.deadlineMs(), remainingMs)
                        : remainingMs;
                ManagedChannel channel = open.computeIfAbsent(
                        step.target() + (step.tls() ? "+tls" : ""),
                        key -> channels.open(step));
                DynamicMessage response;
                try {
                    response = DynamicGrpcCalls.call(channel, step.method(), request,
                            CallOptions.DEFAULT.withDeadlineAfter(callMs, TimeUnit.MILLISECONDS),
                            new Metadata(), 1).get(0);
                } catch (StatusRuntimeException e) {
                    throw new ChainExecutionException(step.name(),
                            "gRPC " + e.getStatus().getCode() + " from " + step.target()
                                    + ": " + e.getStatus().getDescription(), e);
                }
                if (step.validate()) {
                    ValidationResult result = ProtoValidator
                            .forMessageType(step.method().getOutputType())
                            .validate(response);
                    if (!result.valid()) {
                        throw new ChainExecutionException(step.name(),
                                "response failed validation: " + summary(result), null);
                    }
                }
                values.put(step.name(), response);
                outcomes.add(new StepOutcome(step.name(), false));
                last = response;
            }
            Message output = chain.output() == null
                    ? last
                    : buildMessage(mapper, evaluator(values, chain.output().type()), values,
                            chain.output().type(), chain.output().rules(),
                            chain.output().celRules(), "output");
            return new Result(output, List.copyOf(outcomes));
        } finally {
            for (ManagedChannel channel : open.values()) {
                channel.shutdown();
            }
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
            throw new ChainExecutionException(where, "mapping failed: " + e.getMessage(), e);
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
