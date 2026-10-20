package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.mapper.cel.CelProtoMapper;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.CelMappingRule;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.inference.structured.StructuredGenerationException;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.inference.v1.GenerateStructuredRequest;
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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

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

    /** Checks and executes one pipeline against the exact supplied descriptor set. */
    public Result run(Pipeline pipeline, List<FileDescriptor> files, DynamicMessage input)
            throws PipelineExecutionException {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(files, "files");
        Objects.requireNonNull(input, "input");
        List<PipelineChecker.Finding> findings = new PipelineChecker().verify(pipeline, files);
        if (!findings.isEmpty()) {
            throw failure("", FailureKind.PREFLIGHT,
                    "pipeline failed static checking: " + findingsSummary(findings), null);
        }

        DescriptorRegistry registry = DescriptorRegistry.create(false);
        files.forEach(registry::registerFile);
        Descriptor inputType = registry.findDescriptorByFullName(pipeline.getInputType());
        if (!input.getDescriptorForType().getFullName().equals(inputType.getFullName())) {
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

        for (PipelineStep step : pipeline.getStepsList()) {
            StepDeadlines.requireTime(step.getName(), deadlineNanos);
            if (step.hasUnnest()) {
                last = runUnnest(step, scope, pipeline.getMaxStreamMessages());
            } else if (step.hasCollect()) {
                last = runCollect(step, scope, registry);
            } else if (step.hasStructured()) {
                last = runStructured(step, scope, registry, deadlineNanos,
                        pipeline.getMaxStreamMessages(), branches);
            } else {
                last = runGrpc(step, scope, registry, files, dependencies,
                        deadlineNanos, pipeline.getMaxStreamMessages(), outcomes, branches);
                continue;
            }
            scope.put(step.getName(), last);
            outcomes.add(new StepOutcome(step.getName(), false, 0, last.values().size()));
        }

        if (pipeline.hasOutput()) {
            Descriptor outputType = registry.findDescriptorByFullName(
                    pipeline.getOutput().getType());
            DynamicMessage output = map(outputType, singleValues(scope),
                    pipeline.getOutput().getRulesList(),
                    pipeline.getOutput().getCelRulesList(), registry, "output");
            last = one(output);
        }
        return new Result(last.cardinality(), last.values(), outcomes, branches);
    }

    private Binding runGrpc(PipelineStep step, Map<String, Binding> scope,
                            DescriptorRegistry registry, List<FileDescriptor> files,
                            Map<String, ServiceDependency> dependencies,
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
                    dependencies.get(step.getDependency()), registry,
                    stepDeadlineNanos, streamLimit, branches);
            scope.put(step.getName(), result);
            outcomes.add(new StepOutcome(step.getName(), false,
                    fanOutItems(produced.getFirst(), call.getFanOut()).size(), 1));
            return result;
        }

        List<DynamicMessage> requests = deliver(produced, call.getEdge(), registry,
                step.getName());
        long callMillis = StepDeadlines.remainingMillis(step.getName(), stepDeadlineNanos);
        List<DynamicMessage> responses;
        try {
            responses = transport.invoke(dependencies.get(step.getDependency()), method,
                    requests, callMillis, streamLimit + 1);
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
                                  DescriptorRegistry registry, long deadlineNanos,
                                  int streamLimit, List<BranchOutcome> branches)
            throws PipelineExecutionException {
        FanOutSpec fanOut = step.getGrpcCall().getFanOut();
        List<DynamicMessage> items = prepareFanOut(step.getName(), holder,
                step.getGrpcCall().getEdge(), fanOut, registry);
        if (items.size() > streamLimit) {
            throw failure(step.getName(), FailureKind.FAN_OUT,
                    "fan-out exceeds max_stream_messages " + streamLimit, null);
        }
        List<DynamicMessage> outputs = FanOutBranches.run(step.getName(), items, fanOut,
                item -> {
                    long callMillis = StepDeadlines.remainingMillis(step.getName(), deadlineNanos);
                    List<DynamicMessage> responses;
                    try {
                        responses = transport.invoke(dependency, method, List.of(item),
                                callMillis, 2);
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
