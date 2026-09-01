package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.pipestream.proto.grpc.workflow.v1.CelMappingRule;
import ai.pipestream.proto.grpc.workflow.v1.FanOutSpec;
import ai.pipestream.proto.grpc.workflow.v1.ServiceDependency;
import ai.pipestream.proto.grpc.workflow.v1.StepCompletion;
import ai.pipestream.proto.grpc.workflow.v1.TypedEdge;
import ai.pipestream.proto.inference.structured.StructuredGenerationException;
import ai.pipestream.proto.inference.structured.StructuredGenerator;
import ai.pipestream.proto.inference.v1.GenerateStructuredRequest;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.GrpcCallStep;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import ai.pipestream.proto.pipeline.v1.StructuredStep;
import ai.pipestream.proto.projection.MessageProjection;
import ai.pipestream.proto.shapes.MessageScope;
import ai.pipestream.proto.shapes.ScopedProtoMapper;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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
        long deadlineNanos = deadlineFromNow(pipeline);
        Binding last = scope.get("input");

        for (PipelineStep step : pipeline.getStepsList()) {
            requireTime(step.getName(), deadlineNanos);
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
        long stepDeadlineNanos = stepDeadline(step, pipelineDeadlineNanos);

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
        long callMillis = remainingMillis(step.getName(), stepDeadlineNanos);
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
        long stepDeadlineNanos = stepDeadline(step, deadlineNanos);
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
        Message holder = source.one();
        String[] segments = step.getUnnest().getPath().split("\\.");
        for (int i = 0; i < segments.length - 1; i++) {
            holder = (Message) holder.getField(
                    holder.getDescriptorForType().findFieldByName(segments[i]));
        }
        List<?> raw = (List<?>) holder.getField(field);
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
        List<DynamicMessage> outputs = parallelBranches(step.getName(), items, fanOut,
                item -> {
                    long callMillis = remainingMillis(step.getName(), deadlineNanos);
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
        List<DynamicMessage> outputs = parallelBranches(step.getName(), items, fanOut,
                grounding -> generate(step.getName(), structured, target, grounding,
                        deadlineNanos), deadlineNanos, branches);
        return one(collect(fanOut, outputs, registry));
    }

    @FunctionalInterface
    private interface BranchCall {
        DynamicMessage run(DynamicMessage input) throws PipelineExecutionException;
    }

    private List<DynamicMessage> parallelBranches(String step, List<DynamicMessage> items,
                                                  FanOutSpec fanOut, BranchCall call,
                                                  long deadlineNanos,
                                                  List<BranchOutcome> branchEvidence)
            throws PipelineExecutionException {
        DynamicMessage[] slots = new DynamicMessage[items.size()];
        PipelineExecutionException[] failures = new PipelineExecutionException[items.size()];
        Semaphore permits = new Semaphore(fanOut.getMaxConcurrency());
        AtomicBoolean abandoned = new AtomicBoolean();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<?>> futures = new ArrayList<>(items.size());
            for (int i = 0; i < items.size(); i++) {
                int index = i;
                futures.add(executor.submit(() -> {
                    if (abandoned.get()) {
                        return;
                    }
                    boolean acquired = false;
                    try {
                        permits.acquire();
                        acquired = true;
                        if (!abandoned.get()) {
                            slots[index] = call.run(items.get(index));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failures[index] = failure(step, FailureKind.FAN_OUT,
                                "branch interrupted", e);
                    } catch (PipelineExecutionException e) {
                        failures[index] = e;
                        if (fanOut.getFailurePolicy()
                                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST) {
                            abandoned.set(true);
                        }
                    } catch (RuntimeException e) {
                        failures[index] = failure(step, FailureKind.FAN_OUT,
                                "branch failed: " + e.getMessage(), e);
                        if (fanOut.getFailurePolicy()
                                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST) {
                            abandoned.set(true);
                        }
                    } finally {
                        if (acquired) {
                            permits.release();
                        }
                    }
                }));
            }
            for (Future<?> future : futures) {
                try {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) {
                        throw new TimeoutException("pipeline step deadline exhausted");
                    }
                    future.get(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw failure(step, FailureKind.FAN_OUT,
                            "interrupted awaiting branches", e);
                } catch (ExecutionException e) {
                    throw failure(step, FailureKind.FAN_OUT,
                            "branch worker failed: " + e.getCause().getMessage(), e.getCause());
                } catch (TimeoutException e) {
                    futures.forEach(pending -> pending.cancel(true));
                    throw failure(step, FailureKind.DEADLINE,
                            "pipeline step deadline exhausted during fan-out", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }
        List<DynamicMessage> outputs = new ArrayList<>();
        PipelineExecutionException firstFailure = null;
        for (int i = 0; i < slots.length; i++) {
            DynamicMessage slot = slots[i];
            if (slot != null) {
                outputs.add(slot);
                branchEvidence.add(new BranchOutcome(step + "#" + i, i, true,
                        null, ""));
            } else {
                PipelineExecutionException branchFailure = failures[i];
                if (firstFailure == null && branchFailure != null) {
                    firstFailure = branchFailure;
                }
                branchEvidence.add(new BranchOutcome(step + "#" + i, i, false,
                        branchFailure == null ? FailureKind.FAN_OUT : branchFailure.kind(),
                        branchFailure == null
                                ? "abandoned after an earlier branch failure"
                                : bounded(branchFailure.getMessage())));
            }
        }
        if (firstFailure != null && fanOut.getFailurePolicy()
                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST) {
            throw firstFailure;
        }
        return outputs;
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
        FieldDescriptor field = StreamPaths.repeatedField(holder.getDescriptorForType(),
                fanOut.getItems());
        Message current = holder;
        String[] segments = fanOut.getItems().split("\\.");
        for (int i = 0; i < segments.length - 1; i++) {
            current = (Message) current.getField(
                    current.getDescriptorForType().findFieldByName(segments[i]));
        }
        return ((List<?>) current.getField(field)).stream()
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
        Future<ai.pipestream.proto.inference.v1.GenerateStructuredResponse> future =
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
                        new LinkedHashMap<>(values)).map(target, celRules.stream()
                        .map(PipelineExecutor::celRule).toList());
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

    private static ai.pipestream.proto.cel.CelMappingRule celRule(CelMappingRule rule) {
        return new ai.pipestream.proto.cel.CelMappingRule(
                rule.getFilter().isBlank() ? null : rule.getFilter(),
                rule.getSelector().isBlank() ? null : rule.getSelector(),
                rule.getTarget(), rule.getFallbackList());
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

    private static long deadlineFromNow(Pipeline pipeline) throws PipelineExecutionException {
        try {
            return Math.addExact(System.nanoTime(),
                    durationNanos(pipeline.getDeadline(), "pipeline deadline"));
        } catch (ArithmeticException e) {
            throw failure("", FailureKind.PREFLIGHT,
                    "pipeline deadline is too large", e);
        }
    }

    private static long stepDeadline(PipelineStep step, long pipelineDeadlineNanos)
            throws PipelineExecutionException {
        if (step.getDeadline().getSeconds() == 0 && step.getDeadline().getNanos() == 0) {
            return pipelineDeadlineNanos;
        }
        try {
            long declared = durationNanos(step.getDeadline(), "step deadline");
            return Math.min(pipelineDeadlineNanos,
                    Math.addExact(System.nanoTime(), declared));
        } catch (ArithmeticException e) {
            return pipelineDeadlineNanos;
        }
    }

    private static long remainingMillis(String step, long deadlineNanos)
            throws PipelineExecutionException {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw failure(step, FailureKind.DEADLINE,
                    "pipeline step deadline exhausted", null);
        }
        return Math.floorDiv(remainingNanos - 1L, 1_000_000L) + 1L;
    }

    private static long durationNanos(com.google.protobuf.Duration duration, String field)
            throws PipelineExecutionException {
        try {
            long nanos = Math.addExact(
                    Math.multiplyExact(duration.getSeconds(), 1_000_000_000L),
                    duration.getNanos());
            if (nanos <= 0) {
                throw failure("", FailureKind.PREFLIGHT, field + " must be positive", null);
            }
            return nanos;
        } catch (ArithmeticException e) {
            throw failure("", FailureKind.PREFLIGHT, field + " is too large", e);
        }
    }

    private static void requireTime(String step, long deadlineNanos)
            throws PipelineExecutionException {
        if (System.nanoTime() >= deadlineNanos) {
            throw failure(step, FailureKind.DEADLINE,
                    "pipeline deadline exhausted before the step ran", null);
        }
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

    private static String bounded(String value) {
        if (value == null) {
            return "branch failed";
        }
        return value.length() <= 4_000 ? value : value.substring(0, 4_000);
    }

    private static PipelineExecutionException failure(String step, FailureKind kind,
                                                      String message, Throwable cause) {
        return failure(step, kind, null, message, cause);
    }

    private static PipelineExecutionException failure(String step, FailureKind kind,
                                                      Status.Code grpcCode,
                                                      String message, Throwable cause) {
        return new PipelineExecutionException(step, kind, grpcCode, message, cause);
    }
}
