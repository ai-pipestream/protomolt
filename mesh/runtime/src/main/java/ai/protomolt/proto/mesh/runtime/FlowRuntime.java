package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.MeshGate;
import ai.protomolt.proto.mesh.runtime.CompiledDirectedFlow.EdgeBinding;
import ai.protomolt.proto.mesh.runtime.CompiledDirectedFlow.NodeBinding;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import com.google.protobuf.DynamicMessage;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Executes a compiled directed flow over exact protobuf mesh envelopes. */
public final class FlowRuntime {

    private final DescriptorRegistry descriptors;
    private final PayloadResolver payloads;
    private final MeshGate gate;
    private final Clock clock;

    public FlowRuntime(DescriptorRegistry descriptors) {
        this(descriptors, PayloadResolver.inlineOnly(descriptors), Clock.systemUTC());
    }

    public FlowRuntime(
            DescriptorRegistry descriptors,
            PayloadResolver payloads,
            Clock clock) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.payloads = Objects.requireNonNull(payloads, "payloads");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.gate = new MeshGate(descriptors);
    }

    /** Executes under a generated run id. */
    public FlowExecutionResult execute(CompiledDirectedFlow flow, EntityEnvelope input) {
        return execute(flow, input, UUID.randomUUID().toString());
    }

    /** Executes under a caller-supplied stable run id for idempotent replay. */
    public FlowExecutionResult execute(
            CompiledDirectedFlow flow,
            EntityEnvelope input,
            String runId) {
        Objects.requireNonNull(flow, "flow");
        Objects.requireNonNull(input, "input");
        requireUuid(runId, "runId");
        HistoryRecorder history = new HistoryRecorder(clock, runId, flow);
        Deque<Pending> pending = new ArrayDeque<>();
        Deque<PendingSettlement> settlements = new ArrayDeque<>();
        List<EntityEnvelope> outputs = new ArrayList<>();
        Set<String> messageIds = new HashSet<>();
        int[] messageCount = {0};
        try {
            Instant started = clock.instant();
            Instant deadline = deadline(flow, input, started);
            gate.admit(input, started);
            requireSchema("flow input", flow.plan().getDefinition().getInputSchema(),
                    input.getSchema());
            account(input, messageIds, messageCount, flow);
            history.event(HistoryEventKind.HISTORY_EVENT_KIND_RUN_STARTED,
                    "", "", "", "", "", null);
            history.event(HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ACCEPTED,
                    "", "", "", "", "", input);
            route(flow, CompiledDirectedFlow.INPUT, input, runId, deadline,
                    pending, messageIds, messageCount, history);

            long invocationOrdinal = 0;
            while (!pending.isEmpty()) {
                requireBeforeDeadline(deadline);
                Pending next = pending.removeFirst();
                NodeBinding node = flow.nodes().get(next.nodeId());
                if (node == null) {
                    throw new IllegalStateException(
                            "compiled flow lost node " + next.nodeId());
                }
                gate.admit(next.input(), clock.instant());
                requireSchema("node " + next.nodeId() + " input",
                        node.definition().getInputSchema(), next.input().getSchema());
                DynamicMessage inputMessage = resolvePayload(next.input());
                invocationOrdinal++;
                String invocationId = stableInvocationId(
                        runId, next.nodeId(), invocationOrdinal,
                        next.input().getHeader().getEntityId());
                ProcessorContext context = new ProcessorContext(
                        runId, next.nodeId(), invocationId, invocationOrdinal, deadline);
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_PROCESSOR_STARTED,
                        next.nodeId(), node.definition().getProcessorId(),
                        next.edgeId(), invocationId, "", next.input());

                ProcessorInvocationResult result = node.invoker().invoke(
                        new ProcessorInvocation(context, next.input(), inputMessage));
                validateResult(node.invoker().contract(), result);
                PendingSettlement settlement = new PendingSettlement(
                        next.nodeId(), node.definition().getProcessorId(), invocationId,
                        next.input(), result.settlement());
                settlements.addLast(settlement);
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_PROCESSOR_COMPLETED,
                        next.nodeId(), node.definition().getProcessorId(),
                        next.edgeId(), invocationId, result.settlement().deliveryId(),
                        next.input());

                int outputOrdinal = 0;
                for (TypedPayload typed : result.outputs()) {
                    outputOrdinal++;
                    RuntimeSchemas.unpack(descriptors, typed);
                    EntityEnvelope produced = EntityEnvelopes.child(
                            runId, invocationId, outputOrdinal, next.input(), typed,
                            clock.instant());
                    gate.admit(produced, clock.instant());
                    account(produced, messageIds, messageCount, flow);
                    history.event(HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_PRODUCED,
                            next.nodeId(), node.definition().getProcessorId(), "",
                            invocationId, result.settlement().deliveryId(), produced);
                    if (flow.retains(next.nodeId(), produced.getSchema())) {
                        outputs.add(produced);
                        history.event(HistoryEventKind.HISTORY_EVENT_KIND_OUTPUT_RETAINED,
                                next.nodeId(), node.definition().getProcessorId(), "",
                                invocationId, result.settlement().deliveryId(), produced);
                    }
                    route(flow, next.nodeId(), produced, runId, deadline,
                            pending, messageIds, messageCount, history);
                }
            }

            while (!settlements.isEmpty()) {
                PendingSettlement settlement = settlements.removeLast();
                settlement.settlement().settle();
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_DOWNSTREAM_SETTLED,
                        settlement.nodeId(), settlement.processorId(), "",
                        settlement.invocationId(), settlement.settlement().deliveryId(),
                        settlement.input());
            }
            history.event(HistoryEventKind.HISTORY_EVENT_KIND_RUN_COMPLETED,
                    "", "", "", "", "", null);
            var completed = history.complete(outputs);
            return new FlowExecutionResult(outputs, completed);
        } catch (Throwable failure) {
            release(settlements, failure);
            var failed = history.fail(failure);
            if (failure instanceof Error error) {
                throw error;
            }
            throw new FlowExecutionException(
                    "flow " + flow.plan().getDefinition().getName()
                            + " failed: " + failure.getMessage(), failure, failed);
        }
    }

    private void route(
            CompiledDirectedFlow flow,
            String source,
            EntityEnvelope message,
            String runId,
            Instant deadline,
            Deque<Pending> pending,
            Set<String> messageIds,
            int[] messageCount,
            HistoryRecorder history) {
        DynamicMessage sourceMessage = null;
        for (EdgeBinding edge : flow.edgesFrom(source)) {
            if (!RuntimeSchemas.same(edge.definition().getSourceSchema(), message.getSchema())) {
                continue;
            }
            if (sourceMessage == null) {
                sourceMessage = resolvePayload(message);
            }
            if (edge.optionalPredicate().isPresent()) {
                boolean accepted;
                try {
                    accepted = edge.optionalPredicate().orElseThrow().evaluateBooleanOrFail(
                            edge.definition().getWhen(), Map.of("message", sourceMessage));
                } catch (CelEvaluationException e) {
                    throw new IllegalArgumentException("edge "
                            + edge.definition().getEdgeId()
                            + " predicate failed for message "
                            + message.getHeader().getEntityId(), e);
                }
                if (!accepted) {
                    continue;
                }
            }
            EntityEnvelope delivered = message;
            if (edge.optionalProjection().isPresent()) {
                DynamicMessage projected = edge.optionalProjection().orElseThrow()
                        .project(sourceMessage);
                delivered = EntityEnvelopes.child(
                        runId,
                        "edge:" + edge.definition().getEdgeId() + ":"
                                + message.getHeader().getEntityId(),
                        1,
                        message,
                        RuntimeSchemas.pack(projected),
                        clock.instant());
                gate.admit(delivered, clock.instant());
                account(delivered, messageIds, messageCount, flow);
            }
            requireBeforeDeadline(deadline);
            pending.addLast(new Pending(
                    edge.target(), edge.definition().getEdgeId(), delivered));
            history.event(HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED,
                    edge.target(), flow.nodes().get(edge.target())
                            .definition().getProcessorId(),
                    edge.definition().getEdgeId(), "", "", delivered);
        }
    }

    private static void validateResult(
            ProcessorContract contract, ProcessorInvocationResult result) {
        Objects.requireNonNull(result, "processor result");
        if (result.outputs().size() > contract.getMaxOutputs()) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " exceeded max_outputs " + contract.getMaxOutputs());
        }
        for (TypedPayload output : result.outputs()) {
            DescriptorIdentity actual = RuntimeSchemas.identity(output.getSchema());
            boolean declared = contract.getOutputSchemasList().stream()
                    .map(RuntimeSchemas::identity)
                    .anyMatch(actual::equals);
            if (!declared) {
                throw new IllegalArgumentException("processor " + contract.getProcessorId()
                        + " returned undeclared exact schema " + actual);
            }
        }
    }

    private DynamicMessage resolvePayload(EntityEnvelope envelope) {
        DynamicMessage message = Objects.requireNonNull(payloads.resolve(envelope),
                "payload resolver returned null");
        DescriptorIdentity expected = RuntimeSchemas.identity(envelope.getSchema());
        DescriptorIdentity actual = DescriptorIdentity.of(message.getDescriptorForType());
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException("payload resolver returned " + actual
                    + " for envelope schema " + expected);
        }
        return message;
    }

    private static void account(
            EntityEnvelope message,
            Set<String> messageIds,
            int[] count,
            CompiledDirectedFlow flow) {
        if (!messageIds.add(message.getHeader().getEntityId())) {
            throw new IllegalArgumentException("flow produced duplicate message id "
                    + message.getHeader().getEntityId());
        }
        count[0]++;
        if (count[0] > flow.plan().getDefinition().getMaxMessages()) {
            throw new IllegalArgumentException("flow exceeded max_messages "
                    + flow.plan().getDefinition().getMaxMessages());
        }
    }

    private Instant deadline(
            CompiledDirectedFlow flow, EntityEnvelope input, Instant started) {
        com.google.protobuf.Duration proto = flow.plan().getDefinition().getDeadline();
        Instant deadline;
        try {
            deadline = started.plus(Duration.ofSeconds(proto.getSeconds(), proto.getNanos()));
        } catch (DateTimeException | ArithmeticException e) {
            throw new IllegalArgumentException("flow deadline overflows Instant", e);
        }
        if (input.getHeader().hasDeadline()) {
            var inputDeadline = input.getHeader().getDeadline();
            Instant inherited = Instant.ofEpochSecond(
                    inputDeadline.getSeconds(), inputDeadline.getNanos());
            if (inherited.isBefore(deadline)) {
                deadline = inherited;
            }
        }
        return deadline;
    }

    private void requireBeforeDeadline(Instant deadline) {
        if (!clock.instant().isBefore(deadline)) {
            throw new IllegalStateException("flow deadline has elapsed at " + deadline);
        }
    }

    private static void release(Deque<PendingSettlement> settlements, Throwable failure) {
        String reason = failure.getMessage() == null
                ? failure.getClass().getSimpleName() : failure.getMessage();
        while (!settlements.isEmpty()) {
            try {
                settlements.removeLast().settlement().release(reason);
            } catch (RuntimeException releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
    }

    private static void requireSchema(
            String subject,
            ai.protomolt.proto.mesh.v1.SchemaReference expected,
            ai.protomolt.proto.mesh.v1.SchemaReference actual) {
        if (!RuntimeSchemas.same(expected, actual)) {
            throw new IllegalArgumentException(subject + " exact schema mismatch: expected "
                    + RuntimeSchemas.identity(expected) + " but received "
                    + RuntimeSchemas.identity(actual));
        }
    }

    private static String stableInvocationId(
            String runId, String nodeId, long ordinal, String inputId) {
        return EntityEnvelopes.stableUuid(
                runId + '\0' + nodeId + '\0' + ordinal + '\0' + inputId);
    }

    private static void requireUuid(String value, String field) {
        try {
            UUID.fromString(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(field + " must be a UUID: " + value, e);
        }
    }

    private record Pending(String nodeId, String edgeId, EntityEnvelope input) {
    }

    private record PendingSettlement(
            String nodeId,
            String processorId,
            String invocationId,
            EntityEnvelope input,
            InvocationSettlement settlement) {
    }
}
