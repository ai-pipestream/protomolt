package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.MeshGate;
import ai.protomolt.proto.mesh.runtime.CompiledDirectedFlow.EdgeBinding;
import ai.protomolt.proto.mesh.runtime.CompiledDirectedFlow.NodeBinding;
import ai.protomolt.proto.mesh.runtime.v1.ActiveFlowInvocation;
import ai.protomolt.proto.mesh.runtime.v1.FlowExecutionCheckpoint;
import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.PendingFlowMessage;
import ai.protomolt.proto.mesh.runtime.v1.PendingFlowSettlement;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLeaseFrontier;
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

/** One execution engine for fresh, checkpointed, and frontier-replay runs. */
final class ResumableFlowRuntime {

    private final DescriptorRegistry descriptors;
    private final PayloadResolver payloads;
    private final MeshGate gate;
    private final Clock clock;
    private final FlowRunMetadata runMetadata;
    private final PayloadLifecycle payloadLifecycle;

    ResumableFlowRuntime(
            DescriptorRegistry descriptors,
            PayloadResolver payloads,
            Clock clock,
            FlowRunMetadata runMetadata,
            PayloadLifecycle payloadLifecycle) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.payloads = Objects.requireNonNull(payloads, "payloads");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.runMetadata = Objects.requireNonNull(runMetadata, "runMetadata");
        this.payloadLifecycle = Objects.requireNonNull(payloadLifecycle, "payloadLifecycle");
        this.gate = new MeshGate(descriptors);
    }

    FlowExecutionResult execute(
            CompiledDirectedFlow flow,
            EntityEnvelope input,
            String runId,
            FlowHistory persistedHistory,
            FlowExecutionCheckpoint persistedCheckpoint,
            FlowRunControl control) {
        Objects.requireNonNull(flow, "flow");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(control, "control");
        requireUuid(runId, "runId");

        boolean resumed = persistedHistory != null
                && persistedHistory.getEventsCount() > 0;
        HistoryRecorder history = resumed
                ? new HistoryRecorder(clock, persistedHistory)
                : new HistoryRecorder(clock, runId, flow);
        Deque<Pending> pending = new ArrayDeque<>();
        Deque<PendingSettlement> settlements = new ArrayDeque<>();
        List<EntityEnvelope> outputs = new ArrayList<>();
        List<PayloadLeaseFrontier> payloadLeases = new ArrayList<>();
        Set<String> messageIds = new HashSet<>();
        long invocationOrdinal = 0;
        boolean settlementStarted = false;
        Active active = null;
        Instant deadline = null;

        try {
            if (resumed) {
                requireHistoryIdentity(flow, runId, persistedHistory);
                FlowExecutionCheckpoint checkpoint = Objects.requireNonNull(
                        persistedCheckpoint, "persistedCheckpoint");
                if (!checkpoint.hasDeadline()) {
                    throw new IllegalArgumentException(
                            "resumable flow checkpoint requires deadline");
                }
                deadline = RemoteValidation.instant(checkpoint.getDeadline());
                invocationOrdinal = checkpoint.getNextInvocationOrdinal();
                settlementStarted = checkpoint.getSettlementStarted();
                payloadLeases.addAll(checkpoint.getPayloadLeasesList());
                payloadLeases.forEach(payloadLifecycle::restore);
                checkpoint.getPendingList().forEach(value -> pending.addLast(fromProto(value)));
                if (checkpoint.hasActive()) {
                    ActiveFlowInvocation value = checkpoint.getActive();
                    active = new Active(fromProto(value.getPending()),
                            value.getInvocationOrdinal(), value.getInvocationId());
                }
                restoreSettlements(flow, checkpoint, settlements);
                for (var event : persistedHistory.getEventsList()) {
                    if (!event.getMessageId().isBlank()) {
                        messageIds.add(event.getMessageId());
                    }
                    if (event.getKind() == HistoryEventKind.HISTORY_EVENT_KIND_OUTPUT_RETAINED) {
                        outputs.add(event.getMessage());
                    }
                }
            } else {
                Instant started = clock.instant();
                deadline = deadline(flow, input, started);
                gate.admit(input, started);
                requireSchema("flow input", flow.plan().getDefinition().getInputSchema(),
                        input.getSchema());
                account(input, messageIds, flow);
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_RUN_STARTED,
                        "", "", "", "", "", null);
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ACCEPTED,
                        "", "", "", "", "", input);
                route(flow, CompiledDirectedFlow.INPUT, input, runId, deadline,
                        pending, messageIds, payloadLeases, history);
                checkpoint(control, history, pending, null, settlements,
                        payloadLeases, invocationOrdinal, deadline, false);
            }

            checkCancellation(control, history, pending, active, settlements,
                    payloadLeases, invocationOrdinal, deadline);
            while (active != null || !pending.isEmpty()) {
                requireBeforeDeadline(deadline);
                checkCancellation(control, history, pending, active, settlements,
                        payloadLeases, invocationOrdinal, deadline);
                if (active == null) {
                    Pending next = pending.removeFirst();
                    invocationOrdinal++;
                    String invocationId = stableInvocationId(
                            runId, next.nodeId(), invocationOrdinal,
                            next.input().getHeader().getEntityId());
                    active = new Active(next, invocationOrdinal, invocationId);
                    NodeBinding node = requireNode(flow, next.nodeId());
                    history.event(HistoryEventKind.HISTORY_EVENT_KIND_PROCESSOR_STARTED,
                            next.nodeId(), node.definition().getProcessorId(),
                            next.edgeId(), invocationId, "", next.input());
                    checkpoint(control, history, pending, active, settlements,
                            payloadLeases, invocationOrdinal, deadline, false);
                }

                Pending next = active.pending();
                NodeBinding node = requireNode(flow, next.nodeId());
                gate.admit(next.input(), clock.instant());
                requireSchema("node " + next.nodeId() + " input",
                        node.definition().getInputSchema(), next.input().getSchema());
                DynamicMessage inputMessage = resolvePayload(next.input());
                if (next.input().hasClaimCheck()) {
                    history.event(HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_HYDRATED,
                            next.nodeId(), node.definition().getProcessorId(),
                            next.edgeId(), active.invocationId(), "", next.input());
                    checkpoint(control, history, pending, active, settlements,
                            payloadLeases, invocationOrdinal, deadline, false);
                }
                EdgeBinding incoming = flow.edge(next.edgeId());
                ProcessorContext context = new ProcessorContext(
                        runId, next.nodeId(), active.invocationId(),
                        active.invocationOrdinal(), deadline, ProcessorCancellation.none(),
                        new ProcessorContext.WorkRecoveryIdentity(
                                flow.plan().getDefinition().getName(),
                                runMetadata.workflowVersion(),
                                flow.plan().getPlanFingerprint(),
                                runMetadata.deploymentRevision(),
                                next.edgeId(), incoming.definition().getChannelPolicyId(),
                                next.sourceHistorySequence(),
                                next.input().getHeader().getScopeId(),
                                incoming.channelPolicy().getRetentionPolicyReference(),
                                incoming.channelPolicy().getLegalHoldPolicyReference(),
                                incoming.channelPolicy().getPayloadStoreProfile(),
                                incoming.channelPolicy(),
                                incoming.channelPolicy().getDurableSpillPolicyId().isBlank()
                                        ? ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy
                                                .getDefaultInstance()
                                        : flow.channelPolicy(incoming.channelPolicy()
                                                .getDurableSpillPolicyId())));
                ProcessorInvocationResult result = node.invoker().invoke(
                        new ProcessorInvocation(context, next.input(), inputMessage));
                PendingSettlement settlement = new PendingSettlement(
                        next.nodeId(), node.definition().getProcessorId(),
                        next.edgeId(), active.invocationId(), next.input(),
                        result.settlement());
                settlements.addLast(settlement);
                validateResult(node.invoker().contract(), result);
                checkCancellation(control, history, pending, active, settlements,
                        payloadLeases, invocationOrdinal, deadline);
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_PROCESSOR_COMPLETED,
                        next.nodeId(), node.definition().getProcessorId(),
                        next.edgeId(), active.invocationId(),
                        result.settlement().deliveryId(), next.input());

                int outputOrdinal = 0;
                for (TypedPayload typed : result.outputs()) {
                    outputOrdinal++;
                    RuntimeSchemas.unpack(descriptors, typed);
                    EntityEnvelope produced = EntityEnvelopes.child(
                            runId, active.invocationId(), outputOrdinal,
                            next.input(), typed, clock.instant());
                    gate.admit(produced, clock.instant());
                    account(produced, messageIds, flow);
                    history.event(HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_PRODUCED,
                            next.nodeId(), node.definition().getProcessorId(), "",
                            active.invocationId(), result.settlement().deliveryId(), produced);
                    if (flow.retains(next.nodeId(), produced.getSchema())) {
                        outputs.add(produced);
                        history.event(HistoryEventKind.HISTORY_EVENT_KIND_OUTPUT_RETAINED,
                                next.nodeId(), node.definition().getProcessorId(), "",
                                active.invocationId(), result.settlement().deliveryId(), produced);
                    }
                    route(flow, next.nodeId(), produced, runId, deadline,
                            pending, messageIds, payloadLeases, history);
                }
                active = null;
                checkpoint(control, history, pending, null, settlements,
                        payloadLeases, invocationOrdinal, deadline, false);
            }

            // Cancellation is accepted before settlement begins. Once the first
            // descendant commit starts, completion runs to the terminal record.
            checkCancellation(control, history, pending, null, settlements,
                    payloadLeases, invocationOrdinal, deadline);
            if (!settlements.isEmpty()
                    && !settlementStarted) {
                checkpoint(control, history, pending, null, settlements,
                        payloadLeases, invocationOrdinal, deadline, true);
                settlementStarted = true;
            }
            while (!settlements.isEmpty()) {
                PendingSettlement settlement = settlements.getLast();
                settlement.settlement().settle();
                settlements.removeLast();
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_DOWNSTREAM_SETTLED,
                        settlement.nodeId(), settlement.processorId(), settlement.edgeId(),
                        settlement.invocationId(), settlement.settlement().deliveryId(),
                        settlement.input());
                settlePayloadDescendant(payloadLeases, settlement, runId, history);
                checkpoint(control, history, pending, null, settlements,
                        payloadLeases, invocationOrdinal, deadline, true);
            }
            if (!payloadLeases.isEmpty()) {
                throw new IllegalStateException(
                        "payload-descendant-unsettled: completed work still owns "
                                + payloadLeases.size() + " payload leases");
            }
            history.event(HistoryEventKind.HISTORY_EVENT_KIND_RUN_COMPLETED,
                    "", "", "", "", "", null);
            FlowHistory completed = history.complete(outputs);
            control.checkpoint(completed, FlowExecutionCheckpoint.getDefaultInstance());
            return new FlowExecutionResult(outputs, completed);
        } catch (FlowExecutionSuspendedException | FlowCancellationException expected) {
            throw expected;
        } catch (Throwable failure) {
            if (control.suspending()) {
                throw new FlowExecutionSuspendedException(
                        "flow execution suspended for coordinator shutdown", failure);
            }
            FlowRunControl.Cancellation cancellation = control.cancellation();
            if (cancellation.requested()) {
                history.reset(cancellation.durableHistory());
                cancel(settlements, cancellation.reason());
                releasePayloadLeases(payloadLeases, history);
                FlowHistory cancelled = history.cancel(cancellation.reason());
                control.checkpoint(cancelled, FlowExecutionCheckpoint.getDefaultInstance());
                throw new FlowCancellationException(cancellation.reason(), cancelled);
            }
            release(settlements, failure);
            FlowHistory failed = history.fail(failure);
            control.checkpoint(failed, terminalPayloadCheckpoint(payloadLeases, deadline));
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
            List<PayloadLeaseFrontier> payloadLeases,
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
                account(delivered, messageIds, flow);
            }
            requireBeforeDeadline(deadline);
            PayloadExternalizer.Externalized staged = payloadLifecycle.stage(
                    delivered, edge.channelPolicy(),
                    delivered.getHeader().getScopeId(),
                    "flow:" + runId + ":" + edge.definition().getEdgeId() + ":"
                            + delivered.getHeader().getEntityId(),
                    deadline);
            delivered = staged.envelope();
            if (staged.lease() != null) {
                payloadLeases.add(staged.lease());
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_EXTERNALIZED,
                        edge.target(), flow.nodes().get(edge.target())
                                .definition().getProcessorId(),
                        edge.definition().getEdgeId(), "", "", delivered);
            }
            long sourceSequence = history.event(
                    HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED,
                    edge.target(), flow.nodes().get(edge.target())
                            .definition().getProcessorId(),
                    edge.definition().getEdgeId(), "", "", delivered);
            pending.addLast(new Pending(
                    edge.target(), edge.definition().getEdgeId(), delivered, sourceSequence));
        }
    }

    private void checkCancellation(
            FlowRunControl control,
            HistoryRecorder history,
            Deque<Pending> pending,
            Active active,
            Deque<PendingSettlement> settlements,
            List<PayloadLeaseFrontier> payloadLeases,
            long invocationOrdinal,
            Instant deadline) {
        FlowRunControl.Cancellation cancellation = control.cancellation();
        if (!cancellation.requested()) {
            return;
        }
        history.reset(cancellation.durableHistory());
        cancel(settlements, cancellation.reason());
        releasePayloadLeases(payloadLeases, history);
        FlowHistory cancelled = history.cancel(cancellation.reason());
        control.checkpoint(cancelled, FlowExecutionCheckpoint.getDefaultInstance());
        throw new FlowCancellationException(cancellation.reason(), cancelled);
    }

    private static void checkpoint(
            FlowRunControl control,
            HistoryRecorder history,
            Deque<Pending> pending,
            Active active,
            Deque<PendingSettlement> settlements,
            List<PayloadLeaseFrontier> payloadLeases,
            long invocationOrdinal,
            Instant deadline,
            boolean settlementStarted) {
        FlowExecutionCheckpoint.Builder checkpoint = FlowExecutionCheckpoint.newBuilder()
                .setNextInvocationOrdinal(invocationOrdinal)
                .setDeadline(RemoteValidation.timestamp(deadline))
                .setSettlementStarted(settlementStarted);
        pending.stream().map(ResumableFlowRuntime::toProto)
                .forEach(checkpoint::addPending);
        if (active != null) {
            checkpoint.setActive(ActiveFlowInvocation.newBuilder()
                    .setPending(toProto(active.pending()))
                    .setInvocationOrdinal(active.invocationOrdinal())
                    .setInvocationId(active.invocationId()));
        }
        settlements.stream().map(ResumableFlowRuntime::toProto)
                .forEach(checkpoint::addSettlements);
        checkpoint.addAllPayloadLeases(payloadLeases);
        control.checkpoint(history.current(), checkpoint.build());
    }

    private void settlePayloadDescendant(
            List<PayloadLeaseFrontier> payloadLeases,
            PendingSettlement settlement,
            String runId,
            HistoryRecorder history) {
        String messageId = settlement.input().getHeader().getEntityId();
        String ownerId = "flow:" + runId + ":" + settlement.edgeId() + ":" + messageId;
        boolean matched = false;
        for (int index = payloadLeases.size() - 1; index >= 0; index--) {
            PayloadLeaseFrontier current = payloadLeases.get(index);
            if (!current.getOwnerId().equals(ownerId)
                    || !current.getDescendantMessageIdsList().contains(messageId)) {
                continue;
            }
            matched = true;
            PayloadLeaseFrontier.Builder remaining = current.toBuilder()
                    .clearDescendantMessageIds();
            current.getDescendantMessageIdsList().stream()
                    .filter(descendant -> !descendant.equals(messageId))
                    .forEach(remaining::addDescendantMessageIds);
            if (remaining.getDescendantMessageIdsCount() == 0) {
                payloadLifecycle.settle(current);
                payloadLeases.remove(index);
                history.event(HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_RETAINED,
                        settlement.nodeId(), settlement.processorId(), settlement.edgeId(),
                        settlement.invocationId(), settlement.settlement().deliveryId(),
                        settlement.input());
            } else {
                payloadLeases.set(index, remaining.build());
            }
        }
        if (settlement.input().hasClaimCheck() && !matched) {
            throw new IllegalStateException(
                    "payload-descendant-lease-missing: no exact lease for edge "
                            + settlement.edgeId() + " and message " + messageId);
        }
    }

    private void releasePayloadLeases(
            List<PayloadLeaseFrontier> payloadLeases, HistoryRecorder history) {
        while (!payloadLeases.isEmpty()) {
            PayloadLeaseFrontier lease = payloadLeases.removeLast();
            payloadLifecycle.settle(lease);
            history.event(HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_RETAINED,
                    "", "", "", "", "", null);
        }
    }

    private static FlowExecutionCheckpoint terminalPayloadCheckpoint(
            List<PayloadLeaseFrontier> payloadLeases, Instant deadline) {
        FlowExecutionCheckpoint.Builder checkpoint = FlowExecutionCheckpoint.newBuilder()
                .addAllPayloadLeases(payloadLeases);
        if (deadline != null) {
            checkpoint.setDeadline(RemoteValidation.timestamp(deadline));
        }
        return checkpoint.build();
    }

    private static PendingFlowMessage toProto(Pending pending) {
        return PendingFlowMessage.newBuilder()
                .setNodeId(pending.nodeId())
                .setEdgeId(pending.edgeId())
                .setInput(pending.input())
                .setSourceHistorySequence(pending.sourceHistorySequence())
                .build();
    }

    private static Pending fromProto(PendingFlowMessage pending) {
        return new Pending(pending.getNodeId(), pending.getEdgeId(), pending.getInput(),
                pending.getSourceHistorySequence());
    }

    private static PendingFlowSettlement toProto(PendingSettlement settlement) {
        return PendingFlowSettlement.newBuilder()
                .setNodeId(settlement.nodeId())
                .setProcessorId(settlement.processorId())
                .setInvocationId(settlement.invocationId())
                .setDeliveryId(settlement.settlement().deliveryId())
                .setInput(settlement.input())
                .setEdgeId(settlement.edgeId())
                .build();
    }

    private static void restoreSettlements(
            CompiledDirectedFlow flow,
            FlowExecutionCheckpoint checkpoint,
            Deque<PendingSettlement> settlements) {
        for (PendingFlowSettlement stored : checkpoint.getSettlementsList()) {
            NodeBinding node = requireNode(flow, stored.getNodeId());
            if (!node.definition().getProcessorId().equals(stored.getProcessorId())) {
                throw new IllegalArgumentException("settlement processor drift at node "
                        + stored.getNodeId());
            }
            flow.edge(stored.getEdgeId());
            InvocationSettlement settlement = node.invoker()
                    .recoverSettlement(stored.getDeliveryId());
            settlements.addLast(new PendingSettlement(
                    stored.getNodeId(), stored.getProcessorId(), stored.getEdgeId(),
                    stored.getInvocationId(), stored.getInput(), settlement));
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
            CompiledDirectedFlow flow) {
        if (!messageIds.add(message.getHeader().getEntityId())) {
            throw new IllegalArgumentException("flow produced duplicate message id "
                    + message.getHeader().getEntityId());
        }
        if (messageIds.size() > flow.plan().getDefinition().getMaxMessages()) {
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
            Instant inherited = RemoteValidation.instant(input.getHeader().getDeadline());
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

    private static void cancel(
            Deque<PendingSettlement> settlements, String reason) {
        RuntimeException failure = null;
        while (!settlements.isEmpty()) {
            try {
                settlements.removeLast().settlement().release(reason);
            } catch (RuntimeException releaseFailure) {
                if (failure == null) {
                    failure = releaseFailure;
                } else {
                    failure.addSuppressed(releaseFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static NodeBinding requireNode(CompiledDirectedFlow flow, String nodeId) {
        NodeBinding node = flow.nodes().get(nodeId);
        if (node == null) {
            throw new IllegalStateException("compiled flow lost node " + nodeId);
        }
        return node;
    }

    private static void requireHistoryIdentity(
            CompiledDirectedFlow flow, String runId, FlowHistory history) {
        if (!history.getRunId().equals(runId)
                || !history.getFlowName().equals(flow.plan().getDefinition().getName())
                || !history.getPlanFingerprint().equals(flow.plan().getPlanFingerprint())) {
            throw new IllegalArgumentException("persisted flow history identity drift");
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

    private record Pending(
            String nodeId,
            String edgeId,
            EntityEnvelope input,
            long sourceHistorySequence) {
    }

    private record Active(Pending pending, long invocationOrdinal, String invocationId) {
    }

    private record PendingSettlement(
            String nodeId,
            String processorId,
            String edgeId,
            String invocationId,
            EntityEnvelope input,
            InvocationSettlement settlement) {
    }
}
