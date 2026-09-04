package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.FlowFailure;
import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEvent;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.RunState;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import com.google.protobuf.Timestamp;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/** Single sequencer for every local, routed, remote, and settlement event in a run. */
final class HistoryRecorder {

    private final Clock clock;
    private final FlowHistory.Builder history;
    private long sequence;

    HistoryRecorder(Clock clock, String runId, CompiledDirectedFlow flow) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.history = FlowHistory.newBuilder()
                .setRunId(runId)
                .setFlowName(flow.plan().getDefinition().getName())
                .setPlanFingerprint(flow.plan().getPlanFingerprint())
                .setState(RunState.RUN_STATE_RUNNING);
    }

    HistoryRecorder(Clock clock, FlowHistory persisted) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.history = Objects.requireNonNull(persisted, "persisted").toBuilder()
                .setState(RunState.RUN_STATE_RUNNING)
                .clearFailure();
        this.sequence = persisted.getEventsCount();
    }

    long event(
            HistoryEventKind kind,
            String nodeId,
            String processorId,
            String edgeId,
            String invocationId,
            String deliveryId,
            EntityEnvelope message) {
        HistoryEvent.Builder event = HistoryEvent.newBuilder()
                .setSequence(++sequence)
                .setOccurredAt(timestamp(clock.instant()))
                .setKind(kind)
                .setNodeId(nullToEmpty(nodeId))
                .setProcessorId(nullToEmpty(processorId))
                .setEdgeId(nullToEmpty(edgeId))
                .setInvocationId(nullToEmpty(invocationId))
                .setDeliveryId(nullToEmpty(deliveryId));
        if (message != null) {
            event.setMessageId(message.getHeader().getEntityId())
                    .setParentMessageId(message.getHeader().getParentEntityId())
                    .setSchema(message.getSchema())
                    .setMessage(message);
        }
        history.addEvents(event);
        return sequence;
    }

    FlowHistory complete(java.util.List<EntityEnvelope> outputs) {
        history.setState(RunState.RUN_STATE_COMPLETED)
                .addAllOutputs(outputs);
        return history.build();
    }

    FlowHistory current() {
        return history.build();
    }

    void reset(FlowHistory durable) {
        Objects.requireNonNull(durable, "durable");
        history.clear().mergeFrom(durable).setState(RunState.RUN_STATE_RUNNING)
                .clearFailure();
        sequence = durable.getEventsCount();
    }

    FlowHistory cancel(String reason) {
        String message = bounded(reason, "run cancellation requested");
        FlowFailure detail = FlowFailure.newBuilder()
                .setCode("run-cancelled")
                .setMessage(message)
                .build();
        history.addEvents(HistoryEvent.newBuilder()
                        .setSequence(++sequence)
                        .setOccurredAt(timestamp(clock.instant()))
                        .setKind(HistoryEventKind.HISTORY_EVENT_KIND_RUN_CANCELLED)
                        .setFailure(detail))
                .setState(RunState.RUN_STATE_CANCELLED)
                .setFailure(detail);
        return history.build();
    }

    FlowHistory fail(Throwable failure) {
        String message = bounded(failure.getMessage(), failure.getClass().getSimpleName());
        FlowFailure detail = FlowFailure.newBuilder()
                .setCode("flow-execution-failed")
                .setMessage(message)
                .build();
        history.addEvents(HistoryEvent.newBuilder()
                        .setSequence(++sequence)
                        .setOccurredAt(timestamp(clock.instant()))
                        .setKind(HistoryEventKind.HISTORY_EVENT_KIND_RUN_FAILED)
                        .setFailure(detail))
                .setState(RunState.RUN_STATE_FAILED)
                .setFailure(detail);
        return history.build();
    }

    private static String bounded(String value, String fallback) {
        String result = value == null || value.isBlank() ? fallback : value;
        return result.length() <= 8_192 ? result : result.substring(0, 8_192);
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
