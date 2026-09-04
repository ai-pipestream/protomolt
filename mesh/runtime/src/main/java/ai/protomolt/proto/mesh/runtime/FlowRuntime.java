package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.FlowExecutionCheckpoint;
import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;

import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

/** Executes a compiled directed flow over exact protobuf mesh envelopes. */
public final class FlowRuntime {

    private final ResumableFlowRuntime runtime;

    public FlowRuntime(DescriptorRegistry descriptors) {
        this(descriptors, PayloadResolver.inlineOnly(descriptors), Clock.systemUTC());
    }

    public FlowRuntime(
            DescriptorRegistry descriptors,
            PayloadResolver payloads,
            Clock clock) {
        runtime = new ResumableFlowRuntime(
                Objects.requireNonNull(descriptors, "descriptors"),
                Objects.requireNonNull(payloads, "payloads"),
                Objects.requireNonNull(clock, "clock"), FlowRunMetadata.transientRun(),
                PayloadLifecycle.inlineOnly());
    }

    public FlowRuntime(
            DescriptorRegistry descriptors,
            PayloadResolver payloads,
            PayloadLifecycle payloadLifecycle,
            Clock clock) {
        runtime = new ResumableFlowRuntime(
                Objects.requireNonNull(descriptors, "descriptors"),
                Objects.requireNonNull(payloads, "payloads"),
                Objects.requireNonNull(clock, "clock"), FlowRunMetadata.transientRun(),
                Objects.requireNonNull(payloadLifecycle, "payloadLifecycle"));
    }

    FlowRuntime(
            DescriptorRegistry descriptors,
            PayloadResolver payloads,
            Clock clock,
            FlowRunMetadata runMetadata,
            PayloadLifecycle payloadLifecycle) {
        runtime = new ResumableFlowRuntime(
                Objects.requireNonNull(descriptors, "descriptors"),
                Objects.requireNonNull(payloads, "payloads"),
                Objects.requireNonNull(clock, "clock"),
                Objects.requireNonNull(runMetadata, "runMetadata"),
                Objects.requireNonNull(payloadLifecycle, "payloadLifecycle"));
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
        return runtime.execute(flow, input, runId,
                FlowHistory.getDefaultInstance(),
                FlowExecutionCheckpoint.getDefaultInstance(),
                FlowRunControl.none());
    }

    FlowExecutionResult resume(
            CompiledDirectedFlow flow,
            EntityEnvelope input,
            String runId,
            FlowHistory history,
            FlowExecutionCheckpoint checkpoint,
            FlowRunControl control) {
        return runtime.execute(flow, input, runId, history, checkpoint, control);
    }
}
