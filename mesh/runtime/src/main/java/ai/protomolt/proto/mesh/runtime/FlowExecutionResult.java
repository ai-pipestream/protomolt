package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;

import java.util.List;
import java.util.Objects;

/** Completed flow outputs and their unified protobuf history. */
public record FlowExecutionResult(List<EntityEnvelope> outputs, FlowHistory history) {
    public FlowExecutionResult {
        outputs = List.copyOf(Objects.requireNonNull(outputs, "outputs"));
        Objects.requireNonNull(history, "history");
    }
}
