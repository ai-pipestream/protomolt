package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;

import java.util.Objects;

/** A failed run with the complete protobuf history recorded before refusal. */
public final class FlowExecutionException extends RuntimeException {

    private final FlowHistory history;

    FlowExecutionException(String message, Throwable cause, FlowHistory history) {
        super(message, cause);
        this.history = Objects.requireNonNull(history, "history");
    }

    public FlowHistory history() {
        return history;
    }
}
