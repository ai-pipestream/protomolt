package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;

import java.util.Objects;

/** A durable run observed a persisted cancellation request and stopped. */
public final class FlowCancellationException extends RuntimeException {

    private final FlowHistory history;

    FlowCancellationException(String message, FlowHistory history) {
        super(message);
        this.history = Objects.requireNonNull(history, "history");
    }

    public FlowHistory history() {
        return history;
    }
}
