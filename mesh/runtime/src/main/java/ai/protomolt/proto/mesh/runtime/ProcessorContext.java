package ai.protomolt.proto.mesh.runtime;

import java.time.Instant;
import java.util.Objects;

/** Immutable identity and deadline for one processor invocation. */
public record ProcessorContext(
        String runId,
        String nodeId,
        String invocationId,
        long invocationOrdinal,
        Instant deadline) {

    public ProcessorContext {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(invocationId, "invocationId");
        Objects.requireNonNull(deadline, "deadline");
        if (runId.isBlank() || nodeId.isBlank() || invocationId.isBlank()) {
            throw new IllegalArgumentException("processor context ids must not be blank");
        }
        if (invocationOrdinal < 1) {
            throw new IllegalArgumentException("invocationOrdinal must be positive");
        }
    }
}
