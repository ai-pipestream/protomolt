package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;

import java.util.List;
import java.util.Objects;

/** Ordered processor outputs plus their deferred settlement handle. */
public record ProcessorInvocationResult(
        List<TypedPayload> outputs,
        InvocationSettlement settlement) {

    public ProcessorInvocationResult {
        outputs = List.copyOf(Objects.requireNonNull(outputs, "outputs"));
        outputs.forEach(output -> Objects.requireNonNull(output, "outputs contains null"));
        Objects.requireNonNull(settlement, "settlement");
    }

    /** Constructs a completed in-process result. */
    public static ProcessorInvocationResult local(List<TypedPayload> outputs) {
        return new ProcessorInvocationResult(outputs, InvocationSettlement.local());
    }
}
