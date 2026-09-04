package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import com.google.protobuf.Message;

import java.util.Objects;

/** One invocation passed to a local or remote processor endpoint. */
public record ProcessorInvocation(
        ProcessorContext context,
        EntityEnvelope input,
        Message inputMessage) {

    public ProcessorInvocation {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(inputMessage, "inputMessage");
    }
}
