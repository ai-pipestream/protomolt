package ai.protomolt.proto.emit.renderer;

import ai.protomolt.proto.emit.v1.RendererCapability;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.OutputStream;

/** Trusted installed adapter. Discovery has no storage or publication side effects. */
public interface RendererProvider {
    RendererCapability capability();
    Descriptor optionsDescriptor();
    /** Inspect the actual descriptor and options; names alone never establish compatibility. */
    void check(Message input, Message options) throws RenderFailure;
    /** Write exactly one artifact. Output is bounded; interruption cancels the operation. */
    void render(Message input, Message options, OutputStream output, Cancellation cancellation)
            throws IOException;

    @FunctionalInterface
    interface Cancellation {
        boolean cancelled();
        default void check() throws RenderFailure {
            if (cancelled() || Thread.currentThread().isInterrupted())
                throw new RenderFailure(RenderFailure.Kind.CANCELLED, "Rendering cancelled");
        }
    }
}
