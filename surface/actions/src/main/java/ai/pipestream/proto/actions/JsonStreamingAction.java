package ai.pipestream.proto.actions;

import com.google.protobuf.Message;

/**
 * A {@link StreamingAction} still written against JSON envelopes, bridged the same way
 * {@link JsonAction} bridges a unary verb. It goes away with the last streaming verb
 * converted to emit its own messages.
 */
public interface JsonStreamingAction extends JsonAction, StreamingAction {

    /** Executes the action, emitting result documents as they are produced. */
    void executeStreaming(com.fasterxml.jackson.databind.node.ObjectNode input,
            ActionContext context, JsonStreamEmitter emitter) throws ActionException;

    @Override
    default void executeStreaming(Message request, ActionContext context, StreamEmitter emitter)
            throws ActionException {
        executeStreaming(CatalogContract.toEnvelope(request, name()), context,
                node -> emitter.emit(CatalogContract.toResponse(node, responseType(), name())));
    }
}
