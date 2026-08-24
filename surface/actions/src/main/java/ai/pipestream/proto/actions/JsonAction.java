package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;

/**
 * A verb still written against JSON envelopes rather than against its own messages.
 *
 * <p>The bridge here is the cost of that: the typed request is printed to JSON on the way in
 * and the JSON result is parsed back into the response message on the way out, so a call that
 * was typed at both ends pays two conversions to reach a handler that immediately undoes
 * them. Every verb converted to {@link ProtoAction#execute(Message, ActionContext)} drops off
 * this interface, and the interface goes away with the last of them.
 */
public interface JsonAction extends ProtoAction {

    /**
     * Executes the action.
     *
     * @param input   the input envelope, already checked against {@link #requestType()}
     * @param context type resolution and JSON machinery shared across actions
     * @return the structured result document
     * @throws ActionException with a stable code on any failure
     */
    ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException;

    @Override
    default Message execute(Message request, ActionContext context) throws ActionException {
        ObjectNode output = execute(CatalogContract.toEnvelope(request, name()), context);
        return CatalogContract.toResponse(output, responseType(), name());
    }
}
