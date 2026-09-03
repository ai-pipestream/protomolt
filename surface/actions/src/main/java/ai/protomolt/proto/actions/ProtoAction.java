package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/**
 * One verb over the toolkit: a named operation a UI form, an HTTP endpoint, or a tool-using
 * LLM can drive blind.
 *
 * <p>A verb describes itself by naming the protobuf messages it accepts and answers with, not
 * by writing a schema. The catalog derives the published schema from the request message and
 * enforces it before dispatch, so the bounds a caller reads are the bounds the verb applies
 * and neither can drift from the other.
 *
 * <p>A verb runs on messages, not on JSON. The service definition is the one description of
 * the surface, so the descriptors it declares are what a verb reads and writes; a front that
 * speaks JSON converts at its own edge and every verb sees the same typed request whichever
 * surface the call arrived on. That is what lets an action be registered live against any
 * service the runtime can describe, whether or not stubs were ever generated for it.
 *
 * <p>An action is stateless; everything it needs beyond the request comes from the
 * {@link ActionContext}. Failures are structured {@link ActionException}s with a stable
 * kebab-case code, so callers can branch on machine-readable errors instead of parsing
 * messages.</p>
 */
public interface ProtoAction {

    /** Kebab-case, verb-first action name, e.g. {@code "validate-message"}. */
    String name();

    /** One sentence describing the action, written for a tool-using LLM. */
    String description();

    /**
     * The scope this action requires, from {@link Scopes#VOCABULARY}. An action that keeps
     * the blank default is served normally by an unrestricted caller and refused by name for
     * a scoped one — a plugin that has not declared authorization never grants silently.
     */
    default String requiredScope() {
        return "";
    }

    /** The request message this verb accepts. */
    Descriptor requestType();

    /**
     * The response message this verb answers with.
     *
     * <p>Declared rather than inferred from what a run happens to produce: the reply has a
     * contract of its own, and a verb that answers with something else has broken it whether
     * or not any caller reads the missing part.
     */
    Descriptor responseType();

    /**
     * JSON Schema (draft 2020-12) for the input envelope, derived from {@link #requestType()}.
     *
     * <p>Override only where the contract genuinely depends on how the node is configured
     * rather than on the message, which is rare: the same message being correct with and
     * without a field is the only reason to publish something the message does not say.
     */
    default ObjectNode inputSchema() {
        return CatalogContract.schemaFor(requestType());
    }

    /**
     * Executes the action.
     *
     * @param request a {@link #requestType()} message, already checked against its rules
     * @param context type resolution and JSON machinery shared across actions
     * @return a {@link #responseType()} message
     * @throws ActionException with a stable code on any failure
     */
    Message execute(Message request, ActionContext context) throws ActionException;
}
