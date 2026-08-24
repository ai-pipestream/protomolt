package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

/**
 * One verb over the toolkit: a named operation a UI form, an HTTP endpoint, or a tool-using
 * LLM can drive blind.
 *
 * <p>A verb describes itself by naming the protobuf message it accepts, not by writing a
 * schema. The catalog derives the published schema from that message and enforces it before
 * dispatch, so the bounds a caller reads are the bounds the verb applies and neither can
 * drift from the other.
 *
 * <p>An action is stateless; everything it needs beyond the input envelope comes from the
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

    /** The request message this verb accepts; the envelope is its canonical proto3 JSON. */
    Descriptor requestType();

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
     * @param input   the input envelope, already checked against {@link #requestType()}
     * @param context type resolution and JSON machinery shared across actions
     * @return the structured result document
     * @throws ActionException with a stable code on any failure, including envelope violations
     *         ({@code invalid-input})
     */
    ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException;
}
