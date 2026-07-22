package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * One JSON-in/JSON-out verb over the toolkit: a named, self-describing operation that a UI form,
 * an HTTP endpoint, or a tool-using LLM can drive blind.
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

    /** JSON Schema (draft 2020-12) for the input envelope accepted by {@link #execute}. */
    ObjectNode inputSchema();

    /**
     * Executes the action.
     *
     * @param input   the input envelope; must satisfy {@link #inputSchema()}
     * @param context type resolution and JSON machinery shared across actions
     * @return the structured result document
     * @throws ActionException with a stable code on any failure, including envelope violations
     *         ({@code invalid-input})
     */
    ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException;
}
