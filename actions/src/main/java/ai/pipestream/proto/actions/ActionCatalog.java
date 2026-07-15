package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.List;

/**
 * A framework-agnostic catalog of {@link ProtoAction}s: one registry of JSON-in/JSON-out verbs,
 * mountable as HTTP endpoints or MCP tools. {@link #list()} is the machine-readable tool
 * manifest; {@link #execute(String, ObjectNode)} is the single dispatch point.
 */
public final class ActionCatalog {

    private final ActionContext context;
    private final Map<String, ProtoAction> actions = new LinkedHashMap<>();

    private ActionCatalog(ActionContext context) {
        this.context = Objects.requireNonNull(context, "context");
    }

    /** A catalog with every built-in action registered. */
    public static ActionCatalog defaults(ActionContext context) {
        ActionCatalog catalog = new ActionCatalog(context);
        catalog.register(new CompileAction());
        catalog.register(new ValidateMessageAction());
        catalog.register(new DiffSchemasAction());
        catalog.register(new CheckCompatAction());
        catalog.register(new RenderJsonSchemaAction());
        catalog.register(new RenderIndexMappingsAction());
        catalog.register(new EvalCelAction());
        catalog.register(new MapMessageAction());
        catalog.register(new SynthesizeShapeAction());
        catalog.register(new JoinMessagesAction());
        catalog.register(new MergeSchemasAction());
        catalog.register(new CheckRulesAction());
        catalog.register(new InferSchemaAction());
        catalog.register(new MaskMessageAction());
        catalog.register(new ExtractMetadataAction());
        catalog.register(new ListTypesAction());
        return catalog;
    }

    /**
     * Registers an action under its {@link ProtoAction#name()}.
     *
     * @throws IllegalStateException when the name is taken — a plugin or built-in silently
     *         shadowing another action would change behavior by registration order; use
     *         {@link #replace} when overriding is the intent
     */
    public ActionCatalog register(ProtoAction action) {
        String name = Objects.requireNonNull(action, "action").name();
        ProtoAction existing = actions.putIfAbsent(name, action);
        if (existing != null) {
            throw new IllegalStateException("Action '" + name + "' is already registered ("
                    + existing.getClass().getName() + "); use replace() to override it");
        }
        return this;
    }

    /** Deliberately replaces (or adds) an action — the explicit override path. */
    public ActionCatalog replace(ProtoAction action) {
        actions.put(Objects.requireNonNull(action, "action").name(), action);
        return this;
    }

    /**
     * The action registered under {@code name}.
     *
     * @throws ActionException {@code unknown-action} listing the available names
     */
    public ProtoAction get(String name) throws ActionException {
        ProtoAction action = actions.get(name);
        if (action == null) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("action", String.valueOf(name));
            ArrayNode available = details.putArray("available");
            actions.keySet().forEach(available::add);
            throw new ActionException("unknown-action",
                    "Unknown action '" + name + "'. Available actions: "
                            + String.join(", ", actions.keySet()),
                    details);
        }
        return action;
    }

    /** Registered action names, in registration order. */
    public List<String> names() {
        return List.copyOf(actions.keySet());
    }

    /** The tool manifest: {@code [{name, description, inputSchema}, ...]}. */
    public ArrayNode list() {
        ArrayNode manifest = context.objectMapper().createArrayNode();
        for (ProtoAction action : actions.values()) {
            ObjectNode entry = manifest.addObject();
            entry.put("name", action.name());
            entry.put("description", action.description());
            entry.set("inputSchema", action.inputSchema());
        }
        return manifest;
    }

    /** Dispatches {@code input} to the named action with this catalog's context. */
    public ObjectNode execute(String name, ObjectNode input) throws ActionException {
        ProtoAction action = get(name);
        return action.execute(Inputs.requireEnvelope(input), context);
    }
}
