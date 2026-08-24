package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A framework-agnostic catalog of {@link ProtoAction}s: one registry of JSON-in/JSON-out verbs,
 * mountable as HTTP endpoints or MCP tools. {@link #list()} is the machine-readable tool
 * manifest; {@link #execute(String, ObjectNode)} is the single dispatch point. Registration,
 * replacement, and manifest snapshots are synchronized so a host that installs a plugin while
 * serving requests cannot corrupt iteration order or expose a partial catalog update. Action
 * execution itself runs outside that catalog monitor.
 */
public final class ActionCatalog {

    private final ActionContext context;
    private final Map<String, ProtoAction> actions = new LinkedHashMap<>();
    private final ScopeBudgets budgets;

    private ActionCatalog(ActionContext context, ScopeBudgets budgets) {
        this.context = Objects.requireNonNull(context, "context");
        this.budgets = Objects.requireNonNull(budgets, "budgets");
    }

    /**
     * A catalog with every built-in action registered, spending on its own ledger, for a
     * catalog that is the node's only enforcement point. A node that also serves another
     * enforcement point passes one shared ledger through
     * {@link #defaults(ActionContext, ScopeBudgets)}, or a principal gets a separate
     * allowance per surface.
     */
    public static ActionCatalog defaults(ActionContext context) {
        return defaults(context, new ScopeBudgets());
    }

    /**
     * A catalog with every built-in action registered, spending on {@code budgets}: the
     * node's other enforcement points take the same ledger, so a principal's per-scope
     * budget is one allowance however it reaches the node.
     */
    public static ActionCatalog defaults(ActionContext context, ScopeBudgets budgets) {
        ActionCatalog catalog = new ActionCatalog(context, budgets);
        catalog.register(new CompileAction());
        catalog.register(new ValidateMessageAction());
        catalog.register(new DiffSchemasAction());
        catalog.register(new CheckCompatAction());
        catalog.register(new RenderJsonSchemaAction());
        catalog.register(new RenderPromptAction());
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
    public synchronized ActionCatalog register(ProtoAction action) {
        String name = Objects.requireNonNull(action, "action").name();
        ProtoAction existing = actions.putIfAbsent(name, action);
        if (existing != null) {
            throw new IllegalStateException("Action '" + name + "' is already registered ("
                    + existing.getClass().getName() + "); use replace() to override it");
        }
        return this;
    }

    /** Deliberately replaces (or adds) an action — the explicit override path. */
    public synchronized ActionCatalog replace(ProtoAction action) {
        actions.put(Objects.requireNonNull(action, "action").name(), action);
        return this;
    }

    /**
     * The action registered under {@code name}.
     *
     * @throws ActionException {@code unknown-action} listing the available names
     */
    public synchronized ProtoAction get(String name) throws ActionException {
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
    public synchronized List<String> names() {
        return List.copyOf(actions.keySet());
    }

    /**
     * Returns an independent catalog with the same context, action references, and registration
     * order. Subsequent registration or replacement on either catalog does not mutate the other.
     * The fork spends on the same ledger: a fork per session or per plugin set is one more way
     * into the same node, not a second allowance.
     */
    public synchronized ActionCatalog fork() {
        ActionCatalog fork = new ActionCatalog(context, budgets);
        fork.actions.putAll(actions);
        return fork;
    }

    /**
     * The ledger this catalog spends on, for a host wiring another enforcement point over
     * the same node.
     */
    public ScopeBudgets budgets() {
        return budgets;
    }

    /** The tool manifest: {@code [{name, description, inputSchema}, ...]}. */
    public synchronized ArrayNode list() {
        return list(Caller.operator());
    }

    /**
     * The tool manifest as {@code caller} sees it: only actions whose required scope the
     * caller holds. An action with no declared scope is invisible to a scoped caller for
     * the same reason {@link #execute(String, ObjectNode, Caller)} refuses it.
     */
    public synchronized ArrayNode list(Caller caller) {
        ArrayNode manifest = context.objectMapper().createArrayNode();
        for (ProtoAction action : actions.values()) {
            if (!caller.unrestricted()) {
                String scope = action.requiredScope();
                if (scope.isBlank() || !caller.holds(scope)) {
                    continue;
                }
            }
            ObjectNode entry = manifest.addObject();
            entry.put("name", action.name());
            entry.put("description", action.description());
            entry.set("inputSchema", action.inputSchema());
        }
        return manifest;
    }

    /**
     * Dispatches {@code input} to the named action with process authority.
     *
     * <p>The envelope is checked against the action's request message first. Calls arriving
     * over gRPC pass a validating interceptor before they reach a handler; calls arriving as
     * catalog verbs do not, so without this the same request would be refused on one surface
     * and accepted on the other.
     */
    public ObjectNode execute(String name, ObjectNode input) throws ActionException {
        return execute(name, input, Caller.operator());
    }

    /**
     * Dispatches {@code input} to the named action as {@code caller}, refusing before
     * dispatch when the caller does not hold the action's required scope.
     */
    public ObjectNode execute(String name, ObjectNode input, Caller caller)
            throws ActionException {
        // get() takes the catalog monitor only long enough to resolve a stable action reference.
        ProtoAction action = get(name);
        requireScope(action, caller);
        requireBudget(action, caller, input);
        ObjectNode envelope = Inputs.requireEnvelope(input);
        CatalogContract.check(envelope, action.requestType(), name);
        return action.execute(envelope, context);
    }

    /**
     * Dispatches like {@link #execute}, but lets a {@link StreamingAction} emit results
     * incrementally. Unary actions emit their single result, so fronts that stream get one
     * contract for every verb.
     */
    public void executeStreaming(String name, ObjectNode input, StreamEmitter emitter)
            throws ActionException {
        executeStreaming(name, input, Caller.operator(), emitter);
    }

    /** Dispatches like {@link #executeStreaming}, refusing first when {@code caller} lacks the scope. */
    public void executeStreaming(String name, ObjectNode input, Caller caller,
            StreamEmitter emitter) throws ActionException {
        ProtoAction action = get(name);
        requireScope(action, caller);
        requireBudget(action, caller, input);
        ObjectNode envelope = Inputs.requireEnvelope(input);
        CatalogContract.check(envelope, action.requestType(), name);
        if (action instanceof StreamingAction streaming) {
            streaming.executeStreaming(envelope, context, emitter);
        } else {
            emitter.emit(action.execute(envelope, context));
        }
    }

    /** Spends the caller's budget on the action's scope, refusing when it is exhausted. */
    private void requireBudget(ProtoAction action, Caller caller, ObjectNode input)
            throws ActionException {
        if (caller.unrestricted() || caller.budgets().isEmpty()) {
            return;
        }
        String scope = action.requiredScope();
        Caller.Budget budget = caller.budgets().get(scope);
        if (budget == null) {
            return;
        }
        long payloadBytes = budget.maxPayloadBytes() > 0 && input != null
                ? input.toString().getBytes(StandardCharsets.UTF_8).length
                : -1;
        Optional<String> refusal = budgets.refuse(caller, scope, payloadBytes);
        if (refusal.isPresent()) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("action", action.name());
            details.put("caller", caller.name());
            details.put("requiredScope", scope);
            throw new ActionException("resource-exhausted", refusal.get(), details);
        }
    }

    private static void requireScope(ProtoAction action, Caller caller) throws ActionException {
        if (caller.unrestricted()) {
            return;
        }
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("action", action.name());
        details.put("caller", caller.name());
        String scope = action.requiredScope();
        if (scope.isBlank()) {
            throw new ActionException("permission-denied",
                    "Action '" + action.name() + "' declares no required scope; "
                            + "a scoped caller cannot execute it", details);
        }
        details.put("requiredScope", scope);
        if (!caller.holds(scope)) {
            throw new ActionException("permission-denied",
                    "caller '" + caller.name() + "' does not hold '" + scope + "', which "
                            + action.name() + " requires", details);
        }
    }
}
