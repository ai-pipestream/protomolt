package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * A framework-agnostic catalog of {@link ProtoAction}s: one registry of JSON-in/JSON-out verbs,
 * mountable as HTTP endpoints or MCP tools. {@link #list()} is the machine-readable tool
 * manifest; {@link #execute(String, ObjectNode)} is the single dispatch point. Registration,
 * replacement, and manifest snapshots are synchronized so a host that installs a plugin while
 * serving requests cannot corrupt iteration order or expose a partial catalog update. Action
 * execution itself runs outside that catalog monitor.
 *
 * <p>Optional actions are discovered from the thread context class loader through
 * {@link ActionProvider}. Only installed providers are advertised. Add
 * {@code protomolt-actions-toolkit} for toolkit verbs and {@code protomolt-actions-index}
 * for index rendering; the full launchers include both explicitly. Provider loading failures abort
 * construction rather than silently returning a partially populated catalog.
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
     * A catalog with the installed action providers registered,
     * spending on its own ledger, for the node's only enforcement point. A node that also serves another
     * enforcement point passes one shared ledger through
     * {@link #defaults(ActionContext, ScopeBudgets)}, or a principal gets a separate
     * allowance per surface.
     */
    public static ActionCatalog defaults(ActionContext context) {
        return defaults(context, new ScopeBudgets());
    }

    /**
     * A catalog with the installed action providers registered,
     * spending on {@code budgets}. The node's other enforcement points take the same ledger, so a principal's per-scope
     * budget is one allowance however it reaches the node.
     */
    public static ActionCatalog defaults(ActionContext context, ScopeBudgets budgets) {
        ActionCatalog catalog = new ActionCatalog(context, budgets);
        for (ActionProvider provider : ServiceLoader.load(ActionProvider.class).stream()
                .sorted(Comparator.comparing(p -> p.type().getName()))
                .map(ServiceLoader.Provider::get).toList()) {
            for (ProtoAction action : provider.actions()) {
                catalog.register(action);
            }
        }
        return catalog;
    }

    /** An empty catalog with the same authorization, budgets and contract checks as defaults. */
    public static ActionCatalog empty(ActionContext context, ScopeBudgets budgets) {
        return new ActionCatalog(context, budgets);
    }

    /** An empty catalog with a private budget ledger. */
    public static ActionCatalog empty(ActionContext context) {
        return empty(context, new ScopeBudgets());
    }

    /**
     * Registers an action under its {@link ProtoAction#name()}.
     *
     * @throws IllegalStateException when the name is taken — a plugin or built-in silently
     *         shadowing another action would change behavior by registration order; use
     *         {@link #replace} when overriding is the intent
     */
    public synchronized ActionCatalog register(ProtoAction action) {
        String name = checkedName(action);
        ProtoAction existing = actions.putIfAbsent(name, action);
        if (existing != null) {
            throw new IllegalStateException("Action '" + name + "' is already registered ("
                    + existing.getClass().getName() + "); use replace() to override it");
        }
        return this;
    }

    private static String checkedName(ProtoAction action) {
        Objects.requireNonNull(action, "action");
        String name = Objects.requireNonNull(action.name(), "action name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Action name must not be blank");
        }
        Objects.requireNonNull(action.requestType(), "request contract for " + name);
        Objects.requireNonNull(action.responseType(), "response contract for " + name);
        return name;
    }

    /** Deliberately replaces (or adds) an action — the explicit override path. */
    public synchronized ActionCatalog replace(ProtoAction action) {
        actions.put(checkedName(action), action);
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
     * Dispatches a JSON envelope to the named action with process authority, answering with
     * the result as JSON.
     */
    public ObjectNode execute(String name, ObjectNode input) throws ActionException {
        return execute(name, input, Caller.operator());
    }

    /**
     * Dispatches a JSON envelope to the named action as {@code caller}, refusing before
     * dispatch when the caller does not hold the action's required scope.
     *
     * <p>This is the JSON edge, not a second dispatch path: the envelope is parsed into the
     * action's request message and the reply is rendered back from its response message, so a
     * JSON caller reaches the same verb, under the same contract, as a typed one.
     */
    public ObjectNode execute(String name, ObjectNode input, Caller caller)
            throws ActionException {
        // get() takes the catalog monitor only long enough to resolve a stable action reference.
        ProtoAction action = get(name);
        // Before the envelope is looked at: a caller who may not run the verb learns only
        // that, not whether the request they sent would have been accepted.
        requireScope(action, caller);
        ObjectNode envelope = Inputs.requireEnvelope(input);
        com.google.protobuf.util.JsonFormat.TypeRegistry registry =
                action.typeRegistry(envelope);
        Message request = CatalogContract.toRequest(
                envelope, action.requestType(), name, registry);
        return action.renderJsonReply(dispatch(action, name, request, caller), registry);
    }

    /** Dispatches a typed request to the named action with process authority. */
    public Message execute(String name, Message request) throws ActionException {
        return execute(name, request, Caller.operator());
    }

    /**
     * Dispatches a typed request to the named action as {@code caller} — the path every
     * surface converges on.
     *
     * <p>The request is checked against the message's own rules here rather than at each
     * front. A verb is reachable over gRPC, over the JSON gateway, as a REST method and as an
     * MCP tool; enforcing the contract at the one point they share is what stops the same
     * request from being refused on one surface and accepted on another.
     */
    public Message execute(String name, Message request, Caller caller) throws ActionException {
        return dispatch(get(name), name, request, caller);
    }

    private Message dispatch(ProtoAction action, String name, Message request, Caller caller)
            throws ActionException {
        // Idempotent when the JSON edge already refused: the check is by scope, not by call.
        requireScope(action, caller);
        requireBudget(action, caller, request);
        request = CatalogContract.checkedRequest(request, action.requestType(), name);
        return CatalogContract.checkedResponse(action.execute(request, context, caller), action.responseType(), name);
    }

    /**
     * Dispatches like {@link #execute}, but lets a {@link StreamingAction} emit results
     * incrementally. Unary actions emit their single result, so fronts that stream get one
     * contract for every verb.
     */
    public void executeStreaming(String name, Message request, StreamEmitter emitter)
            throws ActionException {
        executeStreaming(name, request, Caller.operator(), emitter);
    }

    /** Dispatches like {@link #executeStreaming}, refusing first when {@code caller} lacks the scope. */
    public void executeStreaming(String name, Message request, Caller caller,
            StreamEmitter emitter) throws ActionException {
        ProtoAction action = get(name);
        requireScope(action, caller);
        requireBudget(action, caller, request);
        request = CatalogContract.checkedRequest(request, action.requestType(), name);
        var checked = new ContractStreamEmitter(emitter, action.responseType(), name);
        try {
            if (action instanceof StreamingAction streaming) {
                streaming.executeStreaming(request, context, caller, checked);
            } else {
                checked.emit(action.execute(request, context, caller));
            }
            checked.complete();
        } finally {
            checked.finish();
        }
    }

    /**
     * Dispatches a JSON envelope like {@link #executeStreaming(String, Message, StreamEmitter)},
     * rendering each emission back as JSON — the streaming half of the JSON edge.
     */
    public void executeStreaming(String name, ObjectNode input, JsonStreamEmitter emitter)
            throws ActionException {
        executeStreaming(name, input, Caller.operator(), emitter);
    }

    /** Dispatches like {@link #executeStreaming(String, ObjectNode, JsonStreamEmitter)}, as {@code caller}. */
    public void executeStreaming(String name, ObjectNode input, Caller caller,
            JsonStreamEmitter emitter) throws ActionException {
        ProtoAction action = get(name);
        requireScope(action, caller);
        ObjectNode envelope = Inputs.requireEnvelope(input);
        com.google.protobuf.util.JsonFormat.TypeRegistry registry =
                action.typeRegistry(envelope);
        Message request = CatalogContract.toRequest(
                envelope, action.requestType(), name, registry);
        executeStreaming(name, request, caller,
                message -> emitter.emit(action.renderJsonReply(message, registry)));
    }

    /** Spends the caller's budget on the action's scope, refusing when it is exhausted. */
    private void requireBudget(ProtoAction action, Caller caller, Message request)
            throws ActionException {
        if (caller.unrestricted() || caller.budgets().isEmpty()) {
            return;
        }
        String scope = action.requiredScope();
        Caller.Budget budget = caller.budgets().get(scope);
        if (budget == null) {
            return;
        }
        long payloadBytes = budget.maxPayloadBytes() > 0 && request != null
                ? request.getSerializedSize()
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
