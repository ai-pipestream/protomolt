package ai.protomolt.proto.schema.registry.composer;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.ScopeBudgets;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import ai.protomolt.proto.registry.CompatibilityWriteGate;
import ai.protomolt.proto.registry.ConfigDocumentStore;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaRegistryStores;
import ai.protomolt.proto.registry.WorkflowDocumentStore;
import ai.protomolt.proto.registry.service.PublishConfigAction;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.registry.service.SchemaRegistryServerConfig;
import com.google.protobuf.Descriptors;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/** Registry HTTP role with explicit storage, bind, authentication and compatibility choices. */
public final class SchemaRegistryModule implements ServiceModule {
    private static final String PREFIX = "PROTOMOLT_REGISTRY_";
    private static final String OPTION_PREFIX = PREFIX + "OPTION_";
    private static final Set<String> SETTINGS = Set.of("STORE", "HOST", "PORT", "AUTH", "TOKEN", "COMPATIBILITY");
    private final Supplier<SchemaRegistryStores> providers;

    public SchemaRegistryModule() { this(SchemaRegistryStores::discover); }

    /** Supplies provider factories for embedding; they are consulted only during wiring. */
    public SchemaRegistryModule(Supplier<SchemaRegistryStores> providers) {
        this.providers = java.util.Objects.requireNonNull(providers, "providers");
    }

    @Override
    public String role() { return "registry"; }

    @Override
    public ServiceMount wire(NodeContext context) {
        Map<String, String> env = context.environment();
        Map<String, String> options = new TreeMap<>();
        for (var entry : env.entrySet()) {
            if (entry.getKey().startsWith(OPTION_PREFIX)) {
                String name = entry.getKey().substring(OPTION_PREFIX.length()).toLowerCase(Locale.ROOT).replace('_', '-');
                if (name.isBlank() || options.putIfAbsent(name, entry.getValue()) != null) {
                    throw new IllegalArgumentException("Invalid or duplicate registry option: " + entry.getKey());
                }
            } else if (entry.getKey().startsWith(PREFIX) && !SETTINGS.contains(entry.getKey().substring(PREFIX.length()))) {
                throw new IllegalArgumentException("Unknown registry setting: " + entry.getKey());
            }
        }
        String provider = required(env, "STORE");
        String host = required(env, "HOST");
        int port = Integer.parseInt(required(env, "PORT"));
        String token = switch (required(env, "AUTH")) {
            case "token" -> required(env, "TOKEN");
            case "none" -> {
                if (env.containsKey(PREFIX + "TOKEN")) throw new IllegalArgumentException("TOKEN cannot be set with AUTH=none");
                yield null;
            }
            default -> throw new IllegalArgumentException("Registry AUTH must be token or none");
        };
        SchemaRegistryStore.WriteGate gate = switch (required(env, "COMPATIBILITY")) {
            case "wire" -> new CompatibilityWriteGate();
            case "none" -> null;
            default -> throw new IllegalArgumentException("Registry COMPATIBILITY must be wire or none");
        };
        var config = new SchemaRegistryServerConfig(host, port, "/health", "/protomolt",
                SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES, token);
        var store = providers.get().open(provider, options, gate);
        context.onClose(store);
        context.contributions().contribute(SchemaRegistryStore.class, store);
        if (store instanceof ConfigDocumentStore configs) {
            context.contributions().contribute(ConfigDocumentStore.class, configs);
            context.contributions().contribute(ProtoAction.class, new PublishConfigAction(configs));
        }
        if (store instanceof WorkflowDocumentStore workflows) {
            context.contributions().contribute(WorkflowDocumentStore.class, workflows);
        }
        ScopeBudgets budgets = context.contributions().shared(ScopeBudgets.class, ScopeBudgets::new);
        return new ServiceMount() {
            @Override
            public void start() {
                ActionContext actions = single(context.contributions().all(ActionContext.class), "ActionContext");
                if (actions == null) actions = ActionContext.create();
                for (var file : context.contributions().all(Descriptors.FileDescriptor.class)) actions.registry().registerFile(file);
                ActionCatalog catalog = ActionCatalog.empty(actions, budgets);
                for (var action : context.contributions().all(ProtoAction.class)) catalog = catalog.register(action);
                CallerResolver callers = single(context.contributions().all(CallerResolver.class), "CallerResolver");
                var server = new SchemaRegistryServer(config, store, catalog, callers);
                context.onClose(server);
                server.start();
            }

            @Override
            public void close() { /* Resources are registered independently for failure-safe cleanup. */ }
        };
    }

    private static <T> T single(List<T> values, String kind) {
        if (values.size() > 1) throw new IllegalStateException("Multiple registry " + kind + " contributions");
        return values.isEmpty() ? null : values.getFirst();
    }

    private static String required(Map<String, String> env, String key) {
        String value = env.get(PREFIX + key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(PREFIX + key + " is required");
        return value;
    }
}
