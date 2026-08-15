package ai.pipestream.proto.registry.server;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.workflow.WorkflowRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * The git-backed schema registry as a mountable role. Wiring creates the
 * store and contributes it (plus a {@link WorkflowRepository} view of it)
 * for later-wired modules: the parse module registers workflows, the jobs
 * module reads definitions and contributes its verbs. Starting builds the
 * actions catalog from everything contributed and serves HTTP.
 */
public final class RegistryModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "registry";

    private final Path repositoryDir;
    private final SchemaRegistryServerConfig serverConfig;
    private GitSchemaRegistryStore store;
    private SchemaRegistryServer server;
    private int httpPort = -1;

    /**
     * Creates the module.
     *
     * @param repositoryDir the git repository backing the registry
     * @param serverConfig the HTTP server configuration
     */
    public RegistryModule(Path repositoryDir, SchemaRegistryServerConfig serverConfig) {
        if (repositoryDir == null) {
            throw new IllegalArgumentException("repositoryDir must not be null");
        }
        if (serverConfig == null) {
            throw new IllegalArgumentException("serverConfig must not be null");
        }
        this.repositoryDir = repositoryDir;
        this.serverConfig = serverConfig;
    }

    @Override
    public String role() {
        return ROLE;
    }

    @Override
    public ServiceMount wire(NodeContext context) {
        store = GitSchemaRegistryStore.builder().repositoryDir(repositoryDir).build();
        context.contributions().contribute(GitSchemaRegistryStore.class, store);
        context.contributions().contribute(WorkflowRepository.class, workflowRepository(store));
        return new ServiceMount() {
            @Override
            public void start() {
                List<ActionContext> contexts = context.contributions().all(ActionContext.class);
                ActionContext actionContext =
                        contexts.isEmpty() ? ActionContext.create() : contexts.getFirst();
                for (Descriptors.FileDescriptor descriptor
                        : context.contributions().all(Descriptors.FileDescriptor.class)) {
                    actionContext.registry().registerFile(descriptor);
                }
                ActionCatalog catalog = ActionCatalog.defaults(actionContext);
                for (ProtoAction action : context.contributions().all(ProtoAction.class)) {
                    catalog = catalog.register(action);
                }
                server = new SchemaRegistryServer(serverConfig, store, catalog);
                httpPort = server.start();
            }

            @Override
            public void close() {
                if (server != null) {
                    server.close();
                }
                store.close();
            }
        };
    }

    /** The store this node serves; valid after wiring. */
    public GitSchemaRegistryStore store() {
        if (store == null) {
            throw new IllegalStateException("registry module has not wired");
        }
        return store;
    }

    /** The bound HTTP port; only valid after start. */
    public int httpPort() {
        if (httpPort < 0) {
            throw new IllegalStateException("registry module has not started");
        }
        return httpPort;
    }

    /**
     * A read view of the store's workflow definitions as parsed JSON. A stored
     * workflow that is not a JSON object is corrupt state, not a missing
     * workflow: it fails loudly instead of reading as "not found".
     */
    public static WorkflowRepository workflowRepository(GitSchemaRegistryStore store) {
        return workflowRepository(store::workflow);
    }

    /** The same view over any workflow-text lookup; the seam for tests. */
    static WorkflowRepository workflowRepository(Function<String, Optional<String>> store) {
        ObjectMapper json = new ObjectMapper();
        return name -> store.apply(name).map(t -> {
            final com.fasterxml.jackson.databind.JsonNode node;
            try {
                node = json.readTree(t);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new IllegalStateException(
                        "Stored workflow '" + name + "' is not valid JSON", e);
            }
            if (!(node instanceof ObjectNode workflow)) {
                throw new IllegalStateException(
                        "Stored workflow '" + name + "' is not a JSON object");
            }
            return workflow;
        });
    }
}
