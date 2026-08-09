package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.codegen.GenerateStubsAction;
import ai.pipestream.proto.gather.git.GatherGitAction;
import ai.pipestream.proto.grpc.invoke.GrpcInvokeAction;
import ai.pipestream.proto.grpc.invoke.ReflectAction;
import ai.pipestream.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.workspace.ServiceWorkspaceActions;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;

import java.nio.file.Path;

/**
 * Stdio entry point: {@code protomolt-mcp [--registry-git <path>]
 * [--service-workspace <path>]}.
 *
 * <p>Without arguments the server exposes the action catalog as tools. With
 * {@code --registry-git}, the git-backed registry at the given path is additionally exposed
 * as MCP resources (subjects, version indexes, schema texts). Protocol traffic owns stdout;
 * diagnostics go to stderr, as the stdio transport requires.</p>
 */
public final class McpMain {

    private McpMain() {
    }

    public static void main(String[] args) throws Exception {
        Path registryPath = null;
        Path serviceWorkspacePath = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--registry-git" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("--registry-git requires a path");
                        System.exit(2);
                    }
                    registryPath = Path.of(args[++i]);
                }
                case "--service-workspace" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("--service-workspace requires a path");
                        System.exit(2);
                    }
                    serviceWorkspacePath = Path.of(args[++i]);
                }
                case "--help", "-h" -> {
                    System.err.println("usage: protomolt-mcp [--registry-git <path>] "
                            + "[--service-workspace <path>]");
                    return;
                }
                default -> {
                    System.err.println("unknown argument: " + args[i]);
                    System.exit(2);
                }
            }
        }

        ServiceProfileRepository serviceProfiles = serviceWorkspacePath == null
                ? null : new FileSystemServiceProfileRepository(serviceWorkspacePath);
        ActionCatalog catalog = catalog(ActionContext.create(), serviceProfiles);
        String version = McpMain.class.getPackage().getImplementationVersion();
        if (registryPath == null) {
            McpServer server = new McpServer(catalog,
                    serviceProfiles == null ? null : new ServiceProfileResources(serviceProfiles),
                    "protomolt", version != null ? version : "dev");
            System.err.println("protomolt-mcp: serving " + catalog.names().size() + " tools on stdio");
            server.run(System.in, System.out);
            return;
        }
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(registryPath)
                .build()) {
            McpServer server = new McpServer(catalog, CompositeResources.of(
                    new RegistryResources(store), serviceProfiles == null
                            ? null : new ServiceProfileResources(serviceProfiles)),
                    "protomolt", version != null ? version : "dev");
            System.err.println("protomolt-mcp: serving " + catalog.names().size()
                    + " tools and registry resources from " + registryPath + " on stdio");
            server.run(System.in, System.out);
        }
    }

    /** Builds the standalone MCP inventory, shared by the launcher and its inventory tests. */
    static ActionCatalog catalog(ActionContext context) {
        return catalog(context, null);
    }

    /** Builds the standalone MCP inventory with optional durable service-profile storage. */
    static ActionCatalog catalog(ActionContext context, ServiceProfileRepository serviceProfiles) {
        ActionCatalog catalog = ActionCatalog.defaults(context)
                .register(new GrpcInvokeAction())
                .register(new ReflectAction())
                .register(new GenerateStubsAction())
                .register(new GatherGitAction());
        return ServiceWorkspaceActions.register(catalog, serviceProfiles);
    }
}
