package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.codegen.GenerateStubsAction;
import ai.pipestream.proto.gather.git.GatherGitAction;
import ai.pipestream.proto.grpc.invoke.GrpcInvokeAction;
import ai.pipestream.proto.grpc.invoke.ReflectAction;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;

import java.nio.file.Path;

/**
 * Stdio entry point: {@code protomolt-mcp [--registry-git <path>]}.
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
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--registry-git" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("--registry-git requires a path");
                        System.exit(2);
                    }
                    registryPath = Path.of(args[++i]);
                }
                case "--help", "-h" -> {
                    System.err.println("usage: protomolt-mcp [--registry-git <path>]");
                    return;
                }
                default -> {
                    System.err.println("unknown argument: " + args[i]);
                    System.exit(2);
                }
            }
        }

        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .register(new GrpcInvokeAction())
                .register(new ReflectAction())
                .register(new GenerateStubsAction())
                .register(new GatherGitAction());
        String version = McpMain.class.getPackage().getImplementationVersion();
        if (registryPath == null) {
            McpServer server = new McpServer(catalog, null,
                    "protomolt", version != null ? version : "dev");
            System.err.println("protomolt-mcp: serving " + catalog.names().size() + " tools on stdio");
            server.run(System.in, System.out);
            return;
        }
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(registryPath)
                .build()) {
            McpServer server = new McpServer(catalog, new RegistryResources(store),
                    "protomolt", version != null ? version : "dev");
            System.err.println("protomolt-mcp: serving " + catalog.names().size()
                    + " tools and registry resources from " + registryPath + " on stdio");
            server.run(System.in, System.out);
        }
    }
}
