package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.codegen.GenerateStubsAction;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.workflow.WorkflowWorkbenchActions;
import ai.pipestream.proto.gather.git.GatherGitAction;
import ai.pipestream.proto.grpc.invoke.GrpcInvokeAction;
import ai.pipestream.proto.grpc.invoke.ReflectAction;
import ai.pipestream.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.pipestream.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.WorkflowVersionRepository;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workspace.ServiceWorkspaceActions;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.RegistryWorkflowVersionRepository;

import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stdio entry point: {@code protomolt-mcp [--registry-git <path>]
 * [--service-workspace <path>] [--workflow-workspace <path>]}.
 *
 * <p>Without arguments the server exposes the action catalog as tools. With
 * {@code --registry-git}, the git-backed registry at the given path is additionally exposed
 * as MCP resources (subjects, version indexes, schema texts). Protocol traffic owns stdout;
 * diagnostics go to stderr, as the stdio transport requires.</p>
 */
public final class McpMain {

    private static final Logger LOG = LoggerFactory.getLogger(McpMain.class);

    private McpMain() {
    }

    public static void main(String[] args) throws Exception {
        Path registryPath = null;
        Path serviceWorkspacePath = null;
        Path workflowWorkspacePath = null;
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
                case "--workflow-workspace" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("--workflow-workspace requires a path");
                        System.exit(2);
                    }
                    workflowWorkspacePath = Path.of(args[++i]);
                }
                case "--help", "-h" -> {
                    System.err.println("usage: protomolt-mcp [--registry-git <path>] "
                            + "[--service-workspace <path>] [--workflow-workspace <path>]");
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
        ArtifactRepository artifacts = workflowWorkspacePath == null ? null
                : new FileSystemArtifactRepository(workflowWorkspacePath.resolve("artifacts"));
        RunEvidenceRepository runs = workflowWorkspacePath == null ? null
                : new FileSystemRunEvidenceRepository(workflowWorkspacePath.resolve("runs"));
        String version = McpMain.class.getPackage().getImplementationVersion();
        if (registryPath == null) {
            ActionCatalog catalog = catalog(ActionContext.create(), serviceProfiles,
                    artifacts, runs, null, null);
            McpServer server = new McpServer(catalog,
                    serviceProfiles == null ? null : new ServiceProfileResources(serviceProfiles),
                    "protomolt", version != null ? version : "dev");
            LOG.info("protomolt-mcp: serving {} tools on stdio", catalog.names().size());
            server.run(System.in, System.out);
            return;
        }
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(registryPath)
                .build()) {
            ActionCatalog catalog = catalog(ActionContext.create(), serviceProfiles,
                    artifacts, runs, new RegistryWorkflowVersionRepository(store), store);
            McpServer server = new McpServer(catalog, CompositeResources.of(
                    new RegistryResources(store), serviceProfiles == null
                            ? null : new ServiceProfileResources(serviceProfiles, store)),
                    "protomolt", version != null ? version : "dev");
            LOG.info("protomolt-mcp: serving {} tools and registry resources from {} on stdio",
                    catalog.names().size(), registryPath);
            server.run(System.in, System.out);
        }
    }

    /** Builds the standalone MCP inventory, shared by the launcher and its inventory tests. */
    static ActionCatalog catalog(ActionContext context) {
        return catalog(context, null);
    }

    /** Builds the standalone MCP inventory with optional durable service-profile storage. */
    static ActionCatalog catalog(ActionContext context, ServiceProfileRepository serviceProfiles) {
        return catalog(context, serviceProfiles, null, null, null, null);
    }

    private static ActionCatalog catalog(ActionContext context,
                                         ServiceProfileRepository serviceProfiles,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows,
                                         ai.pipestream.proto.registry.SchemaRegistryStore registry) {
        ActionCatalog catalog = ActionCatalog.defaults(context)
                .register(new GrpcInvokeAction())
                .register(new ReflectAction())
                .register(new GenerateStubsAction())
                .register(new GatherGitAction());
        ServiceWorkspaceActions.register(catalog, serviceProfiles, registry,
                ai.pipestream.proto.grpc.invoke.ChannelFactory.standard());
        return WorkflowWorkbenchActions.register(catalog, new WorkflowRunner(), artifacts, runs,
                workflows);
    }
}
