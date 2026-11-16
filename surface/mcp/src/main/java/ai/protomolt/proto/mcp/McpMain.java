package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.codegen.GenerateStubsAction;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.WorkflowWorkbenchActions;
import ai.protomolt.proto.acquire.gather.git.GatherGitAction;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.GrpcInvokeAction;
import ai.protomolt.proto.grpc.invoke.ReflectAction;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workspace.ProfileCredentialResolver;
import ai.protomolt.proto.grpc.workspace.ProfileTlsConfigs;
import ai.protomolt.proto.grpc.workspace.ReflectedServiceActions;
import ai.protomolt.proto.grpc.workspace.ServiceWorkspaceActions;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.schema.registry.git.RegistryWorkflowVersionRepository;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stdio entry point: {@code protomolt-mcp [--registry-git <path>]
 * [--service-workspace <path>] [--profile-tls-config <path>] [--workflow-workspace <path>]}.
 *
 * <p>Without arguments the server exposes the action catalog as tools. With
 * {@code --registry-git}, the git-backed registry at the given path is additionally exposed
 * as MCP resources (subjects, version indexes, schema texts). With
 * {@code --profile-tls-config} (or {@code PROTOMOLT_PROFILE_TLS_CONFIG}), service-profile
 * {@code trust_ref} and {@code client_certificate_ref} bindings resolve against the configured
 * PEM mounts over pooled outbound TLS channels. Protocol traffic owns stdout; diagnostics go to
 * stderr, as the stdio transport requires.</p>
 */
public final class McpMain {

    static final String ENV_PROFILE_TLS_CONFIG = "PROTOMOLT_PROFILE_TLS_CONFIG";
    static final String FLAG_PROFILE_TLS_CONFIG = "--profile-tls-config";

    private static final Logger LOG = LoggerFactory.getLogger(McpMain.class);

    private McpMain() {
    }

    public static void main(String[] args) throws Exception {
        String envTlsConfig = System.getenv(ENV_PROFILE_TLS_CONFIG);
        run(args, envTlsConfig, System.in, System.out);
    }

    static void run(String[] args, String envTlsConfig, InputStream in, OutputStream out)
            throws Exception {
        Path registryPath = null;
        Path serviceWorkspacePath = null;
        Path profileTlsConfigPath = envTlsConfig == null || envTlsConfig.isBlank()
                ? null : Path.of(envTlsConfig.trim());
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
                case FLAG_PROFILE_TLS_CONFIG -> {
                    if (i + 1 >= args.length) {
                        System.err.println("--profile-tls-config requires a path");
                        System.exit(2);
                    }
                    profileTlsConfigPath = Path.of(args[++i].trim());
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
                            + "[--service-workspace <path>] "
                            + "[--profile-tls-config <path>] (or PROTOMOLT_PROFILE_TLS_CONFIG) "
                            + "[--workflow-workspace <path>]");
                    return;
                }
                default -> {
                    System.err.println("unknown argument: " + args[i]);
                    System.exit(2);
                }
            }
        }

        ProfileCredentialResolver credentials = profileTlsConfigPath == null
                ? null
                : ProfileCredentialResolver.withTls(
                        null, ProfileTlsConfigs.loadResolver(profileTlsConfigPath));
        ServiceProfileRepository serviceProfiles = serviceWorkspacePath == null
                ? null : new FileSystemServiceProfileRepository(serviceWorkspacePath);
        ArtifactRepository artifacts = workflowWorkspacePath == null ? null
                : new FileSystemArtifactRepository(workflowWorkspacePath.resolve("artifacts"));
        RunEvidenceRepository runs = workflowWorkspacePath == null ? null
                : new FileSystemRunEvidenceRepository(workflowWorkspacePath.resolve("runs"));
        String version = McpMain.class.getPackage().getImplementationVersion();
        try (ChannelFactory.Closeable channels = ChannelFactory.pooled()) {
            if (registryPath == null) {
                ActionCatalog catalog = catalog(ActionContext.create(), serviceProfiles,
                        artifacts, runs, null, null, channels, credentials);
                McpServer server = new McpServer(catalog,
                        serviceProfiles == null ? null : new ServiceProfileResources(serviceProfiles),
                        "protomolt", version != null ? version : "dev");
                LOG.info("protomolt-mcp: serving {} tools on stdio", catalog.names().size());
                server.run(in, out);
                return;
            }
            try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                    .repositoryDir(registryPath)
                    .build()) {
                ActionCatalog catalog = catalog(ActionContext.create(), serviceProfiles,
                        artifacts, runs, new RegistryWorkflowVersionRepository(store), store,
                        channels, credentials);
                McpServer server = new McpServer(catalog, CompositeResources.of(
                        new RegistryResources(store), serviceProfiles == null
                                ? null : new ServiceProfileResources(serviceProfiles, store)),
                        "protomolt", version != null ? version : "dev");
                LOG.info("protomolt-mcp: serving {} tools and registry resources from {} on stdio",
                        catalog.names().size(), registryPath);
                server.run(in, out);
            }
        }
    }

    /** Builds the standalone MCP inventory, shared by the launcher and its inventory tests. */
    static ActionCatalog catalog(ActionContext context) {
        return catalog(context, null);
    }

    /** Builds the standalone MCP inventory with optional durable service-profile storage. */
    static ActionCatalog catalog(ActionContext context, ServiceProfileRepository serviceProfiles) {
        return catalog(context, serviceProfiles, null, null, null, null,
                ChannelFactory.standard(), null);
    }

    /** Builds the standalone MCP inventory with explicit outbound channel and credential wiring. */
    static ActionCatalog catalog(ActionContext context,
                                 ServiceProfileRepository serviceProfiles,
                                 SchemaRegistryStore registry,
                                 ChannelFactory channels,
                                 ProfileCredentialResolver credentials) {
        return catalog(context, serviceProfiles, null, null, null, registry, channels, credentials);
    }

    private static ActionCatalog catalog(ActionContext context,
                                         ServiceProfileRepository serviceProfiles,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows,
                                         SchemaRegistryStore registry,
                                         ChannelFactory channels,
                                         ProfileCredentialResolver credentials) {
        ActionCatalog catalog = ActionCatalog.defaults(context)
                .register(new GrpcInvokeAction())
                .register(new ReflectAction())
                .register(new GenerateStubsAction())
                .register(new GatherGitAction());
        ChannelFactory effectiveChannels = channels == null ? ChannelFactory.standard() : channels;
        ServiceWorkspaceActions.register(
                catalog, serviceProfiles, registry, effectiveChannels, credentials);
        ReflectedServiceActions.registerStored(
                catalog, serviceProfiles, registry, effectiveChannels, credentials)
                .forEach((profile, why) -> LOG.warn(
                        "stored service profile '{}' did not become verbs: {}", profile, why));
        return WorkflowWorkbenchActions.register(catalog, new WorkflowRunner(), artifacts, runs,
                workflows);
    }
}
