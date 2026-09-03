package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AccessPolicies;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.authz.OidcCallerResolver;
import ai.protomolt.proto.authz.jdbc.CallerStoreConfig;
import ai.protomolt.proto.authz.jdbc.JdbcCallerResolver;
import ai.protomolt.proto.config.DistributedConfig;
import ai.protomolt.proto.config.TrustSnapshotMounts;
import ai.protomolt.proto.config.registry.RegistryConfigSource;
import ai.protomolt.proto.delegation.DelegationActions;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.service.ProtoMoltCatalog;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ProtoApiTokenValidator;
import ai.protomolt.proto.http.rest.ProtoRestGateway;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.inference.spi.CredentialResolutionException;
import ai.protomolt.proto.inference.spi.CredentialResolver;
import ai.protomolt.proto.inference.spi.InferenceCatalog;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.inference.v1.ModelCapabilities;
import ai.protomolt.proto.inference.v1.ModelEntry;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventRelay;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.mcp.CompositeResources;
import ai.protomolt.proto.mcp.DelegationResources;
import ai.protomolt.proto.mcp.McpServer;
import ai.protomolt.proto.mcp.RegistryResources;
import ai.protomolt.proto.mcp.ServiceProfileResources;
import ai.protomolt.proto.mesh.cluster.ClusterActions;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDirectoryServiceProto;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.registry.RegistryWorkflowVersionRepository;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.registry.service.SchemaRegistryServerConfig;
import ai.protomolt.proto.server.ProtoToolsServerConfig;
import ai.protomolt.proto.server.jdk.JdkProtoRestServer;
import ai.protomolt.proto.workflow.TrustPin;
import ai.protomolt.proto.workflow.RecordSigning;
import ai.protomolt.proto.workflow.WorkflowRepository;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one-process ProtoMolt server: {@code ProtoMoltService} over gRPC (reflection enabled),
 * the same verbs over JSON/REST with OpenAPI and Swagger UI, the MCP server on
 * streamable HTTP at {@code /mcp} (with registry resources when a registry is mounted), and
 * optionally the git-backed schema registry speaking the Confluent protocol.
 *
 * <pre>
 * protomolt-serve [--host 0.0.0.0] [--grpc-port 9090] [--http-port 8080]
 *                 [--registry-git /srv/schemas.git [--registry-port 8081]]
 *                 [--service-workspace /srv/protomolt-services]
 *                 [--workflow-workspace /srv/protomolt-workflows]
 * </pre>
 */
public final class ProtoMoltServe implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoMoltServe.class);

    /**
     * Launcher options; a port of 0 picks a free port. A non-null {@code apiToken} guards
     * every operational surface (gRPC calls, REST verbs, the MCP endpoint) with a shared
     * secret; documentation surfaces (health, OpenAPI, Swagger UI) stay open.
     *
     * @param jobs workflow-runs configuration; null disables the jobs worker (the jobs
     *        verbs stay in the catalog and answer {@code unavailable})
     * @param outboundPolicy one process-wide policy shared by catalog actions and the jobs worker;
     *        null selects the permissive defaults
     */
    public record Options(String host, int grpcPort, int httpPort,
                          Path registryGit, int registryPort, String apiToken, boolean demo,
                          Path gatherCache, JobsOptions jobs,
                          java.util.List<String> inferenceModels, Path serviceWorkspace,
                          OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                          DelegationOptions delegation, TaskConsoleOptions taskConsole,
                          MeshClusterOptions meshCluster, Path accessPolicy,
                          IdentityStoreOptions identityStores,
                          ConfigLaneOptions configLane) {

        public Options {
            if (outboundPolicy == null) {
                outboundPolicy = OutboundChannelPolicy.defaults();
            }
            if (accessPolicy != null && apiToken == null) {
                throw new IllegalArgumentException("an access policy requires the operator "
                        + "api token; set --api-token alongside --access-policy");
            }
            if (identityStores != null && apiToken == null) {
                throw new IllegalArgumentException("external caller stores require the "
                        + "operator api token; set --api-token (or PROTOMOLT_API_TOKEN) "
                        + "alongside them");
            }
        }

        /** Binary/source-compatible constructor retaining the pre-config-lane surface. */
        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels,
                       Path serviceWorkspace, OutboundChannelPolicy outboundPolicy,
                       Path workflowWorkspace, DelegationOptions delegation,
                       TaskConsoleOptions taskConsole, MeshClusterOptions meshCluster,
                       Path accessPolicy, IdentityStoreOptions identityStores) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, taskConsole, meshCluster, accessPolicy,
                    identityStores, null);
        }

        /** Binary/source-compatible constructor retaining the pre-identity-store surface. */
        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels,
                       Path serviceWorkspace, OutboundChannelPolicy outboundPolicy,
                       Path workflowWorkspace, DelegationOptions delegation,
                       TaskConsoleOptions taskConsole, MeshClusterOptions meshCluster,
                       Path accessPolicy) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, taskConsole, meshCluster, accessPolicy,
                    null, null);
        }

        /** Binary/source-compatible constructor retaining the pre-authorization options surface. */
        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels,
                       Path serviceWorkspace, OutboundChannelPolicy outboundPolicy,
                       Path workflowWorkspace, DelegationOptions delegation,
                       TaskConsoleOptions taskConsole, MeshClusterOptions meshCluster) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, taskConsole, meshCluster, null, null,
                    null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit, int registryPort) {
            this(host, grpcPort, httpPort, registryGit, registryPort, null, false, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, false, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, java.util.List.of());
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, null);
        }

        /** Binary/source-compatible constructor retaining the pre-policy options surface. */
        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels,
                       Path serviceWorkspace) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, null, null);
        }

        /** Binary/source-compatible constructor retaining the pre-workbench options surface. */
        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels,
                       Path serviceWorkspace, OutboundChannelPolicy outboundPolicy) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy, null);
        }

        /** Binary/source-compatible constructor retaining the pre-delegation options surface. */
        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels,
                       Path serviceWorkspace, OutboundChannelPolicy outboundPolicy,
                       Path workflowWorkspace) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, null);
        }

        /** Binary/source-compatible constructor retaining the pre-console options surface. */
        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels,
                       Path serviceWorkspace, OutboundChannelPolicy outboundPolicy,
                       Path workflowWorkspace, DelegationOptions delegation) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, null);
        }

        /** Binary/source-compatible constructor retaining the pre-mesh options surface. */
        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, java.util.List<String> inferenceModels,
                       Path serviceWorkspace, OutboundChannelPolicy outboundPolicy,
                       Path workflowWorkspace, DelegationOptions delegation,
                       TaskConsoleOptions taskConsole) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, taskConsole, null);
        }

        public static Options defaults() {
            return new Options("0.0.0.0", 9090, 8080, null, 8081, null, false, null);
        }

        static Options parse(String[] args) {
            String host = "0.0.0.0";
            int grpcPort = 9090;
            int httpPort = 8080;
            Path registryGit = null;
            int registryPort = 8081;
            String apiToken = System.getenv("PROTOMOLT_API_TOKEN");
            String accessPolicyEnv = System.getenv("PROTOMOLT_ACCESS_POLICY");
            Path accessPolicy = accessPolicyEnv == null || accessPolicyEnv.isBlank()
                    ? null : Path.of(accessPolicyEnv);
            boolean demo = false;
            String gatherCacheEnv = System.getenv("PROTOMOLT_GATHER_CACHE");
            Path gatherCache = gatherCacheEnv == null || gatherCacheEnv.isBlank()
                    ? null
                    : Path.of(gatherCacheEnv);
            String jobsJdbc = System.getenv("PROTOMOLT_JOBS_JDBC");
            String jobsUser = System.getenv("PROTOMOLT_JOBS_USER");
            String jobsPassword = System.getenv("PROTOMOLT_JOBS_PASSWORD");
            String jobsKafka = System.getenv("PROTOMOLT_JOBS_KAFKA");
            String jobsRequestTopic = System.getenv("PROTOMOLT_JOBS_REQUEST_TOPIC");
            int jobsWorkers = envInt("PROTOMOLT_JOBS_WORKERS", 0);
            int jobsTargetConcurrency = envInt("PROTOMOLT_JOBS_TARGET_CONCURRENCY", 0);
            java.util.List<String> inferenceModels = new java.util.ArrayList<>();
            String serviceWorkspaceEnv = System.getenv("PROTOMOLT_SERVICE_WORKSPACE");
            Path serviceWorkspace = serviceWorkspaceEnv == null || serviceWorkspaceEnv.isBlank()
                    ? null : Path.of(serviceWorkspaceEnv);
            String workflowWorkspaceEnv = System.getenv("PROTOMOLT_WORKFLOW_WORKSPACE");
            Path workflowWorkspace = workflowWorkspaceEnv == null || workflowWorkspaceEnv.isBlank()
                    ? null : Path.of(workflowWorkspaceEnv);
            String allowedSchemes = System.getenv("PROTOMOLT_GRPC_ALLOWED_SCHEMES");
            String allowedHosts = System.getenv("PROTOMOLT_GRPC_ALLOWED_HOSTS");
            String allowedPorts = System.getenv("PROTOMOLT_GRPC_ALLOWED_PORTS");
            boolean allowPlaintext = envBoolean("PROTOMOLT_GRPC_ALLOW_PLAINTEXT", true);
            boolean allowTls = envBoolean("PROTOMOLT_GRPC_ALLOW_TLS", true);
            long maxDeadlineMs = envLong("PROTOMOLT_GRPC_MAX_DEADLINE_MS", 60_000L);
            int maxActiveChannels = envInt("PROTOMOLT_GRPC_MAX_ACTIVE_CHANNELS", 64);
            String inferenceModelsEnv = System.getenv("PROTOMOLT_INFERENCE_MODELS");
            String delegationRepoEndpoint = System.getenv("PROTOMOLT_DELEGATION_REPO_ENDPOINT");
            boolean delegationRepoTls = envBoolean("PROTOMOLT_DELEGATION_REPO_TLS", false);
            boolean delegationRepoTlsSet = System.getenv("PROTOMOLT_DELEGATION_REPO_TLS") != null;
            String delegationRepoDrive = System.getenv("PROTOMOLT_DELEGATION_REPO_DRIVE");
            String delegationTranscriptObject =
                    System.getenv("PROTOMOLT_DELEGATION_TRANSCRIPT_OBJECT");
            String delegationStateKeyRef = System.getenv("PROTOMOLT_DELEGATION_STATE_KEY_REF");
            String configUrl = System.getenv("PROTOMOLT_CONFIG_URL");
            long configRefreshSeconds = envLong("PROTOMOLT_CONFIG_REFRESH_SECONDS", 0L);
            String oidcIntrospection = System.getenv(OidcCallerResolver.ENV_INTROSPECTION_URL);
            String oidcClientId = System.getenv(OidcCallerResolver.ENV_CLIENT_ID);
            String oidcClientSecret = System.getenv(OidcCallerResolver.ENV_CLIENT_SECRET);
            String authzKeysJdbc = System.getenv(CallerStoreConfig.ENV_JDBC_URL);
            String taskConsoleToken = System.getenv("PROTOMOLT_TASK_CONSOLE_TOKEN");
            long taskConsoleSessionSeconds = envLong(
                    "PROTOMOLT_TASK_CONSOLE_SESSION_SECONDS", 43_200L);
            String meshClusterId = System.getenv("PROTOMOLT_MESH_CLUSTER_ID");
            String meshClusterName = System.getenv("PROTOMOLT_MESH_CLUSTER_NAME");
            String meshTrustDomain = System.getenv("PROTOMOLT_MESH_TRUST_DOMAIN");
            String meshCreatedAt = System.getenv("PROTOMOLT_MESH_CREATED_AT");
            if (inferenceModelsEnv != null && !inferenceModelsEnv.isBlank()) {
                for (String spec : inferenceModelsEnv.split(";")) {
                    if (!spec.isBlank()) {
                        inferenceModels.add(spec.trim());
                    }
                }
            }
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--host" -> host = requireValue(args, ++i);
                    case "--grpc-port" -> grpcPort = Integer.parseInt(requireValue(args, ++i));
                    case "--http-port" -> httpPort = Integer.parseInt(requireValue(args, ++i));
                    case "--registry-git" -> registryGit = Path.of(requireValue(args, ++i));
                    case "--registry-port" -> registryPort = Integer.parseInt(requireValue(args, ++i));
                    case "--api-token" -> apiToken = requireValue(args, ++i);
                    case "--access-policy" -> accessPolicy = Path.of(requireValue(args, ++i));
                    case "--gather-cache" -> gatherCache = Path.of(requireValue(args, ++i));
                    case "--jobs-jdbc" -> jobsJdbc = requireValue(args, ++i);
                    case "--jobs-user" -> jobsUser = requireValue(args, ++i);
                    case "--jobs-password" -> jobsPassword = requireValue(args, ++i);
                    case "--jobs-kafka" -> jobsKafka = requireValue(args, ++i);
                    case "--jobs-request-topic" -> jobsRequestTopic = requireValue(args, ++i);
                    case "--jobs-workers" -> jobsWorkers = Integer.parseInt(requireValue(args, ++i));
                    case "--jobs-target-concurrency" ->
                            jobsTargetConcurrency = Integer.parseInt(requireValue(args, ++i));
                    case "--inference-model" -> inferenceModels.add(requireValue(args, ++i));
                    case "--delegation-repo-endpoint" ->
                            delegationRepoEndpoint = requireValue(args, ++i);
                    case "--delegation-repo-tls" -> {
                        delegationRepoTls = parseBoolean(requireValue(args, ++i),
                                "--delegation-repo-tls");
                        delegationRepoTlsSet = true;
                    }
                    case "--delegation-repo-drive" ->
                            delegationRepoDrive = requireValue(args, ++i);
                    case "--delegation-transcript-object" ->
                            delegationTranscriptObject = requireValue(args, ++i);
                    case "--delegation-state-key-ref" ->
                            delegationStateKeyRef = requireValue(args, ++i);
                    case "--mesh-cluster-id" -> meshClusterId = requireValue(args, ++i);
                    case "--mesh-cluster-name" -> meshClusterName = requireValue(args, ++i);
                    case "--mesh-trust-domain" -> meshTrustDomain = requireValue(args, ++i);
                    case "--mesh-created-at" -> meshCreatedAt = requireValue(args, ++i);
                    case "--service-workspace" ->
                            serviceWorkspace = Path.of(requireValue(args, ++i));
                    case "--workflow-workspace" ->
                            workflowWorkspace = Path.of(requireValue(args, ++i));
                    case "--grpc-allowed-schemes" ->
                            allowedSchemes = requireValue(args, ++i);
                    case "--grpc-allowed-hosts" ->
                            allowedHosts = requireValue(args, ++i);
                    case "--grpc-allowed-ports" ->
                            allowedPorts = requireValue(args, ++i);
                    case "--grpc-allow-plaintext" ->
                            allowPlaintext = parseBoolean(requireValue(args, ++i),
                                    "--grpc-allow-plaintext");
                    case "--grpc-allow-tls" ->
                            allowTls = parseBoolean(requireValue(args, ++i), "--grpc-allow-tls");
                    case "--grpc-max-deadline-ms" ->
                            maxDeadlineMs = parseLong(requireValue(args, ++i),
                                    "--grpc-max-deadline-ms");
                    case "--grpc-max-active-channels" ->
                            maxActiveChannels = parseInt(requireValue(args, ++i),
                                    "--grpc-max-active-channels");
                    case "--demo" -> demo = true;
                    case "--help", "-h" -> {
                        System.err.println("usage: protomolt-serve [--host <addr>] [--grpc-port <n>] "
                                + "[--http-port <n>] [--registry-git <path> [--registry-port <n>]] "
                                + "[--api-token <secret>]  (or PROTOMOLT_API_TOKEN) "
                                + "[--gather-cache <dir>]  (or PROTOMOLT_GATHER_CACHE) [--demo] "
                                + "[--jobs-jdbc <url> --jobs-user <u> [--jobs-password <p>] "
                                + "[--jobs-kafka <bootstrap>] [--jobs-request-topic <name>] "
                                + "[--jobs-workers <n>] [--jobs-target-concurrency <n>]] "
                                + "[--inference-model <id|provider|endpoint"
                                + "[|backend[|k:v,...[|capability,...[|credential-ref]]]]> ...] "
                                + "(or PROTOMOLT_INFERENCE_MODELS, ';'-separated) "
                                + "[--service-workspace <dir>] (or PROTOMOLT_SERVICE_WORKSPACE) "
                                + "[--workflow-workspace <dir>] (or PROTOMOLT_WORKFLOW_WORKSPACE) "
                                + "[--grpc-allowed-schemes <csv>] [--grpc-allowed-hosts <csv>] "
                                + "[--grpc-allowed-ports <csv>] "
                                + "[--grpc-allow-plaintext <true|false>] "
                                + "[--grpc-allow-tls <true|false>] "
                                + "[--grpc-max-deadline-ms <n>] "
                                + "[--grpc-max-active-channels <n>] "
                                + "[--delegation-repo-endpoint <host:port|in-process:name> "
                                + "[--delegation-repo-tls <true|false>] "
                                + "[--delegation-repo-drive <name>] "
                                + "[--delegation-transcript-object <key>] "
                                + "[--delegation-state-key-ref <ref>]] "
                                + "[--mesh-cluster-id <id> --mesh-created-at <ISO-8601> "
                                + "[--mesh-cluster-name <name>] [--mesh-trust-domain <domain>]] "
                                + "(task console login: PROTOMOLT_TASK_CONSOLE_TOKEN; "
                                + "session duration: PROTOMOLT_TASK_CONSOLE_SESSION_SECONDS)");
                        System.exit(0);
                    }
                    default -> {
                        System.err.println("unknown argument: " + args[i]);
                        System.exit(2);
                    }
                }
            }
            if (apiToken != null && apiToken.isBlank()) {
                apiToken = null;
            }
            JobsOptions jobs = null;
            if (jobsJdbc != null && !jobsJdbc.isBlank()) {
                if (jobsUser == null || jobsUser.isBlank()) {
                    System.err.println("--jobs-jdbc requires --jobs-user (or PROTOMOLT_JOBS_USER)");
                    System.exit(2);
                }
                jobs = new JobsOptions(jobsJdbc, jobsUser, jobsPassword, jobsKafka,
                        jobsRequestTopic, jobsWorkers, jobsTargetConcurrency);
            } else if ((jobsKafka != null && !jobsKafka.isBlank())
                    || (jobsRequestTopic != null && !jobsRequestTopic.isBlank())) {
                System.err.println("--jobs-kafka/--jobs-request-topic require --jobs-jdbc: "
                        + "the job row is the truth; the broker is propagation");
                System.exit(2);
            }
            DelegationOptions delegation = null;
            if (delegationRepoEndpoint != null && !delegationRepoEndpoint.isBlank()) {
                delegation = new DelegationOptions(delegationRepoEndpoint, delegationRepoTls,
                        delegationRepoDrive == null || delegationRepoDrive.isBlank()
                                ? DelegationOptions.DEFAULT_DRIVE : delegationRepoDrive,
                        delegationTranscriptObject == null || delegationTranscriptObject.isBlank()
                                ? DelegationOptions.DEFAULT_OBJECT_KEY : delegationTranscriptObject,
                        delegationStateKeyRef == null || delegationStateKeyRef.isBlank()
                                ? DelegationOptions.DEFAULT_KEY_REFERENCE : delegationStateKeyRef);
            } else if (delegationRepoTlsSet
                    || (delegationRepoDrive != null && !delegationRepoDrive.isBlank())
                    || (delegationTranscriptObject != null && !delegationTranscriptObject.isBlank())
                    || (delegationStateKeyRef != null && !delegationStateKeyRef.isBlank())) {
                System.err.println("--delegation-repo-drive/--delegation-transcript-object/"
                        + "--delegation-state-key-ref require --delegation-repo-endpoint: "
                        + "without a repository service the transcript stays in memory");
                System.exit(2);
            }
            TaskConsoleOptions taskConsole = null;
            if (taskConsoleToken != null && !taskConsoleToken.isBlank()) {
                taskConsole = new TaskConsoleOptions(taskConsoleToken,
                        Duration.ofSeconds(taskConsoleSessionSeconds));
            }
            ConfigLaneOptions configLane = null;
            if (configUrl != null && !configUrl.isBlank()) {
                configLane = new ConfigLaneOptions(configUrl.trim(), configRefreshSeconds);
            } else if (configRefreshSeconds > 0) {
                throw new IllegalArgumentException(
                        "PROTOMOLT_CONFIG_REFRESH_SECONDS needs PROTOMOLT_CONFIG_URL: a"
                                + " refresh interval with nowhere to pull from reads"
                                + " nothing");
            }
            IdentityStoreOptions identityStores = null;
            boolean oidcConfigured = oidcIntrospection != null && !oidcIntrospection.isBlank();
            boolean authzKeysConfigured = authzKeysJdbc != null && !authzKeysJdbc.isBlank();
            if (oidcConfigured || authzKeysConfigured) {
                identityStores = new IdentityStoreOptions(
                        oidcConfigured ? URI.create(oidcIntrospection.trim()) : null,
                        oidcClientId, oidcClientSecret,
                        authzKeysConfigured
                                ? CallerStoreConfig.fromEnvironmentMap(System.getenv())
                                : null);
            }
            MeshClusterOptions meshCluster = null;
            if (meshClusterId != null && !meshClusterId.isBlank()) {
                if (meshCreatedAt == null || meshCreatedAt.isBlank()) {
                    throw new IllegalArgumentException(
                            "--mesh-cluster-id requires --mesh-created-at so the durable "
                                    + "cluster fingerprint stays stable across restarts");
                }
                try {
                    meshCluster = new MeshClusterOptions(meshClusterId,
                            meshClusterName == null || meshClusterName.isBlank()
                                    ? meshClusterId : meshClusterName,
                            meshTrustDomain == null ? "" : meshTrustDomain,
                            Instant.parse(meshCreatedAt));
                } catch (java.time.format.DateTimeParseException e) {
                    throw new IllegalArgumentException(
                            "mesh created-at must be an ISO-8601 instant", e);
                }
            } else if ((meshClusterName != null && !meshClusterName.isBlank())
                    || (meshTrustDomain != null && !meshTrustDomain.isBlank())
                    || (meshCreatedAt != null && !meshCreatedAt.isBlank())) {
                throw new IllegalArgumentException(
                        "mesh cluster name, trust domain, and created-at require a mesh cluster id");
            }
            Set<String> schemeSet = allowedSchemes == null
                    ? null : parseCsv(allowedSchemes, "grpc allowed schemes");
            Set<String> hostSet = allowedHosts == null
                    ? null : parseCsv(allowedHosts, "grpc allowed hosts");
            Set<Integer> portSet = allowedPorts == null
                    ? null : parsePorts(allowedPorts);
            OutboundChannelPolicy outboundPolicy = outboundPolicy(schemeSet, hostSet, portSet,
                    allowPlaintext, allowTls, maxDeadlineMs, maxActiveChannels);
            return new Options(host, grpcPort, httpPort, registryGit, registryPort, apiToken,
                    demo, gatherCache, jobs, java.util.List.copyOf(inferenceModels),
                    serviceWorkspace, outboundPolicy, workflowWorkspace, delegation, taskConsole,
                    meshCluster, accessPolicy, identityStores, configLane);
        }

        private static int envInt(String name, int fallback) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? fallback : parseInt(value, name);
        }

        private static long envLong(String name, long fallback) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? fallback : parseLong(value, name);
        }

        private static boolean envBoolean(String name, boolean fallback) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? fallback : parseBoolean(value, name);
        }

        private static int parseInt(String value, String name) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be an integer: " + value, e);
            }
        }

        private static long parseLong(String value, String name) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be an integer: " + value, e);
            }
        }

        private static boolean parseBoolean(String value, String name) {
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                return Boolean.parseBoolean(value);
            }
            throw new IllegalArgumentException(name + " must be true or false: " + value);
        }

        private static Set<String> parseCsv(String value, String name) {
            Set<String> values = new LinkedHashSet<>();
            if (value != null && !value.isBlank()) {
                for (String item : value.split(",", -1)) {
                    if (item.isBlank()) {
                        throw new IllegalArgumentException(name + " contains an empty value");
                    }
                    values.add(item.trim());
                }
            }
            return Set.copyOf(values);
        }

        private static Set<Integer> parsePorts(String value) {
            Set<Integer> ports = new LinkedHashSet<>();
            for (String item : value.split(",", -1)) {
                if (item.isBlank()) {
                    throw new IllegalArgumentException("grpc allowed ports contains an empty value");
                }
                ports.add(parseInt(item.trim(), "grpc allowed port"));
            }
            return Set.copyOf(ports);
        }

        private static OutboundChannelPolicy outboundPolicy(Set<String> schemes,
                                                            Set<String> hosts,
                                                            Set<Integer> ports,
                                                            boolean allowPlaintext,
                                                            boolean allowTls,
                                                            long maxDeadlineMs,
                                                            int maxActiveChannels) {
            if (maxDeadlineMs <= 0) {
                throw new IllegalArgumentException("grpc max deadline must be positive");
            }
            OutboundChannelPolicy.Builder builder = OutboundChannelPolicy.builder()
                    .allowPlaintext(allowPlaintext)
                    .allowTls(allowTls)
                    .maxDeadline(Duration.ofMillis(maxDeadlineMs))
                    .maxActiveChannels(maxActiveChannels);
            if (schemes != null) {
                builder.allowedSchemes(schemes);
            }
            if (hosts != null) {
                builder.allowedHosts(hosts);
            }
            if (ports != null) {
                builder.allowedPorts(ports);
            }
            return builder.build();
        }

        private static String requireValue(String[] args, int i) {
            if (i >= args.length) {
                System.err.println(args[i - 1] + " requires a value");
                System.exit(2);
            }
            return args[i];
        }
    }

    /**
     * Builds the inference facade from {@code --inference-model} specs
     * ({@code id|provider|endpoint[|backend[|labels[|capabilities[|credential-ref]]]]}).
     * Supported capability tokens are {@code streaming}, {@code thinking}, and
     * {@code structured-output}. The credential reference is an opaque pointer
     * (e.g. {@code env:OPENAI_TOKEN}) resolved host-side at request time, never
     * credential material; a malformed reference fails startup. Empty specs mean
     * inference is not configured (null; the verbs answer {@code unavailable}).
     * A bad spec or an unknown provider fails startup loud. A model the server
     * cannot execute must never sit in the catalog looking runnable.
     */
    static InferenceEngines inferenceEngines(java.util.List<String> specs) {
        if (specs == null || specs.isEmpty()) {
            return null;
        }
        InferenceCatalog catalog = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(catalog);
        for (String spec : specs) {
            String[] parts = spec.split("\\|", -1);
            String displaySpec = redactCredentialReference(spec);
            if (parts.length < 3 || parts.length > 7
                    || parts[0].isBlank() || parts[1].isBlank() || parts[2].isBlank()) {
                throw new IllegalArgumentException("bad --inference-model spec '" + displaySpec
                        + "' (want id|provider|endpoint[|backend[|labels[|capabilities"
                        + "[|credential-ref]]]])");
            }
            ModelEntry.Builder entry = ModelEntry.newBuilder()
                    .setId(parts[0].trim())
                    .setProvider(parts[1].trim())
                    .setEndpoint(parts[2].trim());
            if (parts.length >= 4 && !parts[3].isBlank()) {
                entry.setBackendModel(parts[3].trim());
            }
            if (parts.length >= 5 && !parts[4].isBlank()) {
                for (String label : parts[4].split(",")) {
                    String[] kv = label.split(":", 2);
                    if (kv.length != 2 || kv[0].isBlank()) {
                        throw new IllegalArgumentException("bad label '" + label
                                + "' in --inference-model spec '" + displaySpec
                                + "' (want k:v)");
                    }
                    entry.putLabels(kv[0].trim(), kv[1].trim());
                }
            }
            if (parts.length >= 6 && !parts[5].isBlank()) {
                ModelCapabilities.Builder capabilities = ModelCapabilities.newBuilder();
                for (String capability : parts[5].split(",")) {
                    switch (capability.trim()) {
                        case "streaming" -> capabilities.setStreaming(true);
                        case "thinking" -> capabilities.setThinking(true);
                        case "structured-output" -> capabilities.setStructuredOutput(true);
                        default -> throw new IllegalArgumentException("unknown capability '"
                                + capability + "' in --inference-model spec '"
                                + displaySpec + "'");
                    }
                }
                entry.setCapabilities(capabilities);
            }
            if (parts.length == 7 && !parts[6].isBlank()) {
                String credentialRef = parts[6].trim();
                try {
                    CredentialResolver.checkFormat(credentialRef);
                } catch (CredentialResolutionException e) {
                    throw new IllegalArgumentException("bad credential reference in "
                            + "--inference-model spec '" + displaySpec + "': "
                            + e.getMessage(), e);
                }
                entry.setCredentialRef(credentialRef);
            }
            engines.register(entry.build());
        }
        return engines;
    }

    /** Keeps a secret-sensitive credential reference out of launcher diagnostics. */
    private static String redactCredentialReference(String spec) {
        String[] parts = spec.split("\\|", -1);
        if (parts.length < 7) {
            return spec;
        }
        parts[6] = "<credential-ref>";
        for (int i = 7; i < parts.length; i++) {
            parts[i] = "<redacted>";
        }
        return String.join("|", parts);
    }

    /**
     * Workflow-runs launcher options. {@code kafkaBootstrap} is optional: without it the
     * worker fleet runs verb-submitted jobs with no event relay and no request topic,
     * useful for a single-box deployment; with it, lifecycle events publish to the
     * events topic and the request topic is consumed. Zero {@code workers} /
     * {@code targetConcurrency} take the jobs module's defaults.
     */
    public record JobsOptions(String jdbcUrl, String username, String password,
                              String kafkaBootstrap, String requestTopic,
                              int workers, int targetConcurrency) {

        public JobsOptions {
            if (requestTopic != null && (kafkaBootstrap == null || kafkaBootstrap.isBlank())) {
                throw new IllegalArgumentException("--jobs-request-topic requires --jobs-kafka");
            }
        }
    }

    /**
     * Delegation transcript durability. {@code repoEndpoint} is the repository
     * service gRPC target: {@code host:port} (plaintext unless {@code repoTls}), or
     * {@code in-process:<name>} for a repository service in the same process. When
     * the endpoint is null the coordinator keeps its transcript in memory: a
     * restart loses it and workers re-register as new (development mode). When it
     * is set, the transcript persists encrypted through the repository service and
     * a restart restores tasks, cursors, and sequence scopes.
     */
    public record DelegationOptions(String repoEndpoint, boolean repoTls, String drive,
                                    String objectKey, String keyReference) {

        /** Default repository drive for the delegation transcript blob. */
        public static final String DEFAULT_DRIVE = "protomolt";

        /** Default object key of the delegation transcript blob. */
        public static final String DEFAULT_OBJECT_KEY = "delegation/serve/transcript.pb.enc";

        /** Default reference resolving the transcript encryption key. */
        public static final String DEFAULT_KEY_REFERENCE = "env:PROTOMOLT_TRANSCRIPT_KEY";

        public DelegationOptions {
            if (repoEndpoint == null || repoEndpoint.isBlank()) {
                throw new IllegalArgumentException(
                        "the delegation repo endpoint must not be blank");
            }
            if (drive == null || drive.isBlank()) {
                throw new IllegalArgumentException(
                        "the delegation repo drive must not be blank");
            }
            if (objectKey == null || objectKey.isBlank()) {
                throw new IllegalArgumentException(
                        "the delegation transcript object key must not be blank");
            }
            if (keyReference == null || keyReference.isBlank()) {
                throw new IllegalArgumentException(
                        "the delegation state key reference must not be blank");
            }
        }

        /** Durable transcripts on the endpoint with the default coordinates. */
        public DelegationOptions(String repoEndpoint, boolean repoTls) {
            this(repoEndpoint, repoTls, DEFAULT_DRIVE, DEFAULT_OBJECT_KEY,
                    DEFAULT_KEY_REFERENCE);
        }
    }

    /** Browser-only authentication settings for the bounded task console API. */
    public record TaskConsoleOptions(String loginToken, Duration sessionTtl) {

        public TaskConsoleOptions {
            ConsoleSessions.validateSettings(loginToken, sessionTtl);
        }
    }

    /**
     * The config lane this server follows. Today it carries one subject that matters
     * here: the trust snapshot the verifying verbs fall back to, so custody can be
     * published rather than pinned as a file on every node.
     *
     * @param registryUrl the registry's native route prefix, e.g.
     *        {@code http://registry:8081/protomolt}
     * @param refreshSeconds how often to pull; {@code 0} reads once at startup
     */
    public record ConfigLaneOptions(String registryUrl, long refreshSeconds) {

        public ConfigLaneOptions {
            if (registryUrl == null || registryUrl.isBlank()) {
                throw new IllegalArgumentException("the config lane needs a registry url");
            }
            if (refreshSeconds < 0) {
                throw new IllegalArgumentException("refreshSeconds must not be negative");
            }
        }
    }

    /**
     * External caller stores composed behind the access policy: OIDC introspection
     * (RFC 7662) and the JDBC caller store, mirroring the intake service's key stores.
     * Both are environment-configured — credentials do not ride argv. Either half may be
     * absent; an options value with neither is refused.
     *
     * @param oidcIntrospection the RFC 7662 introspection endpoint; null disables OIDC
     * @param oidcClientId this server's client id at the IdP; required with the endpoint
     * @param oidcClientSecret this server's client secret; required with the endpoint
     * @param callerStore the JDBC caller store settings; null disables the store
     */
    public record IdentityStoreOptions(URI oidcIntrospection, String oidcClientId,
                                       String oidcClientSecret,
                                       CallerStoreConfig callerStore) {

        public IdentityStoreOptions {
            if (oidcIntrospection == null && callerStore == null) {
                throw new IllegalArgumentException(
                        "identity stores require an OIDC endpoint or a JDBC caller store");
            }
            if (oidcIntrospection != null
                    && (oidcClientId == null || oidcClientId.isBlank()
                            || oidcClientSecret == null || oidcClientSecret.isBlank())) {
                throw new IllegalArgumentException("the OIDC introspection endpoint "
                        + "requires PROTOMOLT_AUTHZ_OIDC_CLIENT_ID and "
                        + "PROTOMOLT_AUTHZ_OIDC_CLIENT_SECRET");
            }
        }
    }

    /** Stable identity of the mesh directory hosted by this serve process. */
    public record MeshClusterOptions(String clusterId, String displayName, String trustDomain,
                                     Instant createdAt) {

        public MeshClusterOptions {
            if (clusterId == null || !clusterId.matches(
                    "[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
                throw new IllegalArgumentException("mesh cluster id must be path-safe");
            }
            if (displayName == null || displayName.length() > 256) {
                throw new IllegalArgumentException("mesh cluster name is invalid");
            }
            if (trustDomain == null || !trustDomain.matches(
                    "^$|[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
                throw new IllegalArgumentException("mesh trust domain is invalid");
            }
            Objects.requireNonNull(createdAt, "createdAt");
        }
    }

    private final ProtoMoltGrpcServer grpc;
    private final JdkProtoRestServer http;
    private final McpHttpHandler mcp;
    private final GitSchemaRegistryStore registryStore;
    private final SchemaRegistryServer registry;
    private final int httpPort;
    private final int registryPort;
    private final WorkflowRunDatabase jobsDatabase;
    private final WorkflowRunWorker jobsWorker;
    private final WorkflowRunEventRelay jobsRelay;
    private final DelegationRuntime delegation;
    private final MeshClusterRuntime meshCluster;
    private final JdbcCallerResolver jdbcCallers;

    private ProtoMoltServe(ProtoMoltGrpcServer grpc, JdkProtoRestServer http,
                           McpHttpHandler mcp, int httpPort,
                           GitSchemaRegistryStore registryStore, SchemaRegistryServer registry,
                           int registryPort, WorkflowRunDatabase jobsDatabase,
                           WorkflowRunWorker jobsWorker, WorkflowRunEventRelay jobsRelay,
                           DelegationRuntime delegation, MeshClusterRuntime meshCluster,
                           JdbcCallerResolver jdbcCallers) {
        this.grpc = grpc;
        this.http = http;
        this.mcp = mcp;
        this.httpPort = httpPort;
        this.registryStore = registryStore;
        this.registry = registry;
        this.registryPort = registryPort;
        this.jobsDatabase = jobsDatabase;
        this.jobsWorker = jobsWorker;
        this.jobsRelay = jobsRelay;
        this.delegation = delegation;
        this.meshCluster = meshCluster;
        this.jdbcCallers = jdbcCallers;
    }

    /** Starts every configured surface; closing stops them all. */
    public static ProtoMoltServe start(Options options) {
        ActionContext context = ActionContext.create();

        ProtoMoltGrpcServer grpc = null;
        JdkProtoRestServer http = null;
        GitSchemaRegistryStore store = null;
        SchemaRegistryServer registry = null;
        WorkflowRunDatabase jobsDatabase = null;
        WorkflowRunWorker jobsWorker = null;
        WorkflowRunEventRelay jobsRelay = null;
        ServiceProfileRepository serviceProfiles = null;
        ArtifactRepository artifacts = null;
        RunEvidenceRepository runEvidence = null;
        OutboundChannelPolicy outboundPolicy = options.outboundPolicy();
        try {
            InferenceEngines inference = inferenceEngines(options.inferenceModels());
            StructuredGenerator structured = inference == null
                    ? null : new StructuredGenerator(inference, context.registry());
            Path registryGit = options.registryGit();
            if (registryGit == null && options.demo()) {
                // Demo mode always has a registry; an unnamed one lives in a temp directory.
                try {
                    registryGit = java.nio.file.Files.createTempDirectory("protomolt-demo-registry");
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to create the demo registry directory", e);
                }
            }
            if (registryGit != null) {
                store = GitSchemaRegistryStore.builder()
                        .repositoryDir(registryGit)
                        .build();
            }
            WorkflowRepository workflows = store == null ? null : workflowRepository(store);
            WorkflowVersionRepository workflowVersions = store == null
                    ? null : new RegistryWorkflowVersionRepository(store);
            if (options.serviceWorkspace() != null) {
                try {
                    serviceProfiles = new FileSystemServiceProfileRepository(
                            options.serviceWorkspace());
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to open service workspace at "
                            + options.serviceWorkspace(), e);
                }
            }
            Path workflowWorkspace = options.workflowWorkspace();
            if (workflowWorkspace == null && options.demo()) {
                try {
                    workflowWorkspace = java.nio.file.Files
                            .createTempDirectory("protomolt-demo-workflows");
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to create the demo workflow workspace", e);
                }
            }
            if (workflowWorkspace != null) {
                try {
                    artifacts = new FileSystemArtifactRepository(
                            workflowWorkspace.resolve("artifacts"));
                    runEvidence = new FileSystemRunEvidenceRepository(
                            workflowWorkspace.resolve("runs"));
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to open workflow workspace at "
                            + workflowWorkspace, e);
                }
            }

            // Workflow runs: the store boots (and Flyway-migrates) before the catalog so
            // the jobs verbs serve from the same truth the worker fleet executes from.
            if (options.jobs() != null) {
                JobsOptions jobs = options.jobs();
                jobsDatabase = new WorkflowRunDatabase(
                        new WorkflowRunStoreConfig(jobs.jdbcUrl(), jobs.username(), jobs.password()));
                JdbcWorkflowRunStore jobStore = new JdbcWorkflowRunStore(jobsDatabase);
                String requestTopic = jobs.kafkaBootstrap() == null
                        ? null
                        : jobs.requestTopic() != null ? jobs.requestTopic() : "workflow-run-requests";
                WorkflowRunsConfig jobsConfig = new WorkflowRunsConfig(
                        "serve-" + ManagementFactory.getRuntimeMXBean().getName(),
                        jobs.workers(), null, null, 0, 0, jobs.targetConcurrency(),
                        requestTopic, null, jobs.kafkaBootstrap(), null);
                jobsWorker = new WorkflowRunWorker(jobStore, context, workflows,
                        new WorkflowRunner(outboundPolicy, structured),
                        jobsConfig);
                if (jobs.kafkaBootstrap() != null) {
                    jobsRelay = new WorkflowRunEventRelay(jobStore,
                            WorkflowRunEventRelay.newProducer(jobs.kafkaBootstrap(), null),
                            jobsConfig.eventsTopic(), jobsConfig.pollInterval(), 100);
                }

                // The catalog sees the store so run-workflow resolves stored workflow names and
                // the jobs verbs serve the live job rows.
                ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                        workflows, jobStore, jobsConfig.maxAttemptsDefault(),
                        inference, serviceProfiles,
                        outboundPolicy, artifacts, runEvidence, workflowVersions, store,
                        trustSource(options));
                return startWithJobsCatalog(options, context, catalog, store, workflows,
                        serviceProfiles, jobsDatabase, jobsWorker, jobsRelay);
            }
            // The catalog sees the store so run-workflow resolves stored workflow names.
            ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                    workflows, null, 0, inference, serviceProfiles,
                    outboundPolicy, artifacts, runEvidence, workflowVersions, store,
                    trustSource(options));
            return startWithJobsCatalog(options, context, catalog, store, workflows,
                    serviceProfiles, null, null, null);
        } catch (RuntimeException e) {
            if (registry != null) {
                registry.close();
            }
            if (http != null) {
                http.close();
            }
            if (grpc != null) {
                grpc.close();
            }
            closeQuietly(store);
            closeQuietly(jobsWorker);
            closeQuietly(jobsRelay);
            closeQuietly(jobsDatabase);
            throw e;
        }
    }

    /** The shared tail of {@link #start(Options)}: gRPC, registry, REST, MCP. */
    private static ProtoMoltServe startWithJobsCatalog(Options options, ActionContext context,
                                                       ActionCatalog catalog,
                                                       GitSchemaRegistryStore store,
                                                       WorkflowRepository workflows,
                                                       ServiceProfileRepository serviceProfiles,
                                                       WorkflowRunDatabase jobsDatabase,
                                                       WorkflowRunWorker jobsWorker,
                                                       WorkflowRunEventRelay jobsRelay) {
        ProtoMoltGrpcServer grpc = null;
        JdkProtoRestServer http = null;
        McpHttpHandler mcpHandler = null;
        SchemaRegistryServer registry = null;
        DelegationRuntime delegation = null;
        MeshClusterRuntime meshCluster = null;
        JdbcCallerResolver jdbcCallers = null;
        try {
            if (options.demo()) {
                DemoSchemas.seed(context.registry(), store);
            }

            // The live delegation surface: one coordinator per server, adapted to
            // catalog verbs and MCP resources through the bridge. Durable when a
            // delegation repository endpoint is configured, in-memory otherwise.
            delegation = DelegationRuntime.open(options.delegation());
            DelegationBridge bridge = delegation.bridge();
            DelegationActions.register(catalog, bridge);
            meshCluster = MeshClusterRuntime.open(options.meshCluster(), options.delegation());
            if (meshCluster != null) {
                ClusterActions.register(catalog, meshCluster.directory());
            }

            // The access policy narrows named principals; the operator token keeps every
            // scope. A policy that fails to load or verify refuses startup loudly. The
            // external stores compose behind the policy, first match wins.
            java.util.List<CallerResolver> resolvers = new java.util.ArrayList<>();
            if (options.accessPolicy() != null) {
                try {
                    resolvers.add(new AccessPolicyCallers(
                            AccessPolicies.load(options.accessPolicy())));
                } catch (IOException e) {
                    throw new IllegalStateException("failed to read the access policy at "
                            + options.accessPolicy() + ": " + e.getMessage(), e);
                }
            }
            if (options.identityStores() != null) {
                IdentityStoreOptions stores = options.identityStores();
                if (stores.oidcIntrospection() != null) {
                    resolvers.add(new OidcCallerResolver(stores.oidcIntrospection(),
                            stores.oidcClientId(), stores.oidcClientSecret()));
                }
                if (stores.callerStore() != null) {
                    jdbcCallers = new JdbcCallerResolver(stores.callerStore());
                    resolvers.add(jdbcCallers);
                }
            }
            CallerResolver callers = resolvers.isEmpty() ? null
                    : resolvers.size() == 1 ? resolvers.getFirst()
                            : CallerResolver.chain(resolvers);

            grpc = ProtoMoltGrpcServer.start(options.host(), options.grpcPort(), catalog,
                    options.apiToken(), callers);
            if (options.demo() && store != null) {
                // The demo workflow composes this server's own verbs, so it needs the bound
                // gRPC port - seeded here rather than with the schemas.
                DemoSchemas.seedWorkflow(store, grpc.port());
            }

            // The jobs fleet starts once the catalog is bound: workers execute and the
            // relay drains. Broker-less deployments run the worker only.
            if (jobsWorker != null) {
                jobsWorker.start();
            }
            if (jobsRelay != null) {
                jobsRelay.start();
            }

            // The registry starts before HTTP so the console's same-origin proxy
            // (/api/protomolt) knows the port it bridges to.
            int registryPort = -1;
            if (store != null) {
                // The registry listener honors the same bind address and shared secret as
                // every other surface - one process, one security boundary.
                registry = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults()
                                .withHost(options.host())
                                .withPort(options.registryPort())
                                .withApiToken(options.apiToken()),
                        store, catalog, callers);
                registryPort = registry.start();
            }

            ProtoRestMethodRegistry methods = new ProtoRestMethodRegistry();
            Function<Map<String, String>, Caller> restCallers = callers == null
                    ? null : restCallers(options.apiToken(), callers);
            // The families that contribute verbs declare them on services of their own, so
            // those are mounted alongside ProtoMolt's; without them the same verb is on gRPC
            // and MCP but missing from REST and from the OpenAPI document.
            ProtoMoltRestMount.register(methods, catalog, options.apiToken() == null
                    ? null
                    : ApiTokenRequirement.apiKeyHeader("api_token"), restCallers,
                    contributedServices(meshCluster != null));
            ProtoToolsServerConfig config = ProtoToolsServerConfig.defaults()
                    .withHost(options.host())
                    .withPort(options.httpPort());
            ProtoRestGateway gateway;
            if (options.apiToken() == null) {
                gateway = new ProtoRestGateway(methods, context.transcoder());
            } else if (restCallers == null) {
                gateway = new ProtoRestGateway(methods, context.transcoder(),
                        ProtoApiTokenValidator.sharedSecret(options.apiToken()));
            } else {
                Function<Map<String, String>, Caller> resolved = restCallers;
                gateway = new ProtoRestGateway(methods, context.transcoder(),
                        (tokenConfig, headers, query) -> {
                            String presented = presentedCredential(headers);
                            if (presented == null || presented.isBlank()) {
                                return Optional.of(
                                        "Missing API token '" + tokenConfig.name() + "'");
                            }
                            return resolved.apply(headers) != null
                                    ? Optional.empty()
                                    : Optional.of("Invalid API token");
                        });
            }
            String version = ProtoMoltServe.class.getPackage().getImplementationVersion();
            McpServer mcp = new McpServer(catalog,
                    CompositeResources.of(
                            store != null ? new RegistryResources(store) : null,
                            serviceProfiles != null
                                    ? new ServiceProfileResources(serviceProfiles, store) : null,
                            new DelegationResources(bridge)),
                    "protomolt", version != null ? version : "dev");
            int boundRegistryPort = registryPort;
            int[] selfPort = {-1};
            mcpHandler = new McpHttpHandler(mcp, options.apiToken(), callers);
            http = new JdkProtoRestServer(config, gateway,
                    new ProtoOpenApiGenerator("ProtoMolt", version != null ? version : "dev",
                            "/", config.restPathPrefix()))
                    .withContext("/docs", new SwaggerUiHandler("/docs", config.openApiPath()))
                    .withContext("/mcp", mcpHandler);
            ConsoleSessions taskSessions = options.taskConsole() == null
                    ? TaskConsoleSessions.open()
                    : TaskConsoleSessions.secured(options.taskConsole().loginToken(),
                            options.taskConsole().sessionTtl(), callers);
            if (options.apiToken() == null || options.taskConsole() != null) {
                http.withContext("/console", new ConsoleHandler())
                        .withContext("/api/task-session",
                                new TaskConsoleSessionHandler(taskSessions))
                        .withContext("/api/tasks",
                                new TaskConsoleApiHandler(bridge, taskSessions,
                                        RecordSigning.fromEnvironment()));
            }
            if (options.apiToken() == null) {
                http.withContext("/api/protomolt", new ApiProxyHandler("/api/protomolt",
                            () -> boundRegistryPort,
                            "no registry is running; start with --registry-git or --demo"))
                    .withContext("/api/serve", new ApiProxyHandler("/api/serve",
                            () -> selfPort[0], "server is still starting"));
            } else {
                DisabledSurfaceHandler disabled = new DisabledSurfaceHandler(
                        "this browser surface is disabled when --api-token is set; "
                                + "use the task console or an authenticated protocol client");
                if (options.taskConsole() == null) {
                    http.withContext("/console", disabled)
                            .withContext("/api/task-session", disabled)
                            .withContext("/api/tasks", disabled);
                }
                http.withContext("/api/protomolt", disabled)
                        .withContext("/api/serve", disabled);
            }
            int httpPort = http.start();
            selfPort[0] = httpPort;
            return new ProtoMoltServe(grpc, http, mcpHandler, httpPort, store, registry, registryPort,
                    jobsDatabase, jobsWorker, jobsRelay, delegation, meshCluster, jdbcCallers);
        } catch (RuntimeException e) {
            closeQuietly(jdbcCallers);
            closeQuietly(meshCluster);
            closeQuietly(delegation);
            closeQuietly(mcpHandler);
            if (registry != null) {
                registry.close();
            }
            if (http != null) {
                http.close();
            }
            if (grpc != null) {
                grpc.close();
            }
            throw e;
        }
    }

    private static ai.protomolt.proto.workflow.WorkflowRepository workflowRepository(
            GitSchemaRegistryStore store) {
        return workflowRepository(store::workflow);
    }

    static ai.protomolt.proto.workflow.WorkflowRepository workflowRepository(
            java.util.function.Function<String, java.util.Optional<String>> lookup) {
        com.fasterxml.jackson.databind.ObjectMapper json =
                new com.fasterxml.jackson.databind.ObjectMapper();
        return name -> lookup.apply(name).map(text -> {
            com.fasterxml.jackson.databind.JsonNode node;
            try {
                node = json.readTree(text);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new IllegalStateException(
                        "Stored workflow '" + name + "' is not valid JSON", e);
            }
            if (!(node instanceof com.fasterxml.jackson.databind.node.ObjectNode workflow)) {
                throw new IllegalStateException(
                        "Stored workflow '" + name + "' is not a JSON object");
            }
            return workflow;
        });
    }

    /** The credential a REST request presents: {@code api_token} or a bearer authorization. */
    private static String presentedCredential(Map<String, String> headers) {
        String presented = headers.get("api_token");
        if (presented == null) {
            String authorization = headers.get("authorization");
            if (authorization != null && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = authorization.substring(7).trim();
            }
        }
        return presented;
    }

    /** Header-to-caller resolution for the REST mount: operator token, else the policy. */
    /**
     * The services whose verbs this server wired, beside ProtoMolt's own.
     *
     * <p>Listed by what was actually started rather than by what is on the classpath: the
     * mount publishes only methods the catalog can serve, and naming a service it did not
     * wire would rely on that filter rather than saying so here.
     */
    private static java.util.List<ServiceDescriptor> contributedServices(boolean meshCluster) {
        java.util.List<ServiceDescriptor> services = new java.util.ArrayList<>();
        // Fully qualified: the generated holder and the registrar share a simple name.
        services.add(ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                .findServiceByName("DelegationService"));
        if (meshCluster) {
            services.add(ClusterDirectoryServiceProto.getDescriptor()
                    .findServiceByName("ClusterDirectoryService"));
        }
        return services;
    }

    private static Function<Map<String, String>, Caller> restCallers(
            String apiToken, CallerResolver resolver) {
        byte[] operator = apiToken.getBytes(StandardCharsets.UTF_8);
        return headers -> {
            String presented = presentedCredential(headers);
            if (presented == null || presented.isBlank()) {
                return null;
            }
            if (MessageDigest.isEqual(operator,
                    presented.getBytes(StandardCharsets.UTF_8))) {
                return Caller.operator();
            }
            return resolver.resolve(presented).orElse(null);
        };
    }

    /**
     * The trust snapshot the verifying verbs fall back to. Without a config lane this is
     * the operator's pinned file, resolved once, exactly as before. With one, the lane's
     * snapshot takes precedence when a document has applied and the file remains the
     * floor, so a node keeps verifying across a lane outage instead of losing custody.
     * Either way a request's own {@code trust} still wins.
     */
    private static Supplier<TrustSnapshot> trustSource(Options options) {
        TrustPin pinned = TrustPin.fromEnvironment();
        TrustSnapshot file = pinned == null ? null : pinned.snapshot();
        if (options.configLane() == null) {
            return () -> file;
        }
        ConfigLaneOptions lane = options.configLane();
        DistributedConfig config = DistributedConfig.over(
                new RegistryConfigSource(lane.registryUrl(), options.apiToken()));
        TrustSnapshotMounts mounts = TrustSnapshotMounts.follow(config);
        refreshQuietly(config, "the boot pull");
        if (lane.refreshSeconds() > 0) {
            // Stays a single platform thread: the delay schedule is the backpressure, and
            // serialising refreshes is what keeps two config pulls from overlapping.
            ScheduledExecutorService refresher = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().name("serve-config-refresh").daemon().factory());
            refresher.scheduleWithFixedDelay(
                    () -> refreshQuietly(config, "a config refresh"),
                    lane.refreshSeconds(), lane.refreshSeconds(), TimeUnit.SECONDS);
        }
        return () -> mounts.current()
                .map(TrustSnapshotMounts.Mounted::snapshot)
                .orElse(file);
    }

    /**
     * A lane that cannot be reached must not take the server down or drop the custody it
     * already has: the refresh logs and the previous snapshot stays live.
     */
    private static void refreshQuietly(DistributedConfig config, String what) {
        try {
            config.refresh();
        } catch (RuntimeException e) {
            LOG.warn("{} of the config lane failed; the previous trust snapshot stays"
                    + " live", what, e);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // best-effort teardown on a failed start
            }
        }
    }

    public int grpcPort() {
        return grpc.port();
    }

    public int httpPort() {
        return httpPort;
    }

    /** The registry port, or -1 when no registry is mounted. */
    public int registryPort() {
        return registryPort;
    }

    DelegationBridge delegationBridge() {
        return delegation.bridge();
    }

    /** Blocks until the gRPC server terminates. */
    public void awaitTermination() throws InterruptedException {
        grpc.awaitTermination();
    }

    @Override
    public void close() {
        // Stop the fleet first so nothing claims work mid-shutdown, then drain the
        // relay, then the pool.
        closeQuietly(jobsWorker);
        closeQuietly(jobsRelay);
        closeQuietly(jobsDatabase);
        closeQuietly(meshCluster);
        // Worker streams close before the coordinator so no stream dies mid-frame;
        // the runtime closes the bridge, the coordinator, and the repository channel.
        closeQuietly(delegation);
        if (registry != null) {
            registry.close();
        }
        mcp.close();
        closeQuietly(registryStore);
        http.close();
        grpc.close();
        closeQuietly(jdbcCallers);
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        ProtoMoltServe serve = start(options);
        LOG.info("""
                ProtoMolt serving:
                  gRPC  {}:{}   ai.pipestream.proto.grpc.service.v1.ProtoMoltService (reflection on)
                  REST  http://{}:{}/grpc-json/ProtoMoltService/{Method}
                  API   http://{}:{}/openapi.json
                  Docs  http://{}:{}/docs
                  MCP   http://{}:{}/mcp   (streamable HTTP)
                """, options.host(), serve.grpcPort(), options.host(), serve.httpPort(),
                options.host(), serve.httpPort(), options.host(), serve.httpPort(),
                options.host(), serve.httpPort());
        if (serve.registryPort() >= 0) {
            LOG.info("  Reg   http://{}:{} (Confluent protocol, git-backed)",
                    options.host(), serve.registryPort());
        }
        if (options.jobs() != null) {
            LOG.info("  Jobs  {} (verbs: submit-workflow, get-job, list-jobs, complete-step{})",
                    options.jobs().jdbcUrl(),
                    options.jobs().kafkaBootstrap() != null
                            ? "; Kafka " + options.jobs().kafkaBootstrap()
                            : ", no broker - verb submission only");
        }
        if (options.apiToken() == null || options.taskConsole() != null) {
            LOG.info("  UI    http://{}:{}/console", options.host(), serve.httpPort());
        }
        if (options.apiToken() != null) {
            LOG.info("  Auth  api_token required on gRPC, REST, MCP{}"
                            + " (health, OpenAPI, and docs stay open)",
                    serve.registryPort() >= 0 ? ", and the registry" : "");
            if (options.taskConsole() == null) {
                LOG.info("  UI    console disabled in token mode");
            } else {
                LOG.info("  UI    task console uses a scoped browser session; "
                        + "registry and serve proxies stay disabled");
            }
            if (options.accessPolicy() != null) {
                LOG.info("  Auth  access policy mounted from {}; named principals are "
                        + "scope-checked, the operator token keeps every scope",
                        options.accessPolicy());
            }
            if (options.identityStores() != null) {
                LOG.info("  Auth  external caller stores mounted:{}{}",
                        options.identityStores().oidcIntrospection() != null
                                ? " OIDC introspection at "
                                        + options.identityStores().oidcIntrospection()
                                : "",
                        options.identityStores().callerStore() != null
                                ? " JDBC caller store" : "");
            }
        }
        if (options.demo()) {
            LOG.info("""
                    Demo schema seeded: subject {} (types demo.shop.v1.Order, Customer, ...)
                      Try: curl -s -H 'content-type: application/json' \\
                             -d '{"schema": {"type": "demo.shop.v1.Order"}}' \\
                             http://{}:{}/grpc-json/ProtoMoltService/RenderJsonSchema
                      Or open http://{}:{}/docs and call ValidateMessage on a demo.shop.v1.Order.
                    """, DemoSchemas.SHOP_SUBJECT,
                    options.host(), serve.httpPort(), options.host(), serve.httpPort());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(serve::close));
        serve.awaitTermination();
    }
}
