package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.authz.OidcCallerResolver;
import ai.protomolt.proto.authz.jdbc.CallerStoreConfig;
import ai.protomolt.proto.authz.jdbc.JdbcCallerResolver;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.service.ProtoMoltCatalog;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ProtoRestGateway;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventRelay;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.mesh.cluster.ClusterActions;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.server.ProtoToolsServerConfig;
import ai.protomolt.proto.server.jdk.JdkProtoRestServer;
import ai.protomolt.proto.workflow.WorkflowRepository;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
                          ConfigLaneOptions configLane,
                          WorkflowAuthoringOptions workflowAuthoring,
                          WorkflowPreparationOptions workflowPreparation) {

        public Options {
            if (apiToken != null && apiToken.isBlank()) {
                throw new IllegalArgumentException("API token must be nonblank when supplied; use null for an open node");
            }
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
            if (workflowAuthoring != null && (apiToken == null || apiToken.isBlank()
                    || registryGit == null || workflowWorkspace == null
                    || delegation == null || jobs == null)) {
                throw new IllegalArgumentException("workflow authoring requires an API token, "
                        + "explicit persistent registry and workflow workspaces, durable "
                        + "delegation repository, and jobs database");
            }
            if (workflowPreparation != null && (workflowAuthoring == null
                    || delegation == null || delegation.repoEndpoint() == null
                    || delegation.repoEndpoint().isBlank())) {
                throw new IllegalArgumentException("workflow preparation requires workflow authoring "
                        + "and a durable delegation repository");
            }
        }

        /** Binary/source-compatible constructor retaining the pre-preparation options surface. */
        public Options(String host, int grpcPort, int httpPort,
                       Path registryGit, int registryPort, String apiToken, boolean demo,
                       Path gatherCache, JobsOptions jobs,
                       java.util.List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole,
                       MeshClusterOptions meshCluster, Path accessPolicy,
                       IdentityStoreOptions identityStores, ConfigLaneOptions configLane,
                       WorkflowAuthoringOptions workflowAuthoring) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, taskConsole, meshCluster, accessPolicy,
                    identityStores, configLane, workflowAuthoring, null);
        }

        /** Binary/source-compatible constructor retaining the pre-authoring options surface. */
        public Options(String host, int grpcPort, int httpPort,
                       Path registryGit, int registryPort, String apiToken, boolean demo,
                       Path gatherCache, JobsOptions jobs,
                       java.util.List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole,
                       MeshClusterOptions meshCluster, Path accessPolicy,
                       IdentityStoreOptions identityStores, ConfigLaneOptions configLane) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, taskConsole, meshCluster, accessPolicy,
                    identityStores, configLane, null);
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
            int jobsWorkers = envInt("PROTOMOLT_JOBS_WORKERS", WorkflowRunsConfig.DEFAULT_WORKER_COUNT);
            int jobsTargetConcurrency = envInt("PROTOMOLT_JOBS_TARGET_CONCURRENCY",
                    WorkflowRunsConfig.DEFAULT_MAX_CONCURRENT_PER_TARGET);
            java.util.List<String> inferenceModels = new java.util.ArrayList<>();
            String serviceWorkspaceEnv = System.getenv("PROTOMOLT_SERVICE_WORKSPACE");
            Path serviceWorkspace = serviceWorkspaceEnv == null || serviceWorkspaceEnv.isBlank()
                    ? null : Path.of(serviceWorkspaceEnv);
            String workflowWorkspaceEnv = System.getenv("PROTOMOLT_WORKFLOW_WORKSPACE");
            Path workflowWorkspace = workflowWorkspaceEnv == null || workflowWorkspaceEnv.isBlank()
                    ? null : Path.of(workflowWorkspaceEnv);
            String authoringPolicySha256 = System.getenv("PROTOMOLT_WORKFLOW_AUTHORING_POLICY_SHA256");
            String authorizationDirEnv = System.getenv("PROTOMOLT_WORKFLOW_AUTHORING_AUTHORIZATION_DIR");
            Path authoringAuthorizationDir = authorizationDirEnv == null || authorizationDirEnv.isBlank()
                    ? null : Path.of(authorizationDirEnv);
            String preparationDirEnv = System.getenv("PROTOMOLT_WORKFLOW_PREPARATION_INTENT_DIR");
            Path preparationIntentDir = preparationDirEnv == null || preparationDirEnv.isBlank()
                    ? null : Path.of(preparationDirEnv);
            String preparationTemplateProvider = System.getenv("PROTOMOLT_WORKFLOW_PREPARATION_TEMPLATE_PROVIDER");
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
                int start = 0;
                while (start <= inferenceModelsEnv.length()) {
                    int semi = inferenceModelsEnv.indexOf(';', start);
                    String spec = semi < 0 ? inferenceModelsEnv.substring(start)
                            : inferenceModelsEnv.substring(start, semi);
                    if (!spec.isBlank()) {
                        inferenceModels.add(spec.trim());
                    }
                    if (semi < 0) {
                        break;
                    }
                    start = semi + 1;
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
                    case "--workflow-authoring-policy-sha256" ->
                            authoringPolicySha256 = requireValue(args, ++i);
                    case "--workflow-authoring-authorization-dir" ->
                            authoringAuthorizationDir = Path.of(requireValue(args, ++i));
                    case "--workflow-preparation-intent-dir" ->
                            preparationIntentDir = Path.of(requireValue(args, ++i));
                    case "--workflow-preparation-template-provider" ->
                            preparationTemplateProvider = requireValue(args, ++i);
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
                                + "[--workflow-authoring-policy-sha256 <sha256> "
                                + "--workflow-authoring-authorization-dir <persistent-dir>] "
                                + "[--workflow-preparation-intent-dir <persistent-dir> "
                                + "--workflow-preparation-template-provider <id>] "
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
            WorkflowAuthoringOptions workflowAuthoring = null;
            if ((authoringPolicySha256 != null && !authoringPolicySha256.isBlank())
                    || authoringAuthorizationDir != null) {
                workflowAuthoring = new WorkflowAuthoringOptions(
                        authoringPolicySha256, authoringAuthorizationDir);
            }
            WorkflowPreparationOptions workflowPreparation = null;
            if (preparationIntentDir != null || preparationTemplateProvider != null) {
                workflowPreparation = new WorkflowPreparationOptions(
                        preparationIntentDir, preparationTemplateProvider);
            }
            return new Options(host, grpcPort, httpPort, registryGit, registryPort, apiToken,
                    demo, gatherCache, jobs, java.util.List.copyOf(inferenceModels),
                    serviceWorkspace, outboundPolicy, workflowWorkspace, delegation, taskConsole,
                    meshCluster, accessPolicy, identityStores, configLane, workflowAuthoring,
                    workflowPreparation);
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
                for (String item : commaSeparated(value)) {
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
            for (String item : commaSeparated(value)) {
                if (item.isBlank()) {
                    throw new IllegalArgumentException("grpc allowed ports contains an empty value");
                }
                ports.add(parseInt(item.trim(), "grpc allowed port"));
            }
            return Set.copyOf(ports);
        }

        /** Comma-separated segments of {@code value} with {@code split(",", -1)} semantics: empty segments are kept, including trailing ones. */
        private static java.util.List<String> commaSeparated(String value) {
            java.util.List<String> items = new java.util.ArrayList<>();
            int start = 0;
            while (true) {
                int comma = value.indexOf(',', start);
                items.add(comma < 0 ? value.substring(start) : value.substring(start, comma));
                if (comma < 0) {
                    return items;
                }
                start = comma + 1;
            }
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
            if (workers <= 0) throw new IllegalArgumentException("jobs workers must be positive");
            if (targetConcurrency <= 0) throw new IllegalArgumentException("jobs targetConcurrency must be positive");
            if (requestTopic != null && (kafkaBootstrap == null || kafkaBootstrap.isBlank())) {
                throw new IllegalArgumentException("--jobs-request-topic requires --jobs-kafka");
            }
        }
    }

    /** Opt-in trusted authoring policy and write-once launch authorization ledger. */
    public record WorkflowAuthoringOptions(String policySha256, Path authorizationDirectory) {
        public WorkflowAuthoringOptions {
            if (policySha256 == null || !isLowercaseSha256Hex(policySha256)) {
                throw new IllegalArgumentException("workflow authoring policy must be a lowercase SHA-256 digest");
            }
            Objects.requireNonNull(authorizationDirectory, "authorizationDirectory");
        }
    }

    /** Lowercase 64-character hex, the {@code [0-9a-f]{64}} full match. */
    private static boolean isLowercaseSha256Hex(String value) {
        if (value.length() != 64) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!(c >= '0' && c <= '9' || c >= 'a' && c <= 'f')) {
                return false;
            }
        }
        return true;
    }

    /** Opt-in durable preparation intent and trusted source restriction provider. */
    public record WorkflowPreparationOptions(Path intentDirectory, String templateProviderId) {
        public WorkflowPreparationOptions {
            Objects.requireNonNull(intentDirectory, "workflow preparation intent directory");
            if (templateProviderId == null || templateProviderId.isBlank()
                    || !templateProviderId.equals(templateProviderId.trim())) {
                throw new IllegalArgumentException("workflow preparation template provider ID is required");
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
            if (clusterId == null || !isPathSafeName(clusterId)) {
                throw new IllegalArgumentException("mesh cluster id must be path-safe");
            }
            if (displayName == null || displayName.length() > 256) {
                throw new IllegalArgumentException("mesh cluster name is invalid");
            }
            if (trustDomain == null || !(trustDomain.isEmpty() || isPathSafeName(trustDomain))) {
                throw new IllegalArgumentException("mesh trust domain is invalid");
            }
            Objects.requireNonNull(createdAt, "createdAt");
        }
    }

    /**
     * The {@code [A-Za-z0-9][A-Za-z0-9._-]{0,127}} full match: one leading ASCII
     * alphanumeric followed by up to 127 ASCII alphanumerics, dots, underscores, or dashes.
     */
    private static boolean isPathSafeName(String value) {
        int length = value.length();
        if (length < 1 || length > 128 || !isAsciiAlphanumeric(value.charAt(0))) {
            return false;
        }
        for (int i = 1; i < length; i++) {
            char c = value.charAt(i);
            if (!isAsciiAlphanumeric(c) && c != '.' && c != '_' && c != '-') {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiAlphanumeric(char c) {
        return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9';
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
    private final ConfigLaneMount.TrustSource trustSource;

    private ProtoMoltServe(ProtoMoltGrpcServer grpc, JdkProtoRestServer http,
                           McpHttpHandler mcp, int httpPort,
                           GitSchemaRegistryStore registryStore, SchemaRegistryServer registry,
                           int registryPort, WorkflowRunDatabase jobsDatabase,
                           WorkflowRunWorker jobsWorker, WorkflowRunEventRelay jobsRelay,
                           DelegationRuntime delegation, MeshClusterRuntime meshCluster,
                           JdbcCallerResolver jdbcCallers,
                           ConfigLaneMount.TrustSource trustSource) {
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
        this.trustSource = trustSource;
    }

    /** Starts every configured surface; closing stops them all. */
    public static ProtoMoltServe start(Options options) {
        return start(options, CorrectionStarter.fromEnvironment(System.getenv()));
    }

    static ProtoMoltServe start(Options options, CorrectionStarter starter) {
        if (starter != null && (options.apiToken() == null || options.apiToken().isBlank()
                || options.taskConsole() == null)) {
            throw new IllegalArgumentException("the correction starter requires API and console authentication");
        }
        ActionContext context = ActionContext.create();

        ProtoMoltGrpcServer grpc = null;
        JdkProtoRestServer http = null;
        GitSchemaRegistryStore store = null;
        SchemaRegistryServer registry = null;
        WorkflowRunDatabase jobsDatabase = null;
        WorkflowRunWorker jobsWorker = null;
        WorkflowRunEventRelay jobsRelay = null;
        ConfigLaneMount.TrustSource trust = null;
        ServiceProfileRepository serviceProfiles = null;
        ArtifactRepository artifacts = null;
        RunEvidenceRepository runEvidence = null;
        OutboundChannelPolicy outboundPolicy = options.outboundPolicy();
        try {
            InferenceEngines inference = InferenceMount.engines(options.inferenceModels());
            StructuredGenerator structured = inference == null
                    ? null : new StructuredGenerator(inference, context.registry());
            store = RegistryStoreMount.openStore(options.registryGit(), options.demo());
            WorkflowRepository workflows = store == null
                    ? null : RegistryStoreMount.workflowRepository(store);
            WorkflowVersionRepository workflowVersions = store == null
                    ? null : RegistryStoreMount.workflowVersions(store);
            serviceProfiles = WorkspaceMount.serviceProfiles(options.serviceWorkspace());
            WorkspaceMount.WorkflowRepositories workspace =
                    WorkspaceMount.workflowRepositories(options.workflowWorkspace(), options.demo());
            artifacts = workspace.artifacts();
            runEvidence = workspace.runEvidence();

            trust = ConfigLaneMount.trustSource(options.configLane(),
                    options.apiToken());

            // Workflow runs: the store boots (and Flyway-migrates) before the catalog so
            // the jobs verbs serve from the same truth the worker fleet executes from.
            if (options.jobs() != null) {
                JobsFleetMount.Prepared jobs = JobsFleetMount.prepare(options.jobs(), context,
                        workflows, structured, outboundPolicy);
                jobsDatabase = jobs.database();
                jobsWorker = jobs.worker();
                jobsRelay = jobs.relay();
                WorkflowAuthoringMount.Prepared authoring = options.workflowAuthoring() == null
                        ? null : WorkflowAuthoringMount.prepare(options.workflowAuthoring(),
                                artifacts, runEvidence, workflowVersions, jobs.store(), context,
                                jobs.runner(), jobs.config().maxAttemptsDefault(), trust);
                WorkflowPreparationMount.Prepared preparation = options.workflowPreparation() == null
                        ? null : WorkflowPreparationMount.prepare(options.workflowPreparation(), authoring);

                // The catalog sees the store so run-workflow resolves stored workflow names and
                // the jobs verbs serve the live job rows.
                ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                        workflows, jobs.store(), jobs.config().maxAttemptsDefault(),
                        inference, serviceProfiles,
                        outboundPolicy, artifacts, runEvidence, workflowVersions, store,
                        trust, starter == null ? null : starter.credentials());
                return startWithJobsCatalog(options, context, catalog, store,
                        serviceProfiles, jobs, starter, authoring, preparation, trust);
            }
            // The catalog sees the store so run-workflow resolves stored workflow names.
            ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                    workflows, null, 0, inference, serviceProfiles,
                    outboundPolicy, artifacts, runEvidence, workflowVersions, store,
                    trust, starter == null ? null : starter.credentials());
            return startWithJobsCatalog(options, context, catalog, store,
                    serviceProfiles, null, starter, null, null, trust);
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
            closeQuietly(trust);
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
                                                       ServiceProfileRepository serviceProfiles,
                                                       JobsFleetMount.Prepared jobs,
                                                       CorrectionStarter starter,
                                                       WorkflowAuthoringMount.Prepared authoring,
                                                       WorkflowPreparationMount.Prepared preparation,
                                                       ConfigLaneMount.TrustSource trust) {
        ProtoMoltGrpcServer grpc = null;
        JdkProtoRestServer http = null;
        McpHttpHandler mcpHandler = null;
        SchemaRegistryServer registry = null;
        DelegationRuntime delegation = null;
        MeshClusterRuntime meshCluster = null;
        JdbcCallerResolver jdbcCallers = null;
        try {
            if (starter != null) starter.install(catalog, serviceProfiles);
            if (options.demo()) {
                DemoSchemas.seed(context.registry(), store);
            }

            // The live delegation surface: one coordinator per server, adapted to
            // catalog verbs and MCP resources through the bridge. Durable when a
            // delegation repository endpoint is configured, in-memory otherwise.
            delegation = DelegationMount.open(options.delegation(), authoring);
            DelegationBridge bridge = delegation.bridge();
            Map<String, Map<String, String>> authorTaskBindings =
                    DelegationMount.registerAll(catalog, delegation, authoring, preparation);

            meshCluster = MeshClusterRuntime.open(options.meshCluster(), options.delegation());
            if (meshCluster != null) {
                ClusterActions.register(catalog, meshCluster.directory());
            }

            // The access policy narrows named principals; the operator token keeps every
            // scope. A policy that fails to load or verify refuses startup loudly. The
            // external stores compose behind the policy, first match wins.
            AccessPolicyMount.Callers resolved =
                    AccessPolicyMount.resolve(options.accessPolicy(), options.identityStores());
            CallerResolver callers = resolved.callers();
            jdbcCallers = resolved.jdbcCallers();

            java.util.List<ServiceDescriptor> contributed = GrpcMount.contributedServices(
                    meshCluster != null, authoring != null, preparation != null);
            grpc = GrpcMount.start(options, catalog, callers, contributed, authorTaskBindings,
                    authoring != null);
            if (options.demo() && store != null) {
                // The demo workflow composes this server's own verbs, so it needs the bound
                // gRPC port - seeded here rather than with the schemas.
                DemoSchemas.seedWorkflow(store, grpc.port());
            }

            // The jobs fleet starts once the catalog is bound: workers execute and the
            // relay drains. Broker-less deployments run the worker only.
            if (jobs != null) {
                jobs.worker().start();
                if (jobs.relay() != null) {
                    jobs.relay().start();
                }
            }

            // The registry starts before HTTP so the console's same-origin proxy
            // (/api/protomolt) knows the port it bridges to.
            int registryPort = -1;
            if (store != null) {
                registry = RegistryStoreMount.server(options.host(), options.registryPort(),
                        options.apiToken(), store, catalog, callers);
                registryPort = registry.start();
            }

            ProtoRestGateway gateway = RestGatewayMount.mount(context, catalog,
                    options.apiToken(), callers, contributed, authorTaskBindings);
            String version = serveVersion();
            mcpHandler = McpMount.mount(catalog, store, serviceProfiles, bridge,
                    options.apiToken(), callers, version);
            ProtoToolsServerConfig config = ProtoToolsServerConfig.defaults()
                    .withHost(options.host())
                    .withPort(options.httpPort());
            http = new JdkProtoRestServer(config, gateway,
                    new ProtoOpenApiGenerator("ProtoMolt", version, "/", config.restPathPrefix()))
                    .withContext("/docs", new SwaggerUiHandler("/docs", config.openApiPath()))
                    .withContext("/mcp", mcpHandler);
            int boundRegistryPort = registryPort;
            int[] selfPort = {-1};
            ConsoleMount.mount(http, options.apiToken(), options.taskConsole(), callers, bridge,
                    starter, catalog, authoring, () -> boundRegistryPort, () -> selfPort[0]);
            int httpPort = http.start();
            selfPort[0] = httpPort;
            return new ProtoMoltServe(grpc, http, mcpHandler, httpPort, store, registry, registryPort,
                    jobs == null ? null : jobs.database(),
                    jobs == null ? null : jobs.worker(),
                    jobs == null ? null : jobs.relay(),
                    delegation, meshCluster, jdbcCallers, trust);
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

    /** The implementation version stamped on the OpenAPI document and the MCP server. */
    private static String serveVersion() {
        String version = ProtoMoltServe.class.getPackage().getImplementationVersion();
        return version != null ? version : "dev";
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
        closeQuietly(trustSource);
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        ProtoMoltServe serve = start(options);
        LOG.info("""
                ProtoMolt serving:
                  gRPC  {}:{}   ai.protomolt.proto.grpc.service.v1.ProtoMoltService (reflection on)
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
