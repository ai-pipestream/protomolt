package ai.protomolt.proto.serve;

import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.authz.jdbc.CallerStoreConfig;
import ai.protomolt.proto.authz.jdbc.JdbcCallerResolver;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventRelay;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.server.jdk.JdkProtoRestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * The one-process ProtoMolt server: {@code ProtoMoltService} over gRPC (reflection enabled),
 * the same verbs over JSON/REST with OpenAPI and Swagger UI, the MCP server on
 * streamable HTTP at {@code /mcp} (with registry resources when a registry is mounted), and
 * optionally the git-backed schema registry speaking the Confluent protocol.
 */
public final class ProtoMoltServe implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoMoltServe.class);

    /**
     * Launcher options; a port of 0 picks a free port. A non-null {@code apiToken} guards
     * every operational surface (gRPC calls, REST verbs, the MCP endpoint) with a shared
     * secret; documentation surfaces (health, OpenAPI, Swagger UI) stay open.
     *
     * @param jobs workflow-runs configuration; null disables the jobs worker
     * @param outboundPolicy process-wide outbound policy; null selects permissive defaults
     * @param profileTlsConfig optional host TLS configuration for {@code trust_ref} and
     *        {@code client_certificate_ref} mounts
     */
    public record Options(String host, int grpcPort, int httpPort,
                          Path registryGit, int registryPort, String apiToken, boolean demo,
                          Path gatherCache, JobsOptions jobs, List<String> inferenceModels,
                          Path serviceWorkspace, OutboundChannelPolicy outboundPolicy,
                          Path workflowWorkspace, DelegationOptions delegation,
                          TaskConsoleOptions taskConsole, MeshClusterOptions meshCluster,
                          Path accessPolicy, IdentityStoreOptions identityStores,
                          ConfigLaneOptions configLane, WorkflowAuthoringOptions workflowAuthoring,
                          WorkflowPreparationOptions workflowPreparation, Path profileTlsConfig) {

        public Options {
            outboundPolicy = ProtoMoltServeOptionBuilders.validateOptions(
                    apiToken, registryGit, outboundPolicy, workflowWorkspace, jobs, delegation,
                    accessPolicy, identityStores, workflowAuthoring, workflowPreparation);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit, int registryPort,
                       String apiToken, boolean demo, Path gatherCache, JobsOptions jobs,
                       List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole,
                       MeshClusterOptions meshCluster, Path accessPolicy,
                       IdentityStoreOptions identityStores, ConfigLaneOptions configLane,
                       WorkflowAuthoringOptions workflowAuthoring,
                       WorkflowPreparationOptions workflowPreparation) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo, gatherCache,
                    jobs, inferenceModels, serviceWorkspace, outboundPolicy, workflowWorkspace,
                    delegation, taskConsole, meshCluster, accessPolicy, identityStores, configLane,
                    workflowAuthoring, workflowPreparation, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit, int registryPort,
                       String apiToken, boolean demo, Path gatherCache, JobsOptions jobs,
                       List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole,
                       MeshClusterOptions meshCluster, Path accessPolicy,
                       IdentityStoreOptions identityStores, ConfigLaneOptions configLane,
                       WorkflowAuthoringOptions workflowAuthoring) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo, gatherCache,
                    jobs, inferenceModels, serviceWorkspace, outboundPolicy, workflowWorkspace,
                    delegation, taskConsole, meshCluster, accessPolicy, identityStores, configLane,
                    workflowAuthoring, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit, int registryPort,
                       String apiToken, boolean demo, Path gatherCache, JobsOptions jobs,
                       List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole,
                       MeshClusterOptions meshCluster, Path accessPolicy,
                       IdentityStoreOptions identityStores, ConfigLaneOptions configLane) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo, gatherCache,
                    jobs, inferenceModels, serviceWorkspace, outboundPolicy, workflowWorkspace,
                    delegation, taskConsole, meshCluster, accessPolicy, identityStores, configLane,
                    null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit, int registryPort,
                       String apiToken, boolean demo, Path gatherCache, JobsOptions jobs,
                       List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole,
                       MeshClusterOptions meshCluster, Path accessPolicy,
                       IdentityStoreOptions identityStores) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo, gatherCache,
                    jobs, inferenceModels, serviceWorkspace, outboundPolicy, workflowWorkspace,
                    delegation, taskConsole, meshCluster, accessPolicy, identityStores, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit, int registryPort,
                       String apiToken, boolean demo, Path gatherCache, JobsOptions jobs,
                       List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole,
                       MeshClusterOptions meshCluster, Path accessPolicy) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo, gatherCache,
                    jobs, inferenceModels, serviceWorkspace, outboundPolicy, workflowWorkspace,
                    delegation, taskConsole, meshCluster, accessPolicy, null, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit, int registryPort,
                       String apiToken, boolean demo, Path gatherCache, JobsOptions jobs,
                       List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole,
                       MeshClusterOptions meshCluster) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo, gatherCache,
                    jobs, inferenceModels, serviceWorkspace, outboundPolicy, workflowWorkspace,
                    delegation, taskConsole, meshCluster, null, null, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort) {
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
                    gatherCache, jobs, List.of());
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, List<String> inferenceModels) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, List<String> inferenceModels, Path serviceWorkspace) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, null, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo, Path gatherCache,
                       JobsOptions jobs, List<String> inferenceModels, Path serviceWorkspace,
                       OutboundChannelPolicy outboundPolicy, Path workflowWorkspace,
                       DelegationOptions delegation, TaskConsoleOptions taskConsole) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo,
                    gatherCache, jobs, inferenceModels, serviceWorkspace, outboundPolicy,
                    workflowWorkspace, delegation, taskConsole, null);
        }

        static final String ENV_PROFILE_TLS_CONFIG = "PROTOMOLT_PROFILE_TLS_CONFIG";
        static final String FLAG_PROFILE_TLS_CONFIG = "--profile-tls-config";

        public static Options defaults() {
            return new Options("0.0.0.0", 9090, 8080, null, 8081, null, false, null);
        }

        static Options parse(String[] args) {
            return ProtoMoltServeArgs.parse(args);
        }
    }

    /** Workflow-runs launcher options. */
    public record JobsOptions(String jdbcUrl, String username, String password,
                              String kafkaBootstrap, String requestTopic,
                              int workers, int targetConcurrency) {
        public JobsOptions {
            if (workers <= 0) {
                throw new IllegalArgumentException("jobs workers must be positive");
            }
            if (targetConcurrency <= 0) {
                throw new IllegalArgumentException("jobs targetConcurrency must be positive");
            }
            if (requestTopic != null && (kafkaBootstrap == null || kafkaBootstrap.isBlank())) {
                throw new IllegalArgumentException("--jobs-request-topic requires --jobs-kafka");
            }
        }
    }

    /** Opt-in trusted authoring policy and write-once launch authorization ledger. */
    public record WorkflowAuthoringOptions(String policySha256, Path authorizationDirectory) {
        public WorkflowAuthoringOptions {
            if (policySha256 == null
                    || !ProtoMoltServeOptionBuilders.isLowercaseSha256Hex(policySha256)) {
                throw new IllegalArgumentException(
                        "workflow authoring policy must be a lowercase SHA-256 digest");
            }
            Objects.requireNonNull(authorizationDirectory, "authorizationDirectory");
        }
    }

    /** Opt-in durable preparation intent and trusted source restriction provider. */
    public record WorkflowPreparationOptions(Path intentDirectory, String templateProviderId) {
        public WorkflowPreparationOptions {
            Objects.requireNonNull(intentDirectory, "workflow preparation intent directory");
            if (templateProviderId == null || templateProviderId.isBlank()
                    || !templateProviderId.equals(templateProviderId.trim())) {
                throw new IllegalArgumentException(
                        "workflow preparation template provider ID is required");
            }
        }
    }

    /** Delegation transcript durability settings. */
    public record DelegationOptions(String repoEndpoint, boolean repoTls, String drive,
                                    String objectKey, String keyReference) {
        public static final String DEFAULT_DRIVE = "protomolt";
        public static final String DEFAULT_OBJECT_KEY = "delegation/serve/transcript.pb.enc";
        public static final String DEFAULT_KEY_REFERENCE = "env:PROTOMOLT_TRANSCRIPT_KEY";

        public DelegationOptions {
            ProtoMoltServeOptionBuilders.validateDelegationOptions(
                    repoEndpoint, drive, objectKey, keyReference);
        }

        public DelegationOptions(String repoEndpoint, boolean repoTls) {
            this(repoEndpoint, repoTls, DEFAULT_DRIVE, DEFAULT_OBJECT_KEY, DEFAULT_KEY_REFERENCE);
        }
    }

    /** Browser-only authentication settings for the bounded task console API. */
    public record TaskConsoleOptions(String loginToken, Duration sessionTtl) {
        public TaskConsoleOptions {
            ConsoleSessions.validateSettings(loginToken, sessionTtl);
        }
    }

    /** Config lane settings for trust snapshot updates. */
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

    /** External caller stores composed behind the access policy. */
    public record IdentityStoreOptions(URI oidcIntrospection, String oidcClientId,
                                       String oidcClientSecret, CallerStoreConfig callerStore) {
        public IdentityStoreOptions {
            ProtoMoltServeOptionBuilders.validateIdentityStores(
                    oidcIntrospection, oidcClientId, oidcClientSecret, callerStore);
        }
    }

    /** Stable identity of the mesh directory hosted by this serve process. */
    public record MeshClusterOptions(String clusterId, String displayName, String trustDomain,
                                     Instant createdAt) {
        public MeshClusterOptions {
            ProtoMoltServeOptionBuilders.validateMeshCluster(
                    clusterId, displayName, trustDomain, createdAt);
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
    private final ConfigLaneMount.TrustSource trustSource;
    private final ChannelFactory.Closeable outboundChannels;

    ProtoMoltServe(ProtoMoltGrpcServer grpc, JdkProtoRestServer http, McpHttpHandler mcp,
                   int httpPort, GitSchemaRegistryStore registryStore,
                   SchemaRegistryServer registry, int registryPort,
                   WorkflowRunDatabase jobsDatabase, WorkflowRunWorker jobsWorker,
                   WorkflowRunEventRelay jobsRelay, DelegationRuntime delegation,
                   MeshClusterRuntime meshCluster, JdbcCallerResolver jdbcCallers,
                   ConfigLaneMount.TrustSource trustSource,
                   ChannelFactory.Closeable outboundChannels) {
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
        this.outboundChannels = outboundChannels;
    }

    /** Starts every configured surface; closing stops them all. */
    public static ProtoMoltServe start(Options options) {
        return start(options, CorrectionStarter.fromEnvironment(System.getenv()));
    }

    static ProtoMoltServe start(Options options, CorrectionStarter starter) {
        return ProtoMoltServeBootstrap.start(options, starter);
    }

    static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // best-effort teardown on shutdown
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
        closeQuietly(jobsWorker);
        closeQuietly(jobsRelay);
        closeQuietly(jobsDatabase);
        closeQuietly(meshCluster);
        closeQuietly(delegation);
        if (registry != null) {
            registry.close();
        }
        mcp.close();
        closeQuietly(registryStore);
        http.close();
        grpc.close();
        closeQuietly(outboundChannels);
        closeQuietly(jdbcCallers);
        closeQuietly(trustSource);
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        ProtoMoltServe serve = start(options);
        ProtoMoltServeBootstrap.logStartupBanner(LOG, options, serve);
        Runtime.getRuntime().addShutdownHook(new Thread(serve::close));
        serve.awaitTermination();
    }
}
