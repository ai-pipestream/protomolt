package ai.pipestream.proto.serve;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainRepository;
import ai.pipestream.proto.chain.ChainRunner;
import ai.pipestream.proto.delegation.DelegationActions;
import ai.pipestream.proto.delegation.DelegationBridge;
import ai.pipestream.proto.delegation.InProcessDelegationCoordinator;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import ai.pipestream.proto.grpc.service.ProtoMoltGrpcServer;
import ai.pipestream.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.recipe.ArtifactRepository;
import ai.pipestream.proto.grpc.recipe.FileSystemArtifactRepository;
import ai.pipestream.proto.grpc.recipe.FileSystemRunEvidenceRepository;
import ai.pipestream.proto.grpc.recipe.RecipeRepository;
import ai.pipestream.proto.grpc.recipe.RunEvidenceRepository;
import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import ai.pipestream.proto.jobs.service.ChainJobsConfig;
import ai.pipestream.proto.jobs.service.events.ChainJobEventRelay;
import ai.pipestream.proto.jobs.service.store.ChainJobDatabase;
import ai.pipestream.proto.inference.spi.CredentialResolutionException;
import ai.pipestream.proto.inference.spi.CredentialResolver;
import ai.pipestream.proto.inference.spi.InferenceCatalog;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.structured.StructuredGenerator;
import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import ai.pipestream.proto.jobs.service.store.ChainJobStoreConfig;
import ai.pipestream.proto.jobs.service.store.JdbcChainJobStore;
import ai.pipestream.proto.jobs.service.worker.ChainJobWorker;
import ai.pipestream.proto.mcp.McpServer;
import ai.pipestream.proto.mcp.CompositeResources;
import ai.pipestream.proto.mcp.DelegationResources;
import ai.pipestream.proto.mcp.RegistryResources;
import ai.pipestream.proto.mcp.ServiceProfileResources;
import ai.pipestream.proto.openapi.ProtoOpenApiGenerator;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.RegistryRecipeRepository;
import ai.pipestream.proto.registry.server.SchemaRegistryServer;
import ai.pipestream.proto.registry.server.SchemaRegistryServerConfig;
import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import ai.pipestream.proto.server.jdk.JdkProtoRestServer;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;

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
 *                 [--recipe-workspace /srv/protomolt-recipes]
 * </pre>
 */
public final class ProtoMoltServe implements AutoCloseable {

    /**
     * Launcher options; a port of 0 picks a free port. A non-null {@code apiToken} guards
     * every operational surface (gRPC calls, REST verbs, the MCP endpoint) with a shared
     * secret; documentation surfaces (health, OpenAPI, Swagger UI) stay open.
     *
     * @param jobs chain-jobs configuration; null disables the jobs worker (the jobs
     *        verbs stay in the catalog and answer {@code unavailable})
     * @param outboundPolicy one process-wide policy shared by catalog actions and the jobs worker;
     *        null selects the permissive defaults
     */
    public record Options(String host, int grpcPort, int httpPort,
                          Path registryGit, int registryPort, String apiToken, boolean demo,
                          Path gatherCache, JobsOptions jobs,
                          java.util.List<String> inferenceModels, Path serviceWorkspace,
                          OutboundChannelPolicy outboundPolicy, Path recipeWorkspace) {

        public Options {
            if (outboundPolicy == null) {
                outboundPolicy = OutboundChannelPolicy.defaults();
            }
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
            String recipeWorkspaceEnv = System.getenv("PROTOMOLT_RECIPE_WORKSPACE");
            Path recipeWorkspace = recipeWorkspaceEnv == null || recipeWorkspaceEnv.isBlank()
                    ? null : Path.of(recipeWorkspaceEnv);
            String allowedSchemes = System.getenv("PROTOMOLT_GRPC_ALLOWED_SCHEMES");
            String allowedHosts = System.getenv("PROTOMOLT_GRPC_ALLOWED_HOSTS");
            String allowedPorts = System.getenv("PROTOMOLT_GRPC_ALLOWED_PORTS");
            boolean allowPlaintext = envBoolean("PROTOMOLT_GRPC_ALLOW_PLAINTEXT", true);
            boolean allowTls = envBoolean("PROTOMOLT_GRPC_ALLOW_TLS", true);
            long maxDeadlineMs = envLong("PROTOMOLT_GRPC_MAX_DEADLINE_MS", 60_000L);
            int maxActiveChannels = envInt("PROTOMOLT_GRPC_MAX_ACTIVE_CHANNELS", 64);
            String inferenceModelsEnv = System.getenv("PROTOMOLT_INFERENCE_MODELS");
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
                    case "--service-workspace" ->
                            serviceWorkspace = Path.of(requireValue(args, ++i));
                    case "--recipe-workspace" ->
                            recipeWorkspace = Path.of(requireValue(args, ++i));
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
                                + "[--recipe-workspace <dir>] (or PROTOMOLT_RECIPE_WORKSPACE) "
                                + "[--grpc-allowed-schemes <csv>] [--grpc-allowed-hosts <csv>] "
                                + "[--grpc-allowed-ports <csv>] "
                                + "[--grpc-allow-plaintext <true|false>] "
                                + "[--grpc-allow-tls <true|false>] "
                                + "[--grpc-max-deadline-ms <n>] "
                                + "[--grpc-max-active-channels <n>]");
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
                    serviceWorkspace, outboundPolicy, recipeWorkspace);
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
     * A bad spec or an unknown provider fails startup loud — a model the server
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
     * Chain-jobs launcher options. {@code kafkaBootstrap} is optional: without it the
     * worker fleet runs verb-submitted jobs with no event relay and no request topic —
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

    private final ProtoMoltGrpcServer grpc;
    private final JdkProtoRestServer http;
    private final McpHttpHandler mcp;
    private final GitSchemaRegistryStore registryStore;
    private final SchemaRegistryServer registry;
    private final int httpPort;
    private final int registryPort;
    private final ChainJobDatabase jobsDatabase;
    private final ChainJobWorker jobsWorker;
    private final ChainJobEventRelay jobsRelay;
    private final DelegationBridge delegation;

    private ProtoMoltServe(ProtoMoltGrpcServer grpc, JdkProtoRestServer http,
                           McpHttpHandler mcp, int httpPort,
                           GitSchemaRegistryStore registryStore, SchemaRegistryServer registry,
                           int registryPort, ChainJobDatabase jobsDatabase,
                           ChainJobWorker jobsWorker, ChainJobEventRelay jobsRelay,
                           DelegationBridge delegation) {
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
    }

    /** Starts every configured surface; closing stops them all. */
    public static ProtoMoltServe start(Options options) {
        ActionContext context = ActionContext.create();

        ProtoMoltGrpcServer grpc = null;
        JdkProtoRestServer http = null;
        GitSchemaRegistryStore store = null;
        SchemaRegistryServer registry = null;
        ChainJobDatabase jobsDatabase = null;
        ChainJobWorker jobsWorker = null;
        ChainJobEventRelay jobsRelay = null;
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
                } catch (java.io.IOException e) {
                    throw new IllegalStateException("Failed to create the demo registry directory", e);
                }
            }
            if (registryGit != null) {
                store = GitSchemaRegistryStore.builder()
                        .repositoryDir(registryGit)
                        .build();
            }
            ChainRepository chains = store == null ? null : chainRepository(store);
            RecipeRepository recipes = store == null ? null : new RegistryRecipeRepository(store);
            if (options.serviceWorkspace() != null) {
                try {
                    serviceProfiles = new FileSystemServiceProfileRepository(
                            options.serviceWorkspace());
                } catch (java.io.IOException e) {
                    throw new IllegalStateException("Failed to open service workspace at "
                            + options.serviceWorkspace(), e);
                }
            }
            Path recipeWorkspace = options.recipeWorkspace();
            if (recipeWorkspace == null && options.demo()) {
                try {
                    recipeWorkspace = java.nio.file.Files
                            .createTempDirectory("protomolt-demo-recipes");
                } catch (java.io.IOException e) {
                    throw new IllegalStateException("Failed to create the demo recipe workspace", e);
                }
            }
            if (recipeWorkspace != null) {
                try {
                    artifacts = new FileSystemArtifactRepository(
                            recipeWorkspace.resolve("artifacts"));
                    runEvidence = new FileSystemRunEvidenceRepository(
                            recipeWorkspace.resolve("runs"));
                } catch (java.io.IOException e) {
                    throw new IllegalStateException("Failed to open recipe workspace at "
                            + recipeWorkspace, e);
                }
            }

            // Chain jobs: the store boots (and Flyway-migrates) before the catalog so
            // the jobs verbs serve from the same truth the worker fleet executes from.
            if (options.jobs() != null) {
                JobsOptions jobs = options.jobs();
                jobsDatabase = new ChainJobDatabase(
                        new ChainJobStoreConfig(jobs.jdbcUrl(), jobs.username(), jobs.password()));
                JdbcChainJobStore jobStore = new JdbcChainJobStore(jobsDatabase);
                String requestTopic = jobs.kafkaBootstrap() == null
                        ? null
                        : jobs.requestTopic() != null ? jobs.requestTopic() : "chain-job-requests";
                ChainJobsConfig jobsConfig = new ChainJobsConfig(
                        "serve-" + ManagementFactory.getRuntimeMXBean().getName(),
                        jobs.workers(), null, null, 0, 0, jobs.targetConcurrency(),
                        requestTopic, null, jobs.kafkaBootstrap(), null);
                jobsWorker = new ChainJobWorker(jobStore, context, chains,
                        new ChainRunner(outboundPolicy, structured),
                        jobsConfig);
                if (jobs.kafkaBootstrap() != null) {
                    jobsRelay = new ChainJobEventRelay(jobStore,
                            ChainJobEventRelay.newProducer(jobs.kafkaBootstrap(), null),
                            jobsConfig.eventsTopic(), jobsConfig.pollInterval(), 100);
                }

                // The catalog sees the store so run-chain resolves stored chain names and
                // the jobs verbs serve the live job rows.
                ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                        chains, jobStore, jobsConfig.maxAttemptsDefault(),
                        inference, serviceProfiles,
                        outboundPolicy, artifacts, runEvidence, recipes);
                return startWithJobsCatalog(options, context, catalog, store, chains,
                        serviceProfiles, jobsDatabase, jobsWorker, jobsRelay);
            }
            // The catalog sees the store so run-chain resolves stored chain names.
            ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                    chains, null, 0, inference, serviceProfiles,
                    outboundPolicy, artifacts, runEvidence, recipes);
            return startWithJobsCatalog(options, context, catalog, store, chains,
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
                                                       ChainRepository chains,
                                                       ServiceProfileRepository serviceProfiles,
                                                       ChainJobDatabase jobsDatabase,
                                                       ChainJobWorker jobsWorker,
                                                       ChainJobEventRelay jobsRelay) {
        ProtoMoltGrpcServer grpc = null;
        JdkProtoRestServer http = null;
        McpHttpHandler mcpHandler = null;
        SchemaRegistryServer registry = null;
        DelegationBridge delegation = null;
        try {
            if (options.demo()) {
                DemoSchemas.seed(context.registry(), store);
            }

            // The live delegation surface: one in-process coordinator per server,
            // adapted to catalog verbs and MCP resources through the bridge.
            delegation = new DelegationBridge(new InProcessDelegationCoordinator());
            DelegationActions.register(catalog, delegation);

            grpc = ProtoMoltGrpcServer.start(options.host(), options.grpcPort(), catalog,
                    options.apiToken());
            if (options.demo() && store != null) {
                // The demo chain composes this server's own verbs, so it needs the bound
                // gRPC port - seeded here rather than with the schemas.
                DemoSchemas.seedChain(store, grpc.port());
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
                        store, catalog);
                registryPort = registry.start();
            }

            ProtoRestMethodRegistry methods = new ProtoRestMethodRegistry();
            ProtoMoltRestMount.register(methods, catalog, options.apiToken() == null
                    ? null
                    : ApiTokenRequirement.apiKeyHeader("api_token"));
            ProtoToolsServerConfig config = ProtoToolsServerConfig.defaults()
                    .withHost(options.host())
                    .withPort(options.httpPort());
            ProtoRestGateway gateway = options.apiToken() == null
                    ? new ProtoRestGateway(methods, context.transcoder())
                    : new ProtoRestGateway(methods, context.transcoder(),
                            ProtoApiTokenValidator.sharedSecret(options.apiToken()));
            String version = ProtoMoltServe.class.getPackage().getImplementationVersion();
            McpServer mcp = new McpServer(catalog,
                    CompositeResources.of(
                            store != null ? new RegistryResources(store) : null,
                            serviceProfiles != null
                                    ? new ServiceProfileResources(serviceProfiles) : null,
                            new DelegationResources(delegation)),
                    "protomolt", version != null ? version : "dev");
            int boundRegistryPort = registryPort;
            int[] selfPort = {-1};
            mcpHandler = new McpHttpHandler(mcp, options.apiToken());
            http = new JdkProtoRestServer(config, gateway,
                    new ProtoOpenApiGenerator("ProtoMolt", version != null ? version : "dev",
                            "/", config.restPathPrefix()))
                    .withContext("/docs", new SwaggerUiHandler("/docs", config.openApiPath()))
                    .withContext("/mcp", mcpHandler);
            if (options.apiToken() == null) {
                http.withContext("/console", new ConsoleHandler())
                        .withContext("/api/protomolt", new ApiProxyHandler("/api/protomolt",
                                () -> boundRegistryPort,
                                "no registry is running; start with --registry-git or --demo"))
                        .withContext("/api/serve", new ApiProxyHandler("/api/serve",
                                () -> selfPort[0], "server is still starting"));
            } else {
                // A browser cannot hold the process's shared secret, so a token-mode
                // console would be a half-open door: some calls 401, registry writes
                // silently open. Disable the whole surface with an explicit answer
                // instead of serving a partially secured interface.
                DisabledSurfaceHandler disabled = new DisabledSurfaceHandler(
                        "the console is disabled when --api-token is set; use the gRPC, "
                                + "REST, or MCP surface with the token, or run without one "
                                + "on a trusted network");
                http.withContext("/console", disabled)
                        .withContext("/api/protomolt", disabled)
                        .withContext("/api/serve", disabled);
            }
            int httpPort = http.start();
            selfPort[0] = httpPort;
            return new ProtoMoltServe(grpc, http, mcpHandler, httpPort, store, registry, registryPort,
                    jobsDatabase, jobsWorker, jobsRelay, delegation);
        } catch (RuntimeException e) {
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

    private static ai.pipestream.proto.chain.ChainRepository chainRepository(
            GitSchemaRegistryStore store) {
        com.fasterxml.jackson.databind.ObjectMapper json =
                new com.fasterxml.jackson.databind.ObjectMapper();
        return name -> {
            try {
                return store.chain(name).map(text -> {
                    try {
                        var node = json.readTree(text);
                        return node instanceof com.fasterxml.jackson.databind.node.ObjectNode chain
                                ? chain : null;
                    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                        return null;
                    }
                });
            } catch (Exception e) {
                return java.util.Optional.empty();
            }
        };
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
        // Worker streams close before the coordinator so no stream dies mid-frame.
        closeQuietly(delegation);
        if (delegation != null) {
            closeQuietly(delegation.coordinator());
        }
        if (registry != null) {
            registry.close();
        }
        mcp.close();
        closeQuietly(registryStore);
        http.close();
        grpc.close();
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        ProtoMoltServe serve = start(options);
        System.out.printf("""
                ProtoMolt serving:
                  gRPC  %1$s:%2$d   ai.pipestream.protomolt.v1.ProtoMoltService (reflection on)
                  REST  http://%1$s:%3$d/grpc-json/ProtoMoltService/{Method}
                  API   http://%1$s:%3$d/openapi.json
                  Docs  http://%1$s:%3$d/docs
                  MCP   http://%1$s:%3$d/mcp   (streamable HTTP)
                """, options.host(), serve.grpcPort(), serve.httpPort());
        if (serve.registryPort() >= 0) {
            System.out.printf("  Reg   http://%s:%d (Confluent protocol, git-backed)%n",
                    options.host(), serve.registryPort());
        }
        if (options.jobs() != null) {
            System.out.printf("  Jobs  %s (verbs: submit-chain, get-job, list-jobs, complete-step%s)%n",
                    options.jobs().jdbcUrl(),
                    options.jobs().kafkaBootstrap() != null
                            ? "; Kafka " + options.jobs().kafkaBootstrap()
                            : ", no broker - verb submission only");
        }
        if (options.apiToken() == null) {
            System.out.printf("  UI    http://%s:%d/console%n", options.host(), serve.httpPort());
        } else {
            System.out.println("  Auth  api_token required on gRPC, REST, MCP"
                    + (serve.registryPort() >= 0 ? ", and the registry" : "")
                    + " (health, OpenAPI, and docs stay open)");
            System.out.println("  UI    console disabled in token mode (a browser cannot "
                    + "hold the shared secret)");
        }
        if (options.demo()) {
            System.out.printf("""
                    Demo schema seeded: subject %s (types demo.shop.v1.Order, Customer, ...)
                      Try: curl -s -H 'content-type: application/json' \\
                             -d '{"schema": {"type": "demo.shop.v1.Order"}}' \\
                             http://%s:%d/grpc-json/ProtoMoltService/RenderJsonSchema
                      Or open http://%s:%d/docs and call ValidateMessage on a demo.shop.v1.Order.
                    """, DemoSchemas.SHOP_SUBJECT,
                    options.host(), serve.httpPort(), options.host(), serve.httpPort());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(serve::close));
        serve.awaitTermination();
    }
}
