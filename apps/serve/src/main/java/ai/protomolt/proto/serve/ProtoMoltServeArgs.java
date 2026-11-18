package ai.protomolt.proto.serve;

import ai.protomolt.proto.authz.OidcCallerResolver;
import ai.protomolt.proto.authz.jdbc.CallerStoreConfig;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static ai.protomolt.proto.serve.ProtoMoltServeOptionBuilders.envBoolean;
import static ai.protomolt.proto.serve.ProtoMoltServeOptionBuilders.envInt;
import static ai.protomolt.proto.serve.ProtoMoltServeOptionBuilders.envLong;
import static ai.protomolt.proto.serve.ProtoMoltServeOptionBuilders.envPath;
import static ai.protomolt.proto.serve.ProtoMoltServeOptionBuilders.parseBoolean;
import static ai.protomolt.proto.serve.ProtoMoltServeOptionBuilders.parseInt;
import static ai.protomolt.proto.serve.ProtoMoltServeOptionBuilders.parseLong;

/**
 * Package-private environment and command-line parser for {@link ProtoMoltServe.Options}.
 */
final class ProtoMoltServeArgs {

    private static final String USAGE =
            "usage: protomolt-serve [--host <addr>] [--grpc-port <n>] "
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
                    + "[--profile-tls-config <path>] (or PROTOMOLT_PROFILE_TLS_CONFIG) "
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
                    + "[--grpc-max-inbound-message-bytes <n>] "
                    + "[--grpc-max-outbound-message-bytes <n>] "
                    + "[--delegation-repo-endpoint <host:port|in-process:name> "
                    + "[--delegation-repo-tls <true|false>] "
                    + "[--delegation-repo-drive <name>] "
                    + "[--delegation-transcript-object <key>] "
                    + "[--delegation-state-key-ref <ref>]] "
                    + "[--mesh-cluster-id <id> --mesh-created-at <ISO-8601> "
                    + "[--mesh-cluster-name <name>] [--mesh-trust-domain <domain>]] "
                    + "(task console login: PROTOMOLT_TASK_CONSOLE_TOKEN; "
                    + "session duration: PROTOMOLT_TASK_CONSOLE_SESSION_SECONDS)";

    private ProtoMoltServeArgs() {
    }

    static ProtoMoltServe.Options parse(String[] args) {
        ParsedState state = readEnvironment();
        applyArguments(state, args);
        return ProtoMoltServeOptionBuilders.buildOptions(state);
    }

    static final class ParsedState {
        String host = "0.0.0.0";
        int grpcPort = 9090;
        int httpPort = 8080;
        Path registryGit;
        int registryPort = 8081;
        String apiToken;
        Path accessPolicy;
        boolean demo;
        Path gatherCache;
        String jobsJdbc;
        String jobsUser;
        String jobsPassword;
        String jobsKafka;
        String jobsRequestTopic;
        int jobsWorkers;
        int jobsTargetConcurrency;
        final List<String> inferenceModels = new ArrayList<>();
        Path serviceWorkspace;
        Path profileTlsConfig;
        Path workflowWorkspace;
        String authoringPolicySha256;
        Path authoringAuthorizationDir;
        Path preparationIntentDir;
        String preparationTemplateProvider;
        String allowedSchemes;
        String allowedHosts;
        String allowedPorts;
        boolean allowPlaintext;
        boolean allowTls;
        long maxDeadlineMs;
        int maxActiveChannels;
        int maxInboundMessageBytes;
        int maxOutboundMessageBytes;
        String delegationRepoEndpoint;
        boolean delegationRepoTls;
        boolean delegationRepoTlsSet;
        String delegationRepoDrive;
        String delegationTranscriptObject;
        String delegationStateKeyRef;
        String configUrl;
        long configRefreshSeconds;
        String oidcIntrospection;
        String oidcClientId;
        String oidcClientSecret;
        String authzKeysJdbc;
        String taskConsoleToken;
        long taskConsoleSessionSeconds;
        String meshClusterId;
        String meshClusterName;
        String meshTrustDomain;
        String meshCreatedAt;
    }

    private static ParsedState readEnvironment() {
        ParsedState s = new ParsedState();
        s.apiToken = System.getenv("PROTOMOLT_API_TOKEN");
        s.accessPolicy = envPath("PROTOMOLT_ACCESS_POLICY", false);
        s.gatherCache = envPath("PROTOMOLT_GATHER_CACHE", false);
        s.jobsJdbc = System.getenv("PROTOMOLT_JOBS_JDBC");
        s.jobsUser = System.getenv("PROTOMOLT_JOBS_USER");
        s.jobsPassword = System.getenv("PROTOMOLT_JOBS_PASSWORD");
        s.jobsKafka = System.getenv("PROTOMOLT_JOBS_KAFKA");
        s.jobsRequestTopic = System.getenv("PROTOMOLT_JOBS_REQUEST_TOPIC");
        s.jobsWorkers = envInt("PROTOMOLT_JOBS_WORKERS", WorkflowRunsConfig.DEFAULT_WORKER_COUNT);
        s.jobsTargetConcurrency = envInt("PROTOMOLT_JOBS_TARGET_CONCURRENCY",
                WorkflowRunsConfig.DEFAULT_MAX_CONCURRENT_PER_TARGET);
        s.serviceWorkspace = envPath("PROTOMOLT_SERVICE_WORKSPACE", false);
        s.profileTlsConfig = envPath(ProtoMoltServe.Options.ENV_PROFILE_TLS_CONFIG, true);
        s.workflowWorkspace = envPath("PROTOMOLT_WORKFLOW_WORKSPACE", false);
        s.authoringPolicySha256 = System.getenv("PROTOMOLT_WORKFLOW_AUTHORING_POLICY_SHA256");
        s.authoringAuthorizationDir = envPath("PROTOMOLT_WORKFLOW_AUTHORING_AUTHORIZATION_DIR", false);
        s.preparationIntentDir = envPath("PROTOMOLT_WORKFLOW_PREPARATION_INTENT_DIR", false);
        s.preparationTemplateProvider = System.getenv(
                "PROTOMOLT_WORKFLOW_PREPARATION_TEMPLATE_PROVIDER");
        readTransportAndSecurityEnv(s);
        return s;
    }

    private static void readTransportAndSecurityEnv(ParsedState s) {
        s.allowedSchemes = System.getenv("PROTOMOLT_GRPC_ALLOWED_SCHEMES");
        s.allowedHosts = System.getenv("PROTOMOLT_GRPC_ALLOWED_HOSTS");
        s.allowedPorts = System.getenv("PROTOMOLT_GRPC_ALLOWED_PORTS");
        s.allowPlaintext = envBoolean("PROTOMOLT_GRPC_ALLOW_PLAINTEXT", true);
        s.allowTls = envBoolean("PROTOMOLT_GRPC_ALLOW_TLS", true);
        s.maxDeadlineMs = envLong("PROTOMOLT_GRPC_MAX_DEADLINE_MS", 60_000L);
        s.maxActiveChannels = envInt("PROTOMOLT_GRPC_MAX_ACTIVE_CHANNELS", 64);
        s.maxInboundMessageBytes = envInt("PROTOMOLT_GRPC_MAX_INBOUND_MESSAGE_BYTES",
                OutboundChannelPolicy.DEFAULT_MAX_MESSAGE_BYTES);
        s.maxOutboundMessageBytes = envInt("PROTOMOLT_GRPC_MAX_OUTBOUND_MESSAGE_BYTES",
                OutboundChannelPolicy.DEFAULT_MAX_MESSAGE_BYTES);
        addSemicolonModels(System.getenv("PROTOMOLT_INFERENCE_MODELS"), s.inferenceModels);
        s.delegationRepoEndpoint = System.getenv("PROTOMOLT_DELEGATION_REPO_ENDPOINT");
        s.delegationRepoTls = envBoolean("PROTOMOLT_DELEGATION_REPO_TLS", false);
        s.delegationRepoTlsSet = System.getenv("PROTOMOLT_DELEGATION_REPO_TLS") != null;
        s.delegationRepoDrive = System.getenv("PROTOMOLT_DELEGATION_REPO_DRIVE");
        s.delegationTranscriptObject = System.getenv("PROTOMOLT_DELEGATION_TRANSCRIPT_OBJECT");
        s.delegationStateKeyRef = System.getenv("PROTOMOLT_DELEGATION_STATE_KEY_REF");
        s.configUrl = System.getenv("PROTOMOLT_CONFIG_URL");
        s.configRefreshSeconds = envLong("PROTOMOLT_CONFIG_REFRESH_SECONDS", 0L);
        s.oidcIntrospection = System.getenv(OidcCallerResolver.ENV_INTROSPECTION_URL);
        s.oidcClientId = System.getenv(OidcCallerResolver.ENV_CLIENT_ID);
        s.oidcClientSecret = System.getenv(OidcCallerResolver.ENV_CLIENT_SECRET);
        s.authzKeysJdbc = System.getenv(CallerStoreConfig.ENV_JDBC_URL);
        s.taskConsoleToken = System.getenv("PROTOMOLT_TASK_CONSOLE_TOKEN");
        s.taskConsoleSessionSeconds = envLong("PROTOMOLT_TASK_CONSOLE_SESSION_SECONDS", 43_200L);
        s.meshClusterId = System.getenv("PROTOMOLT_MESH_CLUSTER_ID");
        s.meshClusterName = System.getenv("PROTOMOLT_MESH_CLUSTER_NAME");
        s.meshTrustDomain = System.getenv("PROTOMOLT_MESH_TRUST_DOMAIN");
        s.meshCreatedAt = System.getenv("PROTOMOLT_MESH_CREATED_AT");
    }

    private static void addSemicolonModels(String env, List<String> out) {
        if (env == null || env.isBlank()) {
            return;
        }
        int start = 0;
        while (start <= env.length()) {
            int semi = env.indexOf(';', start);
            String spec = semi < 0 ? env.substring(start) : env.substring(start, semi);
            if (!spec.isBlank()) {
                out.add(spec.trim());
            }
            if (semi < 0) {
                break;
            }
            start = semi + 1;
        }
    }

    private static void applyArguments(ParsedState s, String[] args) {
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if ("--demo".equals(flag)) {
                s.demo = true;
            } else if ("--help".equals(flag) || "-h".equals(flag)) {
                System.err.println(USAGE);
                System.exit(0);
            } else if (applyServerOrJobsFlag(s, flag, args, i)
                    || applyWorkspaceOrPolicyFlag(s, flag, args, i)) {
                i++;
            } else {
                System.err.println("unknown argument: " + flag);
                System.exit(2);
            }
        }
    }

    private static boolean applyServerOrJobsFlag(ParsedState s, String flag, String[] args, int i) {
        switch (flag) {
            case "--host" -> s.host = requireValue(args, i + 1);
            case "--grpc-port" -> s.grpcPort = Integer.parseInt(requireValue(args, i + 1));
            case "--http-port" -> s.httpPort = Integer.parseInt(requireValue(args, i + 1));
            case "--registry-git" -> s.registryGit = Path.of(requireValue(args, i + 1));
            case "--registry-port" -> s.registryPort = Integer.parseInt(requireValue(args, i + 1));
            case "--api-token" -> s.apiToken = requireValue(args, i + 1);
            case "--access-policy" -> s.accessPolicy = Path.of(requireValue(args, i + 1));
            case "--gather-cache" -> s.gatherCache = Path.of(requireValue(args, i + 1));
            case "--jobs-jdbc" -> s.jobsJdbc = requireValue(args, i + 1);
            case "--jobs-user" -> s.jobsUser = requireValue(args, i + 1);
            case "--jobs-password" -> s.jobsPassword = requireValue(args, i + 1);
            case "--jobs-kafka" -> s.jobsKafka = requireValue(args, i + 1);
            case "--jobs-request-topic" -> s.jobsRequestTopic = requireValue(args, i + 1);
            case "--jobs-workers" -> s.jobsWorkers = Integer.parseInt(requireValue(args, i + 1));
            case "--jobs-target-concurrency" ->
                    s.jobsTargetConcurrency = Integer.parseInt(requireValue(args, i + 1));
            case "--inference-model" -> s.inferenceModels.add(requireValue(args, i + 1));
            case "--delegation-repo-endpoint" ->
                    s.delegationRepoEndpoint = requireValue(args, i + 1);
            case "--delegation-repo-tls" -> {
                s.delegationRepoTls = parseBoolean(
                        requireValue(args, i + 1), "--delegation-repo-tls");
                s.delegationRepoTlsSet = true;
            }
            case "--delegation-repo-drive" -> s.delegationRepoDrive = requireValue(args, i + 1);
            case "--delegation-transcript-object" ->
                    s.delegationTranscriptObject = requireValue(args, i + 1);
            case "--delegation-state-key-ref" ->
                    s.delegationStateKeyRef = requireValue(args, i + 1);
            default -> {
                return false;
            }
        }
        return true;
    }

    private static boolean applyWorkspaceOrPolicyFlag(ParsedState s, String flag,
                                                      String[] args, int i) {
        switch (flag) {
            case "--mesh-cluster-id" -> s.meshClusterId = requireValue(args, i + 1);
            case "--mesh-cluster-name" -> s.meshClusterName = requireValue(args, i + 1);
            case "--mesh-trust-domain" -> s.meshTrustDomain = requireValue(args, i + 1);
            case "--mesh-created-at" -> s.meshCreatedAt = requireValue(args, i + 1);
            case "--service-workspace" -> s.serviceWorkspace = Path.of(requireValue(args, i + 1));
            case ProtoMoltServe.Options.FLAG_PROFILE_TLS_CONFIG ->
                    s.profileTlsConfig = Path.of(requireValue(args, i + 1).trim());
            case "--workflow-workspace" -> s.workflowWorkspace = Path.of(requireValue(args, i + 1));
            case "--workflow-authoring-policy-sha256" ->
                    s.authoringPolicySha256 = requireValue(args, i + 1);
            case "--workflow-authoring-authorization-dir" ->
                    s.authoringAuthorizationDir = Path.of(requireValue(args, i + 1));
            case "--workflow-preparation-intent-dir" ->
                    s.preparationIntentDir = Path.of(requireValue(args, i + 1));
            case "--workflow-preparation-template-provider" ->
                    s.preparationTemplateProvider = requireValue(args, i + 1);
            case "--grpc-allowed-schemes" -> s.allowedSchemes = requireValue(args, i + 1);
            case "--grpc-allowed-hosts" -> s.allowedHosts = requireValue(args, i + 1);
            case "--grpc-allowed-ports" -> s.allowedPorts = requireValue(args, i + 1);
            case "--grpc-allow-plaintext" ->
                    s.allowPlaintext = parseBoolean(
                            requireValue(args, i + 1), "--grpc-allow-plaintext");
            case "--grpc-allow-tls" ->
                    s.allowTls = parseBoolean(requireValue(args, i + 1), "--grpc-allow-tls");
            case "--grpc-max-deadline-ms" ->
                    s.maxDeadlineMs = parseLong(
                            requireValue(args, i + 1), "--grpc-max-deadline-ms");
            case "--grpc-max-active-channels" ->
                    s.maxActiveChannels = parseInt(
                            requireValue(args, i + 1), "--grpc-max-active-channels");
            case "--grpc-max-inbound-message-bytes" ->
                    s.maxInboundMessageBytes = parseInt(
                            requireValue(args, i + 1), "--grpc-max-inbound-message-bytes");
            case "--grpc-max-outbound-message-bytes" ->
                    s.maxOutboundMessageBytes = parseInt(
                            requireValue(args, i + 1), "--grpc-max-outbound-message-bytes");
            default -> {
                return false;
            }
        }
        return true;
    }

    private static String requireValue(String[] args, int i) {
        if (i >= args.length) {
            System.err.println(args[i - 1] + " requires a value");
            System.exit(2);
        }
        return args[i];
    }
}
