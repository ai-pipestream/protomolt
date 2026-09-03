package ai.protomolt.proto.platform;

import ai.protomolt.proto.acquire.jdbc.JdbcPullModule;
import ai.protomolt.proto.acquire.s3.S3PullModule;
import ai.protomolt.proto.composer.Channels;
import ai.protomolt.proto.intake.service.IntakeModule;
import ai.protomolt.proto.jobs.service.JobsModule;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.metric.lucene.MetricServiceModule;
import ai.protomolt.proto.parse.playground.PlaygroundModule;
import ai.protomolt.proto.parse.service.ParseModule;
import ai.protomolt.proto.parse.text.TextParserModule;
import ai.protomolt.proto.registry.service.RegistryModule;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServiceModule;
import ai.protomolt.proto.search.console.SearchConsoleModule;
import ai.protomolt.proto.search.service.SearchServiceModule;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Configuration for the one-container document platform.
 *
 * <p>The repository half reuses {@link RepoServiceConfig} and its
 * {@code DOCUMENT_PLATFORM_*} environment family verbatim; the jobs store
 * gets its own database ({@code DOCUMENT_PLATFORM_JOBS_*}) because the
 * platform's stores keep separate Flyway histories. The registry is ON by
 * default: a platform without its schema registry is not the product.
 *
 * @param repo the repository service configuration (ledger, S3, ports)
 * @param jobs the workflow-runs store configuration
 * @param registryGit path of the git-backed registry repository
 * @param registryPort HTTP port of the registry server; {@code 0} picks free
 * @param intakeGrpcPort intake service gRPC port; {@code 0} picks free
 * @param parseGrpcPort parsing coordinator gRPC port; {@code 0} picks free
 * @param playgroundPort parser playground HTTP port; {@code 0} picks free
 * @param rulesJson routing rules as a proto3-JSON array, or null for the
 *        default rule set (text and markdown to the embedded reference
 *        parser)
 * @param profilesDir service-profile store directory for parser discovery,
 *        or null to use only the embedded reference parser
 * @param profileEndpoint endpoint name each parser profile must carry;
 *        required exactly when {@code profilesDir} is set
 * @param parseDeadlineSeconds per-parse deadline for coordinator fan-out
 * @param workerCount jobs worker claim loops
 * @param searchGrpcPort search service gRPC port; {@code 0} picks free
 * @param searchIndexDir directory of the search service's Lucene index
 * @param searchConsolePort search console HTTP port; {@code 0} picks free
 * @param metricsGrpcPort metric service gRPC port; {@code 0} picks free
 * @param roles the roles this node mounts ({@code PROTOMOLT_ROLES}); the
 *        default is the full one-container preset, and configuration is
 *        only required for what is actually selected (a repo-only node
 *        needs no jobs database)
 * @param environment the process environment the node resolves remote
 *        role targets ({@code PROTOMOLT_<ROLE>_TARGET}) and opt-in role
 *        configuration (the acquire connectors) from
 */
public record DocumentPlatformConfig(
        RepoServiceConfig repo,
        WorkflowRunStoreConfig jobs,
        Path registryGit,
        int registryPort,
        int intakeGrpcPort,
        int parseGrpcPort,
        int playgroundPort,
        String rulesJson,
        String profilesDir,
        String profileEndpoint,
        long parseDeadlineSeconds,
        int workerCount,
        int searchGrpcPort,
        Path searchIndexDir,
        int searchConsolePort,
        int metricsGrpcPort,
        List<String> roles,
        Map<String, String> environment) {

    /** The full one-container preset, in canonical mount order. */
    public static final List<String> DEFAULT_ROLES = List.of(
            RepoServiceModule.ROLE, TextParserModule.ROLE, RegistryModule.ROLE,
            ParseModule.ROLE, JobsModule.ROLE, IntakeModule.ROLE,
            PlaygroundModule.ROLE, SearchServiceModule.ROLE,
            MetricServiceModule.ROLE, SearchConsoleModule.ROLE);

    /**
     * Every role this binary can mount, each named by the module that
     * mounts it rather than by a string repeated here. Modules take their
     * configuration through their constructors, so the platform wires them
     * explicitly instead of through {@link java.util.ServiceLoader}, and
     * this set is the only place the roster is written down. Naming each
     * module's own constant is what keeps the two from drifting: a role
     * renamed on its module stops compiling here instead of failing an
     * operator's boot with a spelling nothing mounts.
     */
    public static final Set<String> KNOWN_ROLES = Set.of(
            RepoServiceModule.ROLE, TextParserModule.ROLE, RegistryModule.ROLE,
            ParseModule.ROLE, JobsModule.ROLE, IntakeModule.ROLE,
            PlaygroundModule.ROLE, SearchServiceModule.ROLE,
            MetricServiceModule.ROLE, SearchConsoleModule.ROLE,
            S3PullModule.ROLE, JdbcPullModule.ROLE);

    /**
     * Role spellings {@link #ENV_ROLES} accepts as aliases, mapped to the
     * canonical id: {@code metrics} names the {@code metric} role and
     * {@code parser-text} names {@code parse-text}. The same table the
     * composer reads an alias's {@code PROTOMOLT_<ROLE>_TARGET} through.
     */
    public static final Map<String, String> ALIASES = Channels.ROLE_ALIASES;

    /** Env var selecting the roles this node mounts (comma-separated). */
    public static final String ENV_ROLES = "PROTOMOLT_ROLES";

    /**
     * The operator api token. Set, every network surface a serving role
     * exposes (registry HTTP, search and metric gRPC) demands it, and the
     * node's own remote-role channels present it. Unset, the node stays an
     * open, trusted-network surface.
     */
    public static final String ENV_API_TOKEN = "PROTOMOLT_API_TOKEN";

    /**
     * Path of an access-policy file (JSON or binary {@code AccessPolicy})
     * read at boot; the config-lane subject {@code access-policy} re-scopes
     * a running node without a restart. Requires {@link #ENV_API_TOKEN}.
     */
    public static final String ENV_ACCESS_POLICY = "PROTOMOLT_ACCESS_POLICY";

    /** Env var naming a remote actions route for a console mounted without a registry. */
    public static final String ENV_ACTIONS_URL = "DOCUMENT_PLATFORM_ACTIONS_URL";

    /** Env var for the jobs database JDBC URL (required). */
    public static final String ENV_JOBS_JDBC_URL = "DOCUMENT_PLATFORM_JOBS_JDBC_URL";

    /** Env var for the jobs database username. */
    public static final String ENV_JOBS_USERNAME = "DOCUMENT_PLATFORM_JOBS_USERNAME";

    /** Env var for the jobs database password. */
    public static final String ENV_JOBS_PASSWORD = "DOCUMENT_PLATFORM_JOBS_PASSWORD";

    /** Env var for the registry git directory (default {@code /data/registry.git}). */
    public static final String ENV_REGISTRY_GIT = "DOCUMENT_PLATFORM_REGISTRY_GIT";

    /** Env var for the registry HTTP port (default 8081). */
    public static final String ENV_REGISTRY_PORT = "DOCUMENT_PLATFORM_REGISTRY_PORT";

    /** Env var for the intake gRPC port (default 9092). */
    public static final String ENV_INTAKE_GRPC_PORT = "DOCUMENT_PLATFORM_INTAKE_GRPC_PORT";

    /** Env var for the coordinator gRPC port (default 9093). */
    public static final String ENV_PARSE_GRPC_PORT = "DOCUMENT_PLATFORM_PARSE_GRPC_PORT";

    /** Env var for the playground HTTP port (default 8095). */
    public static final String ENV_PLAYGROUND_PORT = "DOCUMENT_PLATFORM_PLAYGROUND_PORT";

    /** Env var carrying inline routing rules (proto3-JSON array), optional. */
    public static final String ENV_PARSE_RULES_JSON = "DOCUMENT_PLATFORM_PARSE_RULES_JSON";

    /** Env var naming the service-profile store for parser discovery, optional. */
    public static final String ENV_PARSE_PROFILES = "DOCUMENT_PLATFORM_PARSE_PROFILES";

    /** Env var naming the endpoint each parser profile must carry. */
    public static final String ENV_PARSE_PROFILE_ENDPOINT =
            "DOCUMENT_PLATFORM_PARSE_PROFILE_ENDPOINT";

    /** Env var for the jobs worker loop count (default 2). */
    public static final String ENV_WORKER_COUNT = "DOCUMENT_PLATFORM_WORKER_COUNT";

    /** The default registry location inside the container. */
    public static final String DEFAULT_REGISTRY_GIT = "/data/registry.git";

    /** The default registry HTTP port. */
    public static final int DEFAULT_REGISTRY_PORT = 8081;

    /** The default intake gRPC port. */
    public static final int DEFAULT_INTAKE_GRPC_PORT = 9092;

    /** The default coordinator gRPC port. */
    public static final int DEFAULT_PARSE_GRPC_PORT = 9093;

    /** The default playground HTTP port. */
    public static final int DEFAULT_PLAYGROUND_PORT = 8095;

    /** Env var for the search service gRPC port. */
    public static final String ENV_SEARCH_GRPC_PORT = "DOCUMENT_PLATFORM_SEARCH_GRPC_PORT";

    /** Env var for the search index directory. */
    public static final String ENV_SEARCH_INDEX_DIR = "DOCUMENT_PLATFORM_SEARCH_INDEX_DIR";

    /** The default search service gRPC port. */
    public static final int DEFAULT_SEARCH_GRPC_PORT = 9094;

    /** The default search index directory. */
    public static final String DEFAULT_SEARCH_INDEX_DIR = "/data/search-index";

    /** Env var for the search console HTTP port. */
    public static final String ENV_SEARCH_CONSOLE_PORT = "DOCUMENT_PLATFORM_SEARCH_CONSOLE_PORT";

    /** The default search console HTTP port. */
    public static final int DEFAULT_SEARCH_CONSOLE_PORT = 8096;

    /** Env var for the metric service gRPC port. */
    public static final String ENV_METRICS_GRPC_PORT = "DOCUMENT_PLATFORM_METRICS_GRPC_PORT";

    /**
     * Env var making the search role a reader ({@code true}/{@code false},
     * absent means writable): no repo channel, no indexing surface, and
     * restore-only snapshots. The remote metrics node is
     * {@code PROTOMOLT_ROLES=search,metric} with this set and the
     * snapshot family pointing at the writer's bucket.
     */
    public static final String ENV_SEARCH_READ_ONLY = "DOCUMENT_PLATFORM_SEARCH_READ_ONLY";

    /**
     * Env var for a reader's snapshot refresh interval in seconds: how
     * often a read-only search node pulls newer snapshots into its live
     * index. Absent means restart-only; a value demands
     * {@link #ENV_SEARCH_READ_ONLY} and the snapshot family, because
     * refresh is the reader's pull and the writer publishes on its commit
     * cadence.
     */
    public static final String ENV_SEARCH_REFRESH_SECONDS =
            "DOCUMENT_PLATFORM_SEARCH_REFRESH_SECONDS";

    /**
     * Env var naming the {@code meta.v1} sensitivity classes this node may send to an
     * embedding provider, comma-separated. Absent means unclassified content only, so a
     * subject whose chunk lane reads a classified field fails the mount by name rather
     * than quietly vectorizing it. The single value {@code *} permits every class, for a
     * deployment whose model runs inside its own trust boundary.
     */
    public static final String ENV_SEARCH_VECTORIZE_SENSITIVITY =
            "DOCUMENT_PLATFORM_SEARCH_VECTORIZE_SENSITIVITY";

    /**
     * Env var for the default local metrics lake: where rebuilt rollups
     * land when no {@code DOCUMENT_PLATFORM_METRICS_ICEBERG_*} family is
     * set. The directory (a sqlite catalog plus Parquet data, created
     * lazily on the first rebuild) defaults to
     * {@value #DEFAULT_METRICS_LAKE_DIR}; with the catalog family set,
     * rollups land in that lake instead and this variable is unused.
     */
    public static final String ENV_METRICS_LAKE_DIR = "DOCUMENT_PLATFORM_METRICS_LAKE_DIR";

    /** The default local metrics lake directory. */
    public static final String DEFAULT_METRICS_LAKE_DIR = "/data/metrics-lake";

    /**
     * Env var for the distributed-config refresh interval in seconds: the
     * switch for the config lane. Set, the node pulls its config subjects
     * from the registry (the co-mounted one, or
     * {@link #ENV_CONFIG_URL}) on that interval, verify-then-swap; absent
     * means environment-only configuration, exactly as before.
     */
    public static final String ENV_CONFIG_REFRESH_SECONDS =
            "DOCUMENT_PLATFORM_CONFIG_REFRESH_SECONDS";

    /**
     * Env var naming a remote registry's native route prefix for config
     * documents (e.g. {@code http://registry:8081/protomolt}), for a node
     * without a co-mounted registry. Ignored when a registry is mounted.
     */
    public static final String ENV_CONFIG_URL = "DOCUMENT_PLATFORM_CONFIG_URL";

    /**
     * Env var naming the Kafka bootstrap servers of the config lane's
     * signal plug: set, the node reads its config subjects off the
     * compacted config topic through the house serde instead of the
     * registry source. Mutually exclusive with {@link #ENV_CONFIG_URL};
     * naming both is a contradiction, refused by name.
     */
    public static final String ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS =
            "DOCUMENT_PLATFORM_CONFIG_KAFKA_BOOTSTRAP_SERVERS";

    /**
     * Env var naming the compacted config topic; only read with
     * {@link #ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS} set. Absent means
     * {@link #DEFAULT_CONFIG_KAFKA_TOPIC}.
     */
    public static final String ENV_CONFIG_KAFKA_TOPIC =
            "DOCUMENT_PLATFORM_CONFIG_KAFKA_TOPIC";

    /**
     * Env var naming the Confluent-compatible schema registry URL the
     * config serde validates against (validate-on-read is forced on).
     * Required with {@link #ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS}: a config
     * consumer that cannot resolve schemas cannot verify, and the lane is
     * verify-then-swap or nothing.
     */
    public static final String ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL =
            "DOCUMENT_PLATFORM_CONFIG_KAFKA_SCHEMA_REGISTRY_URL";

    /** The default compacted config topic. */
    public static final String DEFAULT_CONFIG_KAFKA_TOPIC = "protomolt-config";

    /**
     * Env var naming the taxonomies this node follows off the config lane,
     * as a comma-separated list of names (subjects {@code taxonomy:<name>}).
     * Set, the search service turns on its document gate: fetched documents
     * validate against their declared rules over the live mounts before
     * anything indexes, fail-closed while a declared taxonomy is unmounted.
     * Requires the config lane ({@link #ENV_CONFIG_REFRESH_SECONDS}); absent
     * keeps the service's historical behavior exactly.
     */
    public static final String ENV_TAXONOMIES = "DOCUMENT_PLATFORM_TAXONOMIES";

    /**
     * The screening mount name. Set, the search service screens fetched
     * documents over the mount published at config subject
     * {@code screening:<name>}: fields declaring the mount's sensitivity
     * class run through the mounted model, detected spans act by the
     * mount's policy, and the response carries the model version and
     * threshold as evidence. Fail-closed while no mount is live. Requires
     * the config lane ({@link #ENV_CONFIG_REFRESH_SECONDS}); absent keeps
     * the service's behavior exactly.
     */
    public static final String ENV_SCREENING = "DOCUMENT_PLATFORM_SCREENING";

    /**
     * Set to {@code true}, the search service's gate validates postal codes
     * against the pack published at config subject {@code postal-codes}: a
     * mounted region's non-empty {@code google.type.PostalAddress.postal_code}
     * must match one of the region's masks, an unmounted region stays
     * unchecked — the pack's per-region opt-in stance. Requires the config
     * lane ({@link #ENV_CONFIG_REFRESH_SECONDS}); absent keeps the service's
     * behavior exactly.
     */
    public static final String ENV_POSTAL_CODES = "DOCUMENT_PLATFORM_POSTAL_CODES";

    /** The default metric service gRPC port. */
    public static final int DEFAULT_METRICS_GRPC_PORT = 9095;

    public DocumentPlatformConfig {
        roles = roles == null || roles.isEmpty() ? DEFAULT_ROLES : List.copyOf(roles);
        environment = environment == null ? Map.of() : Map.copyOf(environment);
        for (String role : roles) {
            if (!KNOWN_ROLES.contains(role)) {
                throw new IllegalArgumentException("unknown role '" + role
                        + "'; known roles: " + String.join(", ",
                                KNOWN_ROLES.stream().sorted().toList()));
            }
        }
        if (repo == null && roles.contains("repo")) {
            throw new IllegalArgumentException("repo config is required on a repo node");
        }
        if (jobs == null && roles.contains("jobs")) {
            throw new IllegalArgumentException("jobs config is required on a jobs node");
        }
        if (registryGit == null && roles.contains("registry")) {
            throw new IllegalArgumentException(
                    "registryGit is required: this node runs the registry");
        }
        boolean profiles = profilesDir != null && !profilesDir.isBlank();
        boolean endpoint = profileEndpoint != null && !profileEndpoint.isBlank();
        if (profiles != endpoint) {
            throw new IllegalArgumentException(
                    "profilesDir and profileEndpoint come together: set both or neither");
        }
        if (parseDeadlineSeconds <= 0) {
            throw new IllegalArgumentException("parseDeadlineSeconds must be positive");
        }
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive");
        }
        if (searchIndexDir == null && roles.contains("search")) {
            throw new IllegalArgumentException(
                    "searchIndexDir is required: this node serves search");
        }
        if (roles.contains("metric") && !roles.contains("search")) {
            throw new IllegalArgumentException(
                    "the metric role reads the search index in process: add 'search'"
                            + " to this node's roles");
        }
    }

    /** Whether this node mounts the given role. */
    public boolean mounts(String role) {
        return roles.contains(role);
    }

    /**
     * Reads the platform configuration from the environment. The repository
     * half is {@link RepoServiceConfig#fromEnvironment()}; the jobs JDBC URL
     * is required by name.
     */
    public static DocumentPlatformConfig fromEnvironment() {
        List<String> roles = rolesFromEnvironment(System.getenv(ENV_ROLES));
        WorkflowRunStoreConfig jobs = null;
        if (roles.contains("jobs")) {
            String jobsUrl = System.getenv(ENV_JOBS_JDBC_URL);
            if (jobsUrl == null || jobsUrl.isBlank()) {
                throw new IllegalArgumentException(ENV_JOBS_JDBC_URL + " is required");
            }
            jobs = new WorkflowRunStoreConfig(
                    jobsUrl,
                    env(ENV_JOBS_USERNAME, ""),
                    env(ENV_JOBS_PASSWORD, ""),
                    WorkflowRunStoreConfig.DEFAULT_POOL_SIZE,
                    WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION);
        }
        return new DocumentPlatformConfig(
                roles.contains("repo") ? RepoServiceConfig.fromEnvironment() : null,
                jobs,
                Path.of(env(ENV_REGISTRY_GIT, DEFAULT_REGISTRY_GIT)),
                intEnv(ENV_REGISTRY_PORT, DEFAULT_REGISTRY_PORT),
                intEnv(ENV_INTAKE_GRPC_PORT, DEFAULT_INTAKE_GRPC_PORT),
                intEnv(ENV_PARSE_GRPC_PORT, DEFAULT_PARSE_GRPC_PORT),
                intEnv(ENV_PLAYGROUND_PORT, DEFAULT_PLAYGROUND_PORT),
                blankToNull(System.getenv(ENV_PARSE_RULES_JSON)),
                blankToNull(System.getenv(ENV_PARSE_PROFILES)),
                blankToNull(System.getenv(ENV_PARSE_PROFILE_ENDPOINT)),
                60L,
                intEnv(ENV_WORKER_COUNT, 2),
                intEnv(ENV_SEARCH_GRPC_PORT, DEFAULT_SEARCH_GRPC_PORT),
                Path.of(env(ENV_SEARCH_INDEX_DIR, DEFAULT_SEARCH_INDEX_DIR)),
                intEnv(ENV_SEARCH_CONSOLE_PORT, DEFAULT_SEARCH_CONSOLE_PORT),
                intEnv(ENV_METRICS_GRPC_PORT, DEFAULT_METRICS_GRPC_PORT),
                roles,
                System.getenv());
    }

    /**
     * Parses {@code PROTOMOLT_ROLES}; absent or blank means the full
     * preset. {@linkplain #ALIASES Aliases} normalize to their canonical
     * id, so a list naming one role twice — under either spelling — is a
     * contradiction refused by both spellings.
     */
    static List<String> rolesFromEnvironment(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_ROLES;
        }
        List<String> roles = new ArrayList<>();
        Map<String, String> spellings = new LinkedHashMap<>();
        for (String entry : value.split(",")) {
            if (entry.isBlank()) {
                continue;
            }
            String spelling = entry.trim().toLowerCase(Locale.ROOT);
            String role = ALIASES.getOrDefault(spelling, spelling);
            String first = spellings.putIfAbsent(role, spelling);
            if (first != null) {
                throw new IllegalArgumentException(ENV_ROLES + " names the "
                        + role + " role twice" + (first.equals(spelling)
                                ? "" : ", as '" + first + "' and '" + spelling + "'"));
            }
            roles.add(role);
        }
        return List.copyOf(roles);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int intEnv(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
