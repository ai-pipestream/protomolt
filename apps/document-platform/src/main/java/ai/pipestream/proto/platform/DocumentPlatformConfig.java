package ai.pipestream.proto.platform;

import ai.pipestream.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.pipestream.proto.repo.service.RepoServiceConfig;
import java.nio.file.Path;

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
 * @param intakeGrpcPort intake door gRPC port; {@code 0} picks free
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
        Path searchIndexDir) {

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

    /** Env var for the search door gRPC port. */
    public static final String ENV_SEARCH_GRPC_PORT = "DOCUMENT_PLATFORM_SEARCH_GRPC_PORT";

    /** Env var for the search index directory. */
    public static final String ENV_SEARCH_INDEX_DIR = "DOCUMENT_PLATFORM_SEARCH_INDEX_DIR";

    /** The default search door gRPC port. */
    public static final int DEFAULT_SEARCH_GRPC_PORT = 9094;

    /** The default search index directory. */
    public static final String DEFAULT_SEARCH_INDEX_DIR = "/data/search-index";

    public DocumentPlatformConfig {
        if (repo == null) {
            throw new IllegalArgumentException("repo config is required");
        }
        if (jobs == null) {
            throw new IllegalArgumentException("jobs config is required");
        }
        if (registryGit == null) {
            throw new IllegalArgumentException(
                    "registryGit is required: the platform runs its registry by default");
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
        if (searchIndexDir == null) {
            throw new IllegalArgumentException(
                    "searchIndexDir is required: the platform serves search by default");
        }
    }

    /**
     * Reads the platform configuration from the environment. The repository
     * half is {@link RepoServiceConfig#fromEnvironment()}; the jobs JDBC URL
     * is required by name.
     */
    public static DocumentPlatformConfig fromEnvironment() {
        String jobsUrl = System.getenv(ENV_JOBS_JDBC_URL);
        if (jobsUrl == null || jobsUrl.isBlank()) {
            throw new IllegalArgumentException(ENV_JOBS_JDBC_URL + " is required");
        }
        return new DocumentPlatformConfig(
                RepoServiceConfig.fromEnvironment(),
                new WorkflowRunStoreConfig(
                        jobsUrl,
                        env(ENV_JOBS_USERNAME, ""),
                        env(ENV_JOBS_PASSWORD, ""),
                        WorkflowRunStoreConfig.DEFAULT_POOL_SIZE,
                        WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION),
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
                Path.of(env(ENV_SEARCH_INDEX_DIR, DEFAULT_SEARCH_INDEX_DIR)));
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
