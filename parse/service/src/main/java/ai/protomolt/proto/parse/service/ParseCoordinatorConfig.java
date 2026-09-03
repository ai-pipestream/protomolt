package ai.protomolt.proto.parse.service;

/**
 * Configuration for a standalone or embedded parsing coordinator.
 *
 * @param grpcPort port the Netty server binds; {@code 0} picks a free port
 * @param repoTarget the repo-service {@code DocumentService} endpoint — a
 *        {@code host:port} authority, or {@code inprocess:<name>} for an
 *        in-process repo (tests and all-in-one deployments)
 * @param drive fallback drive for the parsed-part save when the loaded
 *        document's read response does not name one
 * @param parseDeadlineSeconds per-parser deadline of one {@code Parse}
 *        stream, in seconds
 */
public record ParseCoordinatorConfig(
        int grpcPort, String repoTarget, String drive, long parseDeadlineSeconds) {

    /** Prefix selecting an in-process channel: {@code inprocess:<name>}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    /** The default coordinator gRPC port. */
    public static final int DEFAULT_GRPC_PORT = 9093;

    /** The default save-fallback drive. */
    public static final String DEFAULT_DRIVE = "intake";

    /** The default per-parser deadline: 5 minutes. */
    public static final long DEFAULT_PARSE_DEADLINE_SECONDS = 300;

    /** Env var naming the repo-service target ({@code host:port}). */
    public static final String ENV_REPO_TARGET = "DOCUMENT_PLATFORM_PARSE_REPO_TARGET";

    /** Env var for the coordinator gRPC port. */
    public static final String ENV_GRPC_PORT = "DOCUMENT_PLATFORM_PARSE_GRPC_PORT";

    /** Env var for the save-fallback drive. */
    public static final String ENV_DRIVE = "DOCUMENT_PLATFORM_PARSE_DRIVE";

    /** Env var for the per-parser deadline in seconds. */
    public static final String ENV_PARSE_DEADLINE_SECONDS =
            "DOCUMENT_PLATFORM_PARSE_DEADLINE_SECONDS";

    /** Env var carrying the routing rules as an inline JSON array. */
    public static final String ENV_RULES_JSON = "DOCUMENT_PLATFORM_PARSE_RULES_JSON";

    /** Env var naming a file holding the routing-rules JSON array. */
    public static final String ENV_RULES_FILE = "DOCUMENT_PLATFORM_PARSE_RULES_FILE";

    /** Env var listing the parser fleet: {@code name=target,name=target,...}. */
    public static final String ENV_PARSERS = "DOCUMENT_PLATFORM_PARSE_PARSERS";

    /** Env var naming the service-profile store directory for parser discovery. */
    public static final String ENV_PROFILES = "DOCUMENT_PLATFORM_PARSE_PROFILES";

    /** Env var naming the endpoint each parser profile must carry. */
    public static final String ENV_PROFILE_ENDPOINT = "DOCUMENT_PLATFORM_PARSE_PROFILE_ENDPOINT";

    public ParseCoordinatorConfig {
        if (repoTarget == null || repoTarget.isBlank()) {
            throw new IllegalArgumentException("repoTarget must not be blank");
        }
        if (drive == null || drive.isBlank()) {
            throw new IllegalArgumentException("drive must not be blank");
        }
        if (parseDeadlineSeconds <= 0) {
            throw new IllegalArgumentException("parseDeadlineSeconds must be positive");
        }
        if (grpcPort < 0) {
            throw new IllegalArgumentException("grpcPort must not be negative");
        }
    }

    /**
     * Reads the configuration from {@code DOCUMENT_PLATFORM_PARSE_*}
     * environment variables; the repo target is required, everything else
     * falls back to defaults. The rules and parser fleet have their own env
     * vars, read by {@code ParseCoordinatorMain} — they build the
     * {@link RoutingRules} and {@link ParserRegistry} arguments, not this
     * record.
     *
     * @return the environment-derived configuration
     */
    public static ParseCoordinatorConfig fromEnvironment() {
        String repoTarget = System.getenv(ENV_REPO_TARGET);
        if (repoTarget == null || repoTarget.isBlank()) {
            throw new IllegalArgumentException(ENV_REPO_TARGET + " is required");
        }
        String port = System.getenv(ENV_GRPC_PORT);
        String drive = System.getenv(ENV_DRIVE);
        String deadline = System.getenv(ENV_PARSE_DEADLINE_SECONDS);
        return new ParseCoordinatorConfig(
                port == null || port.isBlank() ? DEFAULT_GRPC_PORT : Integer.parseInt(port),
                repoTarget,
                drive == null || drive.isBlank() ? DEFAULT_DRIVE : drive,
                deadline == null || deadline.isBlank()
                        ? DEFAULT_PARSE_DEADLINE_SECONDS
                        : Long.parseLong(deadline));
    }
}
