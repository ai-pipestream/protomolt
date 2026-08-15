package ai.pipestream.proto.intake.service;

/**
 * Configuration for a standalone or embedded intake service.
 *
 * @param grpcPort port the Netty server binds; {@code 0} picks a free port
 * @param repoTarget the repo-service {@code DocumentService} endpoint — a
 *        {@code host:port} authority, or {@code inprocess:<name>} for an
 *        in-process repo (tests and all-in-one deployments)
 * @param maxPayloadBytes service-wide payload cap for the in-memory gRPC
 *        lanes; per-key caps can only narrow it further
 * @param httpPort port the HTTP upload lane binds
 *        ({@code DOCUMENT_PLATFORM_INTAKE_HTTP_PORT}); {@code 0} or absent
 *        means the HTTP lane is off in the standalone main (embedded hosts
 *        call {@code IntakeServices.startHttp(0)} directly for an ephemeral
 *        port)
 */
public record IntakeServiceConfig(
        int grpcPort, String repoTarget, long maxPayloadBytes, int httpPort) {

    /** Prefix selecting an in-process repo channel: {@code inprocess:<name>}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    /** The default service payload cap: 64 MiB. */
    public static final long DEFAULT_MAX_PAYLOAD_BYTES = 64L * 1024 * 1024;

    /** Env var naming the repo-service target ({@code host:port}). */
    public static final String ENV_REPO_TARGET = "DOCUMENT_PLATFORM_INTAKE_REPO_TARGET";

    /** Env var for the intake gRPC port. */
    public static final String ENV_GRPC_PORT = "DOCUMENT_PLATFORM_INTAKE_GRPC_PORT";

    /** Env var for the service payload cap in bytes. */
    public static final String ENV_MAX_PAYLOAD_BYTES = "DOCUMENT_PLATFORM_INTAKE_MAX_PAYLOAD_BYTES";

    /** Env var for the HTTP upload lane's port ({@code 0}/absent = the lane is off). */
    public static final String ENV_HTTP_PORT = "DOCUMENT_PLATFORM_INTAKE_HTTP_PORT";

    /** The default intake gRPC port. */
    public static final int DEFAULT_GRPC_PORT = 9092;

    /**
     * Compatibility constructor: the three pre-HTTP-lane components, with the
     * HTTP lane off.
     */
    public IntakeServiceConfig(int grpcPort, String repoTarget, long maxPayloadBytes) {
        this(grpcPort, repoTarget, maxPayloadBytes, 0);
    }

    public IntakeServiceConfig {
        if (repoTarget == null || repoTarget.isBlank()) {
            throw new IllegalArgumentException("repoTarget must not be blank");
        }
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("maxPayloadBytes must be positive");
        }
        if (grpcPort < 0) {
            throw new IllegalArgumentException("grpcPort must not be negative");
        }
        if (httpPort < 0) {
            throw new IllegalArgumentException("httpPort must not be negative");
        }
    }

    /**
     * Reads the configuration from {@code DOCUMENT_PLATFORM_INTAKE_*}
     * environment variables; the repo target is required, everything else
     * falls back to defaults (HTTP lane off).
     */
    public static IntakeServiceConfig fromEnvironment() {
        String repoTarget = System.getenv(ENV_REPO_TARGET);
        if (repoTarget == null || repoTarget.isBlank()) {
            throw new IllegalArgumentException(ENV_REPO_TARGET + " is required");
        }
        String port = System.getenv(ENV_GRPC_PORT);
        String cap = System.getenv(ENV_MAX_PAYLOAD_BYTES);
        String httpPort = System.getenv(ENV_HTTP_PORT);
        return new IntakeServiceConfig(
                port == null || port.isBlank() ? DEFAULT_GRPC_PORT : Integer.parseInt(port),
                repoTarget,
                cap == null || cap.isBlank() ? DEFAULT_MAX_PAYLOAD_BYTES : Long.parseLong(cap),
                httpPort == null || httpPort.isBlank() ? 0 : Integer.parseInt(httpPort));
    }
}
