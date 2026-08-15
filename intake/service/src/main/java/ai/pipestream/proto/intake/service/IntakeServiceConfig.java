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
 */
public record IntakeServiceConfig(int grpcPort, String repoTarget, long maxPayloadBytes) {

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

    /** The default intake gRPC port. */
    public static final int DEFAULT_GRPC_PORT = 9092;

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
    }

    /**
     * Reads the configuration from {@code DOCUMENT_PLATFORM_INTAKE_*}
     * environment variables; the repo target is required, everything else
     * falls back to defaults.
     */
    public static IntakeServiceConfig fromEnvironment() {
        String repoTarget = System.getenv(ENV_REPO_TARGET);
        if (repoTarget == null || repoTarget.isBlank()) {
            throw new IllegalArgumentException(ENV_REPO_TARGET + " is required");
        }
        String port = System.getenv(ENV_GRPC_PORT);
        String cap = System.getenv(ENV_MAX_PAYLOAD_BYTES);
        return new IntakeServiceConfig(
                port == null || port.isBlank() ? DEFAULT_GRPC_PORT : Integer.parseInt(port),
                repoTarget,
                cap == null || cap.isBlank() ? DEFAULT_MAX_PAYLOAD_BYTES : Long.parseLong(cap));
    }
}
