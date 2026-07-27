package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.ledger.LedgerConfig;

/**
 * Everything repo-service needs from the outside world, in one value object —
 * no framework configuration binding. Production uses {@link #fromEnvironment()};
 * tests construct it directly against their containers.
 *
 * @param grpcPort the gRPC listen port ({@code DOCUMENT_PLATFORM_GRPC_PORT},
 *        default 9090; 0 = ephemeral port, for tests that read it back from
 *        the started server)
 * @param ledger JDBC/pool/migration settings (the ledger module's own
 *        {@code DOCUMENT_PLATFORM_JDBC_URL} / _USERNAME / _PASSWORD /
 *        _POOL_SIZE environment contract)
 * @param s3Endpoint optional S3 endpoint override
 *        ({@code DOCUMENT_PLATFORM_S3_ENDPOINT}) for S3-compatible stores such
 *        as LocalStack; null = the provider default endpoint
 * @param s3Region the S3 region ({@code DOCUMENT_PLATFORM_S3_REGION}, default
 *        {@code us-east-1})
 * @param s3AccessKey static credentials access key
 *        ({@code DOCUMENT_PLATFORM_S3_ACCESS_KEY}); null/blank = the SDK
 *        default credentials chain
 * @param s3SecretKey static credentials secret key
 *        ({@code DOCUMENT_PLATFORM_S3_SECRET_KEY}); required when
 *        {@code s3AccessKey} is set
 * @param defaultBucketBase bucket-name base for provisioned drives
 *        ({@code DOCUMENT_PLATFORM_DEFAULT_BUCKET_BASE}, default
 *        {@code "documents"}): a drive without an explicit bucket gets
 *        {@code <base>-<accountId>-<name>}, sanitized to S3 bucket rules
 */
public record RepoServiceConfig(
        int grpcPort,
        LedgerConfig ledger,
        String s3Endpoint,
        String s3Region,
        String s3AccessKey,
        String s3SecretKey,
        String defaultBucketBase) {

    /** Environment variable for the gRPC listen port. */
    public static final String ENV_GRPC_PORT = "DOCUMENT_PLATFORM_GRPC_PORT";
    /** Environment variable for the S3 endpoint override. */
    public static final String ENV_S3_ENDPOINT = "DOCUMENT_PLATFORM_S3_ENDPOINT";
    /** Environment variable for the S3 region. */
    public static final String ENV_S3_REGION = "DOCUMENT_PLATFORM_S3_REGION";
    /** Environment variable for the static S3 access key. */
    public static final String ENV_S3_ACCESS_KEY = "DOCUMENT_PLATFORM_S3_ACCESS_KEY";
    /** Environment variable for the static S3 secret key. */
    public static final String ENV_S3_SECRET_KEY = "DOCUMENT_PLATFORM_S3_SECRET_KEY";
    /** Environment variable for the provisioned-bucket name base. */
    public static final String ENV_DEFAULT_BUCKET_BASE = "DOCUMENT_PLATFORM_DEFAULT_BUCKET_BASE";

    static final int DEFAULT_GRPC_PORT = 9090;
    static final String DEFAULT_S3_REGION = "us-east-1";
    static final String DEFAULT_BUCKET_BASE = "documents";

    public RepoServiceConfig {
        if (grpcPort < 0) {
            grpcPort = DEFAULT_GRPC_PORT;
        }
        if (ledger == null) {
            ledger = LedgerConfig.fromEnvironment();
        }
        s3Endpoint = blankToNull(s3Endpoint);
        if (s3Region == null || s3Region.isBlank()) {
            s3Region = DEFAULT_S3_REGION;
        }
        s3AccessKey = blankToNull(s3AccessKey);
        s3SecretKey = blankToNull(s3SecretKey);
        if ((s3AccessKey == null) != (s3SecretKey == null)) {
            throw new IllegalArgumentException(
                    "S3 access key and secret key must be configured together (or neither)");
        }
        if (defaultBucketBase == null || defaultBucketBase.isBlank()) {
            defaultBucketBase = DEFAULT_BUCKET_BASE;
        }
    }

    /**
     * Whether static S3 credentials are configured (otherwise the SDK default
     * credentials chain is used).
     *
     * @return true when an access/secret key pair is set
     */
    public boolean hasStaticCredentials() {
        return s3AccessKey != null;
    }

    /**
     * Build the config from the process environment, using the
     * {@code DOCUMENT_PLATFORM_*} variables documented on this record.
     *
     * @return the resolved config
     */
    public static RepoServiceConfig fromEnvironment() {
        return new RepoServiceConfig(
                parseIntOrDefault(System.getenv(ENV_GRPC_PORT), DEFAULT_GRPC_PORT),
                LedgerConfig.fromEnvironment(),
                System.getenv(ENV_S3_ENDPOINT),
                envOrDefault(ENV_S3_REGION, DEFAULT_S3_REGION),
                System.getenv(ENV_S3_ACCESS_KEY),
                System.getenv(ENV_S3_SECRET_KEY),
                envOrDefault(ENV_DEFAULT_BUCKET_BASE, DEFAULT_BUCKET_BASE));
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static int parseIntOrDefault(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
