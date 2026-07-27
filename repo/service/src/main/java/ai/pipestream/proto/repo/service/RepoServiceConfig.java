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
 * @param httpPort the HTTP upload listen port
 *        ({@code DOCUMENT_PLATFORM_HTTP_PORT}, default 8080; {@code 0} or
 *        {@code "off"} disables the HTTP server — see {@link RepoServiceMain};
 *        tests that want an ephemeral port call
 *        {@code RepoServices.startHttp(0)} directly)
 * @param blobStore which {@code BlobStore} implementation backs the services
 *        ({@code DOCUMENT_PLATFORM_BLOB_STORE}): {@code "s3"} (default; the
 *        direct object-storage path), {@code "repo"} (delegate bytes to
 *        another repo-service over gRPC at {@code repoTarget}), or
 *        {@code "repo-inprocess"} (same, over the in-process transport —
 *        {@code repoTarget} is the in-process server name)
 * @param repoTarget the remote repo-service address
 *        ({@code DOCUMENT_PLATFORM_REPO_TARGET}, {@code host:port} for
 *        {@code "repo"}, an in-process server name for
 *        {@code "repo-inprocess"}); required for both {@code repo} modes
 * @param repoDrive the drive name the repo-backed store addresses on the
 *        remote service ({@code DOCUMENT_PLATFORM_REPO_DRIVE}, default
 *        {@code "default"}) — see {@code RemoteBlobStore}
 */
public record RepoServiceConfig(
        int grpcPort,
        LedgerConfig ledger,
        String s3Endpoint,
        String s3Region,
        String s3AccessKey,
        String s3SecretKey,
        String defaultBucketBase,
        int httpPort,
        String blobStore,
        String repoTarget,
        String repoDrive) {

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
    /** Environment variable for the HTTP upload listen port ({@code 0}/"off" disables). */
    public static final String ENV_HTTP_PORT = "DOCUMENT_PLATFORM_HTTP_PORT";
    /** Environment variable selecting the BlobStore implementation. */
    public static final String ENV_BLOB_STORE = "DOCUMENT_PLATFORM_BLOB_STORE";
    /** Environment variable for the remote repo-service target (repo blob-store modes). */
    public static final String ENV_REPO_TARGET = "DOCUMENT_PLATFORM_REPO_TARGET";
    /** Environment variable for the drive the repo-backed store addresses remotely. */
    public static final String ENV_REPO_DRIVE = "DOCUMENT_PLATFORM_REPO_DRIVE";

    static final int DEFAULT_GRPC_PORT = 9090;
    static final String DEFAULT_S3_REGION = "us-east-1";
    static final String DEFAULT_BUCKET_BASE = "documents";
    static final int DEFAULT_HTTP_PORT = 8080;
    /** Blob-store selection value: direct S3 object storage (the default). */
    public static final String BLOB_STORE_S3 = "s3";
    /** Blob-store selection value: a remote repo-service over gRPC. */
    public static final String BLOB_STORE_REPO = "repo";
    /** Blob-store selection value: a remote repo-service over the in-process transport. */
    public static final String BLOB_STORE_REPO_INPROCESS = "repo-inprocess";
    static final String DEFAULT_REPO_DRIVE = "default";

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
        if (httpPort < 0) {
            httpPort = DEFAULT_HTTP_PORT;
        }
        if (blobStore == null || blobStore.isBlank()) {
            blobStore = BLOB_STORE_S3;
        }
        blobStore = blobStore.trim().toLowerCase(java.util.Locale.ROOT);
        if (!blobStore.equals(BLOB_STORE_S3) && !blobStore.equals(BLOB_STORE_REPO)
                && !blobStore.equals(BLOB_STORE_REPO_INPROCESS)) {
            throw new IllegalArgumentException(ENV_BLOB_STORE + " must be one of s3|repo|repo-inprocess"
                    + " (got \"" + blobStore + "\")");
        }
        repoTarget = blankToNull(repoTarget);
        if (!blobStore.equals(BLOB_STORE_S3) && repoTarget == null) {
            throw new IllegalArgumentException(ENV_REPO_TARGET + " is required when " + ENV_BLOB_STORE
                    + "=" + blobStore);
        }
        if (repoDrive == null || repoDrive.isBlank()) {
            repoDrive = DEFAULT_REPO_DRIVE;
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
                envOrDefault(ENV_DEFAULT_BUCKET_BASE, DEFAULT_BUCKET_BASE),
                parseHttpPort(System.getenv(ENV_HTTP_PORT)),
                envOrDefault(ENV_BLOB_STORE, BLOB_STORE_S3),
                System.getenv(ENV_REPO_TARGET),
                envOrDefault(ENV_REPO_DRIVE, DEFAULT_REPO_DRIVE));
    }

    /** HTTP port parse: {@code "off"} (and {@code "0"}) disables the HTTP server. */
    private static int parseHttpPort(String value) {
        if (value != null && value.trim().equalsIgnoreCase("off")) {
            return 0;
        }
        return parseIntOrDefault(value, DEFAULT_HTTP_PORT);
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
