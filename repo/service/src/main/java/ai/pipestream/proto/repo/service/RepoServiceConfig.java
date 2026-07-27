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
 *        another repo-service over gRPC at {@code repoTarget}),
 *        {@code "repo-inprocess"} (same, over the in-process transport —
 *        {@code repoTarget} is the in-process server name), {@code "redis"}
 *        (objects live in Redis at {@code redisUri}), or
 *        {@code "s3-redis-cache"} (S3 of record with a Redis read-through/
 *        write-through cache in front)
 * @param repoTarget the remote repo-service address
 *        ({@code DOCUMENT_PLATFORM_REPO_TARGET}, {@code host:port} for
 *        {@code "repo"}, an in-process server name for
 *        {@code "repo-inprocess"}); required for both {@code repo} modes
 * @param repoDrive the drive name the repo-backed store addresses on the
 *        remote service ({@code DOCUMENT_PLATFORM_REPO_DRIVE}, default
 *        {@code "default"}) — see {@code RemoteBlobStore}
 * @param redisUri the Redis connection URI
 *        ({@code DOCUMENT_PLATFORM_REDIS_URI}, default
 *        {@code redis://localhost:6379}) — used by the {@code redis} and
 *        {@code s3-redis-cache} blob-store modes
 * @param redisTtlSeconds per-object TTL in Redis
 *        ({@code DOCUMENT_PLATFORM_REDIS_TTL_SECONDS}, default 3600; in
 *        {@code s3-redis-cache} mode this is the cache-entry TTL)
 * @param redisMaxObjectBytes largest object admitted to Redis
 *        ({@code DOCUMENT_PLATFORM_REDIS_MAX_OBJECT_BYTES}, default 8388608;
 *        0 = unbounded) — in {@code s3-redis-cache} mode this is the cache
 *        ceiling: larger objects bypass the cache
 * @param lifecycleEnabled whether the background purge lifecycle loops run
 *        when {@link RepoServices#startLifecycle()} is called
 *        ({@code DOCUMENT_PLATFORM_LIFECYCLE_ENABLED}, default true)
 * @param purgeIntervalMs idle pause between purge-drain iterations
 *        ({@code DOCUMENT_PLATFORM_PURGE_INTERVAL_MS}, default 5000); a
 *        non-empty drain loops again immediately, so this is the empty-queue
 *        backoff
 * @param sweepIntervalMs pause between sweeper rescans
 *        ({@code DOCUMENT_PLATFORM_SWEEP_INTERVAL_MS}, default 60000)
 * @param reconcileEnabled whether the slow periodic storage-reconcile loop
 *        runs ({@code DOCUMENT_PLATFORM_RECONCILE_ENABLED}, default false)
 * @param reconcileDryRun whether the periodic reconcile only reports
 *        ({@code DOCUMENT_PLATFORM_RECONCILE_DRY_RUN}, default true)
 * @param reconcileMinAgeMs min-age guard for the periodic reconcile
 *        ({@code DOCUMENT_PLATFORM_RECONCILE_MIN_AGE_MS}, default 3600000)
 * @param kafkaBootstrapServers Kafka bootstrap servers
 *        ({@code DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS}); null/blank =
 *        eventing off: no outbox writes, no relay, no producer
 * @param kafkaTopic the document-events topic
 *        ({@code DOCUMENT_PLATFORM_KAFKA_TOPIC}, default
 *        {@code "document-events"})
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
        String repoDrive,
        String redisUri,
        int redisTtlSeconds,
        long redisMaxObjectBytes,
        boolean lifecycleEnabled,
        long purgeIntervalMs,
        long sweepIntervalMs,
        boolean reconcileEnabled,
        boolean reconcileDryRun,
        long reconcileMinAgeMs,
        String kafkaBootstrapServers,
        String kafkaTopic) {

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
    /** Environment variable for the Redis connection URI (redis blob-store modes). */
    public static final String ENV_REDIS_URI = "DOCUMENT_PLATFORM_REDIS_URI";
    /** Environment variable for the Redis per-object TTL in seconds. */
    public static final String ENV_REDIS_TTL_SECONDS = "DOCUMENT_PLATFORM_REDIS_TTL_SECONDS";
    /** Environment variable for the largest object admitted to Redis. */
    public static final String ENV_REDIS_MAX_OBJECT_BYTES = "DOCUMENT_PLATFORM_REDIS_MAX_OBJECT_BYTES";
    /** Environment variable toggling the background purge lifecycle loops. */
    public static final String ENV_LIFECYCLE_ENABLED = "DOCUMENT_PLATFORM_LIFECYCLE_ENABLED";
    /** Environment variable for the purge-drain idle interval in milliseconds. */
    public static final String ENV_PURGE_INTERVAL_MS = "DOCUMENT_PLATFORM_PURGE_INTERVAL_MS";
    /** Environment variable for the sweeper rescan interval in milliseconds. */
    public static final String ENV_SWEEP_INTERVAL_MS = "DOCUMENT_PLATFORM_SWEEP_INTERVAL_MS";
    /** Environment variable toggling the periodic storage-reconcile loop. */
    public static final String ENV_RECONCILE_ENABLED = "DOCUMENT_PLATFORM_RECONCILE_ENABLED";
    /** Environment variable for the periodic reconcile's dry-run rail. */
    public static final String ENV_RECONCILE_DRY_RUN = "DOCUMENT_PLATFORM_RECONCILE_DRY_RUN";
    /** Environment variable for the periodic reconcile's min-age guard in milliseconds. */
    public static final String ENV_RECONCILE_MIN_AGE_MS = "DOCUMENT_PLATFORM_RECONCILE_MIN_AGE_MS";
    /** Environment variable for the Kafka bootstrap servers (unset = eventing off). */
    public static final String ENV_KAFKA_BOOTSTRAP_SERVERS = "DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS";
    /** Environment variable for the document-events topic. */
    public static final String ENV_KAFKA_TOPIC = "DOCUMENT_PLATFORM_KAFKA_TOPIC";

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
    /** Blob-store selection value: objects live in Redis. */
    public static final String BLOB_STORE_REDIS = "redis";
    /** Blob-store selection value: S3 of record with a Redis cache in front. */
    public static final String BLOB_STORE_S3_REDIS_CACHE = "s3-redis-cache";
    static final String DEFAULT_REPO_DRIVE = "default";
    static final String DEFAULT_REDIS_URI = "redis://localhost:6379";
    static final int DEFAULT_REDIS_TTL_SECONDS = 3600;
    static final long DEFAULT_REDIS_MAX_OBJECT_BYTES = 8388608L;
    static final boolean DEFAULT_LIFECYCLE_ENABLED = true;
    static final long DEFAULT_PURGE_INTERVAL_MS = 5000L;
    static final long DEFAULT_SWEEP_INTERVAL_MS = 60000L;
    static final boolean DEFAULT_RECONCILE_ENABLED = false;
    static final boolean DEFAULT_RECONCILE_DRY_RUN = true;
    static final long DEFAULT_RECONCILE_MIN_AGE_MS = 3600000L;
    /** Default document-events topic. */
    public static final String DEFAULT_KAFKA_TOPIC = "document-events";

    /**
     * Compatibility constructor: the 14 pre-lifecycle components, with the
     * lifecycle and eventing settings at their defaults.
     */
    public RepoServiceConfig(int grpcPort, LedgerConfig ledger, String s3Endpoint, String s3Region,
            String s3AccessKey, String s3SecretKey, String defaultBucketBase, int httpPort,
            String blobStore, String repoTarget, String repoDrive, String redisUri,
            int redisTtlSeconds, long redisMaxObjectBytes) {
        this(grpcPort, ledger, s3Endpoint, s3Region, s3AccessKey, s3SecretKey, defaultBucketBase,
                httpPort, blobStore, repoTarget, repoDrive, redisUri, redisTtlSeconds,
                redisMaxObjectBytes, DEFAULT_LIFECYCLE_ENABLED, DEFAULT_PURGE_INTERVAL_MS,
                DEFAULT_SWEEP_INTERVAL_MS, DEFAULT_RECONCILE_ENABLED, DEFAULT_RECONCILE_DRY_RUN,
                DEFAULT_RECONCILE_MIN_AGE_MS);
    }

    /**
     * Compatibility constructor: the 20 pre-eventing components, with Kafka
     * eventing off (no bootstrap servers).
     */
    public RepoServiceConfig(int grpcPort, LedgerConfig ledger, String s3Endpoint, String s3Region,
            String s3AccessKey, String s3SecretKey, String defaultBucketBase, int httpPort,
            String blobStore, String repoTarget, String repoDrive, String redisUri,
            int redisTtlSeconds, long redisMaxObjectBytes, boolean lifecycleEnabled,
            long purgeIntervalMs, long sweepIntervalMs, boolean reconcileEnabled,
            boolean reconcileDryRun, long reconcileMinAgeMs) {
        this(grpcPort, ledger, s3Endpoint, s3Region, s3AccessKey, s3SecretKey, defaultBucketBase,
                httpPort, blobStore, repoTarget, repoDrive, redisUri, redisTtlSeconds,
                redisMaxObjectBytes, lifecycleEnabled, purgeIntervalMs, sweepIntervalMs,
                reconcileEnabled, reconcileDryRun, reconcileMinAgeMs, null, DEFAULT_KAFKA_TOPIC);
    }

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
                && !blobStore.equals(BLOB_STORE_REPO_INPROCESS)
                && !blobStore.equals(BLOB_STORE_REDIS)
                && !blobStore.equals(BLOB_STORE_S3_REDIS_CACHE)) {
            throw new IllegalArgumentException(ENV_BLOB_STORE
                    + " must be one of s3|repo|repo-inprocess|redis|s3-redis-cache"
                    + " (got \"" + blobStore + "\")");
        }
        repoTarget = blankToNull(repoTarget);
        if ((blobStore.equals(BLOB_STORE_REPO) || blobStore.equals(BLOB_STORE_REPO_INPROCESS))
                && repoTarget == null) {
            throw new IllegalArgumentException(ENV_REPO_TARGET + " is required when " + ENV_BLOB_STORE
                    + "=" + blobStore);
        }
        if (repoDrive == null || repoDrive.isBlank()) {
            repoDrive = DEFAULT_REPO_DRIVE;
        }
        if (redisUri == null || redisUri.isBlank()) {
            redisUri = DEFAULT_REDIS_URI;
        }
        if (redisTtlSeconds < 0) {
            redisTtlSeconds = DEFAULT_REDIS_TTL_SECONDS;
        }
        if (redisMaxObjectBytes < 0) {
            redisMaxObjectBytes = DEFAULT_REDIS_MAX_OBJECT_BYTES;
        }
        if (purgeIntervalMs <= 0) {
            purgeIntervalMs = DEFAULT_PURGE_INTERVAL_MS;
        }
        if (sweepIntervalMs <= 0) {
            sweepIntervalMs = DEFAULT_SWEEP_INTERVAL_MS;
        }
        if (reconcileMinAgeMs < 0) {
            reconcileMinAgeMs = DEFAULT_RECONCILE_MIN_AGE_MS;
        }
        kafkaBootstrapServers = blankToNull(kafkaBootstrapServers);
        if (kafkaTopic == null || kafkaTopic.isBlank()) {
            kafkaTopic = DEFAULT_KAFKA_TOPIC;
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
     * Whether Kafka eventing is on (bootstrap servers configured). When off:
     * no outbox writes, no relay loop, no producer.
     *
     * @return true when eventing is configured
     */
    public boolean kafkaEnabled() {
        return kafkaBootstrapServers != null;
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
                envOrDefault(ENV_REPO_DRIVE, DEFAULT_REPO_DRIVE),
                envOrDefault(ENV_REDIS_URI, DEFAULT_REDIS_URI),
                parseIntOrDefault(System.getenv(ENV_REDIS_TTL_SECONDS), DEFAULT_REDIS_TTL_SECONDS),
                parseLongOrDefault(System.getenv(ENV_REDIS_MAX_OBJECT_BYTES),
                        DEFAULT_REDIS_MAX_OBJECT_BYTES),
                parseBoolOrDefault(System.getenv(ENV_LIFECYCLE_ENABLED), DEFAULT_LIFECYCLE_ENABLED),
                parseLongOrDefault(System.getenv(ENV_PURGE_INTERVAL_MS), DEFAULT_PURGE_INTERVAL_MS),
                parseLongOrDefault(System.getenv(ENV_SWEEP_INTERVAL_MS), DEFAULT_SWEEP_INTERVAL_MS),
                parseBoolOrDefault(System.getenv(ENV_RECONCILE_ENABLED), DEFAULT_RECONCILE_ENABLED),
                parseBoolOrDefault(System.getenv(ENV_RECONCILE_DRY_RUN), DEFAULT_RECONCILE_DRY_RUN),
                parseLongOrDefault(System.getenv(ENV_RECONCILE_MIN_AGE_MS),
                        DEFAULT_RECONCILE_MIN_AGE_MS),
                System.getenv(ENV_KAFKA_BOOTSTRAP_SERVERS),
                envOrDefault(ENV_KAFKA_TOPIC, DEFAULT_KAFKA_TOPIC));
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

    private static long parseLongOrDefault(String value, long fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** Boolean parse: {@code "true"/"1"/"yes"/"on"} (case-insensitive) is true, anything else false. */
    private static boolean parseBoolOrDefault(String value, boolean fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        String v = value.trim().toLowerCase(java.util.Locale.ROOT);
        return v.equals("true") || v.equals("1") || v.equals("yes") || v.equals("on");
    }
}
