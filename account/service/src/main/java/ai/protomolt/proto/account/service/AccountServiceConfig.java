package ai.protomolt.proto.account.service;

import ai.protomolt.proto.account.service.store.AccountStoreConfig;

/**
 * Everything account-service needs from the outside world, in one value
 * object — no framework configuration binding. Production uses
 * {@link #fromEnvironment()}; tests construct it directly against their
 * containers.
 *
 * @param grpcPort the gRPC listen port ({@code DOCUMENT_PLATFORM_ACCOUNT_GRPC_PORT},
 *        default 9091; 0 = ephemeral port, for tests that read it back from
 *        the started server)
 * @param store JDBC/pool/migration settings (the store module's own
 *        {@code DOCUMENT_PLATFORM_ACCOUNT_JDBC_URL} / _USERNAME / _PASSWORD /
 *        _POOL_SIZE environment contract; defaults to its own
 *        {@code accounts} database on local Postgres — account data never
 *        shares repo's schema)
 * @param repoGrpcTarget where repo-service's DriveService lives
 *        ({@code DOCUMENT_PLATFORM_REPO_GRPC_TARGET}, default
 *        {@code localhost:9090}, plaintext {@code host:port}); the
 *        {@code "inprocess:<name>"} prefix selects the gRPC in-process
 *        transport instead — the same-JVM embedding path the integration
 *        tests use against a stub repo
 * @param kafkaBootstrapServers Kafka bootstrap servers
 *        ({@code DOCUMENT_PLATFORM_ACCOUNT_KAFKA_BOOTSTRAP_SERVERS});
 *        null/blank = eventing off: no outbox writes, no relay, no producer
 * @param kafkaTopic the account-events topic
 *        ({@code DOCUMENT_PLATFORM_ACCOUNT_KAFKA_TOPIC}, default
 *        {@code "account-events"})
 * @param schemaRegistryUrl a Confluent-compatible schema registry for the
 *        relay's serde ({@code DOCUMENT_PLATFORM_ACCOUNT_SCHEMA_REGISTRY_URL});
 *        null/blank = registry-free: frames stamp schema id 0, which only
 *        protomolt consumers resolve. Setting it makes the relay stamp the
 *        registry-assigned id, so relayed records are resolvable by standard
 *        Confluent tooling (requires the AccountEvent subject registered
 *        under {@code <topic>-value})
 * @param lifecycleEnabled whether the background relay loop runs when
 *        {@link AccountServices#startLifecycle()} is called
 *        ({@code DOCUMENT_PLATFORM_ACCOUNT_LIFECYCLE_ENABLED}, default true)
 * @param relayIntervalMs idle pause between relay-drain iterations
 *        ({@code DOCUMENT_PLATFORM_ACCOUNT_RELAY_INTERVAL_MS}, default 5000);
 *        a non-empty drain loops again immediately, so this is the
 *        empty-outbox backoff
 */
public record AccountServiceConfig(
        int grpcPort,
        AccountStoreConfig store,
        String repoGrpcTarget,
        String kafkaBootstrapServers,
        String kafkaTopic,
        String schemaRegistryUrl,
        boolean lifecycleEnabled,
        long relayIntervalMs) {

    /** Environment variable for the gRPC listen port. */
    public static final String ENV_GRPC_PORT = "DOCUMENT_PLATFORM_ACCOUNT_GRPC_PORT";
    /** Environment variable for repo-service's gRPC target. */
    public static final String ENV_REPO_GRPC_TARGET = "DOCUMENT_PLATFORM_REPO_GRPC_TARGET";
    /** Environment variable for the Kafka bootstrap servers (unset = eventing off). */
    public static final String ENV_KAFKA_BOOTSTRAP_SERVERS =
            "DOCUMENT_PLATFORM_ACCOUNT_KAFKA_BOOTSTRAP_SERVERS";
    /** Environment variable for the account-events topic. */
    public static final String ENV_KAFKA_TOPIC = "DOCUMENT_PLATFORM_ACCOUNT_KAFKA_TOPIC";
    /**
     * Environment variable for the relay serde's Confluent-compatible schema registry
     * (unset = registry-free, frames stamp schema id 0).
     */
    public static final String ENV_SCHEMA_REGISTRY_URL =
            "DOCUMENT_PLATFORM_ACCOUNT_SCHEMA_REGISTRY_URL";
    /** Environment variable toggling the background relay loop. */
    public static final String ENV_LIFECYCLE_ENABLED = "DOCUMENT_PLATFORM_ACCOUNT_LIFECYCLE_ENABLED";
    /** Environment variable for the relay-drain idle interval in milliseconds. */
    public static final String ENV_RELAY_INTERVAL_MS = "DOCUMENT_PLATFORM_ACCOUNT_RELAY_INTERVAL_MS";

    static final int DEFAULT_GRPC_PORT = 9091;
    static final String DEFAULT_REPO_GRPC_TARGET = "localhost:9090";
    /** Default account-events topic. */
    public static final String DEFAULT_KAFKA_TOPIC = "account-events";
    static final boolean DEFAULT_LIFECYCLE_ENABLED = true;
    static final long DEFAULT_RELAY_INTERVAL_MS = 5000L;

    /**
     * Target prefix selecting the in-process transport for the repo-service
     * channel (same-JVM embedding and tests); anything else is a plaintext
     * {@code host:port}.
     */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    public AccountServiceConfig {
        if (grpcPort < 0) {
            grpcPort = DEFAULT_GRPC_PORT;
        }
        if (store == null) {
            store = AccountStoreConfig.fromEnvironment();
        }
        if (repoGrpcTarget == null || repoGrpcTarget.isBlank()) {
            repoGrpcTarget = DEFAULT_REPO_GRPC_TARGET;
        }
        repoGrpcTarget = repoGrpcTarget.trim();
        kafkaBootstrapServers = blankToNull(kafkaBootstrapServers);
        if (kafkaTopic == null || kafkaTopic.isBlank()) {
            kafkaTopic = DEFAULT_KAFKA_TOPIC;
        }
        schemaRegistryUrl = blankToNull(schemaRegistryUrl);
        if (relayIntervalMs <= 0) {
            relayIntervalMs = DEFAULT_RELAY_INTERVAL_MS;
        }
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
     * Whether the repo target names an in-process server
     * ({@code "inprocess:<name>"}) rather than a plaintext
     * {@code host:port}.
     *
     * @return true for the in-process transport
     */
    public boolean repoTargetIsInProcess() {
        return repoGrpcTarget.startsWith(INPROCESS_TARGET_PREFIX);
    }

    /**
     * The repo target without the {@code "inprocess:"} prefix (the in-process
     * server name when {@link #repoTargetIsInProcess()}, else the
     * {@code host:port} unchanged).
     *
     * @return the transport-specific target
     */
    public String repoTargetName() {
        return repoTargetIsInProcess()
                ? repoGrpcTarget.substring(INPROCESS_TARGET_PREFIX.length())
                : repoGrpcTarget;
    }

    /**
     * Build the config from the process environment, using the
     * {@code DOCUMENT_PLATFORM_*} variables documented on this record.
     *
     * @return the resolved config
     */
    public static AccountServiceConfig fromEnvironment() {
        return new AccountServiceConfig(
                parseIntOrDefault(System.getenv(ENV_GRPC_PORT), DEFAULT_GRPC_PORT),
                AccountStoreConfig.fromEnvironment(),
                envOrDefault(ENV_REPO_GRPC_TARGET, DEFAULT_REPO_GRPC_TARGET),
                System.getenv(ENV_KAFKA_BOOTSTRAP_SERVERS),
                envOrDefault(ENV_KAFKA_TOPIC, DEFAULT_KAFKA_TOPIC),
                System.getenv(ENV_SCHEMA_REGISTRY_URL),
                parseBoolOrDefault(System.getenv(ENV_LIFECYCLE_ENABLED), DEFAULT_LIFECYCLE_ENABLED),
                parseLongOrDefault(System.getenv(ENV_RELAY_INTERVAL_MS), DEFAULT_RELAY_INTERVAL_MS));
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
