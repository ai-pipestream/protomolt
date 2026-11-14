package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the ingress additions to {@link RepoServiceConfig}: the
 * HTTP port convention and the blob-store selection validation. Only the
 * logic that can actually break is tested — no ceremonial default echoing of
 * the pre-existing fields.
 */
class RepoServiceConfigTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"valkey", "s3-valkey-cache", "redis", "s3-redis-cache"})
    void explicitProviderSelectionAndValkeyEnvironmentRetainCanonicalDriveIdentity(String mode) {
        var config = RepoServiceConfig.fromEnvironment(java.util.Map.of(
                RepoServiceConfig.ENV_BLOB_STORE, mode,
                RepoServiceConfig.ENV_VALKEY_URI, "rediss://cache.example:6379/2",
                RepoServiceConfig.ENV_VALKEY_TTL_SECONDS, "0",
                RepoServiceConfig.ENV_VALKEY_MAX_OBJECT_BYTES, "2048"));
        assertThat(config.blobStore()).isEqualTo(mode);
        assertThat(config.redisCompatibleProvider()).isEqualTo(mode.contains("valkey") ? "valkey" : "redis");
        assertThat(config.driveProvider()).isEqualTo(mode.startsWith("s3-") ? "s3" : "redis");
        assertThat(config.redisUri()).isEqualTo("rediss://cache.example:6379/2");
        assertThat(config.redisTtlSeconds()).isZero();
        assertThat(config.redisMaxObjectBytes()).isEqualTo(2048);
    }

    @Test void conflictingEnvironmentAliasesFailWithoutEchoingCredentials() {
        for (var names : java.util.List.of(
                java.util.List.of(RepoServiceConfig.ENV_VALKEY_URI, RepoServiceConfig.ENV_REDIS_URI),
                java.util.List.of(RepoServiceConfig.ENV_VALKEY_TTL_SECONDS, RepoServiceConfig.ENV_REDIS_TTL_SECONDS),
                java.util.List.of(RepoServiceConfig.ENV_VALKEY_MAX_OBJECT_BYTES, RepoServiceConfig.ENV_REDIS_MAX_OBJECT_BYTES))) {
            assertThatThrownBy(() -> RepoServiceConfig.fromEnvironment(java.util.Map.of(
                    names.get(0), "secret-new", names.get(1), "secret-old")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("conflicts")
                    .hasMessageNotContaining("secret").hasNoCause();
        }
        var config = RepoServiceConfig.fromEnvironment(java.util.Map.of(
                RepoServiceConfig.ENV_VALKEY_URI, "redis://same:6379",
                RepoServiceConfig.ENV_REDIS_URI, "redis://same:6379"));
        assertThat(config.redisUri()).isEqualTo("redis://same:6379");
        assertThat(config.blobStore()).isEqualTo("s3");
        for (var blank : java.util.List.of("", " ")) {
            assertThatThrownBy(() -> RepoServiceConfig.fromEnvironment(java.util.Map.of(
                    RepoServiceConfig.ENV_VALKEY_URI, blank, RepoServiceConfig.ENV_REDIS_URI, blank)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(RepoServiceConfig.ENV_VALKEY_URI);
        }
    }

    @Test void retiredRemoteStorageFailsBeforeOpeningTheLedger() {
        for (var mode : java.util.List.of("repo", "repo-inprocess")) {
            assertThatThrownBy(() -> RepoServiceConfig.fromEnvironment(java.util.Map.of(
                    RepoServiceConfig.ENV_BLOB_STORE, mode)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Remote repository blob storage is retired: " + mode);
        }
    }

    @Test void explicitBlankStorageAndDefaultedTextSettingsCannotSelectAnotherConfiguration() {
        for (var name : java.util.List.of(RepoServiceConfig.ENV_BLOB_STORE, RepoServiceConfig.ENV_REDIS_URI, RepoServiceConfig.ENV_VALKEY_URI,
                RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE,
                RepoServiceConfig.ENV_S3_REGION, RepoServiceConfig.ENV_KAFKA_TOPIC,
                RepoServiceConfig.ENV_PURGE_QUEUE, RepoServiceConfig.ENV_KAFKA_PURGE_TOPIC)) {
            for (var blank : java.util.List.of("", " ")) {
                assertThatThrownBy(() -> RepoServiceConfig.fromEnvironment(java.util.Map.of(name, blank)))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(name);
            }
        }
        for (var name : java.util.List.of(RepoServiceConfig.ENV_BLOB_STORE, RepoServiceConfig.ENV_PURGE_QUEUE)) {
            assertThatThrownBy(() -> RepoServiceConfig.fromEnvironment(java.util.Map.of(name, "private-value")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(name)
                    .hasMessageNotContaining("private-value").hasNoCause();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("invalidEnvironmentLimits")
    void refusesConfiguredLimitsInsteadOfReplacingThemWithDefaults(String name, String value) {
        assertThatThrownBy(() -> RepoServiceConfig.fromEnvironment(java.util.Map.of(name, value)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(name)
                .hasMessageNotContaining("private-value").hasNoCause();
    }

    static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> invalidEnvironmentLimits() {
        var names = java.util.List.of(RepoServiceConfig.ENV_GRPC_PORT, RepoServiceConfig.ENV_HTTP_PORT,
                RepoServiceConfig.ENV_REDIS_TTL_SECONDS, RepoServiceConfig.ENV_REDIS_MAX_OBJECT_BYTES,
                RepoServiceConfig.ENV_VALKEY_TTL_SECONDS, RepoServiceConfig.ENV_VALKEY_MAX_OBJECT_BYTES,
                RepoServiceConfig.ENV_PURGE_INTERVAL_MS, RepoServiceConfig.ENV_SWEEP_INTERVAL_MS,
                RepoServiceConfig.ENV_RECONCILE_MIN_AGE_MS, LedgerConfig.ENV_POOL_SIZE,
                RepoServiceConfig.ENV_READ_BUDGET_BYTES);
        var malformed = names.stream().flatMap(name -> java.util.stream.Stream.of("private-value", "", " ",
                "1.5", "999999999999999999999999", "-1")
                .map(value -> org.junit.jupiter.params.provider.Arguments.of(name, value)));
        return java.util.stream.Stream.concat(malformed, java.util.stream.Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_GRPC_PORT, "65536"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_HTTP_PORT, "65536"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_REDIS_TTL_SECONDS, "2147483648"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_PURGE_INTERVAL_MS, "0"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_SWEEP_INTERVAL_MS, "0"),
                org.junit.jupiter.params.provider.Arguments.of(LedgerConfig.ENV_POOL_SIZE, "0"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_READ_BUDGET_BYTES, "0")));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"truee", "private-value", "", " ", "2"})
    void refusesMisspelledConfiguredFlags(String value) {
        for (var name : java.util.List.of(RepoServiceConfig.ENV_LIFECYCLE_ENABLED,
                RepoServiceConfig.ENV_RECONCILE_ENABLED, RepoServiceConfig.ENV_RECONCILE_DRY_RUN,
                RepoServiceConfig.ENV_S3_CONDITIONAL_WRITES)) {
            assertThatThrownBy(() -> RepoServiceConfig.fromEnvironment(java.util.Map.of(name, value)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(name)
                    .hasMessageNotContaining("private-value").hasNoCause();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"true,false", "1,0", "YES,NO", "on,off"})
    void retainsSupportedBooleanSpellings(String enabled, String disabled) {
        var config = RepoServiceConfig.fromEnvironment(java.util.Map.of(
                RepoServiceConfig.ENV_LIFECYCLE_ENABLED, " " + enabled + " ",
                RepoServiceConfig.ENV_RECONCILE_ENABLED, disabled,
                RepoServiceConfig.ENV_RECONCILE_DRY_RUN, enabled,
                RepoServiceConfig.ENV_S3_CONDITIONAL_WRITES, enabled));
        assertThat(config.lifecycleEnabled()).isTrue();
        assertThat(config.reconcileEnabled()).isFalse();
        assertThat(config.reconcileDryRun()).isTrue();
        assertThat(config.s3ConditionalWrites()).isTrue();
    }

    @Test void configuredZeroAndOffKeepTheirDocumentedMeanings() {
        var config = RepoServiceConfig.fromEnvironment(java.util.Map.of(
                RepoServiceConfig.ENV_GRPC_PORT, "0", RepoServiceConfig.ENV_HTTP_PORT, " OFF ",
                RepoServiceConfig.ENV_REDIS_TTL_SECONDS, "0", RepoServiceConfig.ENV_REDIS_MAX_OBJECT_BYTES, "0",
                RepoServiceConfig.ENV_RECONCILE_MIN_AGE_MS, "0", LedgerConfig.ENV_POOL_SIZE, " 3 "));
        assertThat(config.grpcPort()).isZero();
        assertThat(config.httpPort()).isZero();
        assertThat(config.redisTtlSeconds()).isZero();
        assertThat(config.redisMaxObjectBytes()).isZero();
        assertThat(config.reconcileMinAgeMs()).isZero();
        assertThat(config.ledger().maxPoolSize()).isEqualTo(3);
    }

    @Test void omittedEnvironmentUsesDocumentedDefaults() {
        var config = RepoServiceConfig.fromEnvironment(java.util.Map.of());
        assertThat(config.grpcPort()).isEqualTo(RepoServiceConfig.DEFAULT_GRPC_PORT);
        assertThat(config.httpPort()).isEqualTo(8080);
        assertThat(config.lifecycleEnabled()).isTrue();
        assertThat(config.ledger().maxPoolSize()).isEqualTo(LedgerConfig.DEFAULT_POOL_SIZE);
        assertThat(config.readBudgetBytes()).isEqualTo(256L * 1024 * 1024);
    }

    @Test void managedQualificationSurvivesConfigurationCopies() {
        var policy = new ManagedStoragePolicy("nas-v1", "account-a", true);
        var configured = config(0, "s3").withManagedStorage(policy);
        assertThat(configured.managedStorage()).isEqualTo(policy);
        assertThat(config(0, "s3").managedStorage()).isEqualTo(ManagedStoragePolicy.disabled());
    }

    @Test void unsupportedManagedCompositionFailsBeforeOpeningExternalResources() {
        var configured = config(0, "redis")
                .withManagedStorage(new ManagedStoragePolicy("nas-v1", "account-a", true));
        assertThatThrownBy(() -> RepoServices.build(configured))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Managed storage requires an S3 backing store and enabled lifecycle recovery");
    }

    private static final LedgerConfig LEDGER =
            new LedgerConfig("jdbc:postgresql://localhost:5432/x", "u", "p");

    private static RepoServiceConfig config(int httpPort, String blobStore) {
        return new RepoServiceConfig(0, LEDGER, null, null, null, null, null,
                httpPort, blobStore, null, -1, -1L);
    }

    @Test
    void defaultsKeepTodaysBehavior() {
        RepoServiceConfig config = config(-1, null);
        assertThat(config.httpPort()).isEqualTo(8080);
        assertThat(config.blobStore()).isEqualTo("s3");
        assertThat(config.redisUri()).isEqualTo("redis://localhost:6379");
        assertThat(config.redisTtlSeconds()).isZero();
        assertThat(config.redisMaxObjectBytes()).isEqualTo(8388608L);
    }

    @Test
    void valkeyAndRedisTtlDefaultsAreNonExpiringForDurableHostModesAndPositiveForCacheModes() {
        for (String durableMode : java.util.List.of("s3", "valkey", "redis")) {
            assertThat(config(0, durableMode).redisTtlSeconds())
                    .as("constructor default for %s", durableMode)
                    .isZero();
            RepoServiceConfig fromEnv = RepoServiceConfig.fromEnvironment(java.util.Map.of(
                    RepoServiceConfig.ENV_BLOB_STORE, durableMode));
            assertThat(fromEnv.redisTtlSeconds())
                    .as("environment default for %s", durableMode)
                    .isZero();
        }

        for (String cacheMode : java.util.List.of("s3-valkey-cache", "s3-redis-cache")) {
            assertThat(config(0, cacheMode).redisTtlSeconds())
                    .as("constructor default for %s", cacheMode)
                    .isEqualTo(3600);
            RepoServiceConfig fromEnv = RepoServiceConfig.fromEnvironment(java.util.Map.of(
                    RepoServiceConfig.ENV_BLOB_STORE, cacheMode));
            assertThat(fromEnv.redisTtlSeconds())
                    .as("environment default for %s", cacheMode)
                    .isEqualTo(3600);
        }

        // Verify startup wiring honors the non-expiring host default and refuses an expiring override
        for (String hostMode : java.util.List.of("valkey", "redis")) {
            var seenTtls = new java.util.ArrayList<String>();
            var closed = new java.util.concurrent.atomic.AtomicInteger();
            var stubProvider = new ai.protomolt.proto.repo.blob.spi.BlobStoreProvider() {
                @Override public String id() { return hostMode; }
                @Override public ai.protomolt.proto.repo.blob.spi.OpenedBlobStore open(
                        java.util.Map<String, String> options) {
                    String ttl = options.get("ttl-seconds");
                    seenTtls.add(ttl);
                    var capabilities = "0".equals(ttl)
                            ? java.util.Set.of(
                                    ai.protomolt.proto.repo.blob.spi.BlobCapability.NON_EXPIRING_WRITES,
                                    ai.protomolt.proto.repo.blob.spi.BlobCapability.PHYSICAL_RECLAMATION)
                            : java.util.Set.of(
                                    ai.protomolt.proto.repo.blob.spi.BlobCapability.OBJECT_EXPIRY,
                                    ai.protomolt.proto.repo.blob.spi.BlobCapability.PHYSICAL_RECLAMATION);
                    var store = (ai.protomolt.proto.repo.blob.spi.BlobStore) java.lang.reflect.Proxy.newProxyInstance(
                            ai.protomolt.proto.repo.blob.spi.BlobStore.class.getClassLoader(),
                            new Class<?>[]{ai.protomolt.proto.repo.blob.spi.BlobStore.class},
                            (proxy, method, args) -> { throw new AssertionError("No byte IO expected"); });
                    return new ai.protomolt.proto.repo.blob.spi.OpenedBlobStore(
                            store, closed::incrementAndGet, capabilities, name -> {}, (b, k) -> true);
                }
            };
            var providers = ai.protomolt.proto.repo.blob.spi.BlobStores.of(java.util.List.of(stubProvider));

            var defaultHost = RepoServiceConfig.fromEnvironment(java.util.Map.of(
                    RepoServiceConfig.ENV_BLOB_STORE, hostMode));
            try (var owned = new OwnedResources()) {
                var wired = RepoStorageWiring.wire(defaultHost, providers, false, owned, null, null);
                assertThat(wired.blobStore()).isNotNull();
                assertThat(seenTtls).containsExactly("0");
            }
            assertThat(closed.get()).isEqualTo(1);

            var expiringHost = RepoServiceConfig.fromEnvironment(java.util.Map.of(
                    RepoServiceConfig.ENV_BLOB_STORE, hostMode,
                    RepoServiceConfig.ENV_VALKEY_TTL_SECONDS, "60"));
            try (var owned = new OwnedResources()) {
                assertThatThrownBy(() -> RepoStorageWiring.wire(expiringHost, providers, false, owned, null, null))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("retain bytes until explicit purge");
            }
            assertThat(seenTtls).containsExactly("0", "60");
            assertThat(closed.get()).isEqualTo(2);
        }
    }

    @Test
    void httpPortZeroMeansDisabled() {
        assertThat(config(0, null).httpPort()).isZero();
    }

    @Test
    void unknownBlobStoreIsRejected() {
        assertThatThrownBy(() -> config(0, "gcs"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_BLOB_STORE");
    }

    @Test
    void constructorsRejectRetiredModes() {
        for (var mode : java.util.List.of("repo", "repo-inprocess")) {
            assertThatThrownBy(() -> config(0, mode))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Remote repository blob storage is retired: " + mode);
        }
    }

    @Test
    void redisModesNeedNoTargetAndCarryRedisProps() {
        RepoServiceConfig redis = config(0, "redis");
        assertThat(redis.blobStore()).isEqualTo("redis");

        RepoServiceConfig cache = config(0, "s3-redis-cache");
        assertThat(cache.blobStore()).isEqualTo("s3-redis-cache");

        RepoServiceConfig explicit = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, "redis",
                "redis://redis.internal:6380/2", 60, 4096L);
        assertThat(explicit.redisUri()).isEqualTo("redis://redis.internal:6380/2");
        assertThat(explicit.redisTtlSeconds()).isEqualTo(60);
        assertThat(explicit.redisMaxObjectBytes()).isEqualTo(4096L);
    }

    @Test
    void kafkaIsOffUnlessBootstrapServersAreSet() {
        RepoServiceConfig off = config(-1, null);
        assertThat(off.kafkaEnabled()).isFalse();
        assertThat(off.kafkaBootstrapServers()).isNull();
        assertThat(off.kafkaTopic()).isEqualTo("document-events");

        RepoServiceConfig on = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker-1:9092,broker-2:9092", null);
        assertThat(on.kafkaEnabled()).isTrue();
        assertThat(on.kafkaBootstrapServers()).isEqualTo("broker-1:9092,broker-2:9092");
        assertThat(on.kafkaTopic()).isEqualTo("document-events");

        RepoServiceConfig blank = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "  ", "other-topic");
        assertThat(blank.kafkaEnabled()).isFalse();
        assertThat(blank.kafkaTopic()).isEqualTo("other-topic");
    }

    @Test
    void schemaRegistryUrlIsNullUnlessSet() {
        RepoServiceConfig unset = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, null);
        assertThat(unset.schemaRegistryUrl()).isNull();

        RepoServiceConfig set = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, "http://registry:8081");
        assertThat(set.schemaRegistryUrl()).isEqualTo("http://registry:8081");

        RepoServiceConfig blank = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, "  ");
        assertThat(blank.schemaRegistryUrl()).isNull();

        // The 22-component compatibility constructor stays registry-free.
        RepoServiceConfig compat = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null);
        assertThat(compat.schemaRegistryUrl()).isNull();
    }

    @Test
    void seedAccountIdIsNullUnlessSet() {
        // Every compatibility constructor leaves seeding off.
        assertThat(config(-1, null).seedAccountId()).isNull();
        RepoServiceConfig compat = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null);
        assertThat(compat.seedAccountId()).isNull();

        // Blank (and whitespace) normalizes to null: no seeding.
        RepoServiceConfig blank = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, "  ");
        assertThat(blank.seedAccountId()).isNull();

        RepoServiceConfig set = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, " standalone ");
        assertThat(set.seedAccountId()).isEqualTo("standalone");
    }

    @Test
    void purgeQueueDefaultsToJdbcWithTheDefaultPurgeTopic() {
        // Every compatibility constructor keeps the JDBC queue.
        assertThat(config(-1, null).purgeQueue()).isEqualTo("jdbc");
        RepoServiceConfig compat = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, null);
        assertThat(compat.purgeQueue()).isEqualTo("jdbc");
        assertThat(compat.kafkaPurgeTopic()).isEqualTo("document-purges");
    }

    @Test
    void unknownPurgeQueueIsRejected() {
        assertThatThrownBy(() -> new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, null, "rabbit", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_PURGE_QUEUE");
    }

    @Test
    void kafkaPurgeQueueRequiresBootstrapServers() {
        assertThatThrownBy(() -> new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, null, "kafka", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS");

        RepoServiceConfig kafka = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, null, null, " KAFKA ", "purges-v2");
        assertThat(kafka.purgeQueue()).isEqualTo("kafka");
        assertThat(kafka.kafkaPurgeTopic()).isEqualTo("purges-v2");

        RepoServiceConfig defaultTopic = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, null, null, "kafka", "  ");
        assertThat(defaultTopic.kafkaPurgeTopic()).isEqualTo("document-purges");
    }
}
