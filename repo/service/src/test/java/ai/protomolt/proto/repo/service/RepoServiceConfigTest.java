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

    @Test void explicitBlankStorageAndDefaultedTextSettingsCannotSelectAnotherConfiguration() {
        for (var name : java.util.List.of(RepoServiceConfig.ENV_BLOB_STORE, RepoServiceConfig.ENV_REDIS_URI,
                RepoServiceConfig.ENV_REPO_DRIVE, RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE,
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
                RepoServiceConfig.ENV_PURGE_INTERVAL_MS, RepoServiceConfig.ENV_SWEEP_INTERVAL_MS,
                RepoServiceConfig.ENV_RECONCILE_MIN_AGE_MS, LedgerConfig.ENV_POOL_SIZE);
        var malformed = names.stream().flatMap(name -> java.util.stream.Stream.of("private-value", "", " ",
                "1.5", "999999999999999999999999", "-1")
                .map(value -> org.junit.jupiter.params.provider.Arguments.of(name, value)));
        return java.util.stream.Stream.concat(malformed, java.util.stream.Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_GRPC_PORT, "65536"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_HTTP_PORT, "65536"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_REDIS_TTL_SECONDS, "2147483648"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_PURGE_INTERVAL_MS, "0"),
                org.junit.jupiter.params.provider.Arguments.of(RepoServiceConfig.ENV_SWEEP_INTERVAL_MS, "0"),
                org.junit.jupiter.params.provider.Arguments.of(LedgerConfig.ENV_POOL_SIZE, "0")));
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
    }

    @Test void managedQualificationSurvivesConfigurationCopies() {
        var policy = new ManagedStoragePolicy("nas-v1", "account-a", true);
        var configured = config(0, "s3", null, null).withManagedStorage(policy);
        assertThat(configured.withRepoBucketBindings(java.util.Map.of()).managedStorage()).isEqualTo(policy);
        assertThat(config(0, "s3", null, null).managedStorage()).isEqualTo(ManagedStoragePolicy.disabled());
    }

    @Test void unsupportedManagedCompositionFailsBeforeOpeningExternalResources() {
        var configured = config(0, "redis", null, null)
                .withManagedStorage(new ManagedStoragePolicy("nas-v1", "account-a", true));
        assertThatThrownBy(() -> RepoServices.build(configured))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Managed storage requires an S3 backing store and enabled lifecycle recovery");
    }

    private static final LedgerConfig LEDGER =
            new LedgerConfig("jdbc:postgresql://localhost:5432/x", "u", "p");

    private static RepoServiceConfig config(int httpPort, String blobStore,
            String repoTarget, String repoDrive) {
        return new RepoServiceConfig(0, LEDGER, null, null, null, null, null,
                httpPort, blobStore, repoTarget, repoDrive, null, -1, -1L);
    }

    @Test
    void defaultsKeepTodaysBehavior() {
        RepoServiceConfig config = config(-1, null, null, null);
        assertThat(config.httpPort()).isEqualTo(8080);
        assertThat(config.blobStore()).isEqualTo("s3");
        assertThat(config.repoDrive()).isEqualTo("default");
        assertThat(config.repoTarget()).isNull();
        assertThat(config.redisUri()).isEqualTo("redis://localhost:6379");
        assertThat(config.redisTtlSeconds()).isEqualTo(3600);
        assertThat(config.redisMaxObjectBytes()).isEqualTo(8388608L);
    }

    @Test
    void httpPortZeroMeansDisabled() {
        assertThat(config(0, null, null, null).httpPort()).isZero();
    }

    @Test
    void unknownBlobStoreIsRejected() {
        assertThatThrownBy(() -> config(0, "gcs", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_BLOB_STORE");
    }

    @Test
    void repoModesRequireATarget() {
        assertThatThrownBy(() -> config(0, "repo", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_REPO_TARGET");
        assertThatThrownBy(() -> config(0, "repo-inprocess", "  ", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_REPO_TARGET");

        RepoServiceConfig repo = config(0, "repo", "repo-backend:9090", null);
        assertThat(repo.repoTarget()).isEqualTo("repo-backend:9090");
        assertThat(repo.blobStore()).isEqualTo("repo");
        RepoServiceConfig inProcess = config(0, "repo-inprocess", "backend-inproc", "blobs");
        assertThat(inProcess.repoDrive()).isEqualTo("blobs");
    }

    @Test
    void redisModesNeedNoTargetAndCarryRedisProps() {
        RepoServiceConfig redis = config(0, "redis", null, null);
        assertThat(redis.blobStore()).isEqualTo("redis");
        assertThat(redis.repoTarget()).isNull();

        RepoServiceConfig cache = config(0, "s3-redis-cache", null, null);
        assertThat(cache.blobStore()).isEqualTo("s3-redis-cache");

        RepoServiceConfig explicit = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, "redis", null, null,
                "redis://redis.internal:6380/2", 60, 4096L);
        assertThat(explicit.redisUri()).isEqualTo("redis://redis.internal:6380/2");
        assertThat(explicit.redisTtlSeconds()).isEqualTo(60);
        assertThat(explicit.redisMaxObjectBytes()).isEqualTo(4096L);
    }

    @Test
    void kafkaIsOffUnlessBootstrapServersAreSet() {
        RepoServiceConfig off = config(-1, null, null, null);
        assertThat(off.kafkaEnabled()).isFalse();
        assertThat(off.kafkaBootstrapServers()).isNull();
        assertThat(off.kafkaTopic()).isEqualTo("document-events");

        RepoServiceConfig on = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker-1:9092,broker-2:9092", null);
        assertThat(on.kafkaEnabled()).isTrue();
        assertThat(on.kafkaBootstrapServers()).isEqualTo("broker-1:9092,broker-2:9092");
        assertThat(on.kafkaTopic()).isEqualTo("document-events");

        RepoServiceConfig blank = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "  ", "other-topic");
        assertThat(blank.kafkaEnabled()).isFalse();
        assertThat(blank.kafkaTopic()).isEqualTo("other-topic");
    }

    @Test
    void schemaRegistryUrlIsNullUnlessSet() {
        RepoServiceConfig unset = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, null);
        assertThat(unset.schemaRegistryUrl()).isNull();

        RepoServiceConfig set = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, "http://registry:8081");
        assertThat(set.schemaRegistryUrl()).isEqualTo("http://registry:8081");

        RepoServiceConfig blank = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, "  ");
        assertThat(blank.schemaRegistryUrl()).isNull();

        // The 22-component compatibility constructor stays registry-free.
        RepoServiceConfig compat = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null);
        assertThat(compat.schemaRegistryUrl()).isNull();
    }

    @Test
    void seedAccountIdIsNullUnlessSet() {
        // Every compatibility constructor leaves seeding off.
        assertThat(config(-1, null, null, null).seedAccountId()).isNull();
        RepoServiceConfig compat = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null);
        assertThat(compat.seedAccountId()).isNull();

        // Blank (and whitespace) normalizes to null: no seeding.
        RepoServiceConfig blank = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, "  ");
        assertThat(blank.seedAccountId()).isNull();

        RepoServiceConfig set = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, " standalone ");
        assertThat(set.seedAccountId()).isEqualTo("standalone");
    }

    @Test
    void purgeQueueDefaultsToJdbcWithTheDefaultPurgeTopic() {
        // Every compatibility constructor keeps the JDBC queue.
        assertThat(config(-1, null, null, null).purgeQueue()).isEqualTo("jdbc");
        RepoServiceConfig compat = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, null);
        assertThat(compat.purgeQueue()).isEqualTo("jdbc");
        assertThat(compat.kafkaPurgeTopic()).isEqualTo("document-purges");
    }

    @Test
    void unknownPurgeQueueIsRejected() {
        assertThatThrownBy(() -> new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, null, "rabbit", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_PURGE_QUEUE");
    }

    @Test
    void kafkaPurgeQueueRequiresBootstrapServers() {
        assertThatThrownBy(() -> new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                null, null, null, null, "kafka", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS");

        RepoServiceConfig kafka = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, null, null, " KAFKA ", "purges-v2");
        assertThat(kafka.purgeQueue()).isEqualTo("kafka");
        assertThat(kafka.kafkaPurgeTopic()).isEqualTo("purges-v2");

        RepoServiceConfig defaultTopic = new RepoServiceConfig(0, LEDGER, null, null, null, null,
                null, 0, null, null, null, null, -1, -1L,
                true, -1L, -1L, false, true, -1L,
                "broker:9092", null, null, null, "kafka", "  ");
        assertThat(defaultTopic.kafkaPurgeTopic()).isEqualTo("document-purges");
    }
}
