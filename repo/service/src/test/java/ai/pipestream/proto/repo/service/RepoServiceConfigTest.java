package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
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
}
