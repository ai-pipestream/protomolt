package ai.pipestream.proto.jobs.service.store;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The store's connection/migration settings record: a missing JDBC URL or
 * username fails construction (no "reasonable default" database), while the
 * password, pool size, and migration location fall back.
 */
class ChainJobStoreConfigTest {

    @Test
    void aMissingJdbcUrlIsRejected() {
        assertThatThrownBy(() -> new ChainJobStoreConfig(null, "jobs", "secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jdbcUrl is required");
        assertThatThrownBy(() -> new ChainJobStoreConfig("  ", "jobs", "secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jdbcUrl is required");
    }

    @Test
    void aMissingUsernameIsRejected() {
        assertThatThrownBy(() -> new ChainJobStoreConfig("jdbc:postgresql://db/jobs", null, "x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("username is required")
                .hasMessageContaining("jdbc:postgresql://db/jobs");
        assertThatThrownBy(() -> new ChainJobStoreConfig("jdbc:postgresql://db/jobs", "", "x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNullPasswordBecomesEmptyForTrustAuth() {
        ChainJobStoreConfig config =
                new ChainJobStoreConfig("jdbc:postgresql://db/jobs", "jobs", null);
        assertThat(config.password()).isEmpty();
    }

    @Test
    void aNonPositivePoolSizeFallsBack() {
        ChainJobStoreConfig zero = new ChainJobStoreConfig(
                "jdbc:postgresql://db/jobs", "jobs", "x", 0, null);
        assertThat(zero.maxPoolSize()).isEqualTo(ChainJobStoreConfig.DEFAULT_POOL_SIZE);
        ChainJobStoreConfig negative = new ChainJobStoreConfig(
                "jdbc:postgresql://db/jobs", "jobs", "x", -4, null);
        assertThat(negative.maxPoolSize()).isEqualTo(ChainJobStoreConfig.DEFAULT_POOL_SIZE);
    }

    @Test
    void aMissingMigrationLocationFallsBack() {
        assertThat(new ChainJobStoreConfig("jdbc:postgresql://db/jobs", "jobs", "x", 5, null)
                .migrationLocation()).isEqualTo(ChainJobStoreConfig.DEFAULT_MIGRATION_LOCATION);
        assertThat(new ChainJobStoreConfig("jdbc:postgresql://db/jobs", "jobs", "x", 5, " ")
                .migrationLocation()).isEqualTo(ChainJobStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void theConvenienceConstructorUsesTheDefaults() {
        ChainJobStoreConfig config =
                new ChainJobStoreConfig("jdbc:postgresql://db/jobs", "jobs", "secret");
        assertThat(config.jdbcUrl()).isEqualTo("jdbc:postgresql://db/jobs");
        assertThat(config.username()).isEqualTo("jobs");
        assertThat(config.password()).isEqualTo("secret");
        assertThat(config.maxPoolSize()).isEqualTo(ChainJobStoreConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation())
                .isEqualTo(ChainJobStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void explicitValuesAreKept() {
        ChainJobStoreConfig config = new ChainJobStoreConfig(
                "jdbc:postgresql://db/jobs", "jobs", "secret", 3, "classpath:other/migrations");
        assertThat(config.maxPoolSize()).isEqualTo(3);
        assertThat(config.migrationLocation()).isEqualTo("classpath:other/migrations");
    }
}
