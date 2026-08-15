package ai.pipestream.proto.jobs.service.store;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The store's connection/migration settings record: a missing JDBC URL or
 * username fails construction (no "reasonable default" database), while the
 * password, pool size, and migration location fall back.
 */
class WorkflowRunStoreConfigTest {

    @Test
    void aMissingJdbcUrlIsRejected() {
        assertThatThrownBy(() -> new WorkflowRunStoreConfig(null, "jobs", "secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jdbcUrl is required");
        assertThatThrownBy(() -> new WorkflowRunStoreConfig("  ", "jobs", "secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jdbcUrl is required");
    }

    @Test
    void aMissingUsernameIsRejected() {
        assertThatThrownBy(() -> new WorkflowRunStoreConfig("jdbc:postgresql://db/jobs", null, "x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("username is required")
                .hasMessageContaining("jdbc:postgresql://db/jobs");
        assertThatThrownBy(() -> new WorkflowRunStoreConfig("jdbc:postgresql://db/jobs", "", "x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNullPasswordBecomesEmptyForTrustAuth() {
        WorkflowRunStoreConfig config =
                new WorkflowRunStoreConfig("jdbc:postgresql://db/jobs", "jobs", null);
        assertThat(config.password()).isEmpty();
    }

    @Test
    void aNonPositivePoolSizeFallsBack() {
        WorkflowRunStoreConfig zero = new WorkflowRunStoreConfig(
                "jdbc:postgresql://db/jobs", "jobs", "x", 0, null);
        assertThat(zero.maxPoolSize()).isEqualTo(WorkflowRunStoreConfig.DEFAULT_POOL_SIZE);
        WorkflowRunStoreConfig negative = new WorkflowRunStoreConfig(
                "jdbc:postgresql://db/jobs", "jobs", "x", -4, null);
        assertThat(negative.maxPoolSize()).isEqualTo(WorkflowRunStoreConfig.DEFAULT_POOL_SIZE);
    }

    @Test
    void aMissingMigrationLocationFallsBack() {
        assertThat(new WorkflowRunStoreConfig("jdbc:postgresql://db/jobs", "jobs", "x", 5, null)
                .migrationLocation()).isEqualTo(WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION);
        assertThat(new WorkflowRunStoreConfig("jdbc:postgresql://db/jobs", "jobs", "x", 5, " ")
                .migrationLocation()).isEqualTo(WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void theConvenienceConstructorUsesTheDefaults() {
        WorkflowRunStoreConfig config =
                new WorkflowRunStoreConfig("jdbc:postgresql://db/jobs", "jobs", "secret");
        assertThat(config.jdbcUrl()).isEqualTo("jdbc:postgresql://db/jobs");
        assertThat(config.username()).isEqualTo("jobs");
        assertThat(config.password()).isEqualTo("secret");
        assertThat(config.maxPoolSize()).isEqualTo(WorkflowRunStoreConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation())
                .isEqualTo(WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void explicitValuesAreKept() {
        WorkflowRunStoreConfig config = new WorkflowRunStoreConfig(
                "jdbc:postgresql://db/jobs", "jobs", "secret", 3, "classpath:other/migrations");
        assertThat(config.maxPoolSize()).isEqualTo(3);
        assertThat(config.migrationLocation()).isEqualTo("classpath:other/migrations");
    }
}
