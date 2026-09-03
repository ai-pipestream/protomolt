package ai.protomolt.proto.repo.container.ledger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LedgerConfig} fallback semantics: every blank component resolves to
 * the docker-compose development default, and {@code fromEnvironment()} is
 * env-or-default per variable.
 */
class LedgerConfigTest {

    @Test
    void blankComponentsFallBackToTheDevelopmentDefaults() {
        LedgerConfig config = new LedgerConfig(null, null, null, 0, null);

        assertThat(config.jdbcUrl()).isEqualTo(LedgerConfig.DEFAULT_JDBC_URL);
        assertThat(config.username()).isEqualTo(LedgerConfig.DEFAULT_USERNAME);
        assertThat(config.password()).isEqualTo(LedgerConfig.DEFAULT_PASSWORD);
        assertThat(config.maxPoolSize()).isEqualTo(LedgerConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation()).isEqualTo(LedgerConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void blankStringsAreTreatedAsAbsent() {
        LedgerConfig config = new LedgerConfig("", "  ", null, -1, " ");

        assertThat(config.jdbcUrl()).isEqualTo(LedgerConfig.DEFAULT_JDBC_URL);
        assertThat(config.username()).isEqualTo(LedgerConfig.DEFAULT_USERNAME);
        assertThat(config.maxPoolSize()).isEqualTo(LedgerConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation()).isEqualTo(LedgerConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void explicitValuesAreKept() {
        LedgerConfig config = new LedgerConfig(
                "jdbc:postgresql://db:5432/x", "user", "secret", 25, "classpath:other");

        assertThat(config.jdbcUrl()).isEqualTo("jdbc:postgresql://db:5432/x");
        assertThat(config.username()).isEqualTo("user");
        assertThat(config.password()).isEqualTo("secret");
        assertThat(config.maxPoolSize()).isEqualTo(25);
        assertThat(config.migrationLocation()).isEqualTo("classpath:other");
    }

    @Test
    void aBlankPasswordStaysBlankOnlyNullIsDefaulted() {
        // The compact constructor defaults a null password but deliberately
        // leaves a blank one alone (password-less local setups stay sayable).
        assertThat(new LedgerConfig("jdbc:x", "u", null, 1, "m").password())
                .isEqualTo(LedgerConfig.DEFAULT_PASSWORD);
        assertThat(new LedgerConfig("jdbc:x", "u", "", 1, "m").password()).isEmpty();
    }

    @Test
    void convenienceConstructorUsesTheDefaultPoolAndMigrationLocation() {
        LedgerConfig config = new LedgerConfig("jdbc:postgresql://h/d", "u", "p");

        assertThat(config.maxPoolSize()).isEqualTo(LedgerConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation()).isEqualTo(LedgerConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void fromEnvironmentResolvesEachVariableOrItsDefault() {
        // Env-aware: whatever the build machine exports, the result must be
        // the env value when set (and parseable) and the default otherwise.
        LedgerConfig config = LedgerConfig.fromEnvironment();

        assertThat(config.jdbcUrl()).isEqualTo(envOrDefault(LedgerConfig.ENV_JDBC_URL,
                LedgerConfig.DEFAULT_JDBC_URL));
        assertThat(config.username()).isEqualTo(envOrDefault(LedgerConfig.ENV_USERNAME,
                LedgerConfig.DEFAULT_USERNAME));
        assertThat(config.password()).isEqualTo(envOrDefault(LedgerConfig.ENV_PASSWORD,
                LedgerConfig.DEFAULT_PASSWORD));
        assertThat(config.migrationLocation()).isEqualTo(LedgerConfig.DEFAULT_MIGRATION_LOCATION);

        String poolEnv = System.getenv(LedgerConfig.ENV_POOL_SIZE);
        int expectedPool = LedgerConfig.DEFAULT_POOL_SIZE;
        if (poolEnv != null && !poolEnv.isBlank()) {
            try {
                int parsed = Integer.parseInt(poolEnv.trim());
                expectedPool = parsed > 0 ? parsed : LedgerConfig.DEFAULT_POOL_SIZE;
            } catch (NumberFormatException ignored) {
                // unparsable env -> default
            }
        }
        assertThat(config.maxPoolSize()).isEqualTo(expectedPool);
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
