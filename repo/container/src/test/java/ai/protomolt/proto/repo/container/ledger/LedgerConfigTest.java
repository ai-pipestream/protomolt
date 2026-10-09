package ai.protomolt.proto.repo.container.ledger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LedgerConfig} fallback semantics: every blank component resolves to
 * the docker-compose development default, and {@code fromEnvironment()} is
 * env-or-default per variable.
 */
class LedgerConfigTest {

    @Test void blankConfiguredDatabaseCannotSelectTheDefaultDatabase() {
        for (var name : java.util.List.of(LedgerConfig.ENV_JDBC_URL, LedgerConfig.ENV_USERNAME)) {
            for (var blank : java.util.List.of("", " ")) {
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> LedgerConfig.fromEnvironment(java.util.Map.of(name, blank)))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(name);
            }
        }
        assertThat(LedgerConfig.fromEnvironment(java.util.Map.of(LedgerConfig.ENV_PASSWORD, "")).password()).isEmpty();
    }

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
    void fromEnvironmentResolvesSuppliedSnapshotWithoutUsingProcessSettings() {
        LedgerConfig config = LedgerConfig.fromEnvironment(java.util.Map.of(
                LedgerConfig.ENV_JDBC_URL, "jdbc:postgresql://configured/database",
                LedgerConfig.ENV_USERNAME, "configured-user", LedgerConfig.ENV_PASSWORD, "configured-password",
                LedgerConfig.ENV_POOL_SIZE, " 7 "));
        assertThat(config.jdbcUrl()).isEqualTo("jdbc:postgresql://configured/database");
        assertThat(config.username()).isEqualTo("configured-user");
        assertThat(config.password()).isEqualTo("configured-password");
        assertThat(config.migrationLocation()).isEqualTo(LedgerConfig.DEFAULT_MIGRATION_LOCATION);
        assertThat(config.maxPoolSize()).isEqualTo(7);
        assertThat(LedgerConfig.fromEnvironment(java.util.Map.of()).maxPoolSize()).isEqualTo(LedgerConfig.DEFAULT_POOL_SIZE);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"", " ", "-1", "0", "1.5", "2147483648", "private-value"})
    void configuredPoolMustBeAPositiveInteger(String value) {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> LedgerConfig.fromEnvironment(
                java.util.Map.of(LedgerConfig.ENV_POOL_SIZE, value)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(LedgerConfig.ENV_POOL_SIZE)
                .hasMessageNotContaining("private-value").hasNoCause();
    }
}
