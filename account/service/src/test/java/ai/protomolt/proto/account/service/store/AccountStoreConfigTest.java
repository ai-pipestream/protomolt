package ai.protomolt.proto.account.service.store;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AccountStoreConfig}: the record's normalization rules — which blanks
 * fall back to the local-Postgres defaults and which values pass through.
 * Only the compact constructor's branches are pinned; {@code fromEnvironment}
 * is exercised through {@code AccountServiceConfigTest}. No containers.
 */
class AccountStoreConfigTest {

    private static final String URL = "jdbc:postgresql://db.internal:5432/accounts";
    private static final String LOCATION = "classpath:db/migration";

    @Test
    void explicitValuesPassThrough() {
        AccountStoreConfig config = new AccountStoreConfig(URL, "alice", "s3cret", 25, LOCATION);
        assertThat(config.jdbcUrl()).isEqualTo(URL);
        assertThat(config.username()).isEqualTo("alice");
        assertThat(config.password()).isEqualTo("s3cret");
        assertThat(config.maxPoolSize()).isEqualTo(25);
        assertThat(config.migrationLocation()).isEqualTo(LOCATION);
    }

    @Test
    void blanksFallBackToTheLocalDefaults() {
        AccountStoreConfig config = new AccountStoreConfig("  ", null, null, 0, " ");
        assertThat(config.jdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5432/accounts");
        assertThat(config.username()).isEqualTo("accounts");
        assertThat(config.password()).isEqualTo("accounts");
        assertThat(config.maxPoolSize()).isEqualTo(AccountStoreConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation())
                .isEqualTo(AccountStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void negativePoolSizeFallsBackToDefault() {
        assertThat(new AccountStoreConfig(URL, "u", "p", -3, LOCATION).maxPoolSize())
                .isEqualTo(AccountStoreConfig.DEFAULT_POOL_SIZE);
    }

    @Test
    void emptyPasswordIsKeptNotDefaulted() {
        // Only a NULL password falls back: an empty password is a deliberate
        // configuration (trust auth), not a missing one.
        assertThat(new AccountStoreConfig(URL, "u", "", 5, LOCATION).password()).isEmpty();
    }

    @Test
    void convenienceConstructorUsesDefaultPoolAndLocation() {
        AccountStoreConfig config = new AccountStoreConfig(URL, "u", "p");
        assertThat(config.jdbcUrl()).isEqualTo(URL);
        assertThat(config.maxPoolSize()).isEqualTo(AccountStoreConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation())
                .isEqualTo(AccountStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }
}
