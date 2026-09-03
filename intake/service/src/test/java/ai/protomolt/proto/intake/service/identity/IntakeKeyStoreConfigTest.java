package ai.protomolt.proto.intake.service.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * {@link IntakeKeyStoreConfig}: the deliberate difference from the account
 * store's config is pinned here — connection settings are REQUIRED, never
 * defaulted (an air-gapped key store silently pointed at localhost would be
 * a lie), while pool size and migration location keep the fallback style.
 * No containers.
 */
class IntakeKeyStoreConfigTest {

    private static final String URL = "jdbc:postgresql://db.internal:5432/intakekeys";

    @Test
    void explicitValuesPassThrough() {
        IntakeKeyStoreConfig config =
                new IntakeKeyStoreConfig(URL, "alice", "s3cret", 25, "classpath:custom");
        assertThat(config.jdbcUrl()).isEqualTo(URL);
        assertThat(config.username()).isEqualTo("alice");
        assertThat(config.password()).isEqualTo("s3cret");
        assertThat(config.maxPoolSize()).isEqualTo(25);
        assertThat(config.migrationLocation()).isEqualTo("classpath:custom");
    }

    @Test
    void connectionSettingsAreRequiredNotDefaulted() {
        assertThatThrownBy(() -> new IntakeKeyStoreConfig(" ", "u", "p"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jdbcUrl");
        assertThatThrownBy(() -> new IntakeKeyStoreConfig(URL, null, "p"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("username");
        assertThatThrownBy(() -> new IntakeKeyStoreConfig(URL, "u", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void emptyPasswordIsKeptNotRejected() {
        // Only a NULL password is missing: an empty password is a deliberate
        // configuration (trust auth).
        assertThat(new IntakeKeyStoreConfig(URL, "u", "").password()).isEmpty();
    }

    @Test
    void onlyPoolAndLocationFallBack() {
        IntakeKeyStoreConfig config = new IntakeKeyStoreConfig(URL, "u", "p", -3, " ");
        assertThat(config.maxPoolSize()).isEqualTo(IntakeKeyStoreConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation())
                .isEqualTo(IntakeKeyStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void convenienceConstructorUsesDefaultPoolAndLocation() {
        IntakeKeyStoreConfig config = new IntakeKeyStoreConfig(URL, "u", "p");
        assertThat(config.maxPoolSize()).isEqualTo(IntakeKeyStoreConfig.DEFAULT_POOL_SIZE);
        assertThat(config.migrationLocation())
                .isEqualTo(IntakeKeyStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void fromEnvironmentMapDemandsTheWholeConnectionFamilyByName() {
        assertThatThrownBy(() -> IntakeKeyStoreConfig.fromEnvironmentMap(Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IntakeKeyStoreConfig.ENV_JDBC_URL);
        assertThatThrownBy(() -> IntakeKeyStoreConfig.fromEnvironmentMap(
                        Map.of(IntakeKeyStoreConfig.ENV_JDBC_URL, URL)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IntakeKeyStoreConfig.ENV_USERNAME);
        assertThatThrownBy(() -> IntakeKeyStoreConfig.fromEnvironmentMap(
                        Map.of(
                                IntakeKeyStoreConfig.ENV_JDBC_URL, URL,
                                IntakeKeyStoreConfig.ENV_USERNAME, "u")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IntakeKeyStoreConfig.ENV_PASSWORD);
    }

    @Test
    void fromEnvironmentMapResolvesTheFamily() {
        IntakeKeyStoreConfig config = IntakeKeyStoreConfig.fromEnvironmentMap(Map.of(
                IntakeKeyStoreConfig.ENV_JDBC_URL, " " + URL + " ",
                IntakeKeyStoreConfig.ENV_USERNAME, "keys",
                IntakeKeyStoreConfig.ENV_PASSWORD, "",
                IntakeKeyStoreConfig.ENV_POOL_SIZE, "4"));
        assertThat(config.jdbcUrl()).isEqualTo(URL);
        assertThat(config.username()).isEqualTo("keys");
        assertThat(config.password()).isEmpty();
        assertThat(config.maxPoolSize()).isEqualTo(4);
        assertThat(config.migrationLocation())
                .isEqualTo(IntakeKeyStoreConfig.DEFAULT_MIGRATION_LOCATION);
    }

    @Test
    void unparseablePoolSizeFallsBackToDefault() {
        IntakeKeyStoreConfig config = IntakeKeyStoreConfig.fromEnvironmentMap(Map.of(
                IntakeKeyStoreConfig.ENV_JDBC_URL, URL,
                IntakeKeyStoreConfig.ENV_USERNAME, "keys",
                IntakeKeyStoreConfig.ENV_PASSWORD, "p",
                IntakeKeyStoreConfig.ENV_POOL_SIZE, "many"));
        assertThat(config.maxPoolSize()).isEqualTo(IntakeKeyStoreConfig.DEFAULT_POOL_SIZE);
    }
}
