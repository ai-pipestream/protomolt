package ai.protomolt.proto.authz.jdbc;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pool-size validation: an unset size falls back, but a malformed or non-positive
 * one is refused by variable name instead of silently starting with the default.
 */
class CallerStoreConfigTest {

    private static Map<String, String> requiredEnv() {
        return Map.of(
                CallerStoreConfig.ENV_JDBC_URL, "jdbc:postgresql://db.internal:5432/authz",
                CallerStoreConfig.ENV_USERNAME, "svc",
                CallerStoreConfig.ENV_PASSWORD, "");
    }

    private static Map<String, String> envWithPoolSize(String poolSize) {
        java.util.Map<String, String> env = new java.util.HashMap<>(requiredEnv());
        env.put(CallerStoreConfig.ENV_POOL_SIZE, poolSize);
        return env;
    }

    @Test
    void unsetPoolSizeFallsBackToTheDefault() {
        CallerStoreConfig config = CallerStoreConfig.fromEnvironmentMap(requiredEnv());
        assertThat(config.maxPoolSize()).isEqualTo(CallerStoreConfig.DEFAULT_POOL_SIZE);
    }

    @Test
    void blankPoolSizeFallsBackToTheDefault() {
        CallerStoreConfig config = CallerStoreConfig.fromEnvironmentMap(envWithPoolSize("  "));
        assertThat(config.maxPoolSize()).isEqualTo(CallerStoreConfig.DEFAULT_POOL_SIZE);
    }

    @Test
    void aPositivePoolSizeIsHonoredTrimmed() {
        CallerStoreConfig config = CallerStoreConfig.fromEnvironmentMap(envWithPoolSize(" 25 "));
        assertThat(config.maxPoolSize()).isEqualTo(25);
    }

    @Test
    void aMalformedPoolSizeIsRefusedByName() {
        assertThatThrownBy(() -> CallerStoreConfig.fromEnvironmentMap(envWithPoolSize("ten")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CallerStoreConfig.ENV_POOL_SIZE)
                .hasMessageContaining("positive integer");
    }

    @Test
    void zeroAndNegativePoolSizesAreRefusedByName() {
        for (String bad : new String[]{"0", "-3"}) {
            assertThatThrownBy(() -> CallerStoreConfig.fromEnvironmentMap(envWithPoolSize(bad)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(CallerStoreConfig.ENV_POOL_SIZE);
        }
    }

    @Test
    void aNonPositivePoolSizeIsRefusedThroughTheConstructorToo() {
        assertThatThrownBy(() -> new CallerStoreConfig(
                "jdbc:postgresql://db.internal:5432/authz", "svc", "", 0,
                CallerStoreConfig.DEFAULT_MIGRATION_LOCATION))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPoolSize")
                .hasMessageContaining(CallerStoreConfig.ENV_POOL_SIZE);
    }
}
