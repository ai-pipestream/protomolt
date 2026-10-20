package ai.protomolt.proto.account.service;

import ai.protomolt.proto.account.service.store.AccountStoreConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link AccountServiceConfig}: the defaults, the kafka-off
 * convention, and the repo-target transport selection. Only the logic that
 * can actually break is tested — no ceremonial default echoing.
 */
class AccountServiceConfigTest {

    private static final AccountStoreConfig STORE =
            new AccountStoreConfig("jdbc:postgresql://localhost:5432/x", "u", "p");

    @Test
    void defaults() {
        AccountServiceConfig config = new AccountServiceConfig(-1, STORE, null, null, null, null,
                true, -1L);
        assertThat(config.grpcPort()).isEqualTo(9091);
        assertThat(config.repoGrpcTarget()).isEqualTo("localhost:9090");
        assertThat(config.repoTargetIsInProcess()).isFalse();
        assertThat(config.repoTargetName()).isEqualTo("localhost:9090");
        assertThat(config.kafkaEnabled()).isFalse();
        assertThat(config.kafkaBootstrapServers()).isNull();
        assertThat(config.kafkaTopic()).isEqualTo("account-events");
        assertThat(config.schemaRegistryUrl()).isNull();
        assertThat(config.relayIntervalMs()).isEqualTo(5000L);
    }

    @Test
    void inprocessTargetConvention() {
        AccountServiceConfig config = new AccountServiceConfig(0, STORE, "inprocess:fake-repo",
                null, null, null, true, -1L);
        assertThat(config.repoTargetIsInProcess()).isTrue();
        assertThat(config.repoTargetName()).isEqualTo("fake-repo");
        assertThat(config.repoGrpcTarget()).isEqualTo("inprocess:fake-repo");
    }

    @Test
    void kafkaIsOffUnlessBootstrapServersAreSet() {
        AccountServiceConfig off = new AccountServiceConfig(0, STORE, null, "  ", null, null,
                true, -1L);
        assertThat(off.kafkaEnabled()).isFalse();
        assertThat(off.kafkaTopic()).isEqualTo("account-events");

        AccountServiceConfig on = new AccountServiceConfig(0, STORE, null,
                "broker-1:9092,broker-2:9092", "other-topic", "http://registry:8081",
                true, -1L);
        assertThat(on.kafkaEnabled()).isTrue();
        assertThat(on.kafkaBootstrapServers()).isEqualTo("broker-1:9092,broker-2:9092");
        assertThat(on.kafkaTopic()).isEqualTo("other-topic");
        assertThat(on.schemaRegistryUrl()).isEqualTo("http://registry:8081");

        AccountServiceConfig blankRegistry = new AccountServiceConfig(0, STORE, null,
                "broker:9092", null, "  ", true, -1L);
        assertThat(blankRegistry.schemaRegistryUrl()).isNull();
    }

    @Test
    void nullStoreFallsBackToEnvironmentDefaults() {
        AccountServiceConfig config = new AccountServiceConfig(0, null, null, null, null, null,
                true, -1L);
        assertThat(config.store()).isNotNull();
        assertThat(config.store().jdbcUrl()).isNotBlank();
        assertThat(config.store().maxPoolSize()).isPositive();
    }

    @Test
    void malformedNumbersAreRefusedByVariableName() {
        assertThatThrownBy(() -> AccountServiceConfig.parseIntOrDefault(
                AccountServiceConfig.ENV_GRPC_PORT, "90x1", 9091))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AccountServiceConfig.ENV_GRPC_PORT + " must be an integer: 90x1");
        assertThatThrownBy(() -> AccountServiceConfig.parseLongOrDefault(
                AccountServiceConfig.ENV_RELAY_INTERVAL_MS, "5s", 5000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AccountServiceConfig.ENV_RELAY_INTERVAL_MS + " must be an integer: 5s");
    }

    @Test
    void unsetOrBlankNumbersKeepTheirDefault() {
        assertThat(AccountServiceConfig.parseIntOrDefault("X", null, 9091)).isEqualTo(9091);
        assertThat(AccountServiceConfig.parseIntOrDefault("X", "  ", 9091)).isEqualTo(9091);
        assertThat(AccountServiceConfig.parseIntOrDefault("X", " 7 ", 9091)).isEqualTo(7);
        assertThat(AccountServiceConfig.parseLongOrDefault("X", "", 5000L)).isEqualTo(5000L);
        assertThat(AccountServiceConfig.parseLongOrDefault("X", "250", 5000L)).isEqualTo(250L);
    }
}
