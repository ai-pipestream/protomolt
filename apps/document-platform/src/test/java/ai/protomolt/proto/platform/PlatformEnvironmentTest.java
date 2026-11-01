package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PlatformEnvironmentTest {

    @Test
    void refreshIntervalsRejectOverflowWithoutLosingTheVariableName() {
        String overflow = "9223372036854775808";
        assertThatThrownBy(() -> PlatformEnvironment.searchRefreshSeconds(Map.of(
                DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS, overflow)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS
                        + " must be a positive number of seconds, got '" + overflow + "'");
        assertThatThrownBy(() -> PlatformEnvironment.configRefreshSeconds(Map.of(
                DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS, overflow)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS
                        + " must be a positive number of seconds, got '" + overflow + "'");
    }

    @Test
    void unusedKafkaFieldsDoNotActivateTheKafkaSource() {
        assertThat(PlatformEnvironment.kafkaConfigFamily(Map.of(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS, "  ",
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_TOPIC, "unused",
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL, "unused",
                DocumentPlatformConfig.ENV_CONFIG_URL, "http://registry/protomolt")))
                .isNull();
    }

    @Test
    void conflictingSourcesRefuseBeforeCheckingTheMissingKafkaRegistry() {
        assertThatThrownBy(() -> PlatformEnvironment.kafkaConfigFamily(Map.of(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS, " kafka:9092 ",
                DocumentPlatformConfig.ENV_CONFIG_URL, " http://registry/protomolt ")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS
                        + " and " + DocumentPlatformConfig.ENV_CONFIG_URL + " are both set:"
                        + " the config lane reads one source, so unset one");
    }

    @Test
    void textParserNodeIgnoresInvalidSettingsForUnmountedSearchAndMetricRoles()
            throws Exception {
        var config = new DocumentPlatformConfig(
                null, null, null, 0, 0, 0, 0, null, null, null,
                60L, 1, 0, null, 0, 0, List.of("parse-text"), Map.of(
                        DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "invalid",
                        DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS, "invalid",
                        DocumentPlatformConfig.ENV_SEARCH_VECTORIZE_SENSITIVITY, ",,,",
                        DocumentPlatformConfig.ENV_METRICS_LAKE_DIR, "\u0000"));
        try (var platform = DocumentPlatform.start(config, null)) {
            assertThatThrownBy(platform::searchPort)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("role 'search' is not mounted on this node");
            assertThatThrownBy(platform::metricsPort)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("role 'metric' is not mounted on this node");
        }
    }
}
