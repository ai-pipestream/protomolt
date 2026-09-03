package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.config.kafka.KafkaConfigSource;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The config lane's source selection: absent Kafka family means the
 * registry source exactly as before; a complete family selects the
 * signal plug with the topic defaulted; a partial family refuses naming
 * the missing variable; and naming both sources is a contradiction,
 * refused rather than resolved by preference.
 */
class PlatformConfigSourceTest {

    @Test
    void anAbsentFamilyMeansTheRegistrySource() {
        assertThat(DocumentPlatform.kafkaConfigFamily(Map.of())).isNull();
    }

    @Test
    void aCompleteFamilySelectsKafkaWithTheTopicDefaulted() {
        KafkaConfigSource.Config config = DocumentPlatform.kafkaConfigFamily(Map.of(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS, "kafka:9092",
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL,
                "http://registry:8081"));
        assertThat(config.bootstrapServers()).isEqualTo("kafka:9092");
        assertThat(config.topic())
                .isEqualTo(DocumentPlatformConfig.DEFAULT_CONFIG_KAFKA_TOPIC);
        assertThat(config.schemaRegistryUrl()).isEqualTo("http://registry:8081");

        assertThat(DocumentPlatform.kafkaConfigFamily(Map.of(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS, "kafka:9092",
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL,
                "http://registry:8081",
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_TOPIC, "my-config"))
                .topic()).isEqualTo("my-config");
    }

    @Test
    void aMissingSchemaRegistryUrlRefusesByName() {
        assertThatThrownBy(() -> DocumentPlatform.kafkaConfigFamily(Map.of(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS, "kafka:9092")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL)
                .hasMessageContaining("verify-then-swap");
    }

    @Test
    void namingBothSourcesIsAContradiction() {
        assertThatThrownBy(() -> DocumentPlatform.kafkaConfigFamily(Map.of(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS, "kafka:9092",
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL,
                "http://registry:8081",
                DocumentPlatformConfig.ENV_CONFIG_URL, "http://other:8081/protomolt")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DocumentPlatformConfig.ENV_CONFIG_URL)
                .hasMessageContaining("unset one");
    }
}
