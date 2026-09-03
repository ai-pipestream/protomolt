package ai.protomolt.proto.kafka.connect;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The source config's own contract: documented defaults (empty subscribe request, no resume
 * token wiring, bounded poll), the value format restricted to the two encodings a source can
 * emit, and the numeric bounds a poll loop relies on.
 */
class GrpcSourceConfigTest {

    private static Map<String, String> required() {
        Map<String, String> props = new HashMap<>();
        props.put(GrpcSourceConfig.TARGET, "localhost:9090");
        props.put(GrpcSourceConfig.METHOD, "shop.v1.Orders/Watch");
        props.put(GrpcSourceConfig.DESCRIPTOR_SET, "unused-by-these-tests");
        props.put(GrpcSourceConfig.TOPIC, "orders");
        return props;
    }

    private static GrpcSourceConfig config(Map<String, String> overrides) {
        Map<String, String> props = required();
        props.putAll(overrides);
        return new GrpcSourceConfig(props);
    }

    @Test
    void documentedDefaultsHold() {
        GrpcSourceConfig config = config(Map.of());
        assertThat(config.target()).isEqualTo("localhost:9090");
        assertThat(config.method()).isEqualTo("shop.v1.Orders/Watch");
        assertThat(config.topic()).isEqualTo("orders");
        assertThat(config.requestJson()).isEqualTo("{}");
        assertThat(config.resumeTokenCel()).isNull();
        assertThat(config.resumeTokenField()).isNull();
        assertThat(config.keyCel()).isNull();
        assertThat(config.valueFormat()).isEqualTo(GrpcSourceConfig.ValueFormat.PROTOBUF);
        assertThat(config.pollMaxRecords()).isEqualTo(500);
        assertThat(config.pollTimeoutMs()).isEqualTo(1_000L);
        assertThat(config.reconnectBackoffMs()).isEqualTo(1_000L);
        assertThat(config.apiToken()).isNull();
        assertThat(config.plaintext()).isTrue();
    }

    @Test
    void theRequiredKeysHaveNoDefaults() {
        assertThatThrownBy(() -> new GrpcSourceConfig(Map.of()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSourceConfig.TARGET);

        Map<String, String> noTopic = required();
        noTopic.remove(GrpcSourceConfig.TOPIC);
        assertThatThrownBy(() -> new GrpcSourceConfig(noTopic))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSourceConfig.TOPIC);
    }

    /**
     * A source emits values, so the Confluent frame (a schema-registry concern on the write
     * side) is not one of its formats; only raw protobuf and proto3 JSON are.
     */
    @Test
    void theValueFormatIsProtobufOrJsonOnly() {
        assertThat(config(Map.of(GrpcSourceConfig.VALUE_FORMAT, "JSON")).valueFormat())
                .isEqualTo(GrpcSourceConfig.ValueFormat.JSON);
        assertThatThrownBy(() -> config(Map.of(GrpcSourceConfig.VALUE_FORMAT, "confluent")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSourceConfig.VALUE_FORMAT);
    }

    @Test
    void thePollBoundsAreEnforced() {
        assertThatThrownBy(() -> config(Map.of(GrpcSourceConfig.POLL_MAX_RECORDS, "0")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSourceConfig.POLL_MAX_RECORDS);
        assertThatThrownBy(() -> config(Map.of(GrpcSourceConfig.POLL_TIMEOUT_MS, "0")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSourceConfig.POLL_TIMEOUT_MS);
        assertThatThrownBy(() -> config(Map.of(GrpcSourceConfig.RECONNECT_BACKOFF_MS, "-1")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSourceConfig.RECONNECT_BACKOFF_MS);
        // Zero backoff is legal: resubscribe as fast as the stream drops.
        assertThat(config(Map.of(GrpcSourceConfig.RECONNECT_BACKOFF_MS, "0"))
                .reconnectBackoffMs()).isZero();
    }

    @Test
    void theApiTokenComesBackAsPlaintext() {
        assertThat(config(Map.of(GrpcSourceConfig.API_TOKEN, "s3cr3t")).apiToken())
                .isEqualTo("s3cr3t");
    }

    @Test
    void theConnectorExposesTheSameDefinition() {
        assertThat(new GrpcSourceConnector().config().names())
                .contains(GrpcSourceConfig.TARGET, GrpcSourceConfig.METHOD,
                        GrpcSourceConfig.DESCRIPTOR_SET, GrpcSourceConfig.TOPIC,
                        GrpcSourceConfig.REQUEST_JSON, GrpcSourceConfig.RESUME_TOKEN_CEL,
                        GrpcSourceConfig.RESUME_TOKEN_FIELD, GrpcSourceConfig.KEY_CEL,
                        GrpcSourceConfig.VALUE_FORMAT, GrpcSourceConfig.POLL_MAX_RECORDS,
                        GrpcSourceConfig.POLL_TIMEOUT_MS, GrpcSourceConfig.RECONNECT_BACKOFF_MS,
                        GrpcSourceConfig.API_TOKEN, GrpcSourceConfig.PLAINTEXT);
    }

    @Test
    void thePluginReportsDevVersionOutsideAJar() {
        assertThat(new GrpcSourceConnector().version()).isEqualTo("dev");
        assertThat(new GrpcSourceTask().version()).isEqualTo("dev");
    }
}
