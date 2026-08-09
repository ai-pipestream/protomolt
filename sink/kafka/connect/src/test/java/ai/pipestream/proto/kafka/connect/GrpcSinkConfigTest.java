package ai.pipestream.proto.kafka.connect;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sink config's own contract: the documented defaults hold, the enumerated value format
 * and the bounded deadline reject bad values at parse time, and the optional API token stays
 * absent unless configured. These are the knobs an operator sets on the worker, so each is
 * pinned by a test.
 */
class GrpcSinkConfigTest {

    private static Map<String, String> required() {
        Map<String, String> props = new HashMap<>();
        props.put(GrpcSinkConfig.TARGET, "localhost:9090");
        props.put(GrpcSinkConfig.METHOD, "shop.v1.Orders/Place");
        props.put(GrpcSinkConfig.DESCRIPTOR_SET, "unused-by-these-tests");
        return props;
    }

    private static GrpcSinkConfig config(Map<String, String> overrides) {
        Map<String, String> props = required();
        props.putAll(overrides);
        return new GrpcSinkConfig(props);
    }

    @Test
    void documentedDefaultsHold() {
        GrpcSinkConfig config = config(Map.of());
        assertThat(config.target()).isEqualTo("localhost:9090");
        assertThat(config.method()).isEqualTo("shop.v1.Orders/Place");
        assertThat(config.valueFormat()).isEqualTo(GrpcSinkConfig.ValueFormat.PROTOBUF);
        assertThat(config.deadlineMs()).isEqualTo(30_000L);
        assertThat(config.plaintext()).isTrue();
        assertThat(config.apiToken()).isNull();
    }

    @Test
    void theRequiredKeysHaveNoDefaults() {
        assertThatThrownBy(() -> new GrpcSinkConfig(Map.of()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSinkConfig.TARGET);

        Map<String, String> noMethod = required();
        noMethod.remove(GrpcSinkConfig.METHOD);
        assertThatThrownBy(() -> new GrpcSinkConfig(noMethod))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSinkConfig.METHOD);

        Map<String, String> noDescriptorSet = required();
        noDescriptorSet.remove(GrpcSinkConfig.DESCRIPTOR_SET);
        assertThatThrownBy(() -> new GrpcSinkConfig(noDescriptorSet))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSinkConfig.DESCRIPTOR_SET);
    }

    @Test
    void valueFormatIsCaseInsensitive() {
        assertThat(config(Map.of(GrpcSinkConfig.VALUE_FORMAT, "JSON")).valueFormat())
                .isEqualTo(GrpcSinkConfig.ValueFormat.JSON);
        assertThat(config(Map.of(GrpcSinkConfig.VALUE_FORMAT, "Confluent")).valueFormat())
                .isEqualTo(GrpcSinkConfig.ValueFormat.CONFLUENT);
    }

    @Test
    void anUnknownValueFormatIsRejected() {
        assertThatThrownBy(() -> config(Map.of(GrpcSinkConfig.VALUE_FORMAT, "avro")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSinkConfig.VALUE_FORMAT);
    }

    /**
     * A deadline of zero would expire every call before it left the worker; the config def
     * bounds it at one millisecond.
     */
    @Test
    void theDeadlineIsBoundedBelow() {
        assertThatThrownBy(() -> config(Map.of(GrpcSinkConfig.DEADLINE_MS, "0")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(GrpcSinkConfig.DEADLINE_MS);
        assertThat(config(Map.of(GrpcSinkConfig.DEADLINE_MS, "1")).deadlineMs()).isEqualTo(1L);
    }

    @Test
    void theApiTokenComesBackAsPlaintext() {
        assertThat(config(Map.of(GrpcSinkConfig.API_TOKEN, "s3cr3t")).apiToken())
                .isEqualTo("s3cr3t");
    }

    @Test
    void plaintextCanBeDisabledForTls() {
        assertThat(config(Map.of(GrpcSinkConfig.PLAINTEXT, "false")).plaintext()).isFalse();
    }

    @Test
    void theConnectorExposesTheSameDefinition() {
        assertThat(new GrpcSinkConnector().config().names())
                .contains(GrpcSinkConfig.TARGET, GrpcSinkConfig.METHOD,
                        GrpcSinkConfig.DESCRIPTOR_SET, GrpcSinkConfig.VALUE_FORMAT,
                        GrpcSinkConfig.DEADLINE_MS, GrpcSinkConfig.API_TOKEN,
                        GrpcSinkConfig.PLAINTEXT);
    }

    /**
     * The jar manifest's Implementation-Version is the version at runtime; from bare class
     * directories (the test classpath) there is no manifest, and the plugin must report
     * "dev" rather than null.
     */
    @Test
    void thePluginReportsDevVersionOutsideAJar() {
        assertThat(new GrpcSinkConnector().version()).isEqualTo("dev");
        assertThat(new GrpcSinkTask().version()).isEqualTo("dev");
    }
}
