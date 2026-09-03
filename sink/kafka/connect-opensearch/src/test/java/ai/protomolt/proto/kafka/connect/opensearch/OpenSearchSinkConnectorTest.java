package ai.protomolt.proto.kafka.connect.opensearch;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenSearchSinkConnectorTest {

    @Test
    void startFailsFastOnAMissingMessageType() throws Exception {
        OpenSearchSinkConnector connector = new OpenSearchSinkConnector();
        Map<String, String> props = props();
        props.put(OpenSearchSinkConfig.MESSAGE_TYPE, "connect.test.Nope");

        assertThatThrownBy(() -> connector.start(props))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("connect.test.Nope");
    }

    @Test
    void startFailsFastOnAGarbageDescriptorSet() {
        OpenSearchSinkConnector connector = new OpenSearchSinkConnector();
        Map<String, String> props = new HashMap<>(Map.of(
                OpenSearchSinkConfig.DESCRIPTOR_SET, "not-base64!!!",
                OpenSearchSinkConfig.MESSAGE_TYPE, "connect.test.Doc",
                OpenSearchSinkConfig.URL, "http://localhost:39999",
                OpenSearchSinkConfig.INDEX, "docs"));

        assertThatThrownBy(() -> connector.start(props))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("schema.descriptor.set.base64");
    }

    @Test
    void taskConfigsFanOutTheSameProps() throws Exception {
        OpenSearchSinkConnector connector = new OpenSearchSinkConnector();
        Map<String, String> props = props();
        connector.start(props);

        assertThat(connector.taskConfigs(3)).hasSize(3).allSatisfy(config ->
                assertThat(config).containsEntry(OpenSearchSinkConfig.INDEX, "docs"));
        assertThat(connector.taskClass()).isEqualTo(OpenSearchSinkTask.class);
        assertThat(connector.version()).isNotBlank();
    }

    @Test
    void configDefinitionRejectsABadValueFormat() throws Exception {
        Map<String, String> props = props();
        props.put(OpenSearchSinkConfig.VALUE_FORMAT, "avro");

        assertThatThrownBy(() -> new OpenSearchSinkConfig(props))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void configDefaultsAreConservative() throws Exception {
        OpenSearchSinkConfig config = new OpenSearchSinkConfig(props());

        assertThat(config.valueFormat()).isEqualTo(OpenSearchSinkConfig.ValueFormat.PROTOBUF);
        assertThat(config.ensureIndex()).isTrue();
        assertThat(config.refresh()).isFalse();
        assertThat(config.validate()).isTrue();
        assertThat(config.documentIdPath()).isEmpty();
    }

    private static Map<String, String> props() throws Exception {
        return new HashMap<>(Map.of(
                OpenSearchSinkConfig.DESCRIPTOR_SET,
                OpenSearchSinkTaskTest.Fixture.create().descriptorSetBase64(),
                OpenSearchSinkConfig.MESSAGE_TYPE, "connect.test.Doc",
                OpenSearchSinkConfig.URL, "http://localhost:39999",
                OpenSearchSinkConfig.INDEX, "docs"));
    }
}
