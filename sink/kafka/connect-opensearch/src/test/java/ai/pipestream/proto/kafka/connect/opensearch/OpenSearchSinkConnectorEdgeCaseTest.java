package ai.pipestream.proto.kafka.connect.opensearch;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Connector-side edges: task fan-out bounds and how far config validation actually reaches. */
class OpenSearchSinkConnectorEdgeCaseTest {

    /** The herder never asks for zero tasks, but the loop must not invent one either. */
    @Test
    void taskConfigsForZeroTasksIsEmpty() throws Exception {
        OpenSearchSinkConnector connector = new OpenSearchSinkConnector();
        connector.start(props());

        assertThat(connector.taskConfigs(0)).isEmpty();
    }

    /**
     * Eager validation covers the descriptor set and the message type only: the URL is a plain
     * string with no validator, so a syntactically impossible endpoint starts the connector and
     * surfaces at the first HTTP call instead (task start, since opensearch.ensure.index
     * defaults to true).
     */
    @Test
    void aMalformedOpenSearchUrlPassesBothConfigValidationAndConnectorStart() throws Exception {
        Map<String, String> props = props();
        props.put(OpenSearchSinkConfig.URL, "definitely not a url");

        assertThat(new OpenSearchSinkConfig(props).url()).isEqualTo("definitely not a url");
        assertThatCode(() -> new OpenSearchSinkConnector().start(props)).doesNotThrowAnyException();
    }

    /** value.format is declared case-insensitively and read through toUpperCase; both ends of
     * that have to agree or a legal config throws at task start. */
    @Test
    void valueFormatIsCaseInsensitiveEndToEnd() throws Exception {
        Map<String, String> props = props();
        props.put(OpenSearchSinkConfig.VALUE_FORMAT, "Confluent");

        assertThat(new OpenSearchSinkConfig(props).valueFormat())
                .isEqualTo(OpenSearchSinkConfig.ValueFormat.CONFLUENT);
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
