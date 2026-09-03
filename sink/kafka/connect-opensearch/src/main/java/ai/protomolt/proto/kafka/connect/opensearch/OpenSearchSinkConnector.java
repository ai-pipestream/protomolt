package ai.protomolt.proto.kafka.connect.opensearch;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Registers ProtoMolt's OpenSearch sink with a Kafka Connect worker: records from the
 * subscribed topics become OpenSearch documents shaped by the schema's declared indexing
 * hints, {@code google.protobuf.Any} payloads expanded and validated on the way in.
 * Configuration validates eagerly - a bad descriptor set or missing message type fails at
 * connector start, not first delivery.
 */
public final class OpenSearchSinkConnector extends SinkConnector {

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = Map.copyOf(props);
        // Fail fast: link the descriptor set and resolve the document message before any
        // task starts.
        OpenSearchSinkConfig config = new OpenSearchSinkConfig(props);
        ConnectDescriptors.messageType(
                ConnectDescriptors.linkedFiles(config.descriptorSetBase64()), config.messageType());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OpenSearchSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing held.
    }

    @Override
    public ConfigDef config() {
        return OpenSearchSinkConfig.definition();
    }

    @Override
    public String version() {
        return pluginVersion();
    }

    static String pluginVersion() {
        String version = OpenSearchSinkConnector.class.getPackage().getImplementationVersion();
        return version != null ? version : "dev";
    }
}
