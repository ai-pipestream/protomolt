package ai.pipestream.proto.kafka.connect.iceberg;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Registers ProtoMolt's Iceberg sink with a Kafka Connect worker: records from the subscribed
 * topics become rows in an Iceberg table, written as snapshots through the descriptor-driven
 * Parquet emitter with no generated stubs. Configuration validates eagerly - a bad descriptor set
 * or missing message type fails at connector start, not first delivery.
 */
public final class IcebergSinkConnector extends SinkConnector {

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = Map.copyOf(props);
        // Fail fast: link the descriptor set and resolve the row message before any task starts.
        IcebergSinkConfig config = new IcebergSinkConfig(props);
        ConnectDescriptors.messageType(
                ConnectDescriptors.linkedFiles(config.descriptorSetBase64()), config.messageType());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IcebergSinkTask.class;
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
        return IcebergSinkConfig.definition();
    }

    @Override
    public String version() {
        return pluginVersion();
    }

    static String pluginVersion() {
        String version = IcebergSinkConnector.class.getPackage().getImplementationVersion();
        return version != null ? version : "dev";
    }
}
