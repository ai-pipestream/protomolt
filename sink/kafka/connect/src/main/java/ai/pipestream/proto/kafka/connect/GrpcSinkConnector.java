package ai.pipestream.proto.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Registers ProtoMolt's gRPC sink with a Kafka Connect worker: records from the subscribed
 * topics become request messages on a configured gRPC method, descriptor-native with no
 * generated stubs. Configuration validates eagerly — a bad method or descriptor set fails
 * at connector start, not first delivery.
 */
public final class GrpcSinkConnector extends SinkConnector {

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = Map.copyOf(props);
        // Fail fast: parse the config and resolve the method before any task starts.
        GrpcSinkTask.resolveMethod(new GrpcSinkConfig(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GrpcSinkTask.class;
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
        return GrpcSinkConfig.definition();
    }

    @Override
    public String version() {
        return pluginVersion();
    }

    static String pluginVersion() {
        return GrpcConnectorSupport.pluginVersion();
    }
}
