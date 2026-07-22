package ai.pipestream.proto.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Registers ProtoMolt's gRPC source with a Kafka Connect worker: a server-streaming method
 * feeds a topic, descriptor-native with no generated stubs. Configuration validates eagerly —
 * a bad method, request template, token field, or CEL expression fails at connector start,
 * not first poll.
 */
public final class GrpcSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcSourceConnector.class);

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = Map.copyOf(props);
        // Fail fast: resolve the method and compile every expression before any task starts.
        GrpcSourceTask.prepare(new GrpcSourceConfig(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GrpcSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // One stream, one task: a second subscriber would duplicate every message.
        if (maxTasks > 1) {
            LOG.warn("tasks.max is {} but the gRPC source runs a single stream; starting 1 task",
                    maxTasks);
        }
        return List.of(props);
    }

    @Override
    public void stop() {
        // Nothing held.
    }

    @Override
    public ConfigDef config() {
        return GrpcSourceConfig.definition();
    }

    @Override
    public String version() {
        return GrpcConnectorSupport.pluginVersion();
    }
}
