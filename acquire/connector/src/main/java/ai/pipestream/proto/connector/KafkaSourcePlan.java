package ai.pipestream.proto.connector;

import java.util.Map;
import java.util.Objects;

/**
 * What a {@link KafkaSource} needs: a topic to subscribe to, a consumer group, and a
 * {@link MessageParser} for the payload bytes. {@code overrides} carries any raw
 * kafka-clients consumer property (commit policy, offset reset, poll batch size, session
 * timeouts), merged over the source's defaults.
 */
public record KafkaSourcePlan(String bootstrapServers,
                              String topic,
                              String groupId,
                              MessageParser parser,
                              Map<String, Object> overrides) implements SourcePlan {

    public KafkaSourcePlan {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(parser, "parser");
        overrides = overrides == null ? Map.of() : Map.copyOf(overrides);
    }

    public KafkaSourcePlan(String bootstrapServers, String topic, String groupId, MessageParser parser) {
        this(bootstrapServers, topic, groupId, parser, Map.of());
    }
}
