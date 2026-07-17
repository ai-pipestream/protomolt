package ai.pipestream.proto.kafka.serde;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test listener registered via META-INF/services, recording every event. A second listener,
 * {@link Hostile}, throws on every call — the serde must contain it and keep serving records
 * (and keep calling the well-behaved listener).
 */
public class RecordingMetricsListener implements SerdeMetricsListener {

    static final Queue<String> EVENTS = new ConcurrentLinkedQueue<>();

    static void reset() {
        EVENTS.clear();
    }

    @Override
    public void onSerialized(String topic, String type) {
        EVENTS.add("serialized " + topic + " " + type);
    }

    @Override
    public void onDeserialized(String topic, String type) {
        EVENTS.add("deserialized " + topic + " " + type);
    }

    @Override
    public void onValidationRejected(String topic, String type, boolean write,
                                     List<String> ruleIds) {
        EVENTS.add("rejected " + (write ? "write" : "read") + " " + topic + " " + type
                + " " + ruleIds);
    }

    @Override
    public void onTypeRefused(String topic, String reason) {
        EVENTS.add("refused " + topic + " " + reason);
    }

    @Override
    public void onRegistryFallback() {
        EVENTS.add("fallback");
    }

    /** Misbehaves on purpose; its existence is the exception-containment test. */
    public static class Hostile implements SerdeMetricsListener {

        @Override
        public void onSerialized(String topic, String type) {
            throw new IllegalStateException("metrics backends misbehave sometimes");
        }

        @Override
        public void onValidationRejected(String topic, String type, boolean write,
                                         List<String> ruleIds) {
            throw new IllegalStateException("metrics backends misbehave sometimes");
        }
    }
}
