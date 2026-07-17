package ai.pipestream.proto.kafka.serde;

/**
 * The subject a schema is registered under, per the three strategies every Confluent-compatible
 * registry and client already speak. Topic-name is the default: one type per topic, found under
 * the same subject their tooling would use. The record-name strategies key the subject by the
 * message's full name instead, which is what lets several event types share one topic — each
 * type resolves its own id.
 */
final class Subjects {

    static final String TOPIC = "topic";
    static final String RECORD = "record";
    static final String TOPIC_RECORD = "topic-record";

    private Subjects() {
    }

    /**
     * Matches Confluent's TopicNameStrategy, RecordNameStrategy and TopicRecordNameStrategy
     * byte for byte, including the detail that only the topic strategy distinguishes keys: a
     * record's name is the same subject whether it rides in the key or the value.
     */
    static String of(String strategy, String topic, String recordName, boolean isKey) {
        return switch (strategy) {
            case TOPIC -> topic + (isKey ? "-key" : "-value");
            case RECORD -> recordName;
            case TOPIC_RECORD -> topic + "-" + recordName;
            default -> throw new IllegalArgumentException("Unknown subject strategy: " + strategy);
        };
    }
}
