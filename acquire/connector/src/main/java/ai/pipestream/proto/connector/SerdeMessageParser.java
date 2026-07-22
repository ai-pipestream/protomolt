package ai.pipestream.proto.connector;

import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import com.google.protobuf.Message;

/** The {@link MessageParser} behind {@link MessageParser#confluent}: one configured deserializer. */
final class SerdeMessageParser implements MessageParser {

    private final ProtoMoltProtobufDeserializer deserializer;

    SerdeMessageParser(ProtoMoltProtobufDeserializer deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public Message parse(byte[] payload) {
        throw new UnsupportedOperationException("the Confluent frame is topic-relative");
    }

    @Override
    public Message parse(String topic, byte[] payload) {
        try {
            return deserializer.deserialize(topic, payload);
        } catch (RuntimeException e) {
            throw new SourceException("payload on " + topic + " is not a readable frame", e);
        }
    }
}
