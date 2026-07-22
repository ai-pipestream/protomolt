package ai.pipestream.proto.connector;

import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.Map;

/**
 * Turns a raw record payload into a protobuf message. A byte-oriented source hands each
 * payload to the parser its plan carries, so the source stays agnostic about how a topic
 * is framed.
 */
@FunctionalInterface
public interface MessageParser {

    /**
     * Parses one payload.
     *
     * @throws SourceException when the payload does not conform
     */
    Message parse(byte[] payload);

    /**
     * Parses one payload with its topic, for parsers whose framing is topic-relative
     * (Confluent subject naming). Defaults to ignoring the topic.
     *
     * @throws SourceException when the payload does not conform
     */
    default Message parse(String topic, byte[] payload) {
        return parse(payload);
    }

    /** Parses each payload as the given message type. */
    static MessageParser forType(Descriptor type) {
        return payload -> {
            try {
                return DynamicMessage.parseFrom(type, payload);
            } catch (InvalidProtocolBufferException e) {
                throw new SourceException("payload is not a valid " + type.getFullName(), e);
            }
        };
    }

    /** Wraps each payload verbatim in a {@link BytesValue}; no schema assumed. */
    static MessageParser bytes() {
        return payload -> BytesValue.of(ByteString.copyFrom(payload));
    }

    /**
     * Reads the Confluent wire format through the ProtoMolt serde: the schema id in each
     * frame is resolved against the registry at {@code registryUrl} (any
     * Confluent-compatible registry, including Redpanda's). Use when the producers write
     * with {@code ProtoMoltProtobufSerializer}; this is the out-of-the-box lane for
     * ProtoMolt-framed topics.
     */
    static MessageParser confluent(String registryUrl) {
        return confluent(Map.of(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl));
    }

    /**
     * Reads the Confluent wire format through the ProtoMolt serde with full control over
     * its config (descriptor sets, pinned message type, validation and quality on read).
     * Keys are the constants on {@link ProtoMoltSerdeConfig}.
     */
    static MessageParser confluent(Map<String, Object> serdeConfig) {
        ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(serdeConfig, false);
        return new SerdeMessageParser(deserializer);
    }
}
