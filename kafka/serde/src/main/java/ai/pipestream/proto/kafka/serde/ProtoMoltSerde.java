package ai.pipestream.proto.kafka.serde;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * The serializer and deserializer as one {@link Serde}, which is the shape Kafka Streams asks
 * for: {@code Consumed.with(Serdes.String(), new ProtoMoltSerde())}. Both halves take the same
 * {@link ProtoMoltSerdeConfig configuration}.
 */
public class ProtoMoltSerde implements Serde<Message> {

    private final ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
    private final ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<Message> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Message> deserializer() {
        return deserializer;
    }
}
