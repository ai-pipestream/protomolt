package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * An {@link OutboxRelay.Publisher} that sends each event to one Kafka topic as a
 * binary protobuf {@link DocumentEvent} under the event's canonical address key.
 * Test scope on purpose: the accession module's production runtime carries no
 * Kafka client, so a host adapter lifts exactly this wiring into its own runtime.
 */
final class KafkaOutboxPublisher implements OutboxRelay.Publisher, AutoCloseable {
    private final KafkaProducer<String, Message> producer;
    private final String topic;

    KafkaOutboxPublisher(String bootstrapServers, String topic) {
        this.topic = Objects.requireNonNull(topic);
        var serializer = new ProtoMoltProtobufSerializer();
        var serde = new HashMap<String, Object>();
        serde.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, documentEventDescriptorSetBase64());
        serde.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName());
        serializer.configure(serde, false);
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        this.producer = new KafkaProducer<>(properties, new StringSerializer(), serializer);
    }

    /** Returns after the broker has acknowledged the record; the relay stamps the row next. */
    @Override public void publish(String key, DocumentEvent event) throws Exception {
        producer.send(new ProducerRecord<>(topic, key, event)).get();
    }

    @Override public void close() {
        producer.close();
    }

    /**
     * The descriptor set the protomolt serde validates against: events.proto
     * and its transitive imports, taken from the generated classes' runtime descriptors.
     */
    static String documentEventDescriptorSetBase64() {
        Map<String, DescriptorProtos.FileDescriptorProto> files = new LinkedHashMap<>();
        ArrayDeque<Descriptors.FileDescriptor> queue = new ArrayDeque<>(List.of(DocumentEvent.getDescriptor().getFile()));
        while (!queue.isEmpty()) {
            var file = queue.pop();
            if (files.put(file.getName(), file.toProto()) == null) queue.addAll(file.getDependencies());
        }
        return Base64.getEncoder().encodeToString(DescriptorProtos.FileDescriptorSet.newBuilder()
                .addAllFile(files.values()).build().toByteArray());
    }
}
