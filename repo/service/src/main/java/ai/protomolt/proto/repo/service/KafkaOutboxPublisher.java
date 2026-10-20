package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.repo.accession.OutboxRelay;
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
 * The parts-based outbox's Kafka publisher: sends each {@link DocumentEvent} to one
 * topic as a binary protobuf record under the event's canonical address key.
 * {@link #publish} returns only after the broker acknowledged the record, so the
 * {@link OutboxRelay} stamps a row as relayed only once it is durable on the topic.
 * The producer is idempotent with {@code acks=all}; a relay retry after a crash
 * republishes the same {@code event_id}, which consumers deduplicate.
 *
 * <p>Registry-free, the serde stamps schema id 0 into every frame: protomolt
 * consumers read the payload against the packaged contract, but standard Confluent
 * tooling resolves frames by id and cannot read id 0. With a registry URL the serde
 * looks up the {@code <topic>-value} subject's id and stamps it; it never registers.
 */
public final class KafkaOutboxPublisher implements OutboxRelay.Publisher, AutoCloseable {
    private final KafkaProducer<String, Message> producer;
    private final String topic;

    /**
     * A registry-free publisher.
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param topic the document-events topic
     */
    public KafkaOutboxPublisher(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, null);
    }

    /**
     * A publisher whose frames carry the registry-assigned schema id when a registry is named.
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param topic the document-events topic
     * @param schemaRegistryUrl base URL of a Confluent-compatible schema registry,
     *        or null or blank for registry-free framing
     */
    public KafkaOutboxPublisher(String bootstrapServers, String topic, String schemaRegistryUrl) {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        this.topic = Objects.requireNonNull(topic, "topic");
        if (bootstrapServers.isBlank()) throw new IllegalArgumentException("bootstrapServers must not be blank");
        if (topic.isBlank()) throw new IllegalArgumentException("topic must not be blank");
        var serde = new HashMap<String, Object>();
        serde.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, documentEventDescriptorSetBase64());
        serde.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName());
        if (schemaRegistryUrl != null && !schemaRegistryUrl.isBlank()) {
            serde.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        }
        var serializer = new ProtoMoltProtobufSerializer();
        serializer.configure(serde, false);
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
     * The descriptor set the protomolt serde validates against: events.proto and its
     * transitive imports, taken from the generated classes' runtime descriptors.
     *
     * @return the base64 of a serialized {@code FileDescriptorSet}
     */
    public static String documentEventDescriptorSetBase64() {
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
