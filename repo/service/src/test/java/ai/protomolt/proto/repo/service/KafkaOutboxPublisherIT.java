package ai.protomolt.proto.repo.service;

import static ai.protomolt.proto.repo.service.KafkaOutboxPublisherTest.accessioned;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import com.google.protobuf.Message;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The production publisher over a real Redpanda broker: acknowledged records keyed
 * as the relay hands them over, readable by the validating protomolt deserializer,
 * and an event that breaks the contract never reaches the topic.
 */
@Testcontainers
class KafkaOutboxPublisherIT {
    // Same baseline image as the service eventing lane (testcontainers' own pinned tag).
    @Container static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    @Test void publishesAcknowledgedEventsUnderTheRelayKey() throws Exception {
        var topic = "outbox-publisher-" + UUID.randomUUID();
        var first = accessioned("event-a", "doc-a");
        var second = accessioned("event-b", "doc-b");
        try (var publisher = new KafkaOutboxPublisher(REDPANDA.getBootstrapServers(), topic)) {
            publisher.publish("key-a", first);
            publisher.publish("key-b", second);
        }
        var records = consume(topic, 2);
        assertThat(records).extracting(ConsumerRecord::key).containsExactly("key-a", "key-b");
        assertThat(records).extracting(ConsumerRecord::value).containsExactly(first, second);
    }

    @Test void anInvalidEventIsRefusedBeforeItReachesTheTopic() throws Exception {
        var topic = "outbox-publisher-" + UUID.randomUUID();
        var kindless = accessioned("event-x", "doc-x").toBuilder().clearKind().build();
        var valid = accessioned("event-y", "doc-y");
        try (var publisher = new KafkaOutboxPublisher(REDPANDA.getBootstrapServers(), topic)) {
            assertThatThrownBy(() -> publisher.publish("key-x", kindless))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("event-kind-required");
            publisher.publish("key-y", valid);
        }
        // The valid record is the topic's only record: the refused one was never sent ahead of it.
        assertThat(consume(topic, 1)).extracting(ConsumerRecord::key).containsExactly("key-y");
    }

    private static List<ConsumerRecord<String, Message>> consume(String topic, int expected) {
        var deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, KafkaOutboxPublisher.documentEventDescriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true), false);
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-publisher-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var records = new ArrayList<ConsumerRecord<String, Message>>();
        try (var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), deserializer)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (records.size() < expected && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) records.add(record);
            }
            // One more poll after the expected count, so a stray extra record would be seen.
            for (var record : consumer.poll(Duration.ofSeconds(1))) records.add(record);
        }
        assertThat(records).as("records consumed from the topic within the deadline").hasSize(expected);
        return records;
    }
}
