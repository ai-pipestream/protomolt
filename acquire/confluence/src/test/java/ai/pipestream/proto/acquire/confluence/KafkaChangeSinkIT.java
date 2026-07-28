package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ChangeSource;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.proto.acquire.confluence.v1.Page;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Kafka sink against a genuine broker: a change and a snapshot published
 * through {@link KafkaChangeSink} read back byte-identical through the
 * protomolt deserializer, and a change that violates the schema's declared
 * rules (UPSERT without its entity, a blank change id) is rejected at the
 * serde edge instead of reaching the topic. The broker is a Testcontainers
 * Redpanda instance; the suite skips when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaChangeSinkIT {

    // The image the connector module's own Kafka suite pins (its baseline).
    @Container
    static final RedpandaContainer KAFKA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    private static String unique(String prefix) {
        return prefix + "-" + Long.toUnsignedString(System.nanoTime(), 36);
    }

    private static void createTopic(String topic) throws Exception {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(config)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private static ConfluenceChange pageChange(String changeId, String pageId, String title) {
        return ConfluenceChange.newBuilder()
                .setChangeId(changeId)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId(pageId)
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1_753_000_000))
                        .setPage(Page.newBuilder()
                                .setId(pageId)
                                .setSpaceId("456")
                                .setTitle(title)))
                .setSource(ChangeSource.CHANGE_SOURCE_CRAWL)
                .setCursor("run-1")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1_753_000_001))
                .build();
    }

    /** A consumer pinned to one type against the packaged descriptor set, registry-free. */
    private static KafkaConsumer<String, Message> newConsumer(String topic, String messageType) {
        ProtoMoltProtobufDeserializer valueDeserializer = new ProtoMoltProtobufDeserializer();
        valueDeserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, KafkaChangeSink.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, messageType), false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, unique("confluence-sink-it"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, Message> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    /** Polls until one record arrives or the deadline passes. */
    private static Message takeOne(KafkaConsumer<String, Message> consumer, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next().value();
            }
        }
        return null;
    }

    @Test
    void changeAndSnapshotRoundTripByteIdentical() throws Exception {
        String topic = unique("confluence-events-it");
        String snapshotsTopic = unique("confluence-snapshots-it");
        createTopic(topic);
        createTopic(snapshotsTopic);

        ConfluenceChange change = pageChange("change-1", "123", "Hello Kafka");
        ConfluenceSnapshot snapshot = ConfluenceSnapshot.newBuilder()
                .setSnapshotId("run-1-ENG")
                .setSpaceKey("ENG")
                .putEntityCounts("page", 1)
                .setCursor("2024-02-01T10:00:00Z")
                .setStartedAt(Timestamp.newBuilder().setSeconds(1_753_000_000))
                .build();

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl("https://example.atlassian.net/wiki")
                .email("bot@example.com")
                .apiToken("token")
                .kafkaBootstrapServers(KAFKA.getBootstrapServers())
                .kafkaTopic(topic)
                .kafkaSnapshotsTopic(snapshotsTopic)
                .build();
        try (KafkaChangeSink sink = KafkaChangeSink.create(config)) {
            sink.emit(change);
            sink.snapshot(snapshot);
        } // close flushes

        try (KafkaConsumer<String, Message> consumer =
                newConsumer(topic, ConfluenceChange.getDescriptor().getFullName())) {
            Message read = takeOne(consumer, Duration.ofSeconds(30));
            assertThat(read).as("the change arrives").isNotNull();
            assertThat(read).isInstanceOf(ConfluenceChange.class);
            assertThat(read.toByteArray()).as("byte-identical round trip")
                    .isEqualTo(change.toByteArray());
        }
        try (KafkaConsumer<String, Message> consumer =
                newConsumer(snapshotsTopic, ConfluenceSnapshot.getDescriptor().getFullName())) {
            Message read = takeOne(consumer, Duration.ofSeconds(30));
            assertThat(read).as("the snapshot arrives").isNotNull();
            assertThat(read.toByteArray()).as("byte-identical round trip")
                    .isEqualTo(snapshot.toByteArray());
        }
    }

    @Test
    void invalidChangeIsRejectedAtTheEdgeAndNeverLands() throws Exception {
        String topic = unique("confluence-events-invalid-it");
        createTopic(topic);

        // UPSERT without its entity violates the change.upsert_has_entity CEL
        // rule, and the blank change id violates the required rule: the serde
        // enforces the schema's declared rules on write.
        ConfluenceChange invalid = ConfluenceChange.newBuilder()
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .build();

        ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
        serializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, KafkaChangeSink.descriptorSetBase64()),
                false);
        assertThatThrownBy(() -> serializer.serialize(topic, invalid))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("change.upsert_has_entity");

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl("https://example.atlassian.net/wiki")
                .email("bot@example.com")
                .apiToken("token")
                .kafkaBootstrapServers(KAFKA.getBootstrapServers())
                .kafkaTopic(topic)
                .build();
        // The sink must not throw into the crawler loop: it logs and moves on.
        try (KafkaChangeSink sink = KafkaChangeSink.create(config)) {
            sink.emit(invalid);
        }
        try (KafkaConsumer<String, Message> consumer =
                newConsumer(topic, ConfluenceChange.getDescriptor().getFullName())) {
            assertThat(takeOne(consumer, Duration.ofSeconds(5)))
                    .as("the invalid change never reaches the topic").isNull();
        }
    }
}
