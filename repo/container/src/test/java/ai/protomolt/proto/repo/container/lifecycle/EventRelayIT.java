package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.repo.container.ledger.DocumentEventRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRowKind;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.v1.DocumentEvent;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The outbox relay against real testcontainers PostgreSQL 17 and Redpanda:
 * a claimed PENDING row is published to the topic through the protomolt serde
 * and read back by a consumer using {@link ProtoMoltProtobufDeserializer}
 * (revalidating on read), and a relay that dies mid-flight leaves the row
 * PENDING for the restarted relay to publish - at-least-once recovery.
 */
@Testcontainers(disabledWithoutDocker = true)
class EventRelayIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    // Same baseline image as the serde lane (testcontainers' own pinned tag).
    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    static LedgerDatabase database;
    static Tx tx;
    static JdbcEventOutbox outbox;
    static EventRelay relay;

    @BeforeAll
    static void boot() {
        database = new LedgerDatabase(new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        outbox = new JdbcEventOutbox(tx);
        relay = new EventRelay(outbox);
    }

    @AfterAll
    static void stop() {
        database.close();
    }

    @Test
    void relayPublishesAndTheConsumerRevalidates() {
        String topic = "relay-it-" + Long.toUnsignedString(System.nanoTime(), 36);
        DocumentEventRecord record = saved("doc-relay-publish", Instant.now());
        tx.inTransaction(em -> {
            outbox.enqueue(em, record);
        });

        try (KafkaProducer<String, Message> producer =
                EventRelay.newProducer(REDPANDA.getBootstrapServers())) {
            assertThat(relay.relayOnce(producer, topic, 100)).isEqualTo(1);
        }

        DocumentEventRecord stored = outbox.findById(record.eventId).orElseThrow();
        assertThat(stored.status).isEqualTo(DocumentEventRecord.STATUS_PUBLISHED);
        assertThat(stored.publishedAt).isNotNull();

        try (KafkaConsumer<String, Message> consumer = newConsumer()) {
            consumer.subscribe(List.of(topic));
            DocumentEvent event = pollFor(consumer, "doc-relay-publish");
            // The deserializer (VALIDATE_ON_READ) revalidated on the way in;
            // the generated class comes back because repo-proto is packaged.
            assertThat(event.getEventId()).isEqualTo(record.eventId.toString());
            assertThat(event.hasSaved()).isTrue();
            assertThat(event.getSaved().getAddress().getDocId()).isEqualTo("doc-relay-publish");
            assertThat(event.getSaved().getAddress().getGraphId()).isEqualTo("intake:acct-relay");
            assertThat(event.getSaved().getChecksum()).isEqualTo("sha256:doc-relay-publish");
            assertThat(event.getSaved().getSavedAt().getSeconds()).isPositive();
        }
    }

    @Test
    void failedBrokerLeavesPendingAndARestartedRelayPublishes() {
        String topic = "relay-it-" + Long.toUnsignedString(System.nanoTime(), 36);
        DocumentEventRecord record = saved("doc-relay-recovery", Instant.now());
        tx.inTransaction(em -> {
            outbox.enqueue(em, record);
        });

        // The relay's broker is dead: the send fails fast (test-tight
        // timeouts), the row stays PENDING with attempts bumped.
        try (KafkaProducer<String, Message> dead = deadBrokerProducer()) {
            assertThat(relay.relayOnce(dead, topic, 100)).isZero();
        }
        DocumentEventRecord afterFailure = outbox.findById(record.eventId).orElseThrow();
        assertThat(afterFailure.status).isEqualTo(DocumentEventRecord.STATUS_PENDING);
        assertThat(afterFailure.attempts).isEqualTo(1);
        assertThat(afterFailure.lastError).isNotBlank();

        // The restarted relay (broker back) publishes the same row.
        try (KafkaProducer<String, Message> producer =
                EventRelay.newProducer(REDPANDA.getBootstrapServers())) {
            assertThat(relay.relayOnce(producer, topic, 100)).isEqualTo(1);
        }
        assertThat(outbox.findById(record.eventId).orElseThrow().status)
                .isEqualTo(DocumentEventRecord.STATUS_PUBLISHED);

        try (KafkaConsumer<String, Message> consumer = newConsumer()) {
            consumer.subscribe(List.of(topic));
            DocumentEvent event = pollFor(consumer, "doc-relay-recovery");
            assertThat(event.getEventId()).isEqualTo(record.eventId.toString());
            assertThat(event.getSaved().getAddress().getDocId()).isEqualTo("doc-relay-recovery");
        }
    }

    @Test
    void crashMidFlightLeavesPendingForTheNextRelay() {
        String topic = "relay-it-" + Long.toUnsignedString(System.nanoTime(), 36);
        DocumentEventRecord record = saved("doc-relay-crash", Instant.now());
        tx.inTransaction(em -> {
            outbox.enqueue(em, record);
        });

        // Crash between claim and publish: the claim's locks died with the
        // transaction, so the row is simply PENDING again for the next relay.
        assertThat(outbox.claimBatch(100)).anyMatch(r -> r.eventId.equals(record.eventId));
        assertThat(outbox.findById(record.eventId).orElseThrow().status)
                .isEqualTo(DocumentEventRecord.STATUS_PENDING);

        try (KafkaProducer<String, Message> producer =
                EventRelay.newProducer(REDPANDA.getBootstrapServers())) {
            assertThat(relay.relayOnce(producer, topic, 100)).isEqualTo(1);
        }
        assertThat(outbox.findById(record.eventId).orElseThrow().status)
                .isEqualTo(DocumentEventRecord.STATUS_PUBLISHED);

        try (KafkaConsumer<String, Message> consumer = newConsumer()) {
            consumer.subscribe(List.of(topic));
            assertThat(pollFor(consumer, "doc-relay-crash").getEventId())
                    .isEqualTo(record.eventId.toString());
        }
    }

    /** A consumer pinned to DocumentEvent that revalidates every record on read. */
    private static KafkaConsumer<String, Message> newConsumer() {
        ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, DocumentEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true), false);
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                REDPANDA.getBootstrapServers());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG,
                "relay-it-" + UUID.randomUUID());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
        return new KafkaConsumer<>(props, new StringDeserializer(), deserializer);
    }

    /** A producer whose broker does not exist, with timeouts tight enough for a test. */
    private static KafkaProducer<String, Message> deadBrokerProducer() {
        ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
        serializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, DocumentEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName()),
                false);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        return new KafkaProducer<>(props, new StringSerializer(), serializer);
    }

    /** Poll until the event for {@code docId} shows up (at-least-once: skip anything else). */
    private static DocumentEvent pollFor(KafkaConsumer<String, Message> consumer, String docId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        List<ConsumerRecord<String, Message>> seen = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, Message> record : records) {
                seen.add(record);
                assertThat(record.key()).isEqualTo(docId);
                Message value = record.value();
                assertThat(value).isInstanceOf(DocumentEvent.class);
                DocumentEvent event = (DocumentEvent) value;
                if (event.hasSaved() && event.getSaved().getAddress().getDocId().equals(docId)) {
                    return event;
                }
            }
        }
        throw new AssertionError("no event for " + docId + " within 60s (saw " + seen.size()
                + " record(s))");
    }

    private static DocumentEventRecord saved(String docId, Instant when) {
        return DocumentEventFactory.saved(row(docId), when);
    }

    private static DocumentRecord row(String docId) {
        DocumentRecord row = new DocumentRecord();
        row.nodeId = UUID.randomUUID();
        row.docId = docId;
        row.graphAddressId = "ds-1";
        row.accountId = "acct-relay";
        row.graphId = "intake:acct-relay";
        row.rowKind = DocumentRowKind.INTAKE;
        row.datasourceId = "ds-1";
        row.checksum = "sha256:" + docId;
        row.driveName = "intake";
        row.objectKey = "documents/acct-relay/" + row.nodeId;
        row.sizeBytes = 42L;
        row.writeManifest(DocumentManifest.newBuilder()
                .setAddress(NodeAddress.newBuilder()
                        .setDocId(docId)
                        .setGraphAddressId("ds-1")
                        .setAccountId("acct-relay")
                        .setGraphId("intake:acct-relay"))
                .setDocVersion(1)
                .build());
        return row;
    }
}
