package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import com.google.protobuf.Message;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The outbox relay over a real Redpanda broker (requirements §10, §15.9): binary
 * DocumentEvent records that the protomolt serde validates on read, keyed by the
 * full canonical address, same-key ordered across partial failures, and
 * republished with an identical event_id when a crash splits ack from relay stamp.
 */
@Testcontainers
class OutboxKafkaIT {
    // Same baseline image as the service eventing lane (testcontainers' own pinned tag).
    @Container static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    private AccessionFixture f;
    private String account;
    private String topic;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        topic = "outbox-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void publishesValidatedEventsKeyedByTheFullCanonicalAddress() throws Exception {
        var a = address(account, "doc-a");
        var b = address(account, "doc-b");
        f.saves.save(processAuthority(), create("a-1", a, document(account, "doc-a", "a1")).build());
        f.saves.save(processAuthority(), create("b-1", b, document(account, "doc-b", "b1")).build());
        f.saves.save(processAuthority(), titleSave("a-2", a, 1, "a2"));

        try (var publisher = new KafkaOutboxPublisher(REDPANDA.getBootstrapServers(), topic)) {
            assertThat(new OutboxRelay(f.catalog, publisher).relayOnce(10)).isEqualTo(3);
        }
        var records = consume(3);
        var keyA = CatalogRecords.eventKey(a);
        var keyB = CatalogRecords.eventKey(b);
        assertThat(records.stream().map(ConsumerRecord::key).toList())
                .containsExactly(keyA, keyB, keyA);
        assertThat(keyA).as("the key is the canonical address, never the doc_id alone").isNotEqualTo("doc-a");
        for (var record : records) {
            assertThat(record.value()).as("the validating deserializer returns the generated event type")
                    .isInstanceOf(DocumentEvent.class);
        }
        var events = records.stream().map(record -> (DocumentEvent) record.value()).toList();
        assertThat(events.stream().map(DocumentEvent::getOperationId).toList()).containsExactly("a-1", "b-1", "a-2");
        var revisionsOfA = events.stream().filter(event -> event.getAddress().equals(a))
                .map(DocumentEvent::getRevision).toList();
        assertThat(revisionsOfA).as("one key keeps revision order").containsExactly(1L, 2L);
    }

    @Test void sameKeyOrderingHoldsWhenOnePublishFailsMidPass() throws Exception {
        var a = address(account, "doc-a");
        f.saves.save(processAuthority(), create("a-1", a, document(account, "doc-a", "a1")).build());
        f.saves.save(processAuthority(), titleSave("a-2", a, 1, "a2"));
        var b = address(account, "doc-b");
        f.saves.save(processAuthority(), create("b-1", b, document(account, "doc-b", "b1")).build());

        var failFirstPublishOfA = new AtomicBoolean(true);
        try (var kafka = new KafkaOutboxPublisher(REDPANDA.getBootstrapServers(), topic)) {
            OutboxRelay.Publisher flaky = (key, event) -> {
                if (event.getAddress().equals(a) && event.getRevision() == 1 && failFirstPublishOfA.getAndSet(false))
                    throw new java.io.IOException("broker timeout");
                kafka.publish(key, event);
            };
            assertThat(new OutboxRelay(f.catalog, flaky).relayOnce(10)).isEqualTo(1);
            assertThat(new OutboxRelay(f.catalog, kafka).relayOnce(10)).isEqualTo(2);
        }
        var records = consume(3);
        var revisionsOfA = records.stream().filter(record -> record.key().equals(CatalogRecords.eventKey(a)))
                .map(record -> ((DocumentEvent) record.value()).getRevision()).toList();
        assertThat(revisionsOfA).as("A@1 reaches the topic strictly before A@2").containsExactly(1L, 2L);
        assertThat(f.catalog.pendingEvents(10)).isEmpty();
    }

    @Test void aCrashBetweenBrokerAckAndRelayStampRepublishesTheSameEventId() throws Exception {
        var a = address(account, "doc-a");
        f.saves.save(processAuthority(), create("a-1", a, document(account, "doc-a", "a1")).build());
        var eventId = f.catalog.events(a).getFirst().getEventId();

        var crashAfterAck = new AtomicBoolean(true);
        try (var kafka = new KafkaOutboxPublisher(REDPANDA.getBootstrapServers(), topic)) {
            OutboxRelay.Publisher crashing = (key, event) -> {
                kafka.publish(key, event);
                if (crashAfterAck.getAndSet(false)) throw new java.io.IOException("host down after the broker ack");
            };
            assertThat(new OutboxRelay(f.catalog, crashing).relayOnce(10)).isZero();
            assertThat(f.catalog.pendingEvents(10)).hasSize(1);
            assertThat(new OutboxRelay(f.catalog, kafka).relayOnce(10)).isEqualTo(1);
        }
        var records = consume(2);
        assertThat(records.stream().map(record -> ((DocumentEvent) record.value()).getEventId()).toList())
                .as("at-least-once delivery replays the identical event_id; consumers deduplicate by it")
                .containsExactly(eventId, eventId);
        assertThat(records.stream().map(record -> ((DocumentEvent) record.value()).getRevision()).toList())
                .containsExactly(1L, 1L);
        assertThat(f.catalog.pendingEvents(10)).isEmpty();
    }

    // ------------------------------------------------------------------ harness

    /** Reads exactly {@code expected} records with the validating protobuf deserializer. */
    private List<ConsumerRecord<String, Message>> consume(int expected) {
        var deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, KafkaOutboxPublisher.documentEventDescriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true), false);
        var properties = new java.util.Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-kafka-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var records = new ArrayList<ConsumerRecord<String, Message>>();
        try (var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), deserializer)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (records.size() < expected && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.topic().equals(topic)) records.add(record);
                }
            }
        }
        assertThat(records).as("records consumed from the topic within the deadline").hasSize(expected);
        return records;
    }

    private static SaveDocumentRequest titleSave(String operation, ai.protomolt.proto.repo.v1.NodeAddress address,
            long expected, String title) {
        var core = ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId(address.getDocId())
                .setOwnership(ownership(address.getAccountId()))
                .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle(title)).build();
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(expected)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
    }
}
