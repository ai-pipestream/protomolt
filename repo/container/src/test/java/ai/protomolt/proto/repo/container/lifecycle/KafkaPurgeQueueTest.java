package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.v1.DocumentPurgeCommand;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import jakarta.persistence.EntityManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Kafka purge queue without containers: {@link MockConsumer}/
 * {@link MockProducer} stand in for the broker and in-test fakes for the DB
 * delegate and relay store. Covers constructor/factory fail-fast, poison
 * frames, already-settled duplicates, and the retry-republish decision; the
 * offset fold itself lives in {@link PurgeOffsetTrackerTest} and the real
 * broker/Postgres behavior in KafkaPurgeQueueIT.
 */
class KafkaPurgeQueueTest {

    private static final String TOPIC = "purges-unit";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    /** In-test {@link PurgeQueue}: scripts the transition results, records the calls. */
    private static final class FakeDelegate implements PurgeQueue {
        boolean transitionResult = true;
        Optional<DocumentPurgeRecord> failedResult = Optional.empty();

        @Override
        public void enqueue(EntityManager em, DocumentPurgeRecord record) {
        }

        @Override
        public List<DocumentPurgeRecord> claimBatch(int limit) {
            throw new UnsupportedOperationException("the Kafka queue never delegates claims");
        }

        @Override
        public boolean markPurged(UUID purgeId) {
            return transitionResult;
        }

        @Override
        public boolean markVoid(UUID purgeId) {
            return transitionResult;
        }

        @Override
        public Optional<DocumentPurgeRecord> markFailed(DocumentPurgeRecord record, String error) {
            return failedResult;
        }

        @Override
        public Map<String, Long> countByStatus() {
            return Map.of();
        }
    }

    /** In-test {@link PurgeRelayStore}: scripts the rows, records the bookkeeping. */
    private static final class FakeRelayStore implements PurgeRelayStore {
        List<DocumentPurgeRecord> unrelayed = new ArrayList<>();
        Map<UUID, Optional<DocumentPurgeRecord>> pending = new HashMap<>();
        Map<UUID, Instant> relayed = new HashMap<>();
        List<UUID> failedInvalid = new ArrayList<>();
        List<UUID> unrelayedAgain = new ArrayList<>();

        @Override
        public List<DocumentPurgeRecord> claimUnrelayed(int limit) {
            List<DocumentPurgeRecord> batch = unrelayed;
            unrelayed = new ArrayList<>();
            return batch;
        }

        @Override
        public boolean markRelayed(UUID purgeId, Instant relayedAt) {
            relayed.put(purgeId, relayedAt);
            return true;
        }

        @Override
        public boolean failInvalid(UUID purgeId, String error) {
            failedInvalid.add(purgeId);
            return true;
        }

        @Override
        public boolean unrelay(UUID purgeId) {
            unrelayedAgain.add(purgeId);
            return true;
        }

        @Override
        public Optional<DocumentPurgeRecord> findPending(UUID purgeId) {
            return pending.getOrDefault(purgeId, Optional.empty());
        }
    }

    // ------------------------------------------------------------ fail fast

    @Test
    void constructorRejectsNullsAndBlankTopic() {
        FakeDelegate delegate = new FakeDelegate();
        FakeRelayStore store = new FakeRelayStore();
        MockProducer<String, Message> producer = mockProducer();
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        Duration poll = Duration.ofMillis(10);

        assertThatThrownBy(() -> new KafkaPurgeQueue(null, store, producer, consumer, TOPIC, poll))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new KafkaPurgeQueue(delegate, null, producer, consumer, TOPIC, poll))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new KafkaPurgeQueue(delegate, store, null, consumer, TOPIC, poll))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new KafkaPurgeQueue(delegate, store, producer, null, TOPIC, poll))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new KafkaPurgeQueue(delegate, store, producer, consumer, TOPIC, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new KafkaPurgeQueue(delegate, store, producer, consumer, null, poll))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaPurgeQueue(delegate, store, producer, consumer, "  ", poll))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void factoriesRejectBlankConfig() {
        assertThatThrownBy(() -> KafkaPurgeQueue.newProducer(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaPurgeQueue.newProducer("  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaPurgeQueue.newProducer(null, "http://registry:8081"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaPurgeQueue.newConsumer(null, "group"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaPurgeQueue.newConsumer("broker:9092", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaPurgeQueue.newConsumer("broker:9092", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------- poison and duplicates

    @Test
    void corruptFrameIsSkippedSettledPastAndNeverThrown() {
        FakeRelayStore store = new FakeRelayStore();
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        KafkaPurgeQueue queue = new KafkaPurgeQueue(new FakeDelegate(), store,
                mockProducer(), consumer, TOPIC, Duration.ofMillis(10));
        assign(consumer);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "poison",
                "not-a-protomolt-frame".getBytes(StandardCharsets.UTF_8)));

        assertThat(queue.claimBatch(10)).isEmpty();
        // The group moved past the poison record: it is never seen again.
        assertThat(consumer.committed(java.util.Set.of(TP)).get(TP)).isEqualTo(new OffsetAndMetadata(1));
    }

    @Test
    void aValidClaimBehindAPoisonFrameCommitsContiguously() {
        UUID purgeId = UUID.randomUUID();
        DocumentPurgeRecord row = pendingRecord(purgeId, UUID.randomUUID(), 0,
                DocumentPurgeRecord.STATUS_PENDING);
        FakeDelegate delegate = new FakeDelegate();
        FakeRelayStore store = new FakeRelayStore();
        store.pending.put(purgeId, Optional.of(row));
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        KafkaPurgeQueue queue = new KafkaPurgeQueue(delegate, store, mockProducer(),
                consumer, TOPIC, Duration.ofMillis(10));
        assign(consumer);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "poison",
                "garbage".getBytes(StandardCharsets.UTF_8)));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, row.nodeId.toString(),
                frame(command(row))));

        List<DocumentPurgeRecord> claimed = queue.claimBatch(10);
        assertThat(claimed).extracting(r -> r.purgeId).containsExactly(purgeId);
        // Only the poison frame is settled: the commit stops at the open claim.
        assertThat(consumer.committed(java.util.Set.of(TP)).get(TP)).isEqualTo(new OffsetAndMetadata(1));

        assertThat(queue.markPurged(purgeId)).isTrue();
        assertThat(consumer.committed(java.util.Set.of(TP)).get(TP)).isEqualTo(new OffsetAndMetadata(2));
    }

    @Test
    void aCommandWhoseRowIsAlreadySettledIsSettledPast() {
        UUID purgeId = UUID.randomUUID();
        DocumentPurgeRecord row = pendingRecord(purgeId, UUID.randomUUID(), 0,
                DocumentPurgeRecord.STATUS_PENDING);
        FakeRelayStore store = new FakeRelayStore();
        // findPending has no PENDING row: the duplicate loses on the spot.
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        KafkaPurgeQueue queue = new KafkaPurgeQueue(new FakeDelegate(), store,
                mockProducer(), consumer, TOPIC, Duration.ofMillis(10));
        assign(consumer);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, row.nodeId.toString(),
                frame(command(row))));

        assertThat(queue.claimBatch(10)).isEmpty();
        assertThat(consumer.committed(java.util.Set.of(TP)).get(TP)).isEqualTo(new OffsetAndMetadata(1));
    }

    // ------------------------------------------------------- retry republication

    @Test
    void markFailedBelowTheCeilingRepublishesAndSettles() {
        UUID purgeId = UUID.randomUUID();
        DocumentPurgeRecord row = pendingRecord(purgeId, UUID.randomUUID(), 0,
                DocumentPurgeRecord.STATUS_PENDING);
        DocumentPurgeRecord retried = pendingRecord(purgeId, row.nodeId, 1,
                DocumentPurgeRecord.STATUS_PENDING);
        FakeDelegate delegate = new FakeDelegate();
        delegate.failedResult = Optional.of(retried);
        FakeRelayStore store = new FakeRelayStore();
        store.pending.put(purgeId, Optional.of(row));
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, Message> producer = mockProducer();
        KafkaPurgeQueue queue = new KafkaPurgeQueue(delegate, store, producer, consumer, TOPIC,
                Duration.ofMillis(10));
        assign(consumer);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, row.nodeId.toString(),
                frame(command(row))));
        assertThat(queue.claimBatch(10)).hasSize(1);

        Optional<DocumentPurgeRecord> updated = queue.markFailed(row, "boom");
        assertThat(updated).containsSame(retried);
        // The retry rides the tail of the topic, keyed by node id.
        assertThat(producer.history()).hasSize(1);
        ProducerRecord<String, Message> republished = producer.history().get(0);
        assertThat(republished.topic()).isEqualTo(TOPIC);
        assertThat(republished.key()).isEqualTo(row.nodeId.toString());
        assertThat(((DocumentPurgeCommand) republished.value()).getPurgeId())
                .isEqualTo(purgeId.toString());
        assertThat(store.unrelayedAgain).isEmpty();
        assertThat(consumer.committed(java.util.Set.of(TP)).get(TP)).isEqualTo(new OffsetAndMetadata(1));
    }

    @Test
    void markFailedAtTheCeilingRepublishesNothing() {
        UUID purgeId = UUID.randomUUID();
        DocumentPurgeRecord row = pendingRecord(purgeId, UUID.randomUUID(), 0,
                DocumentPurgeRecord.STATUS_PENDING);
        DocumentPurgeRecord dead = pendingRecord(purgeId, row.nodeId,
                DocumentPurgeRecord.MAX_ATTEMPTS, DocumentPurgeRecord.STATUS_FAILED);
        FakeDelegate delegate = new FakeDelegate();
        delegate.failedResult = Optional.of(dead);
        FakeRelayStore store = new FakeRelayStore();
        store.pending.put(purgeId, Optional.of(row));
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, Message> producer = mockProducer();
        KafkaPurgeQueue queue = new KafkaPurgeQueue(delegate, store, producer, consumer, TOPIC,
                Duration.ofMillis(10));
        assign(consumer);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, row.nodeId.toString(),
                frame(command(row))));
        assertThat(queue.claimBatch(10)).hasSize(1);

        Optional<DocumentPurgeRecord> updated = queue.markFailed(row, "boom final");
        assertThat(updated).containsSame(dead);
        // The FAILED row IS the DLQ: no extra topic, no republication.
        assertThat(producer.history()).isEmpty();
        assertThat(store.unrelayedAgain).isEmpty();
        assertThat(consumer.committed(java.util.Set.of(TP)).get(TP)).isEqualTo(new OffsetAndMetadata(1));
    }

    @Test
    void markFailedOnARecordACompetitorSettledJustSettlesTheOffset() {
        UUID purgeId = UUID.randomUUID();
        DocumentPurgeRecord row = pendingRecord(purgeId, UUID.randomUUID(), 0,
                DocumentPurgeRecord.STATUS_PENDING);
        FakeDelegate delegate = new FakeDelegate();
        delegate.failedResult = Optional.empty();
        FakeRelayStore store = new FakeRelayStore();
        store.pending.put(purgeId, Optional.of(row));
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, Message> producer = mockProducer();
        KafkaPurgeQueue queue = new KafkaPurgeQueue(delegate, store, producer, consumer, TOPIC,
                Duration.ofMillis(10));
        assign(consumer);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, row.nodeId.toString(),
                frame(command(row))));
        assertThat(queue.claimBatch(10)).hasSize(1);

        assertThat(queue.markFailed(row, "too late")).isEmpty();
        assertThat(producer.history()).isEmpty();
        assertThat(consumer.committed(java.util.Set.of(TP)).get(TP)).isEqualTo(new OffsetAndMetadata(1));
    }

    @Test
    void aFailedRepublicationReturnsTheRowToTheUnrelayedScan() {
        UUID purgeId = UUID.randomUUID();
        DocumentPurgeRecord row = pendingRecord(purgeId, UUID.randomUUID(), 0,
                DocumentPurgeRecord.STATUS_PENDING);
        DocumentPurgeRecord retried = pendingRecord(purgeId, row.nodeId, 1,
                DocumentPurgeRecord.STATUS_PENDING);
        FakeDelegate delegate = new FakeDelegate();
        delegate.failedResult = Optional.of(retried);
        FakeRelayStore store = new FakeRelayStore();
        store.pending.put(purgeId, Optional.of(row));
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, Message> producer = new MockProducer<String, Message>(true, null, new StringSerializer(), messageSerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, Message> record) {
                CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
                future.completeExceptionally(new RuntimeException("broker down"));
                return future;
            }
        };
        KafkaPurgeQueue queue = new KafkaPurgeQueue(delegate, store, producer, consumer, TOPIC,
                Duration.ofMillis(10));
        assign(consumer);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, row.nodeId.toString(),
                frame(command(row))));
        assertThat(queue.claimBatch(10)).hasSize(1);

        assertThat(queue.markFailed(row, "boom")).isPresent();
        // The DB row guarantees the redelivery, so the offset still settles.
        assertThat(store.unrelayedAgain).containsExactly(purgeId);
        assertThat(consumer.committed(java.util.Set.of(TP)).get(TP)).isEqualTo(new OffsetAndMetadata(1));
    }

    // ------------------------------------------------------------------ relay

    @Test
    void relayPublishesUnrelayedRowsAndStampsThem() {
        DocumentPurgeRecord row = pendingRecord(UUID.randomUUID(), UUID.randomUUID(), 0,
                DocumentPurgeRecord.STATUS_PENDING);
        FakeRelayStore store = new FakeRelayStore();
        store.unrelayed.add(row);
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, Message> producer = mockProducer();
        KafkaPurgeQueue queue = new KafkaPurgeQueue(new FakeDelegate(), store, producer, consumer,
                TOPIC, Duration.ofMillis(10));
        assign(consumer);

        assertThat(queue.claimBatch(10)).isEmpty();
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).key()).isEqualTo(row.nodeId.toString());
        assertThat(store.relayed).containsKey(row.purgeId);
        assertThat(store.failedInvalid).isEmpty();
    }

    @Test
    void anUnframeableRowGoesStraightToFailed() {
        DocumentPurgeRecord row = pendingRecord(UUID.randomUUID(), UUID.randomUUID(), 0,
                DocumentPurgeRecord.STATUS_PENDING);
        FakeRelayStore store = new FakeRelayStore();
        store.unrelayed.add(row);
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, Message> producer = new MockProducer<String, Message>(true, null, new StringSerializer(), messageSerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, Message> record) {
                throw new SerializationException("validation rejected the command");
            }
        };
        KafkaPurgeQueue queue = new KafkaPurgeQueue(new FakeDelegate(), store, producer, consumer,
                TOPIC, Duration.ofMillis(10));
        assign(consumer);

        assertThat(queue.claimBatch(10)).isEmpty();
        assertThat(store.failedInvalid).containsExactly(row.purgeId);
        assertThat(store.relayed).isEmpty();
    }

    // ---------------------------------------------------------------- helpers

    /** An auto-completing producer with the serializers MockProducer wants. */
    private static MockProducer<String, Message> mockProducer() {
        return new MockProducer<>(true, null, new StringSerializer(), messageSerializer());
    }

    private static Serializer<Message> messageSerializer() {
        return (topic, data) -> data == null ? null : data.toByteArray();
    }

    /** Assign the single partition at offset 0 (the MockConsumer rebalance dance). */
    private static void assign(MockConsumer<String, byte[]> consumer) {
        consumer.rebalance(List.of(TP));
        consumer.updateBeginningOffsets(Map.of(TP, 0L));
    }

    private static DocumentPurgeCommand command(DocumentPurgeRecord record) {
        return DocumentPurgeCommand.newBuilder()
                .setPurgeId(record.purgeId.toString())
                .setNodeId(record.nodeId.toString())
                .setAccountId(record.accountId)
                .setDriveName(record.driveName)
                .setRequestedAt(Timestamp.newBuilder()
                        .setSeconds(record.requestedAt.getEpochSecond())
                        .setNanos(record.requestedAt.getNano()))
                .addAllObjectKeys(record.readObjectKeys())
                .build();
    }

    /** Serialize a command exactly as the relay's producer would. */
    private static byte[] frame(DocumentPurgeCommand command) {
        ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
        serializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                DocumentPurgeCommandFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                DocumentPurgeCommand.getDescriptor().getFullName()), false);
        return serializer.serialize(TOPIC, command);
    }

    private static DocumentPurgeRecord pendingRecord(UUID purgeId, UUID nodeId, int attempts,
            String status) {
        DocumentPurgeRecord record = new DocumentPurgeRecord();
        record.purgeId = purgeId;
        record.nodeId = nodeId;
        record.docId = "doc-" + purgeId;
        record.graphAddressId = "ds-unit";
        record.accountId = "acct-unit";
        record.graphId = "intake:acct-unit";
        record.driveName = "docs";
        record.writeObjectKeys(List.of("k/" + purgeId));
        record.requestedAt = Instant.now();
        record.attempts = attempts;
        record.status = status;
        return record;
    }
}
