package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.pipestream.proto.repo.container.ledger.LedgerException;
import ai.pipestream.proto.repo.container.ledger.Tx;
import ai.pipestream.proto.repo.v1.DocumentPurgeCommand;
import com.google.protobuf.Message;
import jakarta.persistence.EntityManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

/**
 * The Kafka-backed {@link PurgeQueue}: the {@code document_purges} row stays
 * the ledger of record (Phase A atomicity is untouched - {@link #enqueue}
 * still lands the row in the caller's transaction) and the topic only
 * distributes claims to the purger fleet.
 * <p>
 * <b>Relay.</b> {@link #claimBatch(int)} first relays: PENDING rows with
 * {@code relayed_at IS NULL} are selected {@code FOR UPDATE SKIP LOCKED},
 * each published as a {@link DocumentPurgeCommand} keyed by node id
 * (partition-ordered per document) through the protomolt serde
 * (validate-on-write), the broker ack awaited on the caller's virtual thread
 * (the same blocking contract as {@link EventRelay}), and {@code relayed_at}
 * stamped after. Publish precedes the stamp, so a crash mid-flight
 * republishes - a duplicate on the topic, harmless because every settle is a
 * conditional DB transition. A row whose command fails serialization or
 * validation can never be relayed: it goes straight to FAILED with the error
 * (the row IS the DLQ) instead of wedging the relay scan.
 * <p>
 * <b>Claim.</b> The batch then comes from polling this instance's consumer
 * group. Every polled record is tracked by offset; a frame that does not
 * decode (or fails revalidation on read) is logged and settled past, never
 * thrown out of {@code claimBatch} - one poison record must not wedge the
 * group - and a command whose purge id does not parse as a UUID is handled
 * the same way. A decodable command is re-read from the DB: only a row still
 * PENDING joins the batch (the full row, so downstream eventing keeps the
 * denormalized identity fields); a duplicate of an already settled purge is
 * settled past on the spot.
 * <p>
 * <b>Settle.</b> {@link #markPurged}/{@link #markVoid} run the same
 * conditional transition as the JDBC queue, then settle the record's tracked
 * offsets (whether or not this call won the transition - a double-take means
 * the work is done either way). Offsets commit per partition at the highest
 * CONTIGUOUS settled offset (see {@link PurgeOffsetTracker}), never
 * auto-committed, so a crash before settling redelivers the command to the
 * group. {@link #markFailed} climbs the attempts ladder through the delegate;
 * below {@link DocumentPurgeRecord#MAX_ATTEMPTS} it republishes the command
 * (the retry rides the tail of the topic; if the republication itself fails
 * the row goes back on the unrelayed scan instead) and at the ceiling the row
 * lands FAILED - the DLQ, no extra topic - and either way the offset settles,
 * because the DB row guarantees the redelivery.
 * <p>
 * <b>Threading.</b> NOT thread-safe: the {@link KafkaConsumer} it holds is
 * single-threaded, so exactly one purger loop owns an instance (the producer
 * is thread-safe and may be shared). Every blocking call is meant to run on a
 * virtual thread, like the rest of the lifecycle.
 */
public final class KafkaPurgeQueue implements PurgeQueue {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPurgeQueue.class);

    private final PurgeQueue delegate;
    private final PurgeRelayStore store;
    private final Producer<String, Message> producer;
    private final Consumer<String, byte[]> consumer;
    private final String topic;
    private final Duration pollTimeout;
    private final ProtoMoltProtobufDeserializer decoder;
    private final PurgeOffsetTracker offsets = new PurgeOffsetTracker();
    private final Map<UUID, List<InFlightOffset>> inFlight = new HashMap<>();

    /** One claimed record's poll coordinates, settled by purge id. */
    private record InFlightOffset(TopicPartition partition, long offset) {
    }

    /**
     * The production wiring: a JDBC delegate and relay store over {@code tx},
     * claiming from {@code topic} with the given consumer group member.
     *
     * @param tx the transactional EntityManager wrapper shared by this service
     * @param producer the relay producer (thread-safe), from {@link #newProducer}
     * @param consumer the claim consumer (NOT thread-safe; owned by one purger
     *        loop), from {@link #newConsumer}
     * @param topic the purge-commands topic
     * @param pollTimeout the consumer poll budget per {@link #claimBatch} call
     * @return the wired queue
     */
    public static KafkaPurgeQueue create(Tx tx, Producer<String, Message> producer,
            Consumer<String, byte[]> consumer, String topic, Duration pollTimeout) {
        return new KafkaPurgeQueue(new JdbcPurgeQueue(tx), new JdbcPurgeRelayStore(tx),
                producer, consumer, topic, pollTimeout);
    }

    /**
     * @param delegate the JDBC queue the row-level operations delegate to
     * @param store the relay bookkeeping on the purge rows
     * @param producer the relay producer
     * @param consumer the claim consumer; subscribed to the topic here, and
     *        owned by this instance from then on
     * @param topic the purge-commands topic (must not be blank)
     * @param pollTimeout the consumer poll budget per claim
     * @throws NullPointerException when any argument is null
     * @throws IllegalArgumentException when {@code topic} is blank
     */
    KafkaPurgeQueue(PurgeQueue delegate, PurgeRelayStore store, Producer<String, Message> producer,
            Consumer<String, byte[]> consumer, String topic, Duration pollTimeout) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.store = Objects.requireNonNull(store, "store");
        this.producer = Objects.requireNonNull(producer, "producer");
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        this.topic = topic;
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        // The claim-side frame reader: pinned to DocumentPurgeCommand against
        // the packaged contract, revalidating on read, so a producer that
        // bypassed the serde is caught here instead of wedging the group.
        this.decoder = new ProtoMoltProtobufDeserializer();
        this.decoder.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                DocumentPurgeCommandFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentPurgeCommand.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true), false);
        this.consumer.subscribe(List.of(topic));
    }

    @Override
    public void enqueue(EntityManager em, DocumentPurgeRecord record) {
        // The row lands PENDING in the caller's transaction, exactly as on the
        // JDBC queue; the relay picks it up after the commit.
        delegate.enqueue(em, record);
    }

    @Override
    public List<DocumentPurgeRecord> claimBatch(int limit) {
        relayPending(limit);
        List<DocumentPurgeRecord> claimed = new ArrayList<>();
        ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<String, byte[]> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            offsets.track(partition, record.offset());
            DocumentPurgeCommand command = decode(record);
            if (command == null) {
                // Poison frame: logged in decode; settle past it so the group
                // is never wedged by one bad record.
                offsets.settle(partition, record.offset());
                continue;
            }
            UUID purgeId = parsePurgeId(command, record);
            if (purgeId == null) {
                offsets.settle(partition, record.offset());
                continue;
            }
            Optional<DocumentPurgeRecord> pending = store.findPending(purgeId);
            if (pending.isEmpty()) {
                // A duplicate of an already settled purge (or a command whose
                // transaction rolled back): the work is done or never existed.
                offsets.settle(partition, record.offset());
                continue;
            }
            inFlight.computeIfAbsent(purgeId, k -> new ArrayList<>())
                    .add(new InFlightOffset(partition, record.offset()));
            claimed.add(pending.get());
        }
        commitOffsets();
        return claimed;
    }

    @Override
    public boolean markPurged(UUID purgeId) {
        boolean transitioned = delegate.markPurged(purgeId);
        settle(purgeId);
        return transitioned;
    }

    @Override
    public boolean markVoid(UUID purgeId) {
        boolean transitioned = delegate.markVoid(purgeId);
        settle(purgeId);
        return transitioned;
    }

    @Override
    public Optional<DocumentPurgeRecord> markFailed(DocumentPurgeRecord record, String error) {
        Optional<DocumentPurgeRecord> updated = delegate.markFailed(record, error);
        if (updated.isEmpty()) {
            // Settled by a competing purger: the offset work is done.
            settle(record.purgeId);
            return updated;
        }
        DocumentPurgeRecord after = updated.get();
        if (!DocumentPurgeRecord.STATUS_FAILED.equals(after.status)) {
            // Below the attempts ceiling the retry rides the tail of the
            // topic. If the republication itself fails, the row goes back on
            // the unrelayed scan so the next claim re-relays it instead.
            try {
                producer.send(new ProducerRecord<>(topic, after.nodeId.toString(),
                        DocumentPurgeCommandFactory.command(after))).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                store.unrelay(after.purgeId);
                LOG.warn("Retry republish interrupted for purge_id={}: the row returns to the "
                        + "unrelayed scan", after.purgeId);
            } catch (Exception e) {
                store.unrelay(after.purgeId);
                LOG.warn("Retry republish failed for purge_id={} ({}): the row returns to the "
                        + "unrelayed scan", after.purgeId, e.getMessage());
            }
        }
        // At the ceiling the FAILED row IS the DLQ (nothing republishes);
        // below it the DB row guarantees the redelivery. Either way the
        // record's offsets settle.
        settle(after.purgeId);
        return updated;
    }

    @Override
    public Map<String, Long> countByStatus() {
        return delegate.countByStatus();
    }

    /**
     * Relay one batch: publish every unrelayed PENDING row, oldest first,
     * stamping {@code relayed_at} after each ack. One bad row never kills the
     * batch: a serialization/validation failure lands the row FAILED (it can
     * never be relayed), a broker failure leaves it unrelayed for the next
     * claim.
     */
    private void relayPending(int limit) {
        for (DocumentPurgeRecord record : store.claimUnrelayed(limit)) {
            try {
                producer.send(new ProducerRecord<>(topic, record.nodeId.toString(),
                        DocumentPurgeCommandFactory.command(record))).get();
            } catch (SerializationException | LedgerException e) {
                // The command itself is unframeable (validation, serialization,
                // an unreadable key snapshot): this row can NEVER be relayed,
                // so fail it terminally instead of retrying forever.
                LOG.error("Purge {} can never be relayed (marking FAILED): {}",
                        record.purgeId, e.getMessage());
                store.failInvalid(record.purgeId, e.getMessage());
                continue;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Broker-side failure: the row stays unrelayed, the next claim
                // republishes it.
                LOG.warn("Purge relay failed for purge_id={} (stays unrelayed): {}",
                        record.purgeId, e.getMessage());
                continue;
            }
            store.markRelayed(record.purgeId, Instant.now());
        }
    }

    /** Decode one polled record; null (already logged) means settle past it. */
    private DocumentPurgeCommand decode(ConsumerRecord<String, byte[]> record) {
        byte[] value = record.value();
        if (value == null) {
            LOG.warn("Skipping a null-valued record on {}[{}]@{}",
                    record.topic(), record.partition(), record.offset());
            return null;
        }
        Message decoded;
        try {
            decoded = decoder.deserialize(record.topic(), value);
        } catch (SerializationException e) {
            LOG.warn("Skipping an undecodable record on {}[{}]@{}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            return null;
        }
        if (!(decoded instanceof DocumentPurgeCommand command)) {
            LOG.warn("Skipping a record of unexpected type {} on {}[{}]@{}",
                    decoded.getClass().getName(), record.topic(), record.partition(),
                    record.offset());
            return null;
        }
        return command;
    }

    /** The command's purge id as a UUID; null (already logged) means settle past it. */
    private UUID parsePurgeId(DocumentPurgeCommand command, ConsumerRecord<String, byte[]> record) {
        try {
            return UUID.fromString(command.getPurgeId());
        } catch (IllegalArgumentException e) {
            LOG.warn("Skipping a purge command with unparseable purge_id '{}' on {}[{}]@{}",
                    command.getPurgeId(), record.topic(), record.partition(), record.offset());
            return null;
        }
    }

    /** Settle every offset tracked for {@code purgeId} and commit the advances. */
    private void settle(UUID purgeId) {
        List<InFlightOffset> settled = inFlight.remove(purgeId);
        if (settled != null) {
            for (InFlightOffset offset : settled) {
                offsets.settle(offset.partition(), offset.offset());
            }
        }
        commitOffsets();
    }

    /** Commit the contiguous high-water advances, when any. */
    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> committable = offsets.committable();
        if (!committable.isEmpty()) {
            consumer.commitSync(committable);
            offsets.committed(committable);
        }
    }

    /**
     * The relay's producer: String keys (the node id), values framed and
     * validated by the protomolt serde against the packaged
     * document_purge.proto descriptor set, pinned to the DocumentPurgeCommand
     * type (one type, one subject - no registry needed), acks=all and
     * idempotent, exactly as {@link EventRelay}'s.
     *
     * @param bootstrapServers the Kafka bootstrap servers (must not be blank)
     * @return the configured producer
     * @throws IllegalArgumentException when {@code bootstrapServers} is null or blank
     */
    public static KafkaProducer<String, Message> newProducer(String bootstrapServers) {
        return newProducer(bootstrapServers, null);
    }

    /**
     * The relay's producer, optionally backed by a Confluent-compatible schema
     * registry, with the same registry-free vs registry-stamped framing
     * contract as {@link EventRelay#newProducer(String, String)} (registry
     * mode requires the DocumentPurgeCommand subject registered under
     * {@code <topic>-value}; the serde never registers).
     *
     * @param bootstrapServers the Kafka bootstrap servers (must not be blank)
     * @param schemaRegistryUrl base URL of a Confluent-compatible schema
     *        registry, or null/blank for registry-free framing
     * @return the configured producer
     * @throws IllegalArgumentException when {@code bootstrapServers} is null or blank
     */
    public static KafkaProducer<String, Message> newProducer(String bootstrapServers,
            String schemaRegistryUrl) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        Map<String, Object> serdeConfig = new HashMap<>(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                DocumentPurgeCommandFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentPurgeCommand.getDescriptor().getFullName()));
        if (schemaRegistryUrl != null && !schemaRegistryUrl.isBlank()) {
            serdeConfig.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        }
        ProtoMoltProtobufSerializer valueSerializer = new ProtoMoltProtobufSerializer();
        valueSerializer.configure(serdeConfig, false);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new KafkaProducer<>(props, new StringSerializer(), valueSerializer);
    }

    /**
     * The claim consumer: String keys, raw byte values (frames are decoded
     * per record by the queue, so a poison frame is skipped instead of
     * failing the poll), manual offset management - offsets commit only as
     * records settle, at the highest contiguous settled offset per partition.
     *
     * @param bootstrapServers the Kafka bootstrap servers (must not be blank)
     * @param groupId the purger fleet's consumer group (must not be blank)
     * @return the configured consumer, not yet subscribed
     * @throws IllegalArgumentException when either argument is null or blank
     */
    public static KafkaConsumer<String, byte[]> newConsumer(String bootstrapServers, String groupId) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());
    }
}
