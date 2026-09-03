package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.repo.container.ledger.DocumentEventRecord;
import ai.protomolt.proto.repo.v1.DocumentEvent;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The outbox relay: drains {@code document_events_outbox} to the
 * document-events Kafka topic. One virtual-thread loop calls
 * {@link #relayOnce} forever (see {@code RepoServices.startLifecycle()}); a
 * non-empty drain loops again immediately, an empty one backs off the purge
 * interval.
 * <p>
 * Per claimed record: publish keyed by the doc_id (partition-ordered per
 * document), wait for the broker ack (blocking code on a virtual thread, the
 * same contract as every other worker here), then mark the row PUBLISHED.
 * Publish precedes the PUBLISHED transition, so a relay crash between the two
 * republishes on restart - at-least-once delivery, with
 * {@code DocumentEvent.event_id} as the consumer dedupe key. A failure
 * increments attempts; at {@link DocumentEventRecord#MAX_ATTEMPTS} the row
 * lands FAILED (the DLQ - operator territory, the relay never re-enqueues
 * it). One bad record never kills the batch.
 */
public final class EventRelay {

    private static final Logger LOG = LoggerFactory.getLogger(EventRelay.class);

    private final JdbcEventOutbox outbox;

    /**
     * @param outbox the outbox this relay drains
     */
    public EventRelay(JdbcEventOutbox outbox) {
        this.outbox = outbox;
    }

    /**
     * Drain one batch: claim up to {@code batchSize} PENDING records and
     * publish each.
     *
     * @param producer the Kafka producer (thread-safe, shared by the loop)
     * @param topic the document-events topic
     * @param batchSize the claim batch size
     * @return how many records this call transitioned to PUBLISHED
     */
    public int relayOnce(KafkaProducer<String, Message> producer, String topic, int batchSize) {
        List<DocumentEventRecord> batch = outbox.claimBatch(batchSize);
        int published = 0;
        for (DocumentEventRecord record : batch) {
            try {
                DocumentEvent event = DocumentEvent.parseFrom(record.payload);
                producer.send(new ProducerRecord<>(topic, record.kafkaKey, event)).get();
                if (outbox.markPublished(record.eventId, Instant.now())) {
                    published++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outbox.markFailed(record, e.getMessage());
                return published;
            } catch (Exception e) {
                LOG.warn("Event relay failed for event_id={} type={} (attempt {}): {}",
                        record.eventId, record.eventType, record.attempts + 1, e.getMessage());
                outbox.markFailed(record, e.getMessage());
            }
        }
        return published;
    }

    /**
     * The relay's producer: String keys (the doc_id), values framed and
     * validated by the protomolt serde against the packaged
     * document_events.proto descriptor set, pinned to the DocumentEvent
     * wrapper type (one type, one subject - no registry needed).
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @return the configured producer
     */
    public static KafkaProducer<String, Message> newProducer(String bootstrapServers) {
        return newProducer(bootstrapServers, null);
    }

    /**
     * The relay's producer, optionally backed by a Confluent-compatible schema
     * registry. Registry-free ({@code schemaRegistryUrl} null or blank), the
     * serde stamps schema id 0 into every frame: protomolt consumers read the
     * payload against the packaged contract, but standard Confluent tooling
     * resolves frames by id and cannot read id 0. With a registry URL, the
     * serde looks the subject's id up and stamps it, so relayed records are
     * resolvable by any standard consumer (the DocumentEvent subject must be
     * registered under {@code <topic>-value}; the serde never registers).
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param schemaRegistryUrl base URL of a Confluent-compatible schema
     *        registry, or null/blank for registry-free framing
     * @return the configured producer
     */
    public static KafkaProducer<String, Message> newProducer(String bootstrapServers,
            String schemaRegistryUrl) {
        Map<String, Object> serdeConfig = new HashMap<>(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, DocumentEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName()));
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
}
