package ai.protomolt.proto.account.service.events;

import ai.protomolt.proto.account.v1.AccountEvent;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
 * The outbox relay: drains {@code account_events_outbox} to the
 * account-events Kafka topic. One virtual-thread loop calls
 * {@link #relayOnce} forever (see {@code AccountServices.startLifecycle()}); a
 * non-empty drain loops again immediately, an empty one backs off the relay
 * interval.
 * <p>
 * Per claimed record: publish keyed by the account_id (partition-ordered per
 * account), wait for the broker ack (blocking code on a virtual thread, the
 * same contract as every other worker here), then mark the row PUBLISHED.
 * Publish precedes the PUBLISHED transition, so a relay crash between the two
 * republishes on restart — at-least-once delivery, with
 * {@code AccountEvent.event_id} as the consumer dedupe key. A failure
 * increments attempts; at {@link AccountEventRecord#MAX_ATTEMPTS} the row
 * lands FAILED (the DLQ — operator territory, the relay never re-enqueues
 * it). One bad record never kills the batch.
 * <p>
 * The drain takes the {@link Producer} interface rather than the concrete
 * {@link KafkaProducer} so tests drive it with a mock producer; production
 * producers come from {@link #newProducer}.
 */
public final class AccountEventRelay {

    private static final Logger LOG = LoggerFactory.getLogger(AccountEventRelay.class);

    private final JdbcAccountEventOutbox outbox;

    /**
     * @param outbox the outbox this relay drains
     */
    public AccountEventRelay(JdbcAccountEventOutbox outbox) {
        this.outbox = outbox;
    }

    /**
     * Drain one batch: claim up to {@code batchSize} PENDING records and
     * publish each.
     *
     * @param producer the Kafka producer (thread-safe, shared by the loop)
     * @param topic the account-events topic
     * @param batchSize the claim batch size
     * @return how many records this call transitioned to PUBLISHED
     */
    public int relayOnce(Producer<String, Message> producer, String topic, int batchSize) {
        List<AccountEventRecord> batch = outbox.claimBatch(batchSize);
        int published = 0;
        for (AccountEventRecord record : batch) {
            try {
                AccountEvent event = AccountEvent.parseFrom(record.payload);
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
     * The relay's producer, optionally backed by a Confluent-compatible
     * schema registry: String keys (the account_id), values framed and
     * validated by the protomolt serde against the packaged
     * account_events.proto descriptor set, pinned to the AccountEvent
     * wrapper type (one type, one subject — no registry needed).
     * <p>
     * Registry-free ({@code schemaRegistryUrl} null or blank), the serde
     * stamps schema id 0 into every frame: protomolt consumers read the
     * payload against the packaged contract, but standard Confluent tooling
     * resolves frames by id and cannot read id 0. With a registry URL, the
     * serde looks the subject's id up and stamps it, so relayed records are
     * resolvable by any standard consumer (the AccountEvent subject must be
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
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, AccountEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, AccountEvent.getDescriptor().getFullName()));
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
