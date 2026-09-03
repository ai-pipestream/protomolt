package ai.protomolt.proto.config.kafka;

import ai.protomolt.proto.config.ConfigSource;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import com.google.protobuf.Message;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Kafka plug: config documents on a compacted topic. The record key
 * is the house convention — a deterministic name-based UUID over the
 * subject ({@link #keyFor}), the same key on every publish, which is
 * exactly the identity compaction needs — the subject itself rides the
 * {@link #SUBJECT_HEADER} record header, and the value is the typed
 * message through the house serde against the registry. The serde's {@code validate.on.read} is forced on — the
 * config lane never reads an unvalidated document — so a poisoned record
 * refuses at deserialization with the record's coordinates, and the
 * consumer keeps serving what it runs. Publishers write with the same
 * serde and {@code validate.on.write}, making the topic gate both
 * directions; compaction keeps the latest document per subject, and this
 * source additionally keeps latest-per-key itself, so compaction lag is
 * invisible. The version is the record's {@code partition:offset}.
 *
 * <p>No consumer group, no membership: the source assigns the topic's
 * partitions directly, reads from the beginning, and each fetch drains to
 * the end offsets it observed — a replicated config log consumed as a
 * table, without this codebase writing any consensus.</p>
 */
public final class KafkaConfigSource implements ConfigSource {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfigSource.class);

    /** The record header carrying the config subject: {@value}. */
    public static final String SUBJECT_HEADER = "protomolt-config-subject";

    /**
     * The house key convention: a deterministic name-based UUID over the
     * subject, so identity is derived, never invented — the same record
     * key on every publish of a subject, which is exactly what compaction
     * needs. Publishers use this; the consumer verifies it.
     *
     * @param subject the config subject
     * @return the record key
     */
    public static String keyFor(String subject) {
        if (subject == null || subject.isBlank()) {
            throw new IllegalArgumentException("subject must not be blank");
        }
        return java.util.UUID.nameUUIDFromBytes(
                subject.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
    }

    /**
     * The plug's configuration.
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param topic the compacted config topic
     * @param schemaRegistryUrl the registry the serde resolves and
     *        validates against
     * @param properties extra serde properties; {@code validate.on.read}
     *        cannot be turned off here
     */
    public record Config(String bootstrapServers, String topic, String schemaRegistryUrl,
            Map<String, String> properties) {

        public Config {
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                throw new IllegalArgumentException("bootstrapServers must not be blank");
            }
            if (topic == null || topic.isBlank()) {
                throw new IllegalArgumentException("topic must not be blank");
            }
            if (schemaRegistryUrl == null || schemaRegistryUrl.isBlank()) {
                throw new IllegalArgumentException("schemaRegistryUrl must not be blank");
            }
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }

        /** A configuration with no extra serde properties. */
        public Config(String bootstrapServers, String topic, String schemaRegistryUrl) {
            this(bootstrapServers, topic, schemaRegistryUrl, Map.of());
        }
    }

    private sealed interface Slot {
    }

    private record Valid(String version, byte[] payload) implements Slot {
    }

    private record Poisoned(String version, String reason) implements Slot {
    }

    private final String topic;
    private final KafkaConsumer<String, byte[]> consumer;
    private final ProtoMoltProtobufDeserializer values;
    private final Map<String, Slot> latest = new HashMap<>();

    public KafkaConfigSource(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.topic = config.topic();

        Map<String, Object> serde = new HashMap<>(config.properties());
        serde.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, config.schemaRegistryUrl());
        // The config lane never reads an unvalidated document.
        serde.put(ProtoMoltSerdeConfig.VALIDATE_ON_READ, true);
        this.values = new ProtoMoltProtobufDeserializer();
        this.values.configure(serde, false);

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.bootstrapServers());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(consumerProperties,
                new StringDeserializer(), new ByteArrayDeserializer());
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(partition -> new TopicPartition(topic, partition.partition()))
                .toList();
        if (partitions.isEmpty()) {
            throw new IllegalStateException(
                    "topic '" + topic + "' has no partitions: create the compacted"
                            + " config topic first");
        }
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }

    @Override
    public synchronized Optional<Fetched> fetch(String subject) {
        drainToEnd();
        // Exhaustive over the sealed Slot, so a third variant has to be answered here rather
        // than reaching the reader through an unchecked cast.
        return switch (latest.get(subject)) {
            case null -> Optional.empty();
            case Valid valid -> Optional.of(new Fetched(valid.version(), valid.payload()));
            case Poisoned poisoned -> throw new IllegalStateException("config record for '"
                    + subject + "' at " + poisoned.version() + " refused by the serde: "
                    + poisoned.reason());
        };
    }

    /** Reads every record up to the end offsets observed at entry. */
    private void drainToEnd() {
        Map<TopicPartition, Long> ends = consumer.endOffsets(consumer.assignment());
        while (behind(ends)) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(250));
            for (ConsumerRecord<String, byte[]> record : records) {
                accept(record);
            }
        }
    }

    private boolean behind(Map<TopicPartition, Long> ends) {
        for (Map.Entry<TopicPartition, Long> end : ends.entrySet()) {
            if (consumer.position(end.getKey()) < end.getValue()) {
                return true;
            }
        }
        return false;
    }

    private void accept(ConsumerRecord<String, byte[]> record) {
        var header = record.headers().lastHeader(SUBJECT_HEADER);
        if (header == null) {
            LOG.warn("config record at {}:{} carries no {} header — skipped",
                    record.partition(), record.offset(), SUBJECT_HEADER);
            return;
        }
        String subject = new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
        String version = record.partition() + ":" + record.offset();
        if (!keyFor(subject).equals(record.key())) {
            // Identity is derived, never trusted: a key that does not
            // derive from the subject poisons the subject loudly instead
            // of silently landing under the wrong compaction identity.
            latest.put(subject, new Poisoned(version, "record key '" + record.key()
                    + "' does not derive from subject '" + subject + "'"));
            LOG.warn("config record for '{}' at {} has a key that does not derive from"
                    + " the subject; refused", subject, version);
            return;
        }
        if (record.value() == null) {
            // A compaction tombstone: the subject's document is gone, and
            // absence is not removal at the consumer, so latest emptiness
            // simply stops offering a document.
            latest.remove(subject);
            return;
        }
        try {
            Message message = values.deserialize(topic, record.value());
            latest.put(subject, new Valid(version, message.toByteArray()));
        } catch (RuntimeException e) {
            latest.put(subject, new Poisoned(version, e.getMessage()));
            LOG.warn("config record for '{}' at {} refused by the serde; the consumer"
                    + " keeps its current config", subject, version, e);
        }
    }

    @Override
    public synchronized void close() {
        consumer.close();
        values.close();
    }
}
