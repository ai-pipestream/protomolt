package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayDeque;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Publishes the crawler's output to Kafka. Every {@link ConfluenceChange}
 * lands on the changes topic keyed by its {@code change_id}; every
 * {@link ConfluenceSnapshot} marker lands on the snapshots topic keyed by its
 * {@code snapshot_id}. Values are framed by the protomolt serde
 * ({@link ProtoMoltProtobufSerializer}) against the descriptor set built from
 * this module's own generated classes, so the schema's declared validation
 * rules (required {@code change_id}, the {@code change.upsert_has_entity} CEL
 * rule) are enforced on write: an invalid record never reaches the topic.
 *
 * <p>The sink never throws into the crawler loop. The crawler treats a sink
 * exception as a crawl failure (it aborts the sweep), and a Kafka outage or
 * a rejected record must not kill a multi-hour crawl: sends are async, the
 * serde's synchronous rejections are caught here, and both paths log and move
 * on. The delivery guarantee is therefore at-most-once per record; operators
 * who need the gap filled re-run the crawl, which the deterministic change
 * cursors make safe to do.</p>
 *
 * <p>Thread-safe: {@link KafkaProducer} is, and the topics are final.</p>
 */
public final class KafkaChangeSink implements ChangeSink, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(KafkaChangeSink.class.getName());

    private final KafkaProducer<String, Message> producer;
    private final String topic;
    private final String snapshotsTopic;

    /**
     * @param producer the producer to publish through (String keys, Message
     *        values; see {@link #newProducer})
     * @param topic the topic changes publish to
     * @param snapshotsTopic the topic snapshot markers publish to
     */
    public KafkaChangeSink(KafkaProducer<String, Message> producer, String topic,
            String snapshotsTopic) {
        this.producer = Objects.requireNonNull(producer, "producer");
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic cannot be null or blank");
        }
        if (snapshotsTopic == null || snapshotsTopic.isBlank()) {
            throw new IllegalArgumentException("snapshotsTopic cannot be null or blank");
        }
        this.topic = topic;
        this.snapshotsTopic = snapshotsTopic;
    }

    /**
     * Builds the sink from the connector config: bootstrap servers and the
     * optional schema registry URL come straight off it.
     *
     * @param config the connector config (must have {@code kafkaEnabled()})
     * @return the ready sink
     */
    public static KafkaChangeSink create(ConfluenceConnectorConfig config) {
        return new KafkaChangeSink(
                newProducer(config.kafkaBootstrapServers(), config.schemaRegistryUrl()),
                config.kafkaTopic(), config.kafkaSnapshotsTopic());
    }

    /**
     * The sink's producer: String keys, values framed and validated by the
     * protomolt serde against the packaged events.proto descriptor set. The
     * serde is unpinned (both {@link ConfluenceChange} and
     * {@link ConfluenceSnapshot} are declared in that set, one producer serves
     * both topics); registry-free, it stamps schema id 0, which protomolt
     * consumers read against the packaged contract.
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param schemaRegistryUrl base URL of a Confluent-compatible schema
     *        registry, or null/blank for registry-free framing
     * @return the configured producer
     */
    public static KafkaProducer<String, Message> newProducer(String bootstrapServers,
            String schemaRegistryUrl) {
        Map<String, Object> serdeConfig = new java.util.HashMap<>(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64()));
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

    @Override
    public void emit(ConfluenceChange change) {
        send(topic, change.getChangeId(), change);
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        send(snapshotsTopic, snapshot.getSnapshotId(), snapshot);
    }

    /**
     * One async send. The serde validates inside {@code producer.send}, so its
     * rejection of an invalid record surfaces here as a synchronous
     * SerializationException; broker-side failures surface on the callback.
     * Both are logged, never thrown: the crawl must go on.
     */
    private void send(String topic, String key, Message value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
                if (exception != null) {
                    LOG.log(System.Logger.Level.WARNING,
                            "confluence kafka sink: record key={0} to {1} failed: {2}",
                            key, topic, exception.toString());
                }
            });
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING,
                    "confluence kafka sink: record key={0} to {1} rejected: {2}",
                    key, topic, e.toString());
        }
    }

    /** Flushes and closes the producer. */
    @Override
    public void close() {
        producer.close();
    }

    /**
     * The descriptor set the serde publishes against, base64: events.proto
     * plus its transitive imports, taken from the generated classes' runtime
     * descriptors (the same pattern as the repo service's
     * DocumentEventFactory), so the serde validates and frames every record
     * against exactly the schema this module was compiled with.
     */
    static String descriptorSetBase64() {
        Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> files =
                new LinkedHashMap<>();
        ArrayDeque<FileDescriptor> queue =
                new ArrayDeque<>(java.util.List.of(ConfluenceChange.getDescriptor().getFile()));
        while (!queue.isEmpty()) {
            FileDescriptor file = queue.pop();
            if (files.put(file.getName(), file.toProto()) == null) {
                queue.addAll(file.getDependencies());
            }
        }
        return Base64.getEncoder().encodeToString(FileDescriptorSet.newBuilder()
                .addAllFile(files.values())
                .build().toByteArray());
    }
}
