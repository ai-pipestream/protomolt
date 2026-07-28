package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.lucene.LuceneIndexWriter;
import ai.pipestream.proto.index.lucene.ProtoLuceneMapper;
import ai.pipestream.proto.index.spi.InferringIndexingHintSource;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.IndexingPlanFactory;
import ai.pipestream.proto.index.spi.ProtoOptionsIndexingHintSource;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The Lucene projection of the change feed: consumes the
 * {@code confluence-events} topic with the protomolt deserializer, maps each
 * {@link ConfluenceChange} through {@link ProtoLuceneMapper} (the indexing
 * hints declared on events.proto drive the {@link IndexingPlan}), and writes
 * the documents into a local Lucene index via {@link LuceneIndexWriter}.
 *
 * <p>Environment:</p>
 * <ul>
 *   <li>{@code CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS}: required</li>
 *   <li>{@code CONFLUENCE_LUCENE_INDEX_DIR}: the index directory, required</li>
 *   <li>{@code CONFLUENCE_KAFKA_TOPIC}: default {@code confluence-events}</li>
 *   <li>{@code CONFLUENCE_LUCENE_GROUP_ID}: consumer group, default
 *   {@code confluence-lucene-projector}</li>
 * </ul>
 *
 * <p>The poll loop runs on one virtual thread; the shutdown hook wakes the
 * consumer so the loop exits cleanly. Offsets are the consumer group's; a
 * restart replays from the last committed offset, and re-indexing an UPSERT
 * is harmless (Lucene dedupes nothing, but the change feed is an upsert
 * stream: re-indexed copies simply shadow older ones in search results).</p>
 *
 * <p>Known gap: {@link LuceneIndexWriter} exposes no deletion, so DELETE
 * changes are logged and skipped. Tombstoned content leaves the index only
 * when deletion lands on the writer.</p>
 */
public final class ConfluenceLuceneProjector {

    /** Environment variable for the index directory. */
    public static final String ENV_INDEX_DIR = "CONFLUENCE_LUCENE_INDEX_DIR";
    /** Environment variable for the consumer group id. */
    public static final String ENV_GROUP_ID = "CONFLUENCE_LUCENE_GROUP_ID";
    /** Default consumer group id. */
    public static final String DEFAULT_GROUP_ID = "confluence-lucene-projector";

    private static final System.Logger LOG =
            System.getLogger(ConfluenceLuceneProjector.class.getName());

    private ConfluenceLuceneProjector() {
    }

    /**
     * Boots the projector from the environment and blocks until shutdown.
     *
     * @param args ignored (configuration is env-driven)
     * @throws Exception on boot failure
     */
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv(ConfluenceConnectorConfig.ENV_KAFKA_BOOTSTRAP_SERVERS);
        if (bootstrap == null || bootstrap.isBlank()) {
            throw new IllegalStateException(
                    ConfluenceConnectorConfig.ENV_KAFKA_BOOTSTRAP_SERVERS + " is required");
        }
        String indexDir = System.getenv(ENV_INDEX_DIR);
        if (indexDir == null || indexDir.isBlank()) {
            throw new IllegalStateException(ENV_INDEX_DIR + " is required");
        }
        String topic = System.getenv().getOrDefault(ConfluenceConnectorConfig.ENV_KAFKA_TOPIC,
                ConfluenceConnectorConfig.DEFAULT_KAFKA_TOPIC);
        String groupId = System.getenv().getOrDefault(ENV_GROUP_ID, DEFAULT_GROUP_ID);

        KafkaConsumer<String, Message> consumer = newConsumer(bootstrap, groupId);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup,
                "confluence-lucene-projector-shutdown"));
        Thread loop = Thread.ofVirtual().name("confluence-lucene-projector").start(() -> {
            try (consumer; LuceneIndexWriter writer = new LuceneIndexWriter(Path.of(indexDir))) {
                run(consumer, writer, topic);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (MappingException e) {
                throw new RuntimeException("a change could not be mapped for indexing", e);
            }
        });
        LOG.log(System.Logger.Level.INFO,
                "confluence-lucene-projector consuming {0} from {1} into {2}",
                topic, bootstrap, indexDir);
        loop.join();
    }

    /**
     * The projector's consumer: String keys, values unframed by the protomolt
     * deserializer pinned to {@link ConfluenceChange} against the packaged
     * descriptor set (registry-free, like the sink that wrote them).
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param groupId the consumer group id
     * @return the configured consumer
     */
    public static KafkaConsumer<String, Message> newConsumer(String bootstrapServers,
            String groupId) {
        ProtoMoltProtobufDeserializer valueDeserializer = new ProtoMoltProtobufDeserializer();
        valueDeserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, KafkaChangeSink.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                ConfluenceChange.getDescriptor().getFullName()), false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer);
    }

    /** The indexing plan for the change feed: proto-option hints, then inference. */
    static IndexingPlan indexingPlan() {
        return new IndexingPlanFactory(new ProtoOptionsIndexingHintSource()
                .orElse(new InferringIndexingHintSource()))
                .create(ConfluenceChange.getDescriptor());
    }

    /**
     * The poll loop: map every record of one batch, then commit the index.
     * Returns when the consumer is woken up (shutdown); anything else
     * propagates and kills the projector, which is what a restart is for.
     */
    static void run(KafkaConsumer<String, Message> consumer, LuceneIndexWriter writer,
            String topic) throws IOException, MappingException {
        ProtoLuceneMapper mapper = new ProtoLuceneMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        IndexingPlan plan = indexingPlan();
        consumer.subscribe(List.of(topic));
        try {
            while (true) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, Message> record : records) {
                    if (record.value() instanceof ConfluenceChange change) {
                        project(change, mapper, plan, writer);
                    }
                }
                if (!records.isEmpty()) {
                    writer.commit();
                }
            }
        } catch (WakeupException e) {
            writer.commit();
        }
    }

    /**
     * Projects one change. UPSERTs map and add; DELETEs are logged and
     * skipped because {@link LuceneIndexWriter} exposes no deletion.
     */
    static void project(ConfluenceChange change, ProtoLuceneMapper mapper, IndexingPlan plan,
            LuceneIndexWriter writer) throws IOException, MappingException {
        if (change.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE) {
            LOG.log(System.Logger.Level.INFO,
                    "confluence-lucene-projector: delete of {0} skipped; the index writer "
                            + "has no delete support", change.getChangeId());
            return;
        }
        writer.add(mapper.map(change, plan));
    }
}
