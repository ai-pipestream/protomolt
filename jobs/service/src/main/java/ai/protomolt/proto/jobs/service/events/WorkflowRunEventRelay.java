package ai.protomolt.proto.jobs.service.events;

import ai.protomolt.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.jobs.v1.WorkflowRunEvent;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * The outbox relay: drains {@code workflow_run_events_outbox} to the
 * workflow-run-events Kafka topic. One virtual-thread loop (see {@link #start})
 * calls {@link #relayOnce} forever; a non-empty drain loops again
 * immediately, an empty one backs off the poll interval.
 * <p>
 * Per claimed record: publish keyed by the job_id (partition-ordered per
 * job), wait for the broker ack (blocking code on a virtual thread), then
 * mark the row PUBLISHED. Publish precedes the PUBLISHED transition, so a
 * relay crash between the two republishes on restart — at-least-once
 * delivery, with {@code WorkflowRunEvent.event_id} as the consumer dedupe key.
 * A failure increments attempts; at {@link WorkflowRunEventRecord#MAX_ATTEMPTS}
 * the row lands FAILED (the DLQ — operator territory, the relay never
 * re-enqueues it). One bad record never kills the batch.
 * <p>
 * The drain takes the {@link Producer} interface rather than the concrete
 * {@link KafkaProducer} so tests drive it with a mock producer; production
 * producers come from {@link #newProducer}.
 */
public final class WorkflowRunEventRelay implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowRunEventRelay.class);

    private final WorkflowRunStore store;
    private final Producer<String, Message> producer;
    private final String topic;
    private final Duration pollInterval;
    private final int batchSize;

    private volatile boolean closed;
    private Thread thread;

    /**
     * @param store the jobs store whose outbox this relay drains
     * @param producer the Kafka producer (thread-safe, shared by the loop)
     * @param topic the workflow-run-events topic
     * @param pollInterval idle backoff between empty drains
     * @param batchSize the claim batch size per drain
     */
    public WorkflowRunEventRelay(WorkflowRunStore store, Producer<String, Message> producer,
            String topic, Duration pollInterval, int batchSize) {
        this.store = store;
        this.producer = producer;
        this.topic = topic;
        this.pollInterval = pollInterval;
        this.batchSize = batchSize;
    }

    /**
     * Start the drain loop on a virtual thread. A non-empty drain loops
     * again immediately; an empty one backs off the poll interval. The loop
     * catches and logs per iteration — one store hiccup never kills it —
     * and stops in {@link #close()}.
     */
    public void start() {
        thread = Thread.ofVirtual().name("workflow-run-event-relay").start(() -> {
            while (!closed) {
                try {
                    if (relayOnce() == 0) {
                        sleep(pollInterval.toMillis());
                    }
                } catch (RuntimeException e) {
                    LOG.warn("workflow-run-event-relay iteration failed (loop continues): {}",
                            e.getMessage(), e);
                    sleep(1000);
                }
            }
        });
    }

    /**
     * Drain one batch: claim up to {@code batchSize} PENDING records and
     * publish each.
     *
     * @return how many records this call transitioned to PUBLISHED
     */
    public int relayOnce() {
        List<WorkflowRunEventRecord> batch = store.pollPendingEvents(batchSize);
        int published = 0;
        for (WorkflowRunEventRecord record : batch) {
            try {
                WorkflowRunEvent event = WorkflowRunEvent.parseFrom(record.payload);
                producer.send(new ProducerRecord<>(topic, record.kafkaKey, event)).get();
                if (store.markEventPublished(record.eventId)) {
                    published++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                store.markEventFailed(record, e.getMessage());
                return published;
            } catch (Exception e) {
                LOG.warn("Event relay failed for event_id={} type={} (attempt {}): {}",
                        record.eventId, record.eventType, record.attempts + 1, e.getMessage());
                store.markEventFailed(record, e.getMessage());
            }
        }
        return published;
    }

    /** Stop the drain loop. The producer's lifecycle stays with the caller. */
    @Override
    public void close() {
        closed = true;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * The relay's producer, optionally backed by a Confluent-compatible
     * schema registry: String keys (the job_id), values framed and validated
     * by the protomolt serde against the packaged jobs.proto descriptor set,
     * pinned to the WorkflowRunEvent type (one type, one subject).
     * <p>
     * Registry-free ({@code schemaRegistryUrl} null or blank), the serde
     * stamps schema id 0 into every frame: protomolt consumers read the
     * payload against the packaged contract, but standard Confluent tooling
     * resolves frames by id and cannot read id 0. With a registry URL, the
     * serde looks the subject's id up and stamps it, so relayed records are
     * resolvable by any standard consumer.
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param schemaRegistryUrl base URL of a Confluent-compatible schema
     *        registry, or null/blank for registry-free framing
     * @return the configured producer
     */
    public static KafkaProducer<String, Message> newProducer(String bootstrapServers,
            String schemaRegistryUrl) {
        Map<String, Object> serdeConfig = new HashMap<>(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                WorkflowRunEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                WorkflowRunEvent.getDescriptor().getFullName()));
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

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
