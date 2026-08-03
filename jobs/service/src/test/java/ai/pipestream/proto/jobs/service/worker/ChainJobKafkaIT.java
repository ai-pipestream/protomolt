package ai.pipestream.proto.jobs.service.worker;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainRepository;
import ai.pipestream.proto.jobs.service.ChainJobsConfig;
import ai.pipestream.proto.jobs.service.TestChains;
import ai.pipestream.proto.jobs.service.events.ChainJobEventFactory;
import ai.pipestream.proto.jobs.service.events.ChainJobEventRelay;
import ai.pipestream.proto.jobs.service.store.ChainJobDatabase;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobStoreConfig;
import ai.pipestream.proto.jobs.service.store.JdbcChainJobStore;
import ai.pipestream.proto.jobs.v1.ChainJobEvent;
import ai.pipestream.proto.jobs.v1.ChainJobRequest;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole lane against real testcontainers PostgreSQL 17 and Redpanda,
 * with the chain's services on a real localhost gRPC server (in-process
 * channels cannot cross a real target string): a ChainJobRequest produced
 * to the request topic is consumed by the worker, submitted, claimed,
 * executed, and completed; the relay publishes the lifecycle to the events
 * topic, where a protomolt-serde consumer revalidates every record on read.
 * A request naming an unknown chain fails the job loudly — a FAILED row and
 * a FAILED event, never a silent drop.
 */
@Testcontainers(disabledWithoutDocker = true)
class ChainJobKafkaIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    // Same baseline image as the serde lane (testcontainers' own pinned tag).
    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    static TestChains chains;
    static String target;
    static ChainJobDatabase database;
    static JdbcChainJobStore store;
    static ActionContext context;

    ChainJobWorker worker;
    ChainJobEventRelay relay;
    KafkaProducer<String, Message> relayProducer;

    @BeforeAll
    static void boot() {
        chains = new TestChains();
        target = chains.startTcp();
        database = new ChainJobDatabase(new ChainJobStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        store = new JdbcChainJobStore(database);
        context = ActionContext.create();
    }

    @AfterAll
    static void tearDown() {
        chains.stop();
        database.close();
    }

    @BeforeEach
    void clean() {
        database.inTransaction(c -> {
            try {
                c.createStatement().execute("DELETE FROM chain_job_events_outbox");
                c.createStatement().execute("DELETE FROM chain_job");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    @AfterEach
    void stopLane() {
        if (relay != null) {
            relay.close();
        }
        if (relayProducer != null) {
            relayProducer.close();
        }
        if (worker != null) {
            worker.close();
        }
    }

    /** Start a worker + relay on unique topics. */
    private void startLane(String requestTopic, String eventsTopic) {
        ChainRepository repository = name -> "embed-text".equals(name)
                ? Optional.of(chains.twoStepChain(target, null))
                : Optional.empty();
        ChainJobsConfig config = new ChainJobsConfig("it-worker", 2,
                Duration.ofSeconds(30), Duration.ofMillis(50), 1, 3, 4,
                requestTopic, eventsTopic, REDPANDA.getBootstrapServers(), null);
        worker = new ChainJobWorker(store, context, repository, new ai.pipestream.proto.chain
                .ChainRunner(), config);
        relayProducer = ChainJobEventRelay.newProducer(REDPANDA.getBootstrapServers(), null);
        relay = new ChainJobEventRelay(store, relayProducer, eventsTopic,
                Duration.ofMillis(50), 100);
        worker.start();
        relay.start();
    }

    private static KafkaProducer<String, Message> requestProducer() {
        ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
        serializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                ChainJobEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                ChainJobRequest.getDescriptor().getFullName()), false);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props, new StringSerializer(), serializer);
    }

    /** A consumer pinned to ChainJobEvent that revalidates every record on read. */
    private static KafkaConsumer<String, Message> eventConsumer() {
        ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                ChainJobEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                ChainJobEvent.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true), false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props, new StringDeserializer(), deserializer);
    }

    private static ChainJobRequest request(String jobId, String chainName, String text) {
        return ChainJobRequest.newBuilder()
                .setJobId(jobId)
                .setChainName(chainName)
                .setInput(Struct.newBuilder()
                        .putFields("text", Value.newBuilder().setStringValue(text).build()))
                .build();
    }

    /** Poll the store until the job reaches the wanted status (60s ceiling). */
    private static ChainJobRecord awaitStatus(UUID jobId, String status) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            Optional<ChainJobRecord> job = store.get(jobId);
            if (job.isPresent() && status.equals(job.get().status)) {
                return job.get();
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Optional<ChainJobRecord> last = store.get(jobId);
        throw new AssertionError("job " + jobId + " never reached " + status
                + " (last: " + last.map(row -> row.status + " error=" + row.error).orElse("absent")
                + ")");
    }

    /** Collect up to {@code count} events for the job from the events topic. */
    private static List<ChainJobEvent> collectEvents(String topic, String jobId, int count)
            throws Exception {
        List<ChainJobEvent> events = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        try (KafkaConsumer<String, Message> consumer = eventConsumer()) {
            consumer.subscribe(List.of(topic));
            while (System.nanoTime() < deadline && events.size() < count) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Message> record : records) {
                    if (!jobId.equals(record.key())) {
                        continue;
                    }
                    Message value = record.value();
                    ChainJobEvent event = value instanceof ChainJobEvent typed
                            ? typed
                            : ChainJobEvent.parseFrom(value.toByteArray());
                    events.add(event);
                }
            }
        }
        return events;
    }

    @Test
    void aBrokerNativeSubmitRunsTheChainAndPublishesTheLifecycle() throws Exception {
        String suffix = Long.toUnsignedString(System.nanoTime(), 36);
        String requestTopic = "chain-job-requests-" + suffix;
        String eventsTopic = "chain-job-events-" + suffix;
        startLane(requestTopic, eventsTopic);

        String jobId = UUID.randomUUID().toString();
        try (KafkaProducer<String, Message> producer = requestProducer()) {
            producer.send(new ProducerRecord<>(requestTopic, jobId,
                    request(jobId, "embed-text", "hi"))).get();
        }

        ChainJobRecord done = awaitStatus(UUID.fromString(jobId), ChainJobRecord.STATUS_COMPLETED);
        assertThat(done.chainName).isEqualTo("embed-text");
        assertThat(done.verdict).isEqualTo("2 steps, output jobs.test.Embedding");
        assertThat(done.attempt).isEqualTo(1);

        List<ChainJobEvent> events = collectEvents(eventsTopic, jobId, 4);
        assertThat(events.stream().map(ChainJobEvent::getType)).containsExactly(
                ChainJobEvent.Type.TYPE_ACCEPTED,
                ChainJobEvent.Type.TYPE_STEP_CHECKPOINT,
                ChainJobEvent.Type.TYPE_STEP_CHECKPOINT,
                ChainJobEvent.Type.TYPE_COMPLETED);
        assertThat(events.get(1).getStep()).isEqualTo("tokenize");
        assertThat(events.get(2).getStep()).isEqualTo("embed");
        assertThat(events.get(3).getVerdict()).isEqualTo("2 steps, output jobs.test.Embedding");
        // Every event carries its outbox row id (the dedupe key) and the job.
        assertThat(events).allSatisfy(event -> {
            assertThat(event.getEventId()).isNotBlank();
            assertThat(event.getJobId()).isEqualTo(jobId);
        });
    }

    @Test
    void anUnknownChainNameFailsTheJobLoudly() throws Exception {
        String suffix = Long.toUnsignedString(System.nanoTime(), 36);
        String requestTopic = "chain-job-requests-" + suffix;
        String eventsTopic = "chain-job-events-" + suffix;
        startLane(requestTopic, eventsTopic);

        String jobId = UUID.randomUUID().toString();
        try (KafkaProducer<String, Message> producer = requestProducer()) {
            producer.send(new ProducerRecord<>(requestTopic, jobId,
                    request(jobId, "does-not-exist", "hi"))).get();
        }

        ChainJobRecord failed = awaitStatus(UUID.fromString(jobId), ChainJobRecord.STATUS_FAILED);
        assertThat(failed.chainName).isEqualTo("does-not-exist");
        assertThat(failed.error).contains("No stored chain named 'does-not-exist'");

        List<ChainJobEvent> events = collectEvents(eventsTopic, jobId, 1);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getType()).isEqualTo(ChainJobEvent.Type.TYPE_FAILED);
        assertThat(events.get(0).getError()).contains("No stored chain named");
    }
}
