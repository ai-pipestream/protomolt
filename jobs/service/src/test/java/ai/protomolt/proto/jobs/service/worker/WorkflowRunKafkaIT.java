package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.workflow.WorkflowRepository;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.TestWorkflows;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventRelay;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.v1.WorkflowRunEvent;
import ai.protomolt.proto.jobs.v1.WorkflowRunRequest;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
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
 * with the workflow's services on a real localhost gRPC server (in-process
 * channels cannot cross a real target string): a WorkflowRunRequest produced
 * to the request topic is consumed by the worker, submitted, claimed,
 * executed, and completed; the relay publishes the lifecycle to the events
 * topic, where a protomolt-kafka-serde consumer revalidates every record on read.
 * A request naming an unknown workflow fails the job loudly — a FAILED row and
 * a FAILED event, never a silent drop.
 */
@Testcontainers(disabledWithoutDocker = true)
class WorkflowRunKafkaIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    // Same baseline image as the serde lane (testcontainers' own pinned tag).
    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    static TestWorkflows workflows;
    static String target;
    static WorkflowRunDatabase database;
    static JdbcWorkflowRunStore store;
    static ActionContext context;

    WorkflowRunWorker worker;
    WorkflowRunEventRelay relay;
    KafkaProducer<String, Message> relayProducer;

    @BeforeAll
    static void boot() {
        workflows = new TestWorkflows();
        target = workflows.startTcp();
        database = new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        store = new JdbcWorkflowRunStore(database);
        context = ActionContext.create();
    }

    @AfterAll
    static void tearDown() {
        workflows.stop();
        database.close();
    }

    @BeforeEach
    void clean() {
        database.inTransaction(c -> {
            try {
                c.createStatement().execute("DELETE FROM workflow_run_events_outbox");
                c.createStatement().execute("DELETE FROM workflow_run");
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
        WorkflowRepository repository = name -> "embed-text".equals(name)
                ? Optional.of(workflows.twoStepWorkflow(target, null))
                : Optional.empty();
        WorkflowRunsConfig config = new WorkflowRunsConfig("it-worker", 2,
                Duration.ofSeconds(30), Duration.ofMillis(50), 1, 3, 4,
                requestTopic, eventsTopic, REDPANDA.getBootstrapServers(), null);
        worker = new WorkflowRunWorker(store, context, repository, new ai.protomolt.proto.workflow
                .WorkflowRunner(), config);
        relayProducer = WorkflowRunEventRelay.newProducer(REDPANDA.getBootstrapServers(), null);
        relay = new WorkflowRunEventRelay(store, relayProducer, eventsTopic,
                Duration.ofMillis(50), 100);
        worker.start();
        relay.start();
    }

    private static KafkaProducer<String, Message> requestProducer() {
        ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
        serializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                WorkflowRunEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                WorkflowRunRequest.getDescriptor().getFullName()), false);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props, new StringSerializer(), serializer);
    }

    /** A consumer pinned to WorkflowRunEvent that revalidates every record on read. */
    private static KafkaConsumer<String, Message> eventConsumer() {
        ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                WorkflowRunEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                WorkflowRunEvent.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true), false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props, new StringDeserializer(), deserializer);
    }

    private static WorkflowRunRequest request(String jobId, String workflowName, String text) {
        return WorkflowRunRequest.newBuilder()
                .setJobId(jobId)
                .setWorkflowName(workflowName)
                .setInput(Struct.newBuilder()
                        .putFields("text", Value.newBuilder().setStringValue(text).build()))
                .build();
    }

    /** Poll the store until the job reaches the wanted status (60s ceiling). */
    private static WorkflowRunRecord awaitStatus(UUID jobId, String status) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            Optional<WorkflowRunRecord> job = store.get(jobId);
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
        Optional<WorkflowRunRecord> last = store.get(jobId);
        throw new AssertionError("job " + jobId + " never reached " + status
                + " (last: " + last.map(row -> row.status + " error=" + row.error).orElse("absent")
                + ")");
    }

    /** Collect up to {@code count} events for the job from the events topic. */
    private static List<WorkflowRunEvent> collectEvents(String topic, String jobId, int count)
            throws Exception {
        List<WorkflowRunEvent> events = new ArrayList<>();
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
                    WorkflowRunEvent event = value instanceof WorkflowRunEvent typed
                            ? typed
                            : WorkflowRunEvent.parseFrom(value.toByteArray());
                    events.add(event);
                }
            }
        }
        return events;
    }

    @Test
    void aBrokerNativeSubmitRunsTheWorkflowAndPublishesTheLifecycle() throws Exception {
        String suffix = Long.toUnsignedString(System.nanoTime(), 36);
        String requestTopic = "workflow-run-requests-" + suffix;
        String eventsTopic = "workflow-run-events-" + suffix;
        startLane(requestTopic, eventsTopic);

        String jobId = UUID.randomUUID().toString();
        try (KafkaProducer<String, Message> producer = requestProducer()) {
            producer.send(new ProducerRecord<>(requestTopic, jobId,
                    request(jobId, "embed-text", "hi"))).get();
        }

        WorkflowRunRecord done = awaitStatus(UUID.fromString(jobId), WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(done.workflowName).isEqualTo("embed-text");
        assertThat(done.verdict).isEqualTo("2 steps, output jobs.test.Embedding");
        assertThat(done.attempt).isEqualTo(1);

        List<WorkflowRunEvent> events = collectEvents(eventsTopic, jobId, 4);
        assertThat(events.stream().map(WorkflowRunEvent::getType)).containsExactly(
                WorkflowRunEvent.Type.TYPE_ACCEPTED,
                WorkflowRunEvent.Type.TYPE_STEP_CHECKPOINT,
                WorkflowRunEvent.Type.TYPE_STEP_CHECKPOINT,
                WorkflowRunEvent.Type.TYPE_COMPLETED);
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
    void anUnknownWorkflowNameFailsTheJobLoudly() throws Exception {
        String suffix = Long.toUnsignedString(System.nanoTime(), 36);
        String requestTopic = "workflow-run-requests-" + suffix;
        String eventsTopic = "workflow-run-events-" + suffix;
        startLane(requestTopic, eventsTopic);

        String jobId = UUID.randomUUID().toString();
        try (KafkaProducer<String, Message> producer = requestProducer()) {
            producer.send(new ProducerRecord<>(requestTopic, jobId,
                    request(jobId, "does-not-exist", "hi"))).get();
        }

        WorkflowRunRecord failed = awaitStatus(UUID.fromString(jobId), WorkflowRunRecord.STATUS_FAILED);
        assertThat(failed.workflowName).isEqualTo("does-not-exist");
        assertThat(failed.error).contains("No stored workflow named 'does-not-exist'");

        List<WorkflowRunEvent> events = collectEvents(eventsTopic, jobId, 1);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_FAILED);
        assertThat(events.get(0).getError()).contains("No stored workflow named");
    }
}
