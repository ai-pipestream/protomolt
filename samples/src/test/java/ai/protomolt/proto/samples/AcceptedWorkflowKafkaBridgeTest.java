package ai.protomolt.proto.samples;

import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real-broker offset behavior for the sample launch bridge; launch idempotency is test-owned here. */
@Testcontainers(disabledWithoutDocker = true)
class AcceptedWorkflowKafkaBridgeTest {
    @Container
    static final RedpandaContainer BROKER = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    @Test
    void lostLaunchReplyReplaysSameRecordAndChangedIntentCannotAdvanceOffset() throws Exception {
        String topic = unique("accepted-launch-replay");
        String group = unique("accepted-launch-group");
        createTopic(topic);
        String launchId = UUID.randomUUID().toString();
        WorkflowAuthoringLaunchRequest original = request(launchId, "a");
        WorkflowAuthoringLaunchRequest changed = request(launchId, "b");
        LaunchLedger ledger = new LaunchLedger();
        ledger.loseNextReply.set(true);
        produce(topic, launchId, original.toByteArray());

        TopicPartition partition = new TopicPartition(topic, 0);
        ConsumerRecord<String, byte[]> first;
        try (KafkaConsumer<String, byte[]> consumer = consumer(group)) {
            first = pollOne(consumer, topic);
            assertThat(first.offset()).isZero();
            assertThatThrownBy(() -> bridge(consumer, ledger::launch).process(first))
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                            .isEqualTo(Status.Code.UNAVAILABLE));
            assertThat(ledger.effects.get()).isEqualTo(1);
            assertThat(committed(consumer, partition)).isNull();
        }

        // The same group resumes at the uncommitted record. The launch fake models
        // a keyed create-or-match ledger, while Kafka supplies the actual replay.
        try (KafkaConsumer<String, byte[]> consumer = consumer(group)) {
            ConsumerRecord<String, byte[]> replay = pollOne(consumer, topic);
            assertThat(replay.offset()).isEqualTo(first.offset());
            assertThat(replay.value()).containsExactly(original.toByteArray());
            bridge(consumer, ledger::launch).process(replay);
            assertThat(committed(consumer, partition).offset()).isEqualTo(replay.offset() + 1);
            assertThat(ledger.effects.get()).isEqualTo(1);
            assertThat(ledger.calls.get()).isEqualTo(2);

            produce(topic, launchId, changed.toByteArray());
            ConsumerRecord<String, byte[]> conflict = pollOne(consumer, topic);
            assertThat(conflict.offset()).isEqualTo(replay.offset() + 1);
            assertThatThrownBy(() -> bridge(consumer, ledger::launch).process(conflict))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("different launch intent");
            assertThat(committed(consumer, partition).offset()).isEqualTo(conflict.offset());
        }

        // Restart cannot skip the conflicting second record: only the successful
        // first launch was committed.
        try (KafkaConsumer<String, byte[]> consumer = consumer(group)) {
            ConsumerRecord<String, byte[]> conflict = pollOne(consumer, topic);
            assertThat(conflict.offset()).isEqualTo(1L);
            assertThat(conflict.value()).containsExactly(changed.toByteArray());
            assertThat(committed(consumer, partition).offset()).isEqualTo(1L);
        }
        assertThat(ledger.effects.get()).isEqualTo(1);
    }

    @Test
    void malformedUnknownMiskeyedAndInvalidResponseRecordsRemainUncommitted() throws Exception {
        String launchId = UUID.randomUUID().toString();
        WorkflowAuthoringLaunchRequest valid = request(launchId, "valid");
        WorkflowAuthoringLaunchRequest unknown = valid.toBuilder().setUnknownFields(unknownFields()).build();
        WorkflowAuthoringLaunchRequest nestedUnknown = valid.toBuilder().setAcceptance(
                valid.getAcceptance().toBuilder().setUnknownFields(unknownFields())).build();
        assertRejected(valid.getLaunchId(), unknown.toByteArray(), ignored -> validResult(launchId), 0);
        assertRejected(valid.getLaunchId(), nestedUnknown.toByteArray(), ignored -> validResult(launchId), 0);
        assertRejected(UUID.randomUUID().toString(), valid.toByteArray(), ignored -> validResult(launchId), 0);
        assertRejected(launchId, new byte[] {0x0f}, ignored -> validResult(launchId), 0);
        String nonCanonicalId = launchId.toUpperCase(java.util.Locale.ROOT);
        assertRejected(nonCanonicalId, request(nonCanonicalId, "valid").toByteArray(),
                ignored -> validResult(launchId), 0);
        assertRejected(launchId, valid.toByteArray(), ignored -> WorkflowAuthoringLaunchResult.newBuilder()
                .setJobId(UUID.randomUUID().toString()).setAuthorization(artifact("result")).build(), 1);
        assertRejected(launchId, valid.toByteArray(), ignored -> WorkflowAuthoringLaunchResult.newBuilder()
                .setJobId(launchId).build(), 1);
        assertRejected(launchId, valid.toByteArray(), ignored -> validResult(launchId).toBuilder()
                .setUnknownFields(unknownFields()).build(), 1);
    }

    @Test
    void deniedLaunchLeavesTheRealConsumerOffsetUncommitted() throws Exception {
        String topic = unique("accepted-launch-denied");
        String group = unique("accepted-launch-denied-group");
        createTopic(topic);
        String launchId = UUID.randomUUID().toString();
        produce(topic, launchId, request(launchId, "denied").toByteArray());
        TopicPartition partition = new TopicPartition(topic, 0);
        AtomicInteger calls = new AtomicInteger();
        AcceptedWorkflowKafkaBridge.LaunchClient denied = ignored -> {
            calls.incrementAndGet();
            throw Status.PERMISSION_DENIED.asRuntimeException();
        };

        try (KafkaConsumer<String, byte[]> consumer = consumer(group)) {
            ConsumerRecord<String, byte[]> record = pollOne(consumer, topic);
            assertThatThrownBy(() -> new AcceptedWorkflowKafkaBridge(consumer, denied).process(record))
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED));
            assertThat(calls.get()).isEqualTo(1);
            assertThat(committed(consumer, partition)).isNull();
        }
        try (KafkaConsumer<String, byte[]> consumer = consumer(group)) {
            assertThat(pollOne(consumer, topic).offset()).isZero();
        }
    }

    private void assertRejected(String key, byte[] payload, AcceptedWorkflowKafkaBridge.LaunchClient launcher,
            int expectedCalls) throws Exception {
        String topic = unique("accepted-launch-reject");
        String group = unique("accepted-launch-reject-group");
        createTopic(topic);
        produce(topic, key, payload);
        AtomicInteger calls = new AtomicInteger();
        AcceptedWorkflowKafkaBridge.LaunchClient counted = request -> {
            calls.incrementAndGet();
            return launcher.launch(request);
        };
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, byte[]> consumer = consumer(group)) {
            ConsumerRecord<String, byte[]> record = pollOne(consumer, topic);
            assertThatThrownBy(() -> bridge(consumer, counted).process(record)).isInstanceOf(RuntimeException.class);
            assertThat(calls.get()).isEqualTo(expectedCalls);
            assertThat(committed(consumer, partition)).isNull();
        }
        try (KafkaConsumer<String, byte[]> consumer = consumer(group)) {
            assertThat(pollOne(consumer, topic).offset()).isZero();
        }
    }

    private static AcceptedWorkflowKafkaBridge bridge(KafkaConsumer<String, byte[]> consumer,
            AcceptedWorkflowKafkaBridge.LaunchClient launcher) {
        return new AcceptedWorkflowKafkaBridge(consumer, launcher);
    }

    private static org.apache.kafka.clients.consumer.OffsetAndMetadata committed(
            KafkaConsumer<String, byte[]> consumer, TopicPartition partition) {
        return consumer.committed(java.util.Set.of(partition)).get(partition);
    }

    private static WorkflowAuthoringLaunchRequest request(String launchId, String inputHashSeed) {
        return WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(launchId)
                .setAcceptance(WorkflowAcceptedCandidate.newBuilder()
                        .setTaskId("00000000-0000-4000-8000-000000000771")
                        .setAttempt(1).setRevision(1)
                        .setTaskSpecSha256("1".repeat(64)).setCandidateSha256("2".repeat(64))
                        .setAcceptedEntrySha256("3".repeat(64)))
                .setInput(artifact(inputHashSeed)).build();
    }

    private static ArtifactReference artifact(String seed) {
        return ArtifactReference.newBuilder().setSha256(seed.equals("result") ? "4".repeat(64)
                        : (seed.equals("a") ? "5" : "6").repeat(64))
                .setMediaType("application/x-protobuf").setSizeBytes(0).setRedacted(false).build();
    }

    private static WorkflowAuthoringLaunchResult validResult(String launchId) {
        return WorkflowAuthoringLaunchResult.newBuilder().setJobId(launchId).setAuthorization(artifact("result"))
                .build();
    }

    private static UnknownFieldSet unknownFields() {
        return UnknownFieldSet.newBuilder().addField(123,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static void createTopic(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", BROKER.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(Duration.ofSeconds(20).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }

    private static void produce(String topic, String key, byte[] value) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties,
                new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(20,
                    java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static KafkaConsumer<String, byte[]> consumer(String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
    }

    private static ConsumerRecord<String, byte[]> pollOne(KafkaConsumer<String, byte[]> consumer, String topic) {
        if (consumer.subscription().isEmpty()) consumer.subscribe(List.of(topic));
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(250));
            if (!records.isEmpty()) return records.iterator().next();
        }
        throw new AssertionError("no Kafka record received from " + topic);
    }

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static final class LaunchLedger {
        private final Map<String, WorkflowAuthoringLaunchRequest> intents = new ConcurrentHashMap<>();
        private final AtomicBoolean loseNextReply = new AtomicBoolean();
        private final AtomicInteger effects = new AtomicInteger();
        private final AtomicInteger calls = new AtomicInteger();

        WorkflowAuthoringLaunchResult launch(WorkflowAuthoringLaunchRequest request) {
            calls.incrementAndGet();
            WorkflowAuthoringLaunchRequest existing = intents.putIfAbsent(request.getLaunchId(), request);
            if (existing == null) effects.incrementAndGet();
            else if (!existing.equals(request)) throw new IllegalStateException("different launch intent");
            if (loseNextReply.compareAndSet(true, false)) {
                throw Status.UNAVAILABLE.withDescription("test lost launch response").asRuntimeException();
            }
            return validResult(request.getLaunchId());
        }
    }
}
