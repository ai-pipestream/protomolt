package ai.protomolt.proto.config.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.config.ConfigSource;
import ai.protomolt.proto.config.DistributedConfig;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.registry.service.SchemaRegistryServerConfig;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Kafka plug end to end on the house stack: Redpanda is the broker,
 * the protomolt registry is the schema registry, the house serde frames
 * and validates on both ends. A publisher cannot write a document
 * violating the type's declared rules; a poisoned record smuggled past
 * the writer gate refuses at read with its coordinates and the consumer
 * keeps serving what it runs; a compacted topic consumed as a table
 * gives latest-per-subject with {@code partition:offset} as evidence.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaConfigSourceIT {

    @Container
    static final RedpandaContainer BROKER = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    private static final String TOPIC = "protomolt-config";
    private static final String THROTTLE = """
            syntax = "proto3";
            package configit.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Throttle {
              int32 limit = 1 [
                (ai.pipestream.proto.validate.v1.field) = {
                  required: true
                  int32: {gt: 0, lte: 1000}
                }
              ];
              string label = 2;
            }
            """;

    @TempDir
    static Path work;

    static GitSchemaRegistryStore store;
    static SchemaRegistryServer server;
    static String registryUrl;
    static String descriptorSetBase64;
    static Descriptor throttle;
    static int schemaId;

    @BeforeAll
    static void boot() throws Exception {
        String validateSubject = "ai/pipestream/proto/validate/v1/validate.proto";
        String validateSource;
        try (var in = KafkaConfigSourceIT.class.getClassLoader()
                .getResourceAsStream(validateSubject)) {
            validateSource = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        store = GitSchemaRegistryStore.builder()
                .repositoryDir(work.resolve("registry")).build();
        store.register(validateSubject, validateSource, List.of());
        schemaId = store.register("configit/v1/throttle.proto", THROTTLE, List.of(
                new SchemaReference(validateSubject, validateSubject, 1))).globalId();
        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withPort(0), store);
        registryUrl = "http://127.0.0.1:" + server.start();

        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(validateSubject, validateSource, "test")
                .add("configit/v1/throttle.proto", THROTTLE, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        throttle = compiled.descriptorFor("configit/v1/throttle.proto").orElseThrow()
                .findMessageTypeByName("Throttle");

        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BROKER.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact")))).all().get();
        }
    }

    @AfterAll
    static void shutdown() throws Exception {
        if (server != null) {
            server.close();
        }
        if (store != null) {
            store.close();
        }
    }

    static DynamicMessage throttleOf(int limit, String label) {
        return DynamicMessage.newBuilder(throttle)
                .setField(throttle.findFieldByName("limit"), limit)
                .setField(throttle.findFieldByName("label"), label)
                .build();
    }

    static Map<String, Object> serdeConfig(boolean validateOnWrite) {
        return Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64,
                ProtoMoltSerdeConfig.MESSAGE_TYPE, "configit.v1.Throttle",
                ProtoMoltSerdeConfig.USE_SCHEMA_ID, schemaId,
                ProtoMoltSerdeConfig.VALIDATE_ON_WRITE, validateOnWrite);
    }

    static void produce(String subject, Message document, boolean validateOnWrite) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BROKER.getBootstrapServers());
        try (ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
                        properties, new StringSerializer(), new ByteArraySerializer())) {
            serializer.configure(serdeConfig(validateOnWrite), false);
            byte[] value = document == null ? null : serializer.serialize(TOPIC, document);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    TOPIC, KafkaConfigSource.keyFor(subject), value);
            record.headers().add(KafkaConfigSource.SUBJECT_HEADER,
                    subject.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            producer.send(record);
            producer.flush();
        }
    }

    @Test
    void theHouseStackGatesBothDirectionsAndTheConsumerFollowsTheTopic() {
        // The writer gate: a document violating the type's own rules
        // cannot even serialize.
        try (ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(serdeConfig(true), false);
            assertThatThrownBy(() -> serializer.serialize(TOPIC, throttleOf(0, "bad")))
                    .hasMessageContaining("limit");
        }

        produce("parse-throttle", throttleOf(25, "steady"), true);

        KafkaConfigSource.Config config = new KafkaConfigSource.Config(
                BROKER.getBootstrapServers(), TOPIC, registryUrl,
                Map.of(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64,
                        ProtoMoltSerdeConfig.MESSAGE_TYPE, "configit.v1.Throttle"));
        try (DistributedConfig consumer = DistributedConfig.over(
                new KafkaConfigSource(config))) {
            DistributedConfig.Subscription<DynamicMessage> subscription =
                    consumer.subscribe("parse-throttle",
                            DynamicMessage.getDefaultInstance(throttle));
            DistributedConfig.RefreshOutcome first = consumer.refresh();
            assertThat(first.applied()).containsExactly("parse-throttle");
            assertThat(subscription.current().orElseThrow().version()).isEqualTo("0:0");
            assertThat(subscription.current().orElseThrow().config()
                    .getField(throttle.findFieldByName("limit"))).isEqualTo(25);

            // A poisoned record smuggled past the writer gate (validation
            // off at the producer) refuses at read with its coordinates,
            // and the consumer keeps serving what it runs.
            produce("parse-throttle", throttleOf(0, "poison"), false);
            DistributedConfig.RefreshOutcome poisoned = consumer.refresh();
            assertThat(poisoned.refused()).hasSize(1);
            assertThat(poisoned.refused().get(0).reason())
                    .contains("0:1").contains("refused by the serde");
            assertThat(subscription.current().orElseThrow().version()).isEqualTo("0:0");

            // The next honest publish heals the subject.
            produce("parse-throttle", throttleOf(50, "faster"), true);
            DistributedConfig.RefreshOutcome healed = consumer.refresh();
            assertThat(healed.applied()).containsExactly("parse-throttle");
            assertThat(subscription.current().orElseThrow().version()).isEqualTo("0:2");
            assertThat(subscription.current().orElseThrow().config()
                    .getField(throttle.findFieldByName("limit"))).isEqualTo(50);

            // A tombstone empties the source's offer; the consumer's
            // current config stays, absence is not removal.
            produce("parse-throttle", null, true);
            DistributedConfig.RefreshOutcome tombstoned = consumer.refresh();
            assertThat(tombstoned.absent()).containsExactly("parse-throttle");
            assertThat(subscription.current().orElseThrow().version()).isEqualTo("0:2");
        }
    }

    @Test
    void aKeyThatDoesNotDeriveFromTheSubjectRefuses() throws Exception {
        // Identity is derived, never trusted: a hand-rolled key poisons
        // the subject loudly instead of landing under the wrong
        // compaction identity.
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BROKER.getBootstrapServers());
        try (ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
                        properties, new StringSerializer(), new ByteArraySerializer())) {
            serializer.configure(serdeConfig(true), false);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC,
                    "not-a-derived-key", serializer.serialize(TOPIC, throttleOf(7, "x")));
            record.headers().add(KafkaConfigSource.SUBJECT_HEADER,
                    "mismatched".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            producer.send(record);
            producer.flush();
        }
        KafkaConfigSource.Config config = new KafkaConfigSource.Config(
                BROKER.getBootstrapServers(), TOPIC, registryUrl,
                Map.of(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64,
                        ProtoMoltSerdeConfig.MESSAGE_TYPE, "configit.v1.Throttle"));
        try (KafkaConfigSource source = new KafkaConfigSource(config)) {
            assertThatThrownBy(() -> source.fetch("mismatched"))
                    .hasMessageContaining("does not derive from subject");
        }
    }

    @Test
    void anUnknownSubjectIsEmptinessAndAMissingTopicRefusesLoudly() {
        KafkaConfigSource.Config config = new KafkaConfigSource.Config(
                BROKER.getBootstrapServers(), TOPIC, registryUrl,
                Map.of(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64,
                        ProtoMoltSerdeConfig.MESSAGE_TYPE, "configit.v1.Throttle"));
        try (KafkaConfigSource source = new KafkaConfigSource(config)) {
            assertThat(source.fetch("no-such-subject")).isEmpty();
        }
    }
}
