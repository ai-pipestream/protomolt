package ai.pipestream.proto.connector;

import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Kafka source against a genuine broker: records arrive in order through the pump,
 * and pausing holds back a follow-up batch until resume lets it through. The broker is a
 * Testcontainers Redpanda instance; the suite skips when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaSourceLiveIntegrationTest {

    // The module has no no-arg constructor and no default tag; this is the baseline
    // image its own test suite pins (also its minimum supported version).
    @Container
    static final RedpandaContainer KAFKA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    private static String unique(String prefix) {
        return prefix + "-" + Long.toUnsignedString(System.nanoTime(), 36);
    }

    private static String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    private static void createTopic(String topic) throws Exception {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        try (AdminClient admin = AdminClient.create(config)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private static void produce(String topic, int from, int to) throws Exception {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            for (int i = from; i < to; i++) {
                producer.send(new ProducerRecord<>(topic, ("value-" + i)
                        .getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static String payloadOf(Message message) {
        return ((BytesValue) message).getValue().toStringUtf8();
    }

    @Test
    void topicRecordsArriveInOrder() throws Exception {
        String topic = unique("connector-it");
        createTopic(topic);
        produce(topic, 0, 10);

        KafkaSourcePlan plan = new KafkaSourcePlan(bootstrap(), topic, unique("connector-it-group"),
                MessageParser.bytes(), Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        try (SourcePump pump = new SourcePump(4)) {
            pump.attach(new KafkaSource().open(plan, pump));
            for (int i = 0; i < 10; i++) {
                Message next = pump.take(Duration.ofSeconds(15));
                assertThat(next).as("record %d arrives", i).isNotNull();
                assertThat(payloadOf(next)).isEqualTo("value-" + i);
            }
        }
    }

    @Test
    void pauseHoldsBackNewRecordsUntilResume() throws Exception {
        String topic = unique("connector-it-pause");
        createTopic(topic);
        produce(topic, 0, 5);

        // Small poll batches keep the consumer from fetching the second batch early.
        KafkaSourcePlan plan = new KafkaSourcePlan(bootstrap(), topic, unique("connector-it-group"),
                MessageParser.bytes(), Map.of(
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5));
        SourcePump pump = new SourcePump(8);
        StreamSource.Handle handle = new KafkaSource().open(plan, pump);
        pump.attach(handle);
        try {
            for (int i = 0; i < 5; i++) {
                Message next = pump.take(Duration.ofSeconds(15));
                assertThat(next).as("record %d arrives", i).isNotNull();
            }

            handle.pause();
            Thread.sleep(500);
            produce(topic, 5, 10);

            assertThat(pump.take(Duration.ofMillis(500)))
                    .as("paused source holds back the new batch").isNull();

            handle.resume();
            for (int i = 5; i < 10; i++) {
                Message next = pump.take(Duration.ofSeconds(15));
                assertThat(next).as("record %d arrives after resume", i).isNotNull();
                assertThat(payloadOf(next)).isEqualTo("value-" + i);
            }
        } finally {
            pump.close();
        }
    }

    @Test
    void unparseableRecordFailsWithTopicPartitionOffset() throws Exception {
        String topic = unique("connector-it-bad");
        createTopic(topic);

        // One record whose payload is not a valid message of the plan's type.
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(topic, "not-a-proto-payload"
                    .getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
        }

        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("connector/it/ping.proto", """
                        syntax = "proto3";
                        package connector.it;
                        message Ping { int32 seq = 1; }
                        """, "test").build());
        Descriptor ping = compiled.descriptorFor("connector/it/ping.proto").orElseThrow()
                .findMessageTypeByName("Ping");

        KafkaSourcePlan plan = new KafkaSourcePlan(bootstrap(), topic, unique("connector-it-group"),
                MessageParser.forType(ping), Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        SourcePump pump = new SourcePump(4);
        pump.attach(new KafkaSource().open(plan, pump));
        try {
            assertThatThrownBy(() -> pump.take(Duration.ofSeconds(15)))
                    .isInstanceOf(SourceException.class)
                    .hasMessageContaining(topic)
                    .hasMessageContaining(":0@0")
                    .cause().hasMessageContaining("not a valid connector.it.Ping");
            assertThat(pump.isCompleted()).as("a failure is not a completion").isFalse();
        } finally {
            pump.close();
        }
    }

    @Test
    void confluentWireFormatRoundTripsThroughTheSerde() throws Exception {
        String topic = unique("connector-it-serde");
        createTopic(topic);

        String proto = """
                syntax = "proto3";
                package connector.it;
                message Envelope { string doc_id = 1; int32 seq = 2; }
                """;
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("connector/it/envelope.proto", proto, "test").build());
        Descriptor envelope = compiled.descriptorFor("connector/it/envelope.proto").orElseThrow()
                .findMessageTypeByName("Envelope");
        String descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        String registry = KAFKA.getSchemaRegistryAddress();

        // The value subject the serializer's frames will name.
        register(topic + "-value", proto);

        // Write with the ProtoMolt serializer: Confluent-framed bytes, subject registered.
        ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
        serializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64,
                ProtoMoltSerdeConfig.MESSAGE_TYPE, "connector.it.Envelope",
                ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registry), false);

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < 3; i++) {
                DynamicMessage.Builder builder = DynamicMessage.newBuilder(envelope);
                builder.setField(envelope.findFieldByName("doc_id"), "doc-" + i);
                builder.setField(envelope.findFieldByName("seq"), i);
                producer.send(new ProducerRecord<>(topic, serializer.serialize(topic, builder.build())))
                        .get(10, TimeUnit.SECONDS);
            }
        }

        // Read with only the registry URL: the frame's schema id resolves server-side.
        KafkaSourcePlan plan = new KafkaSourcePlan(bootstrap(), topic, unique("connector-it-group"),
                MessageParser.confluent(registry),
                Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        try (SourcePump pump = new SourcePump(4)) {
            pump.attach(new KafkaSource().open(plan, pump));
            for (int i = 0; i < 3; i++) {
                Message next = pump.take(Duration.ofSeconds(15));
                assertThat(next).as("frame %d arrives", i).isNotNull();
                DynamicMessage message = (DynamicMessage) next;
                // The serde links its own descriptors, so read through the message's type.
                Descriptor actual = message.getDescriptorForType();
                assertThat(actual.getFullName()).isEqualTo("connector.it.Envelope");
                assertThat(message.getField(actual.findFieldByName("doc_id"))).isEqualTo("doc-" + i);
                assertThat(message.getField(actual.findFieldByName("seq"))).isEqualTo(i);
            }
        }
    }

    /** Registers a protobuf schema under the subject; the registry assigns the id. */
    private static void register(String subject, String schema) throws Exception {
        String body = "{\"schemaType\":\"PROTOBUF\",\"schema\":" + quote(schema) + "}";
        try (HttpClient http = HttpClient.newHttpClient()) {
            HttpResponse<String> response = http.send(HttpRequest
                    .newBuilder(URI.create(
                            KAFKA.getSchemaRegistryAddress() + "/subjects/" + subject + "/versions"))
                    .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode())
                    .as("registering %s: %s", subject, response.body())
                    .isEqualTo(200);
        }
    }

    private static String quote(String text) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : text.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                default -> out.append(c);
            }
        }
        return out.append('"').toString();
    }
}
