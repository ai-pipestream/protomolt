package ai.pipestream.proto.connector;

import com.google.protobuf.BytesValue;
import com.google.protobuf.Message;
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
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Kafka source against a genuine broker: records arrive in order through the pump,
 * and pausing holds back a follow-up batch until resume lets it through. The broker is a
 * Testcontainers Apache Kafka instance; the suite skips when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaSourceLiveIntegrationTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

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
}
