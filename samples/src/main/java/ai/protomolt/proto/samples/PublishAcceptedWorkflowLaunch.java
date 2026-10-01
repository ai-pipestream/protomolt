package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.util.JsonFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Publish a saved launch intent; a broker acknowledgement does not mean job completion. */
public final class PublishAcceptedWorkflowLaunch {
    private PublishAcceptedWorkflowLaunch() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("Expected BROKERS TOPIC LAUNCH_JSON");
        var file = Path.of(args[2]);
        if (Files.size(file) > 4 * 1024 * 1024) throw new IllegalArgumentException("Launch JSON exceeds 4 MiB");
        var builder = WorkflowAuthoringLaunchRequest.newBuilder();
        JsonFormat.parser().merge(Files.readString(file), builder);
        var request = builder.build();
        ProtoValidator.forMessageType(request.getDescriptorForType()).validate(request).throwIfInvalid();
        if (!UUID.fromString(request.getLaunchId()).toString().equals(request.getLaunchId())) {
            throw new IllegalArgumentException("Use a canonical lowercase launch UUID");
        }
        var properties = new Properties();
        String propertiesFile = System.getenv("PROTOMOLT_KAFKA_CLIENT_PROPERTIES_FILE");
        if (propertiesFile != null && !propertiesFile.isBlank()) {
            try (var input = Files.newInputStream(Path.of(propertiesFile))) { properties.load(input); }
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        var producer = new KafkaProducer<String, byte[]>(properties,
                new StringSerializer(), new ByteArraySerializer());
        try {
            var result = producer.send(new ProducerRecord<>(args[1], request.getLaunchId(),
                    request.toByteArray())).get(35, TimeUnit.SECONDS);
            System.out.println("Broker acknowledged partition=" + result.partition() + " offset=" + result.offset());
            System.out.println("Inspect the saved launch status to determine job completion.");
        } finally {
            producer.close(Duration.ofSeconds(5));
        }
    }
}
