package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringServiceGrpc;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Sample-only bridge from a private launch-intent topic to the existing scoped
 * launch RPC. The broker topic and this process's credential are operator-owned
 * authority; a Kafka record does not identify or authenticate its producer.
 */
public final class AcceptedWorkflowKafkaBridge {
    private static final int MAX_MESSAGE_BYTES = 4 * 1024 * 1024;
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

    @FunctionalInterface
    interface LaunchClient {
        WorkflowAuthoringLaunchResult launch(WorkflowAuthoringLaunchRequest request);
    }

    private final Consumer<String, byte[]> consumer;
    private final LaunchClient launcher;

    AcceptedWorkflowKafkaBridge(Consumer<String, byte[]> consumer, LaunchClient launcher) {
        this.consumer = Objects.requireNonNull(consumer);
        this.launcher = Objects.requireNonNull(launcher);
    }

    void run(String topic) {
        if (topic == null || topic.isBlank()) throw new IllegalArgumentException("launch topic is required");
        consumer.subscribe(List.of(topic));
        while (!Thread.currentThread().isInterrupted()) {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(POLL_INTERVAL)) {
                process(record);
            }
        }
    }

    /** One RPC and one exact partition-offset commit; any failure leaves this record uncommitted. */
    void process(ConsumerRecord<String, byte[]> record) {
        Objects.requireNonNull(record, "record");
        WorkflowAuthoringLaunchRequest request = parse(record.value());
        String launchId = canonicalId(request.getLaunchId());
        if (!launchId.equals(record.key())) {
            throw new IllegalArgumentException("Kafka key differs from launch UUID");
        }
        WorkflowAuthoringLaunchResult result = launcher.launch(request);
        validate(result);
        if (!launchId.equals(result.getJobId())) {
            throw new IllegalStateException("launch response names a different job");
        }
        consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(Math.addExact(record.offset(), 1L))));
    }

    private static WorkflowAuthoringLaunchRequest parse(byte[] value) {
        if (value == null || value.length == 0 || value.length > MAX_MESSAGE_BYTES) {
            throw new IllegalArgumentException("launch record is empty or exceeds the bound");
        }
        try {
            WorkflowAuthoringLaunchRequest request = WorkflowAuthoringLaunchRequest.parseFrom(value);
            validate(request);
            return request;
        } catch (InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("launch record is not valid protobuf");
        }
    }

    private static String canonicalId(String value) {
        try {
            String canonical = UUID.fromString(value).toString();
            if (canonical.equals(value)) return canonical;
        } catch (IllegalArgumentException ignored) {
            // Validation below reports the same class of invalid identifier.
        }
        throw new IllegalArgumentException("launch UUID is not canonical");
    }

    private static void validate(Message value) {
        if (value == null || value.getSerializedSize() > MAX_MESSAGE_BYTES) {
            throw new IllegalArgumentException("launch contract exceeds the bound");
        }
        rejectUnknown(value);
        if (!ProtoValidator.forMessageType(value.getDescriptorForType()).validate(value).valid()) {
            throw new IllegalArgumentException("launch contract is invalid");
        }
    }

    private static void rejectUnknown(Message value) {
        if (!value.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("launch contract has unknown fields");
        }
        for (var field : value.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else {
                rejectUnknown((Message) field.getValue());
            }
        }
    }

    /** Credentials enter through a mount; this class prints no secret or payload. */
    public static void main(String[] args) {
        if (args.length != 0) {
            System.err.println("Configure the accepted-workflow Kafka bridge through its environment");
            System.exit(2);
        }
        ManagedChannel channel = null;
        try {
            String bootstrap = requiredEnv("PROTOMOLT_KAFKA_BOOTSTRAP_SERVERS");
            String topic = requiredEnv("PROTOMOLT_KAFKA_ACCEPTED_LAUNCH_TOPIC");
            String group = requiredEnv("PROTOMOLT_KAFKA_ACCEPTED_LAUNCH_GROUP");
            String target = requiredEnv("PROTOMOLT_COORDINATOR_TARGET");
            String token = readCredential(Path.of(requiredEnv("PROTOMOLT_WORKFLOW_LAUNCH_TOKEN_FILE")));
            long deadlineSeconds = positiveSeconds(System.getenv("PROTOMOLT_LAUNCH_RPC_TIMEOUT_SECONDS"));
            boolean plaintext = "true".equals(System.getenv("PROTOMOLT_COORDINATOR_PLAINTEXT"));

            Properties properties = new Properties();
            String propertiesFile = System.getenv("PROTOMOLT_KAFKA_CLIENT_PROPERTIES_FILE");
            if (propertiesFile != null && !propertiesFile.isBlank()) {
                try (var input = Files.newInputStream(Path.of(propertiesFile))) {
                    properties.load(input);
                }
            }
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
            properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(target);
            channel = plaintext ? channelBuilder.usePlaintext().build()
                    : channelBuilder.useTransportSecurity().build();
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
            var stub = WorkflowAuthoringServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                    .withMaxInboundMessageSize(MAX_MESSAGE_BYTES);
            try (Consumer<String, byte[]> consumer = new KafkaConsumer<>(properties,
                    new StringDeserializer(), new ByteArrayDeserializer())) {
                new AcceptedWorkflowKafkaBridge(consumer,
                        request -> stub.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS)
                                .launchAcceptedWorkflow(request)).run(topic);
            }
        } catch (Exception failure) {
            // In particular, never print a gRPC error description or Kafka record:
            // both may contain caller-controlled payload or configuration secrets.
            System.err.println("Accepted-workflow Kafka bridge stopped; restart may replay the last launch");
            System.exit(1);
        } finally {
            if (channel != null) channel.shutdownNow();
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing bridge setting");
        return value;
    }

    private static long positiveSeconds(String supplied) {
        if (supplied == null || supplied.isBlank()) return 30L;
        try {
            long seconds = Long.parseLong(supplied);
            if (seconds >= 1 && seconds <= 120) return seconds;
        } catch (NumberFormatException ignored) {
            // Invalid settings fail before connecting.
        }
        throw new IllegalArgumentException("Launch RPC timeout must be 1 to 120 seconds");
    }

    private static String readCredential(Path file) throws IOException {
        if (!Files.isRegularFile(file) || Files.size(file) > 8192) {
            throw new IllegalArgumentException("Launch credential mount is invalid");
        }
        String value = Files.readString(file, StandardCharsets.UTF_8).strip();
        if (value.isEmpty() || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("Launch credential mount is invalid");
        }
        return value;
    }
}
