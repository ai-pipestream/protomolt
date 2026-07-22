package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The id lane against a genuine Confluent-compatible Schema Registry: a serializer stamps the id
 * the registry actually holds for its subject, and a deserializer resolves a frame's id back to
 * the registry's schema.
 *
 * <p>The registered schema is deliberately a superset of the packaged one, carrying a field the
 * deployment's descriptor set has never heard of. That is what makes the assertion mean something:
 * if the resolved descriptor knows {@code note}, it came from the registry, and if it does not,
 * the deserializer quietly fell back to what it had packaged and the lane is not working.</p>
 *
 * <p>The registry is a Testcontainers Redpanda, which serves the Confluent Schema Registry API;
 * the suite skips when Docker is unavailable.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class SerdeRegistryLiveIntegrationTest {

    // Same baseline image as the connector lane (testcontainers' own pinned tag).
    @Container
    static final RedpandaContainer REGISTRY = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    /** What the deployment packages: no `note`. */
    private static final String PACKAGED = """
            syntax = "proto3";
            package serde.it.v1;
            message Ignored { string filler = 1; }
            message Order {
              string id = 1;
              int32 quantity = 2;
            }
            """;

    /** What the registry holds: the writer moved on and added a field. */
    private static final String REGISTERED = """
            syntax = "proto3";
            package serde.it.v1;
            message Ignored { string filler = 1; }
            message Order {
              string id = 1;
              int32 quantity = 2;
              string note = 3;
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor orderType;
    private static String subject;
    private static int registeredId;

    @BeforeAll
    static void setUp() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/it/v1/order.proto", PACKAGED, "test").build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        orderType = compiled.descriptorFor("serde/it/v1/order.proto").orElseThrow()
                .findMessageTypeByName("Order");

        // Unique per run so reruns never collide with an earlier version of the subject.
        subject = "serde-it-" + Long.toUnsignedString(System.nanoTime(), 36) + "-value";
        registeredId = register(subject, REGISTERED);
    }

    private static String registryUrl() {
        return REGISTRY.getSchemaRegistryAddress();
    }

    private static int register(String subject, String schema) throws Exception {
        String body = "{\"schemaType\":\"PROTOBUF\",\"schema\":" + quote(schema) + "}";
        try (HttpClient http = HttpClient.newHttpClient()) {
            HttpResponse<String> response = http.send(HttpRequest
                    .newBuilder(URI.create(registryUrl() + "/subjects/" + subject + "/versions"))
                    .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode())
                    .as("registering %s: %s", subject, response.body())
                    .isEqualTo(200);
            String json = response.body();
            int at = json.indexOf("\"id\"");
            return Integer.parseInt(json.substring(json.indexOf(':', at) + 1)
                    .replaceAll("[^0-9].*$", "").trim());
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

    private static Map<String, Object> config() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.it.v1.Order");
        config.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl());
        config.put(ProtoMoltSerdeConfig.SUBJECT, subject);
        config.put(ProtoMoltSerdeConfig.USE_SCHEMA_ID, 0);
        return config;
    }

    private static Message order(String id, int quantity) {
        return DynamicMessage.newBuilder(orderType)
                .setField(orderType.findFieldByName("id"), id)
                .setField(orderType.findFieldByName("quantity"), quantity)
                .build();
    }

    @Test
    void stampsTheRegistrysIdAndResolvesTheRegistrysSchema() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(), false);
            deserializer.configure(config(), false);

            byte[] bytes = serializer.serialize("orders", order("A-1", 4));

            // The registry's id, not the configured 0.
            assertThat(ConfluentWireFormat.schemaId(bytes)).isEqualTo(registeredId);
            assertThat(registeredId).isPositive();
            // Order is the second message in its file: the case an unsigned index reader breaks on.
            assertThat(ConfluentWireFormat.messageIndex(bytes)).containsExactly(1);

            Message back = deserializer.deserialize("orders", bytes);
            Descriptor resolved = back.getDescriptorForType();

            // Only the registry's copy has `note`, so this is proof of where the schema came from.
            assertThat(resolved.findFieldByName("note"))
                    .as("resolved schema should be the registry's, which has the extra field")
                    .isNotNull();
            assertThat(orderType.findFieldByName("note"))
                    .as("the packaged schema must not have it, or the test proves nothing")
                    .isNull();

            assertThat(back.getField(resolved.findFieldByName("id"))).isEqualTo("A-1");
            assertThat(back.getField(resolved.findFieldByName("quantity"))).isEqualTo(4);
        }
    }

    /** An unregistered subject is not an outage: the configured id stands in. */
    @Test
    void fallsBackWhenTheSubjectIsNotRegistered() {
        Map<String, Object> config = config();
        config.put(ProtoMoltSerdeConfig.SUBJECT, "serde-it-no-such-subject-value");
        config.put(ProtoMoltSerdeConfig.USE_SCHEMA_ID, 42);
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config, false);
            assertThat(ConfluentWireFormat.schemaId(serializer.serialize("orders", order("A", 1))))
                    .isEqualTo(42);
        }
    }

    /**
     * Registry-only, the lane Confluent's deserializer has always had: no descriptor set at all,
     * so the frame's id is resolved against the registry and the record comes back dynamic.
     */
    @Test
    void readsWithOnlyTheRegistryConfigured() {
        Map<String, Object> registryOnly = new HashMap<>();
        registryOnly.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl());
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(), false);
            deserializer.configure(registryOnly, false);

            byte[] bytes = serializer.serialize("orders", order("A-7", 2));
            Message back = deserializer.deserialize("orders", bytes);

            assertThat(back).isInstanceOf(DynamicMessage.class);
            Descriptor resolved = back.getDescriptorForType();
            assertThat(resolved.getFullName()).isEqualTo("serde.it.v1.Order");
            assertThat(resolved.findFieldByName("note"))
                    .as("the resolved schema is the registry's, which carries the extra field")
                    .isNotNull();
            assertThat(back.getField(resolved.findFieldByName("id"))).isEqualTo("A-7");
            assertThat(back.getField(resolved.findFieldByName("quantity"))).isEqualTo(2);
        }
    }
}
