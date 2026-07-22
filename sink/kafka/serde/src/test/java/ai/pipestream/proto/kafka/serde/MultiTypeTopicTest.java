package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Several event types sharing one topic: the serde unpinned. The serializer accepts any type the
 * descriptor set declares and looks each type's subject up under the record-name strategy; the
 * deserializer resolves each frame's type through the registry. What the assertions really pin
 * down is that the frame's id and index describe the same file — the writer's — even when the
 * packaged file lays the types out differently.
 */
class MultiTypeTopicTest {

    /** The packaged layout: Ignored first, Order second. */
    private static final String LOCAL_PROTO = """
            syntax = "proto3";
            package serde.orders.v1;
            message Ignored { string filler = 1; }
            message Order {
              string id = 1;
              int32 quantity = 2;
            }
            """;

    /** The registered layout: same types, opposite order. */
    private static final String WRITER_PROTO = """
            syntax = "proto3";
            package serde.orders.v1;
            message Order {
              string id = 1;
              int32 quantity = 2;
            }
            message Ignored { string filler = 1; }
            """;

    private static String descriptorSetBase64;
    private static Descriptor orderType;
    private static Descriptor ignoredType;
    private static Descriptor strayType;
    private static HttpServer server;
    private static String registryUrl;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/orders/v1/order.proto", LOCAL_PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        var file = compiled.descriptorFor("serde/orders/v1/order.proto").orElseThrow();
        orderType = file.findMessageTypeByName("Order");
        ignoredType = file.findMessageTypeByName("Ignored");

        // A type from a different compilation entirely, declared in no packaged file.
        strayType = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                        .add("other/v1/stray.proto", """
                                syntax = "proto3";
                                package other.v1;
                                message Stray { string x = 1; }
                                """, "test")
                        .build())
                .descriptorFor("other/v1/stray.proto").orElseThrow()
                .findMessageTypeByName("Stray");

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            switch (path) {
                case "/schemas/ids/42" -> respond(exchange, 200,
                        "{\"schemaType\": \"PROTOBUF\", \"schema\": \"" + WRITER_PROTO
                                .replace("\\", "\\\\").replace("\"", "\\\"")
                                .replace("\n", "\\n") + "\"}");
                // Record-name subjects: both types registered under the same schema id.
                case "/subjects/serde.orders.v1.Order/versions/latest",
                     "/subjects/serde.orders.v1.Ignored/versions/latest" ->
                        respond(exchange, 200, "{\"id\": 42}");
                default -> respond(exchange, 404,
                        "{\"error_code\": 40401, \"message\": \"not found\"}");
            }
        });
        server.start();
        registryUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    static void stop() {
        server.stop(0);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status,
                                String body) throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type",
                "application/vnd.schemaregistry.v1+json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** No message.type: the serde takes its types from the frames and the descriptor set. */
    private static Map<String, Object> unpinned(Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl);
        config.put(ProtoMoltSerdeConfig.SUBJECT_NAME_STRATEGY, Subjects.RECORD);
        config.putAll(extra);
        return config;
    }

    private static Message of(Descriptor type, String field, Object value) {
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName(field), value)
                .build();
    }

    @Test
    void carriesTwoTypesOnOneTopic() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(unpinned(Map.of()), false);
            deserializer.configure(unpinned(Map.of()), false);

            byte[] orderFrame = serializer.serialize("mixed", of(orderType, "id", "A-1"));
            byte[] ignoredFrame = serializer.serialize("mixed", of(ignoredType, "filler", "f"));

            // The id names the writer's schema, so the index must be the writer's too: Order is
            // second in the packaged file but first in the registered one.
            assertThat(ConfluentWireFormat.schemaId(orderFrame)).isEqualTo(42);
            assertThat(ConfluentWireFormat.messageIndex(orderFrame)).containsExactly(0);
            assertThat(ConfluentWireFormat.messageIndex(ignoredFrame)).containsExactly(1);

            Message order = deserializer.deserialize("mixed", orderFrame);
            Message ignored = deserializer.deserialize("mixed", ignoredFrame);
            assertThat(order.getDescriptorForType().getFullName())
                    .isEqualTo("serde.orders.v1.Order");
            assertThat(ignored.getDescriptorForType().getFullName())
                    .isEqualTo("serde.orders.v1.Ignored");
        }
    }

    /** The descriptor set is the producer's contract; a type outside it is refused. */
    @Test
    void refusesATypeTheDescriptorSetDoesNotDeclare() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(unpinned(Map.of()), false);
            assertThatThrownBy(() -> serializer.serialize("mixed", of(strayType, "x", "v")))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("not part of this producer's contract");
        }
    }

    /** Unpinned reading needs the registry: an index path alone cannot name a type. */
    @Test
    void unpinnedConsumerRequiresARegistry() {
        Map<String, Object> config = unpinned(Map.of());
        config.remove(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL);
        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            assertThatThrownBy(() -> deserializer.configure(config, false))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("required when no registry is configured");
        }
    }

    /** An id the registry cannot resolve fails that record; unpinned has nothing to guess with. */
    @Test
    void failsARecordWhoseIdCannotBeResolved() {
        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            deserializer.configure(unpinned(Map.of()), false);
            byte[] unknown = ConfluentWireFormat.frame(99, List.of(0), new byte[0]);
            assertThatThrownBy(() -> deserializer.deserialize("mixed", unknown))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("could not resolve");
        }
    }
}
