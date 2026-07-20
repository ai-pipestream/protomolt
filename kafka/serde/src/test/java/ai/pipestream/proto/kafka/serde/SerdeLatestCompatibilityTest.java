package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The strict half of use-latest-version semantics. Registry mode always stamps the id of the
 * subject's <em>latest</em> registered version, and a consumer following that id reads the
 * bytes with the registered schema — not the packaged one that wrote them. These tests pin the
 * guard on that seam: a registered schema that evolved compatibly is stamped and used; one
 * that cannot read what this producer writes refuses the record instead of framing it with an
 * id every reader would misread by. Confluent's {@code latest.compatibility.strict=false}
 * escape hatch is honored under the same name.
 */
class SerdeLatestCompatibilityTest {

    private static final String PACKAGED_PROTO = """
            syntax = "proto3";
            package serde.orders.v1;
            message Order {
              string id = 1;
              int32 quantity = 2;
            }
            """;

    /** A compatible evolution of the packaged schema: one added field. */
    private static final String EVOLVED_PROTO = """
            syntax = "proto3";
            package serde.orders.v1;
            message Order {
              string id = 1;
              int32 quantity = 2;
              string note = 3;
            }
            """;

    /** Wire-incompatible: quantity changed from int32 (VARINT) to string (LEN). */
    private static final String DIVERGED_PROTO = """
            syntax = "proto3";
            package serde.orders.v1;
            message Order {
              string id = 1;
              string quantity = 2;
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor orderType;
    private static HttpServer server;
    private static String registryUrl;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/orders/v1/order.proto", PACKAGED_PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        orderType = compiled.descriptorFor("serde/orders/v1/order.proto").orElseThrow()
                .findMessageTypeByName("Order");

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            switch (path) {
                case "/subjects/orders-value/versions/latest" ->
                        respond(exchange, 200, "{\"id\": 42}");
                case "/schemas/ids/42" -> respond(exchange, 200, schemaJson(EVOLVED_PROTO));
                case "/subjects/diverged-value/versions/latest" ->
                        respond(exchange, 200, "{\"id\": 50}");
                case "/schemas/ids/50" -> respond(exchange, 200, schemaJson(DIVERGED_PROTO));
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

    private static String schemaJson(String proto) {
        return "{\"schemaType\": \"PROTOBUF\", \"schema\": \"" + proto
                .replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"}";
    }

    private static Map<String, Object> config(Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.orders.v1.Order");
        config.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl);
        config.putAll(extra);
        return config;
    }

    private static Message message(String id, int quantity) {
        return DynamicMessage.newBuilder(orderType)
                .setField(orderType.findFieldByName("id"), id)
                .setField(orderType.findFieldByName("quantity"), quantity)
                .build();
    }

    /** An added field is exactly the evolution use-latest is for: the latest id is stamped. */
    @Test
    void aCompatiblyEvolvedLatestSchemaIsStamped() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            byte[] framed = serializer.serialize("orders", message("A-1", 3));
            assertThat(ConfluentWireFormat.schemaId(framed)).isEqualTo(42);
        }
    }

    /**
     * The registered schema changed quantity's wire type, so a reader following the stamped id
     * would misread every record. Strict mode (the default) refuses the write instead — and
     * keeps refusing on the cached verdict, not just on the first record.
     */
    @Test
    void refusesAWriteTheLatestSchemaWouldMisread() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            for (int i = 0; i < 2; i++) {
                assertThatThrownBy(() -> serializer.serialize("diverged", message("A-2", 3)))
                        .isInstanceOf(SerializationException.class)
                        .hasMessageContaining("subject diverged-value")
                        .hasMessageContaining("id 50")
                        .hasMessageContaining("cannot read");
            }
        }
    }

    /** Confluent's escape hatch under the same name: off means stamp without the check. */
    @Test
    void strictOffStampsTheLatestIdWithoutTheCheck() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of(
                    ProtoMoltSerdeConfig.LATEST_COMPATIBILITY_STRICT, false)), false);
            byte[] framed = serializer.serialize("diverged", message("A-3", 3));
            assertThat(ConfluentWireFormat.schemaId(framed)).isEqualTo(50);
        }
    }
}
