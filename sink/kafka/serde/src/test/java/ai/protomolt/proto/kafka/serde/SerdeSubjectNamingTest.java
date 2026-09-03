package ai.protomolt.proto.kafka.serde;

import ai.protomolt.proto.kafka.wire.ConfluentWireFormat;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which subject the serializer asks the registry about, recorded by a fake registry that logs
 * every request path. The strategies are covered end to end here rather than by mocking the
 * lookup, because the whole point is the URL a real registry sees. Also pinned: a resolved
 * subject id and schema are fetched once for the serde's lifetime, and a consumer carrying only
 * a registry URL — no descriptor set at all — resolves each frame's type by id.
 */
class SerdeSubjectNamingTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.naming.v1;
            message Event { string id = 1; }
            """;

    private static String descriptorSetBase64;
    private static Descriptor eventType;
    private static HttpServer server;
    private static String registryUrl;
    private static final List<String> requestedPaths = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/naming/v1/event.proto", PROTO, "test").build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        eventType = compiled.descriptorFor("serde/naming/v1/event.proto").orElseThrow()
                .findMessageTypeByName("Event");

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            requestedPaths.add(path);
            switch (path) {
                case "/subjects/named-value/versions/latest" ->
                        respond(exchange, 200, "{\"id\": 42}");
                case "/schemas/ids/42" -> respond(exchange, 200, schemaJson());
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

    @BeforeEach
    void reset() {
        requestedPaths.clear();
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

    private static String schemaJson() {
        return "{\"schemaType\": \"PROTOBUF\", \"schema\": \"" + PROTO
                .replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"}";
    }

    private static Map<String, Object> config(Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.naming.v1.Event");
        config.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl);
        config.put(ProtoMoltSerdeConfig.USE_SCHEMA_ID, 7);
        config.putAll(extra);
        return config;
    }

    private static Message event(String id) {
        return DynamicMessage.newBuilder(eventType)
                .setField(eventType.findFieldByName("id"), id)
                .build();
    }

    /**
     * The registry has never heard of these subjects (404), so the write is refused. That is
     * beside the point here: the lookup still went out, and the path it took is what these
     * tests are about.
     */
    private static String subjectAskedFor(Map<String, Object> extra, boolean isKey) {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(extra), isKey);
            try {
                serializer.serialize("orders", event("A-1"));
            } catch (SerializationException expected) {
                // An unregistered subject refuses the write; the request was still made.
            }
        }
        return requestedPaths.stream()
                .filter(p -> p.startsWith("/subjects/"))
                .reduce((first, second) -> second)
                .orElseThrow();
    }

    @Test
    void theDefaultSubjectIsTopicValue() {
        assertThat(subjectAskedFor(Map.of(), false))
                .isEqualTo("/subjects/orders-value/versions/latest");
    }

    @Test
    void aKeySerdeAsksForTopicKey() {
        assertThat(subjectAskedFor(Map.of(), true))
                .isEqualTo("/subjects/orders-key/versions/latest");
    }

    @Test
    void theRecordStrategyAsksForTheTypeName() {
        assertThat(subjectAskedFor(Map.of(
                ProtoMoltSerdeConfig.SUBJECT_NAME_STRATEGY, Subjects.RECORD), false))
                .isEqualTo("/subjects/serde.naming.v1.Event/versions/latest");
    }

    @Test
    void theTopicRecordStrategyAsksForBoth() {
        assertThat(subjectAskedFor(Map.of(
                ProtoMoltSerdeConfig.SUBJECT_NAME_STRATEGY, Subjects.TOPIC_RECORD), false))
                .isEqualTo("/subjects/orders-serde.naming.v1.Event/versions/latest");
    }

    @Test
    void aSubjectOverrideWinsOverTheStrategy() {
        assertThat(subjectAskedFor(Map.of(
                ProtoMoltSerdeConfig.SUBJECT_NAME_STRATEGY, Subjects.RECORD,
                ProtoMoltSerdeConfig.SUBJECT, "custom-subject"), false))
                .isEqualTo("/subjects/custom-subject/versions/latest");
    }

    /**
     * A resolved subject id names one schema forever, and the registry client caches the schema:
     * the second record to the same subject must cost no lookup at all.
     */
    @Test
    void aResolvedSubjectAndSchemaAreFetchedOncePerSerde() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);

            assertThat(ConfluentWireFormat.schemaId(serializer.serialize("named", event("A-1"))))
                    .isEqualTo(42);
            assertThat(ConfluentWireFormat.schemaId(serializer.serialize("named", event("A-2"))))
                    .isEqualTo(42);
        }

        assertThat(requestedPaths.stream()
                .filter("/subjects/named-value/versions/latest"::equals).count()).isEqualTo(1);
        assertThat(requestedPaths.stream()
                .filter("/schemas/ids/42"::equals).count()).isEqualTo(1);
    }

    /**
     * The registry-only lane: no descriptor set, no pinned type, just a registry URL. The
     * frame's id resolves to the schema, the index path to the type inside it, and the record
     * comes back as a DynamicMessage — the lane Confluent's deserializer has always had.
     */
    @Test
    void aDeserializerWithOnlyARegistryResolvesTypesById() {
        Map<String, Object> registryOnly = new HashMap<>();
        registryOnly.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl);
        byte[] framed = ConfluentWireFormat.frame(42, List.of(0), event("A-3").toByteArray());

        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            deserializer.configure(registryOnly, false);

            Message back = deserializer.deserialize("named", framed);

            assertThat(back).isInstanceOf(DynamicMessage.class);
            Descriptor type = back.getDescriptorForType();
            assertThat(type.getFullName()).isEqualTo("serde.naming.v1.Event");
            assertThat(back.getField(type.findFieldByName("id"))).isEqualTo("A-3");
        }
    }
}
