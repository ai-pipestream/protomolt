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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The registry-aware paths, against a local fake speaking just enough of the Confluent REST API.
 * What these pin down is type <em>identity</em>: an index path is a position in the writer's
 * file, not a name, so a writer whose file declares the configured type at a different position
 * is still writing the configured type — and only a registry answer can say so. The fake also
 * counts requests, which is how the retry backoff is proven: a dead registry must cost one
 * lookup per backoff window, not one per record.
 */
class SerdeFakeRegistryTest {

    /** The local layout: Ignored first, so Order sits at index path [1]. */
    private static final String LOCAL_PROTO = """
            syntax = "proto3";
            package serde.orders.v1;
            message Ignored { string filler = 1; }
            message Order {
              string id = 1;
              int32 quantity = 2;
            }
            """;

    /** The writer's layout: same types, Order declared first, so its frames carry [0]. */
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
    private static HttpServer server;
    private static String registryUrl;

    /** A registered schema that resolves but declares none of the local types. */
    private static final String UNRELATED_PROTO = """
            syntax = "proto3";
            package serde.other.v1;
            message Shipment { string id = 1; }
            """;

    private static final AtomicInteger schemaId66Requests = new AtomicInteger();
    private static final AtomicInteger eventsSubjectRequests = new AtomicInteger();

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/orders/v1/order.proto", LOCAL_PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        orderType = compiled.descriptorFor("serde/orders/v1/order.proto").orElseThrow()
                .findMessageTypeByName("Order");

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            switch (path) {
                case "/schemas/ids/42", "/schemas/ids/43", "/schemas/ids/88" ->
                        respond(exchange, 200, schemaJson(WRITER_PROTO));
                case "/schemas/ids/66" -> {
                    schemaId66Requests.incrementAndGet();
                    respond(exchange, 500, "{}");
                }
                case "/subjects/events-value/versions/latest" -> {
                    // Down for the first request, answering afterwards: an outage that ends.
                    if (eventsSubjectRequests.incrementAndGet() == 1) {
                        respond(exchange, 500, "{}");
                    } else {
                        respond(exchange, 200, "{\"id\": 88}");
                    }
                }
                case "/subjects/orders-value/versions/latest" ->
                        respond(exchange, 200, "{\"id\": 42}");
                case "/schemas/ids/99" -> respond(exchange, 200, schemaJson(UNRELATED_PROTO));
                case "/subjects/parcels-value/versions/latest" ->
                        respond(exchange, 200, "{\"id\": 99}");
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
        schemaId66Requests.set(0);
        eventsSubjectRequests.set(0);
        RecordingMetricsListener.reset();
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

    private static byte[] payload(String id, int quantity) {
        return DynamicMessage.newBuilder(orderType)
                .setField(orderType.findFieldByName("id"), id)
                .setField(orderType.findFieldByName("quantity"), quantity)
                .build()
                .toByteArray();
    }

    /**
     * The frame carries [0] because the writer's file declares Order first; the packaged file
     * declares it at [1]. Same type, different position — the registry resolves the name, and
     * the record is read. Comparing positions across the two files would have refused it.
     */
    @Test
    void readsAWriterWhoseFileDeclaresTheTypeAtAnotherIndex() {
        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            deserializer.configure(config(Map.of()), false);
            byte[] framed = ConfluentWireFormat.frame(42, List.of(0), payload("A-1", 2));

            Message back = deserializer.deserialize("orders", framed);

            Descriptor type = back.getDescriptorForType();
            assertThat(type.getFullName()).isEqualTo("serde.orders.v1.Order");
            assertThat(back.getField(type.findFieldByName("id"))).isEqualTo("A-1");
        }
    }

    /** The registry names the framed type, and it is not the configured one: refuse by name. */
    @Test
    void refusesARecordTheRegistrySaysIsAnotherType() {
        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            deserializer.configure(config(Map.of()), false);
            // Index [1] in the writer's file is Ignored.
            byte[] framed = ConfluentWireFormat.frame(43, List.of(1), new byte[0]);

            assertThatThrownBy(() -> deserializer.deserialize("orders", framed))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("serde.orders.v1.Ignored")
                    .hasMessageContaining("configured for serde.orders.v1.Order");
        }
    }

    /** A failing id resolution costs one request per backoff window, not one per record. */
    @Test
    void doesNotHammerAFailingRegistryOnTheReadPath() {
        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            deserializer.configure(config(Map.of()), false);
            // The frame's index matches the packaged layout, so the fallback reads every record.
            byte[] framed = ConfluentWireFormat.frame(66, List.of(1), payload("A-2", 1));

            for (int i = 0; i < 50; i++) {
                assertThat(deserializer.deserialize("orders", framed)).isNotNull();
            }

            assertThat(schemaId66Requests.get())
                    .as("one failed lookup inside the default backoff window")
                    .isEqualTo(1);
        }
    }

    /** A registry that recovers repairs the stamped id without a producer restart. */
    @Test
    void asksAgainAfterTheBackoffAndPicksUpTheRegistryId() throws InterruptedException {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of(
                    ProtoMoltSerdeConfig.USE_SCHEMA_ID, 7,
                    ProtoMoltSerdeConfig.REGISTRY_RETRY_BACKOFF_MS, 50L)), false);

            byte[] duringOutage = serializer.serialize("events", message("A-3", 4));
            assertThat(ConfluentWireFormat.schemaId(duringOutage))
                    .as("the configured id stands in while the registry is down")
                    .isEqualTo(7);

            Thread.sleep(150);

            byte[] afterRecovery = serializer.serialize("events", message("A-4", 4));
            assertThat(ConfluentWireFormat.schemaId(afterRecovery))
                    .as("the registry's id once the backoff expired")
                    .isEqualTo(88);
            // And the index is id 88's schema's, where Order is first — id and index pair up.
            assertThat(ConfluentWireFormat.messageIndex(afterRecovery)).containsExactly(0);
        }
    }

    /**
     * The ordinary happy path: the subject resolves and the frame carries the registry's id —
     * paired with the index of the type in the <em>registry's</em> file, not the packaged one.
     * Order is second in the packaged file but first in the registered schema, and a consumer
     * following id 42 will look it up at the registered position.
     */
    @Test
    void stampsTheRegistryIdWithTheRegistrySchemasIndex() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            byte[] framed = serializer.serialize("orders", message("A-5", 9));
            assertThat(ConfluentWireFormat.schemaId(framed)).isEqualTo(42);
            assertThat(ConfluentWireFormat.messageIndex(framed)).containsExactly(0);
        }
    }

    /**
     * A schema that resolves but does not declare the configured type is a failed lookup, not a
     * property of the record: counting it per record would report an outage as traffic volume.
     */
    @Test
    void aSchemaWithoutTheConfiguredTypeCountsOneFallbackPerLookup() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of(ProtoMoltSerdeConfig.USE_SCHEMA_ID, 7)), false);

            for (int i = 0; i < 50; i++) {
                assertThat(serializer.serialize("parcels", message("A-6", 1))).isNotNull();
            }

            assertThat(RecordingMetricsListener.EVENTS.stream().filter("fallback"::equals))
                    .as("one failed lookup inside the default backoff window")
                    .hasSize(1);
        }
    }

    private static Message message(String id, int quantity) {
        return DynamicMessage.newBuilder(orderType)
                .setField(orderType.findFieldByName("id"), id)
                .setField(orderType.findFieldByName("quantity"), quantity)
                .build();
    }
}
