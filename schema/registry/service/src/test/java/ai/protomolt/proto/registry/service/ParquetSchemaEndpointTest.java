package ai.protomolt.proto.registry.service;

import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The derived parquet-schema endpoint: message selection within a subject's descriptor set,
 * version pinning, and the 400/404 contract. The descriptor-to-Parquet mapping itself is
 * proven in {@code ProtoParquetSchemas}' module; here only the HTTP shape is asserted.
 */
class ParquetSchemaEndpointTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private static final String SUBJECT = "sensors/v1/reading.proto";

    private static final String READING_PROTO = """
            syntax = "proto3";
            package sensors.v1;
            import "google/protobuf/timestamp.proto";
            enum Unit {
              UNIT_UNSPECIFIED = 0;
              CELSIUS = 1;
            }
            message Reading {
              string id = 1;
              double value = 2;
              Unit unit = 3;
              google.protobuf.Timestamp at = 4;
              repeated string tags = 5;
              optional int32 retries = 6;
            }
            """;

    /** Version 2 adds a column; adding a field passes the default BACKWARD write gate. */
    private static final String READING_PROTO_V2 = """
            syntax = "proto3";
            package sensors.v1;
            import "google/protobuf/timestamp.proto";
            enum Unit {
              UNIT_UNSPECIFIED = 0;
              CELSIUS = 1;
            }
            message Reading {
              string id = 1;
              double value = 2;
              Unit unit = 3;
              google.protobuf.Timestamp at = 4;
              repeated string tags = 5;
              optional int32 retries = 6;
              string location = 7;
            }
            """;

    private SchemaRegistryStore store;
    private SchemaRegistryServer server;
    private HttpClient client;
    private URI base;

    @BeforeEach
    void startServer() {
        store = new InMemorySchemaRegistryStore();
        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), store);
        base = URI.create("http://127.0.0.1:" + server.start());
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stopServer() {
        client.close();
        server.close();
        store.close();
    }

    @Test
    void servesTheDerivedParquetSchemaAsText() throws Exception {
        registerOk(SUBJECT, READING_PROTO);

        HttpResponse<String> response = parquetSchema(SUBJECT, "sensors.v1.Reading", null);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type"))
                .contains("text/plain; charset=utf-8");

        String schema = response.body();
        assertThat(schema).contains("message sensors.v1.Reading {");
        // Plain proto3 scalars are required; strings and enums are annotated UTF-8 binary.
        assertThat(schema).contains("required binary id (STRING);");
        assertThat(schema).contains("required double value;");
        assertThat(schema).contains("required binary unit (STRING);");
        // Singular messages track presence, and Timestamp is a microsecond UTC column.
        assertThat(schema).contains("optional int64 at (TIMESTAMP(MICROS,true));");
        // Repeated fields are three-level LIST groups.
        assertThat(schema).contains("optional group tags (LIST) {");
        assertThat(schema).contains("repeated group list {");
        assertThat(schema).contains("required binary element (STRING);");
        // Presence-tracking scalars are optional.
        assertThat(schema).contains("optional int32 retries;");
    }

    @Test
    void missingOrBlankMessageIs400() throws Exception {
        registerOk(SUBJECT, READING_PROTO);

        for (String query : List.of("", "?message=")) {
            HttpResponse<String> response = get(parquetPath(SUBJECT) + query);
            assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
            JsonNode error = JSON.readTree(response.body());
            assertThat(error.path("message").asText()).contains("message");
        }
    }

    @Test
    void unknownSubjectIs40401() throws Exception {
        HttpResponse<String> response = parquetSchema("none.proto", "sensors.v1.Reading", null);
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(40401);
    }

    @Test
    void unknownVersionIs40402AndMalformedVersionIs42202() throws Exception {
        registerOk(SUBJECT, READING_PROTO);

        HttpResponse<String> missing = parquetSchema(SUBJECT, "sensors.v1.Reading", "9");
        assertThat(missing.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(missing.body()).path("error_code").asInt()).isEqualTo(40402);

        HttpResponse<String> malformed = parquetSchema(SUBJECT, "sensors.v1.Reading", "abc");
        assertThat(malformed.statusCode()).isEqualTo(422);
        assertThat(JSON.readTree(malformed.body()).path("error_code").asInt()).isEqualTo(42202);
    }

    @Test
    void unknownMessageIs40403() throws Exception {
        registerOk(SUBJECT, READING_PROTO);

        HttpResponse<String> response = parquetSchema(SUBJECT, "sensors.v1.Nope", null);
        assertThat(response.statusCode()).isEqualTo(404);
        JsonNode error = JSON.readTree(response.body());
        assertThat(error.path("error_code").asInt()).isEqualTo(40403);
        assertThat(error.path("message").asText()).contains("sensors.v1.Nope");
    }

    @Test
    void versionPinsTheServedShape() throws Exception {
        registerOk(SUBJECT, READING_PROTO);
        registerOk(SUBJECT, READING_PROTO_V2);

        // Latest carries the added column...
        HttpResponse<String> latest = parquetSchema(SUBJECT, "sensors.v1.Reading", null);
        assertThat(latest.statusCode()).isEqualTo(200);
        assertThat(latest.body()).contains("required binary location (STRING);");

        // ...while the pinned version keeps the older shape.
        HttpResponse<String> pinned = parquetSchema(SUBJECT, "sensors.v1.Reading", "1");
        assertThat(pinned.statusCode()).isEqualTo(200);
        assertThat(pinned.body()).doesNotContain("location");
        assertThat(pinned.body()).contains("required binary id (STRING);");
    }

    // ---------------------------------------------------------------- helpers

    private HttpResponse<String> parquetSchema(String subject, String message, String version)
            throws Exception {
        String query = "?message=" + URLEncoder.encode(message, StandardCharsets.UTF_8);
        if (version != null) {
            query += "&version=" + version;
        }
        return get(parquetPath(subject) + query);
    }

    private static String parquetPath(String subject) {
        return "/protomolt/subjects/" + encode(subject) + "/parquet-schema";
    }

    private int registerOk(String subject, String schema) throws Exception {
        ObjectNode body = JSON.createObjectNode()
                .put("schema", schema)
                .put("schemaType", "PROTOBUF");
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(base.resolve("/subjects/" + encode(subject) + "/versions"))
                        .header("Content-Type", CONTENT_TYPE)
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return JSON.readTree(response.body()).path("id").asInt();
    }

    private HttpResponse<String> get(String pathAndQuery) throws Exception {
        return client.send(HttpRequest.newBuilder(base.resolve(pathAndQuery)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static String encode(String subject) {
        return URLEncoder.encode(subject, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
