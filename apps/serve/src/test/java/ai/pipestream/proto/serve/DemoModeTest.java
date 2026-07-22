package ai.pipestream.proto.serve;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code --demo} gives every surface instant material: the demo types resolve by name on the
 * verbs, and the demo schema is a subject in the (temp-dir) registry.
 */
class DemoModeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;

    @BeforeAll
    static void start() {
        serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0, null, true));
        http = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + serve.httpPort();
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    private static JsonNode post(String path, String body) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(base + path))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return MAPPER.readTree(response.body());
    }

    @Test
    void demoChainIsStoredAndRunsByName() throws Exception {
        HttpResponse<String> listed = http.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + serve.registryPort()
                                + "/protomolt/chains")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(listed.body()).contains("compile-and-list");

        JsonNode run = post("/grpc-json/ProtoMoltService/RunChain", """
                {"chainName": "compile-and-list",
                 "input": {"sources": {"d.proto":
                   "syntax = \\"proto3\\"; package d; message Demo { int32 n = 1; }"}}}
                """);
        assertThat(run.path("ok").asBoolean()).as(run.toString()).isTrue();
        assertThat(run.path("output").path("types").findValuesAsText("fullName"))
                .contains("d.Demo");
    }

    @Test
    void demoTypesResolveByName() throws Exception {
        JsonNode result = post("/grpc-json/ProtoMoltService/ListTypes", """
                {"filter": "demo.shop"}
                """);
        assertThat(result.path("types").findValuesAsText("fullName"))
                .contains("demo.shop.v1.Order", "demo.shop.v1.Customer", "demo.shop.v1.OrderService");
    }

    @Test
    void validationRulesFireOnTheDemoSchema() throws Exception {
        JsonNode result = post("/grpc-json/ProtoMoltService/ValidateMessage", """
                {"schema": {"type": "demo.shop.v1.Order"},
                 "message": {"id": "not-a-uuid", "items": []}}
                """);
        assertThat(result.path("valid").asBoolean()).isFalse();
        assertThat(result.path("violations").findValuesAsText("field").toString())
                .contains("id");
    }

    @Test
    void indexMappingsRenderFromTheDemoHints() throws Exception {
        JsonNode result = post("/grpc-json/ProtoMoltService/RenderIndexMappings", """
                {"schema": {"type": "demo.shop.v1.Order"}, "engine": "opensearch"}
                """);
        assertThat(result.path("properties").isObject()).isTrue();
    }

    @Test
    void demoSubjectsAreInTheRegistry() throws Exception {
        HttpResponse<String> subjects = http.send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + serve.registryPort() + "/subjects")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(subjects.statusCode()).isEqualTo(200);
        assertThat(subjects.body()).contains(DemoSchemas.SHOP_SUBJECT);
    }

    @Test
    void metadataBagCarriesTheDeclaredOptions() throws Exception {
        JsonNode result = post("/grpc-json/ProtoMoltService/ExtractMetadata", """
                {"schema": {"type": "demo.shop.v1.Customer"}}
                """);
        assertThat(result.path("metadata").path("field.email.sensitivity").asText())
                .isEqualTo("pii");
    }
}
