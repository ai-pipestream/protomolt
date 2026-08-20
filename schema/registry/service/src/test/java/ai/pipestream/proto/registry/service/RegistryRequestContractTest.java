package ai.pipestream.proto.registry.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Request-contract edges of {@link SchemaRegistryServer} that
 * {@link SchemaRegistryServerHttpTest} does not already pin: the 413 body cap,
 * malformed register/lookup bodies, malformed id/version coordinates, the
 * Allow-header method checks, and a relocated health endpoint.
 */
class RegistryRequestContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private static final String DOC_PROTO = """
            syntax = "proto3";
            package t;
            message Doc {
              string id = 1;
            }
            """;

    private InMemorySchemaRegistryStore store;
    private SchemaRegistryServer server;
    private HttpClient client;
    private String base;

    @BeforeEach
    void startServer() {
        store = new InMemorySchemaRegistryStore();
        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), store);
        base = "http://127.0.0.1:" + server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stopServer() {
        client.close();
        server.close();
        store.close();
    }

    // ------------------------------------------------------------------ body cap

    @Test
    void anOversizedBodyIs413() throws Exception {
        SchemaRegistryServerConfig config = new SchemaRegistryServerConfig("127.0.0.1", 0,
                "/health", "/protomolt", 64);
        try (InMemorySchemaRegistryStore tinyStore = new InMemorySchemaRegistryStore();
                SchemaRegistryServer tiny = new SchemaRegistryServer(config, tinyStore)) {
            String tinyBase = "http://127.0.0.1:" + tiny.start();
            String body = "{\"schema\":\"" + "x".repeat(128) + "\"}";
            HttpResponse<String> response = send(HttpRequest.newBuilder(
                            URI.create(tinyBase + "/subjects/t.proto/versions"))
                    .header("Content-Type", CONTENT_TYPE)
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build());
            assertThat(response.statusCode()).isEqualTo(413);
            JsonNode error = JSON.readTree(response.body());
            assertThat(error.path("error_code").asInt()).isEqualTo(413);
            assertThat(error.path("message").asText()).contains("exceeds 64 bytes");
        }
    }

    // ------------------------------------------------------------------ malformed bodies

    @Test
    void registerRejectsANonJsonBody() throws Exception {
        HttpResponse<String> response = post("/subjects/t.proto/versions", "not json {");
        assertThat(response.statusCode()).isEqualTo(422);
        JsonNode error = JSON.readTree(response.body());
        assertThat(error.path("error_code").asInt()).isEqualTo(42201);
        assertThat(error.path("message").asText()).contains("not JSON");
    }

    @Test
    void registerRejectsAnEmptySchema() throws Exception {
        for (String body : new String[]{
                "{\"schemaType\":\"PROTOBUF\"}",
                "{\"schemaType\":\"PROTOBUF\",\"schema\":\"   \"}"}) {
            HttpResponse<String> response = post("/subjects/t.proto/versions", body);
            assertThat(response.statusCode()).as(body).isEqualTo(422);
            JsonNode error = JSON.readTree(response.body());
            assertThat(error.path("error_code").asInt()).isEqualTo(42201);
            assertThat(error.path("message").asText()).contains("empty schema");
        }
    }

    @Test
    void lookupByContentRejectsANonJsonBody() throws Exception {
        registerOk("t.proto", DOC_PROTO);
        HttpResponse<String> response = post("/subjects/t.proto", "not json {");
        assertThat(response.statusCode()).isEqualTo(422);
        assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(42201);
    }

    // ------------------------------------------------------------------ malformed coordinates

    @Test
    void aNonNumericSchemaIdIs40403() throws Exception {
        HttpResponse<String> response = get("/schemas/ids/abc");
        assertThat(response.statusCode()).isEqualTo(404);
        JsonNode error = JSON.readTree(response.body());
        assertThat(error.path("error_code").asInt()).isEqualTo(40403);
        assertThat(error.path("message").asText()).contains("abc");
    }

    @Test
    void outOfRangeVersionsAre40402() throws Exception {
        registerOk("t.proto", DOC_PROTO);
        for (String version : new String[]{"0", "-1", "2"}) {
            HttpResponse<String> response = get("/subjects/t.proto/versions/" + version);
            assertThat(response.statusCode()).as(version).isEqualTo(404);
            assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(40402);
        }
    }

    // ------------------------------------------------------------------ method checks

    @Test
    void wrongMethodsAre405WithTheAllowHeader() throws Exception {
        assertAllow(send(HttpRequest.newBuilder(URI.create(base + "/config")).DELETE().build()),
                "GET, PUT");
        assertAllow(send(HttpRequest.newBuilder(URI.create(base + "/schemas/ids/1"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build()),
                "GET");
        assertAllow(send(HttpRequest.newBuilder(URI.create(base + "/health"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build()),
                "GET");
    }

    @Test
    void actionAndWorkflowRoutesEnforceTheirVerbs() throws Exception {
        try (InMemorySchemaRegistryStore actionStore = new InMemorySchemaRegistryStore();
                SchemaRegistryServer actionServer = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        actionStore, ActionCatalog.defaults(ActionContext.create()))) {
            String actionBase = "http://127.0.0.1:" + actionServer.start();
            assertAllow(send(HttpRequest.newBuilder(URI.create(actionBase + "/protomolt/actions"))
                            .POST(HttpRequest.BodyPublishers.noBody()).build()),
                    "GET");
            assertAllow(send(HttpRequest.newBuilder(
                            URI.create(actionBase + "/protomolt/actions/compile")).GET().build()),
                    "POST");
        }
        assertAllow(send(HttpRequest.newBuilder(URI.create(base + "/protomolt/workflows"))
                        .DELETE().build()),
                "GET");
    }

    // ------------------------------------------------------------------ relocated health

    @Test
    void aCustomHealthPathMovesTheLivenessEndpoint() throws Exception {
        SchemaRegistryServerConfig config = new SchemaRegistryServerConfig("127.0.0.1", 0,
                "livez", "/protomolt", SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES);
        try (InMemorySchemaRegistryStore customStore = new InMemorySchemaRegistryStore();
                SchemaRegistryServer custom = new SchemaRegistryServer(config, customStore)) {
            String customBase = "http://127.0.0.1:" + custom.start();

            HttpResponse<String> live = get(customBase + "/livez");
            assertThat(live.statusCode()).isEqualTo(200);
            assertThat(JSON.readTree(live.body()).path("status").asText()).isEqualTo("UP");

            // The conventional path is just another unknown route now.
            HttpResponse<String> old = get(customBase + "/health");
            assertThat(old.statusCode()).isEqualTo(404);
            assertThat(JSON.readTree(old.body()).path("error_code").asInt()).isEqualTo(404);
        }
    }

    // ------------------------------------------------------------------ helpers

    private void registerOk(String subject, String schema) throws Exception {
        HttpResponse<String> response = post("/subjects/" + subject + "/versions",
                JSON.createObjectNode()
                        .put("schema", schema)
                        .put("schemaType", "PROTOBUF")
                        .toString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    }

    private void assertAllow(HttpResponse<String> response, String allow) {
        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.headers().firstValue("Allow")).contains(allow);
    }

    private HttpResponse<String> get(String pathOrUrl) throws Exception {
        String url = pathOrUrl.startsWith("http") ? pathOrUrl : base + pathOrUrl;
        return send(HttpRequest.newBuilder(URI.create(url)).GET().build());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(base + path))
                .header("Content-Type", CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build());
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
