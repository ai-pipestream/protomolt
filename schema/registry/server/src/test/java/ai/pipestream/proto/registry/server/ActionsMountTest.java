package ai.pipestream.proto.registry.server;

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

import static org.assertj.core.api.Assertions.assertThat;

class ActionsMountTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private InMemorySchemaRegistryStore store;
    private SchemaRegistryServer server;
    private HttpClient client;
    private String base;

    @BeforeEach
    void start() {
        store = new InMemorySchemaRegistryStore();
        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withPort(0),
                store,
                ActionCatalog.defaults(ActionContext.create()));
        int port = server.start();
        client = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + port;
    }

    @AfterEach
    void stop() throws Exception {
        server.close();
        store.close();
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void listsActionsWithInputSchemas() throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(URI.create(base + "/protomolt/actions")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode list = JSON.readTree(response.body());
        assertThat(list.isArray()).isTrue();
        assertThat(list).extracting(n -> n.path("name").asText())
                .contains("compile", "validate-message", "diff-schemas", "check-compat",
                        "list-types", "eval-cel");
        assertThat(list.get(0).has("inputSchema")).isTrue();
    }

    @Test
    void executesAnActionOverHttp() throws Exception {
        String input = """
                {"schema": {"sources": {"doc.proto":
                  "syntax = \\"proto3\\"; package t; message Doc { string id = 1; }"}}}
                """;
        HttpResponse<String> response = post("/protomolt/actions/list-types", input);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode result = JSON.readTree(response.body());
        assertThat(result.path("types")).anySatisfy(type ->
                assertThat(type.path("fullName").asText()).isEqualTo("t.Doc"));
    }

    @Test
    void unknownActionIs404WithEnvelope() throws Exception {
        HttpResponse<String> response = post("/protomolt/actions/no-such-verb", "{}");
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(response.body()).path("error").asText())
                .isEqualTo("unknown-action");
    }

    @Test
    void invalidInputIs400WithEnvelope() throws Exception {
        HttpResponse<String> response = post("/protomolt/actions/check-compat", "{}");
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(JSON.readTree(response.body()).path("error").asText())
                .isEqualTo("invalid-input");
    }

    @Test
    void nonObjectBodyIs400() throws Exception {
        HttpResponse<String> response = post("/protomolt/actions/list-types", "[1,2]");
        assertThat(response.statusCode()).isEqualTo(400);
    }

    @Test
    void mountAbsentWithoutCatalog() throws Exception {
        try (var bare = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withPort(0),
                new InMemorySchemaRegistryStore())) {
            int port = bare.start();
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/protomolt/actions"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(404);
        }
    }
}
