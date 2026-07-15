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
 * The MCP streamable HTTP transport on the one-process server: an MCP client needs only
 * {@code http://host:port/mcp} — no local install, no stdio process.
 */
class McpHttpTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String endpoint;

    @BeforeAll
    static void start() {
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0));
        http = HttpClient.newHttpClient();
        endpoint = "http://127.0.0.1:" + serve.httpPort() + "/mcp";
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    private static HttpResponse<String> post(String body, String... headers) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(endpoint))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void initializeNegotiatesTheProtocol() throws Exception {
        HttpResponse<String> response = post("""
                {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                  "protocolVersion":"2025-06-18","capabilities":{},
                  "clientInfo":{"name":"test","version":"0"}}}
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode result = MAPPER.readTree(response.body()).path("result");
        assertThat(result.path("protocolVersion").asText()).isEqualTo("2025-06-18");
        assertThat(result.path("serverInfo").path("name").asText()).isEqualTo("protomolt");
        assertThat(result.path("capabilities").has("tools")).isTrue();
    }

    @Test
    void toolsListServesTheTwentyThreeVerbs() throws Exception {
        HttpResponse<String> response = post("""
                {"jsonrpc":"2.0","id":2,"method":"tools/list"}
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode tools = MAPPER.readTree(response.body()).path("result").path("tools");
        assertThat(tools.size()).isEqualTo(23);
        assertThat(tools.findValuesAsText("name")).contains("reflect", "grpc-invoke", "generate-stubs", "join-messages", "synthesize-shape", "merge-schemas", "check-rules", "run-chain", "check-chain", "infer-schema", "mask-message");
    }

    @Test
    void toolsCallExecutesAnAction() throws Exception {
        HttpResponse<String> response = post("""
                {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                  "name":"compile","arguments":{"sources":{"t.proto":
                    "syntax = \\"proto3\\"; message T { int32 n = 1; }"}}}}
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode result = MAPPER.readTree(response.body()).path("result");
        assertThat(result.path("isError").asBoolean()).isFalse();
        assertThat(result.path("structuredContent").path("ok").asBoolean()).isTrue();
    }

    @Test
    void notificationsAreAcceptedWithNoBody() throws Exception {
        HttpResponse<String> response = post("""
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """);
        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.body()).isEmpty();
    }

    @Test
    void batchesAnswerInKind() throws Exception {
        HttpResponse<String> response = post("""
                [{"jsonrpc":"2.0","id":10,"method":"ping"},
                 {"jsonrpc":"2.0","method":"notifications/initialized"},
                 {"jsonrpc":"2.0","id":11,"method":"ping"}]
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = MAPPER.readTree(response.body());
        assertThat(body.isArray()).isTrue();
        assertThat(body.size()).isEqualTo(2);
    }

    @Test
    void malformedJsonIsAParseError() throws Exception {
        HttpResponse<String> response = post("{not json");
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(MAPPER.readTree(response.body()).path("error").path("code").asInt())
                .isEqualTo(-32700);
    }

    @Test
    void getIsNotSupported() throws Exception {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(endpoint)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.headers().firstValue("allow").orElse("")).isEqualTo("POST");
    }

    @Test
    void foreignBrowserOriginsAreRefused() throws Exception {
        HttpResponse<String> evil = post("""
                {"jsonrpc":"2.0","id":4,"method":"ping"}
                """, "origin", "https://evil.example");
        assertThat(evil.statusCode()).isEqualTo(403);

        HttpResponse<String> local = post("""
                {"jsonrpc":"2.0","id":5,"method":"ping"}
                """, "origin", "http://localhost:3000");
        assertThat(local.statusCode()).isEqualTo(200);
    }
}
