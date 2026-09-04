package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.mcp.McpServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.sun.net.httpserver.HttpServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.Duration;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
    private static Path meshState;

    @BeforeAll
    static void start() {
        try {
            meshState = java.nio.file.Files.createTempDirectory("protomolt-mcp-mesh-");
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, null, false, null,
                null, java.util.List.of(), null, null, null, null, null,
                new ProtoMoltServe.MeshClusterOptions("protomolt", "Test mesh", "test",
                        Instant.parse("2026-08-14T00:00:00Z"), meshState,
                        Duration.ofSeconds(30), Duration.ofSeconds(30), 3, 100)));
        http = HttpClient.newHttpClient();
        endpoint = "http://127.0.0.1:" + serve.httpPort() + "/mcp";
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    private static HttpResponse<String> post(String body, String... headers) throws Exception {
        return send(http, endpoint, body, headers);
    }

    private static HttpResponse<String> send(HttpClient client, String url, String body,
                                             String... headers) throws Exception {
        return client.send(request(url, body, headers), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpRequest request(String url, String body, String... headers) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return request.build();
    }

    private static HttpSession initializeSession() throws Exception {
        HttpResponse<String> response = post("""
                {"jsonrpc":"2.0","id":100,"method":"initialize","params":{
                  "protocolVersion":"2025-06-18","capabilities":{},
                  "clientInfo":{"name":"test","version":"0"}}}
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        String id = response.headers().firstValue("Mcp-Session-Id").orElseThrow();
        String version = MAPPER.readTree(response.body()).path("result")
                .path("protocolVersion").asText();
        return new HttpSession(id, version);
    }

    private static HttpResponse<String> post(HttpSession session, String body) throws Exception {
        return post(body, "Mcp-Session-Id", session.id(),
                "MCP-Protocol-Version", session.version());
    }

    private record HttpSession(String id, String version) {
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
        assertThat(result.path("capabilities").has("resources")).isTrue();
        assertThat(result.path("_meta").path("ai.protomolt/toolCount").asInt())
                .isEqualTo(63);
        assertThat(result.path("_meta").path("ai.protomolt/workspace").asText())
                .isEqualTo("protomolt://workspace");
        assertThat(result.path("instructions").asText())
                .contains("service-register", "service-inspect", "reflect", "grpc-invoke",
                        "generate-stubs");
    }

    @Test
    void malformedInitializeParamsReturnAJsonRpcError() throws Exception {
        HttpResponse<String> response = post("""
                {"jsonrpc":"2.0","id":6,"method":"initialize","params":[]}
                """);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(response.body()).path("error").path("code").asInt())
                .isEqualTo(-32602);
        assertThat(response.headers().firstValue("Mcp-Session-Id")).isEmpty();
    }

    @Test
    void toolsListServesTheFullCatalog() throws Exception {
        HttpSession session = initializeSession();
        assertThat(post(session, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """).statusCode()).isEqualTo(202);
        HttpResponse<String> response = post(session, """
                {"jsonrpc":"2.0","id":2,"method":"tools/list"}
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode result = MAPPER.readTree(response.body()).path("result");
        JsonNode tools = result.path("tools");
        assertThat(tools.size()).isEqualTo(63);
        assertThat(result.path("_meta").path("ai.protomolt/toolCount").asInt())
                .isEqualTo(tools.size());
        assertThat(tools.findValuesAsText("name")).contains("reflect", "grpc-invoke",
                "generate-stubs", "join-messages", "synthesize-shape", "merge-schemas",
                "check-rules", "run-workflow", "check-workflow", "infer-schema", "mask-message",
                "submit-workflow", "get-job", "list-jobs", "complete-step", "service-register",
                "service-list", "service-inspect", "service-refresh", "service-invoke", "suggest-mappings",
                "compile-workflow", "record-workflow-run", "replay-workflow", "promote-workflow",
                "delegation-worker-register", "delegation-offer", "delegation-watch",
                "delegation-message", "delegation-review", "delegation-transcript",
                "mesh-node-register", "mesh-node-heartbeat", "mesh-processor-register",
                "mesh-capacity-update", "mesh-readiness-update", "mesh-snapshot",
                "mesh-sweep");
    }

    @Test
    void workspaceBootstrapIsReadableOverStreamableHttp() throws Exception {
        HttpSession session = initializeSession();
        assertThat(post(session, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """).statusCode()).isEqualTo(202);

        HttpResponse<String> listed = post(session, """
                {"jsonrpc":"2.0","id":21,"method":"resources/list","params":{}}
                """);
        assertThat(MAPPER.readTree(listed.body()).path("result").path("resources")
                .findValuesAsText("uri")).contains("protomolt://workspace");

        HttpResponse<String> read = post(session, """
                {"jsonrpc":"2.0","id":22,"method":"resources/read","params":
                 {"uri":"protomolt://workspace"}}
                """);
        String text = MAPPER.readTree(read.body()).path("result").path("contents")
                .get(0).path("text").asText();
        JsonNode workspace = MAPPER.readTree(text);
        assertThat(workspace.path("toolCatalog").path("count").asInt()).isEqualTo(63);
        assertThat(workspace.path("toolCatalog").path("names"))
                .anySatisfy(name -> assertThat(name.asText()).isEqualTo("service-register"));

        HttpResponse<String> templates = post(session, """
                {"jsonrpc":"2.0","id":23,"method":"resources/templates/list","params":{}}
                """);
        assertThat(templates.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(templates.body()).path("result")
                .path("resourceTemplates").findValuesAsText("name"))
                .containsExactly("delegation-transcript");
    }

    @Test
    void toolsCallExecutesAnAction() throws Exception {
        HttpSession session = initializeSession();
        HttpResponse<String> initialized = post(session, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """);
        assertThat(initialized.statusCode()).isEqualTo(202);
        HttpResponse<String> response = post(session, """
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
    void meshSnapshotIsAvailableOverMcpWhenConfigured() throws Exception {
        HttpSession session = initializeSession();
        assertThat(post(session, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """).statusCode()).isEqualTo(202);

        HttpResponse<String> response = post(session, """
                {"jsonrpc":"2.0","id":31,"method":"tools/call","params":{
                  "name":"mesh-snapshot","arguments":{}}}
                """);

        JsonNode content = MAPPER.readTree(response.body()).path("result")
                .path("structuredContent");
        // GetSnapshotResponse declares the snapshot and nothing else, so the reading
        // itself is the whole answer.
        assertThat(content.path("snapshot").path("cluster").path("clusterId").asText())
                .isEqualTo("protomolt");
    }

    @Test
    void notificationsAreAcceptedWithNoBody() throws Exception {
        HttpSession session = initializeSession();
        HttpResponse<String> response = post(session, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """);
        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.body()).isEmpty();
    }

    @Test
    void concurrentCancellationSuppressesTheHttpToolResponse() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create()).register(new ProtoAction() {
            @Override
            public String name() {
                return "wait-http";
            }

            @Override
            public String description() {
                return "Waits until cancelled.";
            }

            @Override
            public Descriptor requestType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public Descriptor responseType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public com.google.protobuf.Message execute(
                    com.google.protobuf.Message input, ActionContext context) {
                started.countDown();
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return Struct.newBuilder().putFields("completed",
                        com.google.protobuf.Value.newBuilder().setBoolValue(true).build())
                        .build();
            }
        });
        McpServer mcpServer = new McpServer(catalog, null, "test", "0");
        McpHttpHandler handler = new McpHttpHandler(mcpServer);
        HttpServer local = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        local.setExecutor(executor);
        local.createContext("/mcp", handler);
        local.start();
        try {
            HttpClient client = HttpClient.newHttpClient();
            String url = "http://127.0.0.1:" + local.getAddress().getPort() + "/mcp";
            HttpResponse<String> init = send(client, url, """
                    {"jsonrpc":"2.0","id":1,"method":"initialize","params":
                     {"protocolVersion":"2025-06-18","capabilities":{},
                      "clientInfo":{"name":"cancel-test","version":"0"}}}
                    """);
            String session = init.headers().firstValue("Mcp-Session-Id").orElseThrow();
            String version = MAPPER.readTree(init.body()).path("result")
                    .path("protocolVersion").asText();
            String[] sessionHeaders = {"Mcp-Session-Id", session,
                    "MCP-Protocol-Version", version};
            assertThat(send(client, url,
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                    sessionHeaders).statusCode()).isEqualTo(202);
            var tool = client.sendAsync(request(url, """
                    {"jsonrpc":"2.0","id":7,"method":"tools/call","params":
                     {"name":"wait-http","arguments":{}}}
                    """, sessionHeaders), HttpResponse.BodyHandlers.ofString());
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            HttpResponse<String> cancelled = send(client, url, """
                    {"jsonrpc":"2.0","method":"notifications/cancelled",
                     "params":{"requestId":7}}
                    """, sessionHeaders);
            assertThat(cancelled.statusCode()).isEqualTo(202);
            assertThat(tool.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(202);
        } finally {
            handler.close();
            local.stop(0);
            executor.shutdownNow();
        }
    }

    @Test
    void lifecycleRequiresSessionHeadersAndInitializedNotification() throws Exception {
        HttpSession session = initializeSession();
        HttpResponse<String> missing = post("""
                {"jsonrpc":"2.0","id":20,"method":"ping"}
                """);
        assertThat(missing.statusCode()).isEqualTo(404);

        HttpResponse<String> beforeInitialized = post(session, """
                {"jsonrpc":"2.0","id":21,"method":"ping"}
                """);
        assertThat(MAPPER.readTree(beforeInitialized.body()).path("error").path("code").asInt())
                .isEqualTo(-32600);

        HttpResponse<String> initialized = post(session, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """);
        assertThat(initialized.statusCode()).isEqualTo(202);
        HttpResponse<String> ping = post(session, """
                {"jsonrpc":"2.0","id":22,"method":"ping"}
                """);
        assertThat(ping.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(ping.body()).path("result").isObject()).isTrue();

        HttpResponse<String> wrongVersion = post("""
                {"jsonrpc":"2.0","id":23,"method":"ping"}
                """, "Mcp-Session-Id", session.id(),
                "MCP-Protocol-Version", "2024-11-05");
        assertThat(wrongVersion.statusCode()).isEqualTo(400);
    }

    @Test
    void resourcesListRejectsACursorWhenNoRegistryIsMounted() throws Exception {
        HttpSession session = initializeSession();
        assertThat(post(session, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """).statusCode()).isEqualTo(202);
        HttpResponse<String> response = post(session, """
                {"jsonrpc":"2.0","id":22,"method":"resources/list",
                 "params":{"cursor":"not-a-cursor"}}
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(response.body()).path("error").path("code").asInt())
                .isEqualTo(-32602);
    }

    @Test
    void streamableHttpRequiresJsonContentAndBothAcceptedMediaTypes() throws Exception {
        HttpRequest wrongContent = HttpRequest.newBuilder(URI.create(endpoint))
                .header("content-type", "text/plain")
                .header("accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        assertThat(http.send(wrongContent, HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(415);

        HttpRequest incompleteAccept = HttpRequest.newBuilder(URI.create(endpoint))
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        assertThat(http.send(incompleteAccept, HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(406);
    }

    @Test
    void deleteRequiresTheNegotiatedVersionAndClosesTheSession() throws Exception {
        HttpSession session = initializeSession();
        HttpRequest wrongVersion = HttpRequest.newBuilder(URI.create(endpoint))
                .header("Mcp-Session-Id", session.id())
                .header("MCP-Protocol-Version", "2024-11-05")
                .DELETE()
                .build();
        assertThat(http.send(wrongVersion, HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(400);

        HttpRequest delete = HttpRequest.newBuilder(URI.create(endpoint))
                .header("Mcp-Session-Id", session.id())
                .header("MCP-Protocol-Version", session.version())
                .DELETE()
                .build();
        assertThat(http.send(delete, HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(204);
        assertThat(post(session, """
                {"jsonrpc":"2.0","id":24,"method":"ping"}
                """).statusCode()).isEqualTo(404);
    }

    @Test
    void batchesAnswerInKind() throws Exception {
        HttpSession session = initializeSession();
        HttpResponse<String> initialized = post(session, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """);
        assertThat(initialized.statusCode()).isEqualTo(202);
        HttpResponse<String> response = post(session, """
                [{"jsonrpc":"2.0","id":10,"method":"ping"},
                 {"jsonrpc":"2.0","method":"notifications/initialized"},
                 {"jsonrpc":"2.0","id":11,"method":"ping"}]
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(response.body())).hasSize(2);
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
        assertThat(response.headers().firstValue("allow").orElse("")).isEqualTo("POST, DELETE");
    }

    @Test
    void foreignBrowserOriginsAreRefused() throws Exception {
        HttpResponse<String> evil = post("""
                {"jsonrpc":"2.0","id":4,"method":"ping"}
                """, "origin", "https://evil.example");
        assertThat(evil.statusCode()).isEqualTo(403);

        HttpResponse<String> local = post("""
                {"jsonrpc":"2.0","id":5,"method":"initialize","params":
                 {"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}
                """, "origin", "http://localhost:3000");
        assertThat(local.statusCode()).isEqualTo(200);
    }
}
