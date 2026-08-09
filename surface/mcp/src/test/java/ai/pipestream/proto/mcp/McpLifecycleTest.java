package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ProtoAction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Focused MCP lifecycle, cursor, capability, and revision fixtures. */
class McpLifecycleTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void stdioSessionRequiresInitializeAndInitializedBeforeUse() {
        McpServer.Session session = server(null).openSession();

        assertThat(error(session.handle(request(1, "ping", null))))
                .isEqualTo(JsonRpc.INVALID_REQUEST);
        JsonNode initialize = session.handle(request(2, "initialize", params(
                "protocolVersion", "2025-06-18"))).orElseThrow();
        assertThat(initialize.path("result").path("protocolVersion").asText())
                .isEqualTo("2025-06-18");
        assertThat(session.state()).isEqualTo(McpServer.Session.State.INITIALIZED);
        assertThat(error(session.handle(request(3, "ping", null))))
                .isEqualTo(JsonRpc.INVALID_REQUEST);

        session.handle(notification("notifications/initialized", null));
        assertThat(session.state()).isEqualTo(McpServer.Session.State.READY);
        assertThat(session.handle(request(4, "ping", null))).isPresent();
    }

    @Test
    void shutdownIsNotAnMcpMessage() {
        McpServer.Session session = readySession();
        assertThat(session.handle(request(5, "shutdown", null)).orElseThrow()
                .path("error").path("code").asInt())
                .isEqualTo(JsonRpc.METHOD_NOT_FOUND);
    }

    @Test
    void initializeNegotiatesAllSupportedRevisionFixturesAndFallsBackForUnknown() {
        for (String revision : new String[]{"2025-06-18", "2025-03-26", "2024-11-05"}) {
            McpServer.Session session = server(null).openSession();
            JsonNode result = session.handle(request(1, "initialize",
                    params("protocolVersion", revision))).orElseThrow().get("result");
            assertThat(result.path("protocolVersion").asText()).isEqualTo(revision);
        }

        McpServer.Session unknown = server(null).openSession();
        JsonNode result = unknown.handle(request(1, "initialize",
                        params("protocolVersion", "2099-01-01"))).orElseThrow().get("result");
        assertThat(result.path("protocolVersion").asText()).isEqualTo(McpServer.PROTOCOL_VERSION);
    }

    @Test
    void malformedInitializeParamsReturnInvalidParamsAndLeaveTheSessionNew() {
        McpServer.Session session = server(null).openSession();
        ObjectNode initialize = request(1, "initialize", null);
        initialize.put("params", "not-an-object");

        assertThat(error(session.handle(initialize))).isEqualTo(JsonRpc.INVALID_PARAMS);
        assertThat(session.state()).isEqualTo(McpServer.Session.State.NEW);
    }

    @Test
    void initializeAdvertisesNegotiatedToolAndResourceCapabilities() {
        JsonNode capabilities = readySessionWithResources().handle(
                request(1, "initialize", params("protocolVersion", "2025-06-18")))
                .orElseThrow().path("result").path("capabilities");

        assertThat(capabilities.path("tools").path("listChanged").asBoolean()).isFalse();
        assertThat(capabilities.path("resources").path("subscribe").asBoolean()).isFalse();
        assertThat(capabilities.path("resources").path("listChanged").asBoolean()).isFalse();
    }

    @Test
    void resourcesListPaginatesAndRejectsInvalidCursors() {
        McpServer server = server(manyResources());

        JsonNode first = server.handle(request(1, "resources/list", null)).orElseThrow().get("result");
        assertThat(first.path("resources")).hasSize(100);
        assertThat(first.path("nextCursor").asText()).isEqualTo("100");
        JsonNode second = server.handle(request(2, "resources/list",
                        params("cursor", first.path("nextCursor").asText())))
                .orElseThrow().get("result");
        assertThat(second.path("resources")).hasSize(100);
        assertThat(second.path("nextCursor").asText()).isEqualTo("200");
        JsonNode last = server.handle(request(3, "resources/list", params("cursor", "200")))
                .orElseThrow().get("result");
        assertThat(last.path("resources")).hasSize(5);
        assertThat(last.has("nextCursor")).isFalse();

        assertThat(error(server.handle(request(4, "resources/list", params("cursor", "bad")))))
                .isEqualTo(JsonRpc.INVALID_PARAMS);
        assertThat(error(server.handle(request(5, "resources/list", params("cursor", "999")))))
                .isEqualTo(JsonRpc.INVALID_PARAMS);
    }

    @Test
    void absentAndCompositeResourceCollectionsRejectForeignCursors() {
        assertThat(error(server(null).handle(
                request(1, "resources/list", params("cursor", "1")))))
                .isEqualTo(JsonRpc.INVALID_PARAMS);

        McpResources composite = CompositeResources.of(emptyResources(), emptyResources());
        assertThat(error(server(composite).handle(
                request(2, "resources/list", params("cursor", "2:1")))))
                .isEqualTo(JsonRpc.INVALID_PARAMS);
    }

    @Test
    void aClosedSessionRejectsAsynchronousSubmission() {
        McpServer.Session session = readySession();
        session.close();

        assertThatThrownBy(() -> session.submit(request(9, "tools/call",
                params("name", "compile"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("session state");
    }

    @Test
    void aSessionBoundsConcurrentToolWork() {
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .register(new ProtoAction() {
                    @Override
                    public String name() {
                        return "wait-for-capacity";
                    }

                    @Override
                    public String description() {
                        return "Waits until the session closes.";
                    }

                    @Override
                    public ObjectNode inputSchema() {
                        return mapper.createObjectNode().put("type", "object");
                    }

                    @Override
                    public ObjectNode execute(ObjectNode input, ActionContext context) {
                        try {
                            Thread.sleep(60_000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return context.objectMapper().createObjectNode();
                    }
                });
        McpServer.Session session = new McpServer(catalog, null, "test", "0").openSession();
        session.handle(request(1, "initialize", params("protocolVersion", "2025-06-18")));
        session.handle(notification("notifications/initialized", null));
        try {
            for (int id = 1; id <= 64; id++) {
                session.submit(request(id, "tools/call", params("name", "wait-for-capacity")));
            }
            assertThatThrownBy(() -> session.submit(
                    request(65, "tools/call", params("name", "wait-for-capacity"))))
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class)
                    .hasMessageContaining("in-flight tool limit");
        } finally {
            session.close();
        }
    }

    @Test
    void stdioCancellationSuppressesAnInFlightToolResult() throws Exception {
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .register(new ProtoAction() {
                    @Override
                    public String name() {
                        return "wait";
                    }

                    @Override
                    public String description() {
                        return "Waits until cancelled.";
                    }

                    @Override
                    public ObjectNode inputSchema() {
                        return mapper.createObjectNode().put("type", "object");
                    }

                    @Override
                    public ObjectNode execute(ObjectNode input, ActionContext context) {
                        try {
                            Thread.sleep(60_000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return context.objectMapper().createObjectNode().put("completed", true);
                    }
                });
        McpServer server = new McpServer(catalog, null, "protomolt-test", "0.0-test");
        String wire = String.join("\n",
                "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\"}}",
                "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"wait\"}}",
                "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":3}}",
                "\n");

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        server.run(new ByteArrayInputStream(wire.getBytes(StandardCharsets.UTF_8)), output);

        assertThat(output.toString(StandardCharsets.UTF_8)).doesNotContain("\"id\":3");
    }

    private McpServer.Session readySession() {
        McpServer.Session session = server(null).openSession();
        session.handle(request(1, "initialize", params("protocolVersion", "2025-06-18")));
        session.handle(notification("notifications/initialized", null));
        return session;
    }

    private McpServer.Session readySessionWithResources() {
        McpServer.Session session = server(emptyResources()).openSession();
        return session;
    }

    private McpResources manyResources() {
        return new ResourceList() {
            @Override
            public ArrayNode list(ObjectMapper mapper) {
                ArrayNode resources = mapper.createArrayNode();
                for (int i = 0; i < 205; i++) {
                    resources.addObject().put("uri", "protomolt://test/" + i);
                }
                return resources;
            }
        };
    }

    private McpResources emptyResources() {
        return new ResourceList() {
            @Override
            public ArrayNode list(ObjectMapper mapper) {
                return mapper.createArrayNode();
            }
        };
    }

    private McpServer server(McpResources resources) {
        return new McpServer(ActionCatalog.defaults(ActionContext.create()), resources,
                "protomolt-test", "0.0-test");
    }

    private ObjectNode request(int id, String method, ObjectNode params) {
        ObjectNode message = mapper.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", id);
        message.put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        return message;
    }

    private ObjectNode notification(String method, ObjectNode params) {
        ObjectNode message = mapper.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        return message;
    }

    private ObjectNode params(String field, String value) {
        return mapper.createObjectNode().put(field, value);
    }

    private int error(Optional<ObjectNode> response) {
        return response.orElseThrow().path("error").path("code").asInt();
    }

    @FunctionalInterface
    private interface ResourceList extends McpResources {
        @Override
        default Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
            return Optional.empty();
        }
    }
}
