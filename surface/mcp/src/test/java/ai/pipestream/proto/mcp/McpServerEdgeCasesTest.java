package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Protocol edge cases around {@link McpServer#handle(JsonNode)} and the stream loop that the
 * happy-path suite does not exercise: default negotiation, id echoing, argument-shape
 * validation, and registry-backed resource traffic through the server.
 */
class McpServerEdgeCasesTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private McpServer server;

    @BeforeEach
    void setUp() {
        server = new McpServer(ActionCatalog.defaults(ActionContext.create()), null,
                "protomolt-test", "0.0-test");
    }

    private ObjectNode request(JsonNode id, String method, JsonNode params) {
        ObjectNode message = mapper.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.set("id", id);
        message.put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        return message;
    }

    @Test
    void initializeWithoutParamsDefaultsToTheLatestVersion() {
        ObjectNode response = server.handle(request(mapper.getNodeFactory().numberNode(1),
                "initialize", null)).orElseThrow();
        JsonNode result = response.get("result");
        assertThat(result.get("protocolVersion").asText()).isEqualTo(McpServer.PROTOCOL_VERSION);
        assertThat(result.get("serverInfo").get("version").asText()).isEqualTo("0.0-test");
    }

    @Test
    void aStringIdIsEchoedBackVerbatim() {
        ObjectNode response = server.handle(request(mapper.getNodeFactory().textNode("abc-1"),
                "ping", null)).orElseThrow();
        assertThat(response.get("id").asText()).isEqualTo("abc-1");
        assertThat(response.has("result")).isTrue();
    }

    @Test
    void aMessageWithAnErrorButNoMethodIsAClientResponse() {
        ObjectNode message = mapper.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", 5);
        message.putObject("error").put("code", -32601).put("message", "whatever");
        assertThat(server.handle(message)).isEmpty();
    }

    @Test
    void methodNotFoundNamesTheMissingMethod() {
        ObjectNode response = server.handle(request(mapper.getNodeFactory().numberNode(2),
                "roots/list", null)).orElseThrow();
        assertThat(response.get("error").get("code").asInt()).isEqualTo(JsonRpc.METHOD_NOT_FOUND);
        assertThat(response.get("error").get("message").asText()).contains("roots/list");
    }

    @Test
    void toolsCallWithNonObjectArgumentsIsInvalidParams() {
        ObjectNode params = mapper.createObjectNode();
        params.put("name", "compile");
        params.putArray("arguments");
        ObjectNode response = server.handle(request(mapper.getNodeFactory().numberNode(3),
                "tools/call", params)).orElseThrow();
        assertThat(response.get("error").get("code").asInt()).isEqualTo(JsonRpc.INVALID_PARAMS);
    }

    @Test
    void toolsCallWithNullArgumentsRunsWithAnEmptyEnvelope() {
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .register(echoAction());
        McpServer withEcho = new McpServer(catalog, null, "protomolt-test", "0.0-test");

        ObjectNode params = mapper.createObjectNode();
        params.put("name", "echo");
        params.putNull("arguments");
        ObjectNode response = withEcho.handle(request(mapper.getNodeFactory().numberNode(4),
                "tools/call", params)).orElseThrow();

        JsonNode result = response.get("result");
        assertThat(result.get("isError").asBoolean()).isFalse();
        assertThat(result.get("structuredContent").get("fieldCount").asInt()).isZero();
    }

    @Test
    void resourcesReadWithoutAUriIsInvalidParams() {
        ObjectNode response = server.handle(request(mapper.getNodeFactory().numberNode(6),
                "resources/read", mapper.createObjectNode())).orElseThrow();
        assertThat(response.get("error").get("code").asInt()).isEqualTo(JsonRpc.INVALID_PARAMS);
    }

    @Test
    void aRegistryBacksResourcesListAndReadThroughTheServer() throws Exception {
        InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();
        store.register("orders-value", """
                syntax = "proto3";
                package shop;
                message Order { string id = 1; }
                """, List.of());
        McpServer withRegistry = new McpServer(ActionCatalog.defaults(ActionContext.create()),
                new RegistryResources(store), "protomolt-test", "0.0-test");

        // initialize advertises the resources capability only when a registry is attached.
        JsonNode capabilities = withRegistry.handle(request(mapper.getNodeFactory().numberNode(1),
                "initialize", null)).orElseThrow().get("result").get("capabilities");
        assertThat(capabilities.has("tools")).isTrue();
        assertThat(capabilities.has("resources")).isTrue();

        JsonNode list = withRegistry.handle(request(mapper.getNodeFactory().numberNode(2),
                "resources/list", null)).orElseThrow().get("result").get("resources");
        assertThat(list.findValuesAsText("uri"))
                .contains("protomolt://registry/subjects", "protomolt://registry/subjects/orders-value");

        ObjectNode readParams = mapper.createObjectNode();
        readParams.put("uri", "protomolt://registry/subjects/orders-value");
        JsonNode contents = withRegistry.handle(request(mapper.getNodeFactory().numberNode(3),
                "resources/read", readParams)).orElseThrow().get("result").get("contents");
        assertThat(contents.size()).isEqualTo(1);
        assertThat(contents.get(0).get("mimeType").asText()).isEqualTo("application/json");
        assertThat(mapper.readTree(contents.get(0).get("text").asText()).get("subject").asText())
                .isEqualTo("orders-value");

        // An unknown URI under the served root is a resource-level error, not a protocol one.
        ObjectNode unknownParams = mapper.createObjectNode();
        unknownParams.put("uri", "protomolt://registry/subjects/nope");
        ObjectNode miss = withRegistry.handle(request(mapper.getNodeFactory().numberNode(4),
                "resources/read", unknownParams)).orElseThrow();
        assertThat(miss.get("error").get("code").asInt()).isEqualTo(JsonRpc.RESOURCE_NOT_FOUND);
    }

    @Test
    void theStreamLoopSkipsBlankLinesAndEndsQuietlyAtEof() throws Exception {
        String session = "\n"
                + "   \n"
                + "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n"
                + "\n";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        server.run(new ByteArrayInputStream(session.getBytes(StandardCharsets.UTF_8)), out);

        String[] lines = out.toString(StandardCharsets.UTF_8).trim().split("\n");
        assertThat(lines).hasSize(1);
        assertThat(mapper.readTree(lines[0]).get("id").asInt()).isEqualTo(1);

        // An immediately-empty stream produces no output at all.
        ByteArrayOutputStream empty = new ByteArrayOutputStream();
        server.run(new ByteArrayInputStream(new byte[0]), empty);
        assertThat(empty.size()).isZero();
    }

    /** Counts the top-level fields of whatever envelope it is handed. */
    private ProtoAction echoAction() {
        return new ProtoAction() {
            @Override
            public String name() {
                return "echo";
            }

            @Override
            public String description() {
                return "Reports how many top-level fields the input carried.";
            }

            @Override
            public ObjectNode inputSchema() {
                ObjectNode schema = mapper.createObjectNode();
                schema.put("type", "object");
                return schema;
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                ObjectNode out = mapper.createObjectNode();
                out.put("fieldCount", input.size());
                return out;
            }
        };
    }
}
