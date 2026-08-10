package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the server exactly as an MCP client does: newline-delimited JSON-RPC over streams,
 * a full initialize handshake followed by tool and resource traffic in one session.
 */
class StdioRoundTripTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void fullSessionOverStreams() throws Exception {
        InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();
        store.register("orders-value", """
                syntax = "proto3";
                package shop;
                message Order { string id = 1; }
                """, List.of());
        McpServer server = new McpServer(ActionCatalog.defaults(ActionContext.create()),
                new RegistryResources(store), "protomolt", "test");

        String session = String.join("\n",
                "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"test\",\"version\":\"0\"}}}",
                "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}",
                "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"resources/list\"}",
                "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"resources/read\",\"params\":{\"uri\":\"protomolt://registry/subjects/orders-value\"}}",
                "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\",\"params\":{\"name\":\"list-types\",\"arguments\":{\"schema\":{\"sources\":{\"o.proto\":\"syntax = \\\"proto3\\\"; package shop; message Order { string id = 1; }\"}}}}}",
                "not even json",
                "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"ping\"}") + "\n";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        server.run(new ByteArrayInputStream(session.getBytes(StandardCharsets.UTF_8)), out);

        String[] lines = out.toString(StandardCharsets.UTF_8).trim().split("\n");
        // 7 non-notification inputs produce 7 responses (the parse error included).
        assertThat(lines).hasSize(7);

        List<JsonNode> responses = Arrays.stream(lines).map(this::read).toList();
        JsonNode init = byId(responses, 1);
        assertThat(init.get("result").get("protocolVersion").asText()).isEqualTo("2025-06-18");
        assertThat(init.get("result").get("capabilities").has("resources")).isTrue();

        JsonNode tools = byId(responses, 2);
        assertThat(tools.get("result").get("tools").size()).isEqualTo(17);

        JsonNode resources = byId(responses, 3);
        assertThat(resources.get("result").get("resources").findValuesAsText("uri"))
                .contains("protomolt://registry/subjects/orders-value");

        JsonNode read = byId(responses, 4);
        String text = read.get("result").get("contents").get(0).get("text").asText();
        assertThat(mapper.readTree(text).get("latest").get("schemaText").asText())
                .contains("message Order");

        JsonNode call = byId(responses, 5);
        assertThat(call.get("result").get("isError").asBoolean()).isFalse();
        assertThat(call.get("result").get("structuredContent").toString()).contains("shop.Order");

        JsonNode parseError = responses.stream()
                .filter(node -> node.path("error").path("code").asInt() == -32700)
                .findFirst().orElseThrow();
        assertThat(parseError.get("error").get("code").asInt()).isEqualTo(-32700);

        // The session survives the malformed line: ping still answers.
        JsonNode ping = byId(responses, 6);
        assertThat(ping.get("id").asInt()).isEqualTo(6);
        assertThat(ping.has("result")).isTrue();
    }

    @Test
    void concurrentToolResponsesRemainIndividuallyFramed() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create()).register(new ProtoAction() {
            @Override
            public String name() {
                return "rendezvous";
            }

            @Override
            public String description() {
                return "Waits for a peer call before returning.";
            }

            @Override
            public ObjectNode inputSchema() {
                return mapper.createObjectNode().put("type", "object");
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                return context.objectMapper().createObjectNode().put("ok", true);
            }
        });
        McpServer server = new McpServer(catalog, null, "protomolt-test", "0.0-test");
        String wire = String.join("\n",
                "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\"}}",
                "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\",\"params\":{\"name\":\"rendezvous\"}}",
                "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"tools/call\",\"params\":{\"name\":\"rendezvous\"}}") + "\n";

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        server.run(new ByteArrayInputStream(wire.getBytes(StandardCharsets.UTF_8)), output);

        String[] lines = output.toString(StandardCharsets.UTF_8).trim().split("\n");
        assertThat(lines).hasSize(3);
        List<JsonNode> responses = Arrays.stream(lines).map(this::read).toList();
        assertThat(byId(responses, 7).path("result").path("isError").asBoolean()).isFalse();
        assertThat(byId(responses, 8).path("result").path("isError").asBoolean()).isFalse();
    }

    private JsonNode read(String line) {
        try {
            return mapper.readTree(line);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static JsonNode byId(List<JsonNode> responses, int id) {
        return responses.stream().filter(node -> node.path("id").asInt() == id)
                .findFirst().orElseThrow();
    }
}
