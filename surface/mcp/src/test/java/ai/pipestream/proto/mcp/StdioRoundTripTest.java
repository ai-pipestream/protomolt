package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

        JsonNode init = mapper.readTree(lines[0]);
        assertThat(init.get("result").get("protocolVersion").asText()).isEqualTo("2025-06-18");
        assertThat(init.get("result").get("capabilities").has("resources")).isTrue();

        JsonNode tools = mapper.readTree(lines[1]);
        assertThat(tools.get("result").get("tools").size()).isEqualTo(16);

        JsonNode resources = mapper.readTree(lines[2]);
        assertThat(resources.get("result").get("resources").findValuesAsText("uri"))
                .contains("protomolt://registry/subjects/orders-value");

        JsonNode read = mapper.readTree(lines[3]);
        String text = read.get("result").get("contents").get(0).get("text").asText();
        assertThat(mapper.readTree(text).get("latest").get("schemaText").asText())
                .contains("message Order");

        JsonNode call = mapper.readTree(lines[4]);
        assertThat(call.get("result").get("isError").asBoolean()).isFalse();
        assertThat(call.get("result").get("structuredContent").toString()).contains("shop.Order");

        JsonNode parseError = mapper.readTree(lines[5]);
        assertThat(parseError.get("error").get("code").asInt()).isEqualTo(-32700);

        // The session survives the malformed line: ping still answers.
        JsonNode ping = mapper.readTree(lines[6]);
        assertThat(ping.get("id").asInt()).isEqualTo(6);
        assertThat(ping.has("result")).isTrue();
    }
}
