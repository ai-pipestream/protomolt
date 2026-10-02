package ai.protomolt.proto.mcp.transport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * JSON-RPC 2.0 message construction. MCP frames one JSON-RPC message per line over stdio;
 * this class owns the envelope shapes so {@link McpServer} deals only in methods and results.
 */
public final class JsonRpc {

    public static final int PARSE_ERROR = -32700;
    public static final int INVALID_REQUEST = -32600;
    public static final int METHOD_NOT_FOUND = -32601;
    public static final int INVALID_PARAMS = -32602;
    public static final int INTERNAL_ERROR = -32603;
    /** MCP-assigned: resources/read against a URI the server does not serve. */
    public static final int RESOURCE_NOT_FOUND = -32002;

    private JsonRpc() {
    }

    public static ObjectNode result(ObjectMapper mapper, JsonNode id, JsonNode result) {
        ObjectNode response = mapper.createObjectNode();
        response.put("jsonrpc", "2.0");
        response.set("id", id);
        response.set("result", result);
        return response;
    }

    public static ObjectNode error(ObjectMapper mapper, JsonNode id, int code, String message) {
        ObjectNode response = mapper.createObjectNode();
        response.put("jsonrpc", "2.0");
        response.set("id", id == null ? response.nullNode() : id);
        ObjectNode error = response.putObject("error");
        error.put("code", code);
        error.put("message", message);
        return response;
    }

    /** A message with a {@code method} and no {@code id} is a notification: never answered. */
    public static boolean isNotification(JsonNode message) {
        return message.has("method") && (!message.has("id") || message.get("id").isNull());
    }
}
