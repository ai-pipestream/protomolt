package ai.protomolt.proto.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Package compatibility for the shared JSON-RPC envelope helpers. */
final class JsonRpc {
    static final int PARSE_ERROR = ai.protomolt.proto.mcp.transport.JsonRpc.PARSE_ERROR;
    static final int INVALID_REQUEST = ai.protomolt.proto.mcp.transport.JsonRpc.INVALID_REQUEST;
    static final int METHOD_NOT_FOUND = ai.protomolt.proto.mcp.transport.JsonRpc.METHOD_NOT_FOUND;
    static final int INVALID_PARAMS = ai.protomolt.proto.mcp.transport.JsonRpc.INVALID_PARAMS;
    static final int INTERNAL_ERROR = ai.protomolt.proto.mcp.transport.JsonRpc.INTERNAL_ERROR;
    static final int RESOURCE_NOT_FOUND = ai.protomolt.proto.mcp.transport.JsonRpc.RESOURCE_NOT_FOUND;
    private JsonRpc() {}
    static ObjectNode result(ObjectMapper mapper, JsonNode id, JsonNode result) {
        return ai.protomolt.proto.mcp.transport.JsonRpc.result(mapper, id, result);
    }
    static ObjectNode error(ObjectMapper mapper, JsonNode id, int code, String message) {
        return ai.protomolt.proto.mcp.transport.JsonRpc.error(mapper, id, code, message);
    }
    static boolean isNotification(JsonNode message) { return ai.protomolt.proto.mcp.transport.JsonRpc.isNotification(message); }
}
