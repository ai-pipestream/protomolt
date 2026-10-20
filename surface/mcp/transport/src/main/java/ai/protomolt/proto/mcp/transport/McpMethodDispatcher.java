package ai.protomolt.proto.mcp.transport;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

/**
 * Package-private stateless MCP JSON-RPC method dispatcher and payload formatter for
 * {@link McpServer}.
 *
 * <p>Handles envelope validation, method routing ({@code initialize}, {@code ping},
 * {@code tools/list}, {@code tools/call}, {@code resources/list},
 * {@code resources/templates/list}, {@code resources/read}), tool catalog metadata
 * fingerprinting, structured tool execution results, and correlation-ID error redaction.
 * Session state transitions, request cancellation, and concurrency limits remain in
 * {@link McpServer.Session}.
 */
final class McpMethodDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(McpServer.class);
    private static final int RESOURCE_PAGE_SIZE = 100;

    private final ActionCatalog catalog;
    private final McpResources resources;
    private final String serverName;
    private final String serverVersion;
    private final String instructions;
    private final ObjectMapper mapper;

    McpMethodDispatcher(
            ActionCatalog catalog,
            McpResources resources,
            String serverName,
            String serverVersion,
            String instructions,
            ObjectMapper mapper) {
        this.catalog = catalog;
        this.resources = resources;
        this.serverName = serverName;
        this.serverVersion = serverVersion;
        this.instructions = instructions;
        this.mapper = mapper;
    }

    /**
     * Dispatches one JSON-RPC message as {@code caller}. When {@code moonshotDialect} is set,
     * tool schemas in {@code initialize} and {@code tools/list} are rendered in the
     * Moonshot-sanitized form.
     */
    Optional<ObjectNode> handle(JsonNode message, Caller caller, boolean moonshotDialect) {
        if (!message.isObject()) {
            return Optional.of(JsonRpc.error(
                    mapper, null, JsonRpc.INVALID_REQUEST, "Invalid request"));
        }
        if (!message.has("method")) {
            if (message.has("result") || message.has("error")) {
                // A response to a server-initiated request; this server never sends any.
                return Optional.empty();
            }
            return Optional.of(JsonRpc.error(
                    mapper, message.get("id"), JsonRpc.INVALID_REQUEST, "Invalid request"));
        }
        if (JsonRpc.isNotification(message)) {
            return Optional.empty();
        }
        JsonNode id = message.get("id");
        String method = message.get("method").asText();
        JsonNode params = message.has("params")
                ? message.get("params") : mapper.createObjectNode();
        try {
            return switch (method) {
                case "initialize" -> Optional.of(JsonRpc.result(mapper, id,
                        initialize(params, caller, moonshotDialect)));
                case "ping" -> Optional.of(JsonRpc.result(
                        mapper, id, mapper.createObjectNode()));
                case "tools/list" -> Optional.of(JsonRpc.result(
                        mapper, id, listTools(caller, moonshotDialect)));
                case "tools/call" -> Optional.of(JsonRpc.result(
                        mapper, id, callTool(params, caller)));
                case "resources/list" -> Optional.of(JsonRpc.result(
                        mapper, id, listResources(params)));
                case "resources/templates/list" -> Optional.of(JsonRpc.result(
                        mapper, id, listResourceTemplates(params)));
                case "resources/read" -> readResource(params)
                        .map(contents -> JsonRpc.result(mapper, id, contents))
                        .or(() -> Optional.of(JsonRpc.error(
                                mapper, id, JsonRpc.RESOURCE_NOT_FOUND,
                                "Unknown resource: " + params.path("uri").asText())));
                default -> Optional.of(JsonRpc.error(
                        mapper, id, JsonRpc.METHOD_NOT_FOUND,
                        "Method not found: " + method));
            };
        } catch (IllegalArgumentException e) {
            return Optional.of(JsonRpc.error(
                    mapper, id, JsonRpc.INVALID_PARAMS, e.getMessage()));
        } catch (Exception e) {
            // Exception class names and messages can leak paths, targets, or upstream
            // detail; the wire gets a correlation id, the log gets the stack trace.
            String correlationId = UUID.randomUUID().toString();
            LOG.error("MCP request '{}' failed, correlation id {}", method, correlationId, e);
            return Optional.of(JsonRpc.error(
                    mapper, id, JsonRpc.INTERNAL_ERROR,
                    "Internal error (correlation id " + correlationId + ")"));
        }
    }

    ObjectNode initialize(JsonNode params, Caller caller, boolean moonshotDialect) {
        if (params == null || !params.isObject()) {
            throw new IllegalArgumentException("initialize params must be an object");
        }
        String requested = params.path("protocolVersion").asText(McpServer.PROTOCOL_VERSION);
        ObjectNode result = mapper.createObjectNode();
        result.put("protocolVersion",
                McpServer.supportsProtocolVersion(requested)
                        ? requested : McpServer.PROTOCOL_VERSION);
        ObjectNode capabilities = result.putObject("capabilities");
        capabilities.putObject("tools").put("listChanged", false);
        capabilities.putObject("resources")
                .put("subscribe", false)
                .put("listChanged", false);
        ObjectNode serverInfo = result.putObject("serverInfo");
        serverInfo.put("name", serverName);
        serverInfo.put("version", serverVersion);
        ArrayNode manifest = catalog.list(caller);
        if (moonshotDialect) {
            manifest = MoonshotDialect.sanitizeForMoonshot(manifest);
        }
        addToolCatalogMetadata(result, manifest);
        if (!instructions.isEmpty()) {
            result.put("instructions", instructions);
        }
        return result;
    }

    ObjectNode listTools(Caller caller, boolean moonshotDialect) {
        ObjectNode result = mapper.createObjectNode();
        // The catalog manifest entries ({name, description, inputSchema}) are already the
        // MCP tool shape; inputSchema is JSON Schema in both worlds. The manifest is the
        // caller's view: only tools whose scope the caller holds.
        ArrayNode manifest = catalog.list(caller);
        if (moonshotDialect) {
            manifest = MoonshotDialect.sanitizeForMoonshot(manifest);
        }
        result.set("tools", manifest);
        addToolCatalogMetadata(result, manifest);
        return result;
    }

    private void addToolCatalogMetadata(ObjectNode result, ArrayNode manifest) {
        ObjectNode toolCatalog = WorkspaceResources.toolCatalog(manifest, mapper);
        ObjectNode metadata = result.putObject("_meta");
        metadata.put("ai.protomolt/toolCatalogFingerprint",
                toolCatalog.path("fingerprint").asText());
        metadata.put("ai.protomolt/toolCount", toolCatalog.path("count").asInt());
        metadata.put("ai.protomolt/workspace", WorkspaceResources.URI);
    }

    ObjectNode callTool(JsonNode params, Caller caller) {
        String name = params.path("name").asText(null);
        if (name == null) {
            throw new IllegalArgumentException("tools/call requires params.name");
        }
        JsonNode arguments = params.path("arguments");
        if (arguments.isMissingNode() || arguments.isNull()) {
            arguments = mapper.createObjectNode();
        }
        if (!arguments.isObject()) {
            throw new IllegalArgumentException("tools/call arguments must be an object");
        }
        try {
            ObjectNode output = catalog.execute(name, (ObjectNode) arguments, caller);
            return toolResult(output, false);
        } catch (ActionException e) {
            // Tool execution failures are results with isError, not protocol errors, so the
            // calling model sees the structured envelope and can repair its input.
            return toolResult(e.toJson(mapper), true);
        }
    }

    private ObjectNode toolResult(ObjectNode payload, boolean isError) {
        ObjectNode result = mapper.createObjectNode();
        ArrayNode content = result.putArray("content");
        ObjectNode text = content.addObject();
        text.put("type", "text");
        text.put("text", payload.toString());
        result.set("structuredContent", payload);
        result.put("isError", isError);
        return result;
    }

    ObjectNode listResources(JsonNode params) {
        if (params == null || !params.isObject()) {
            throw new IllegalArgumentException("resources/list params must be an object");
        }
        JsonNode cursor = params.get("cursor");
        String cursorValue = null;
        if (cursor != null && !cursor.isNull()) {
            if (!cursor.isTextual() || cursor.asText().isBlank()) {
                throw new IllegalArgumentException(
                        "resources/list cursor must be a non-empty string");
            }
            cursorValue = cursor.asText();
        }
        if (resources == null && cursorValue != null) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
        McpResources.Page page = resources == null
                ? new McpResources.Page(mapper.createArrayNode(), null)
                : resources.page(mapper, cursorValue, RESOURCE_PAGE_SIZE);
        ObjectNode result = mapper.createObjectNode();
        result.set("resources", page.resources());
        if (page.nextCursor() != null) {
            result.put("nextCursor", page.nextCursor());
        }
        return result;
    }

    ObjectNode listResourceTemplates(JsonNode params) {
        if (params == null || !params.isObject()) {
            throw new IllegalArgumentException(
                    "resources/templates/list params must be an object");
        }
        int offset = cursorOffset(params.get("cursor"), "resource template");
        ArrayNode all = resources.templates(mapper);
        if (offset > all.size()) {
            throw new IllegalArgumentException("resource template cursor is invalid");
        }
        int end = Math.min(offset + RESOURCE_PAGE_SIZE, all.size());
        ArrayNode page = mapper.createArrayNode();
        for (int i = offset; i < end; i++) {
            page.add(all.get(i));
        }
        ObjectNode result = mapper.createObjectNode();
        result.set("resourceTemplates", page);
        if (end < all.size()) {
            result.put("nextCursor", Integer.toString(end));
        }
        return result;
    }

    private static int cursorOffset(JsonNode cursor, String label) {
        if (cursor == null || cursor.isNull()) {
            return 0;
        }
        if (!cursor.isTextual() || cursor.asText().isBlank()) {
            throw new IllegalArgumentException(label + " cursor must be a non-empty string");
        }
        try {
            int offset = Integer.parseInt(cursor.asText());
            if (offset < 0) {
                throw new NumberFormatException();
            }
            return offset;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(label + " cursor is invalid");
        }
    }

    Optional<ObjectNode> readResource(JsonNode params) {
        String uri = params.path("uri").asText(null);
        if (uri == null) {
            throw new IllegalArgumentException("resources/read requires params.uri");
        }
        if (resources == null) {
            return Optional.empty();
        }
        return resources.read(mapper, uri).map(contents -> {
            ObjectNode result = mapper.createObjectNode();
            ArrayNode list = result.putArray("contents");
            list.add(contents);
            return result;
        });
    }
}
