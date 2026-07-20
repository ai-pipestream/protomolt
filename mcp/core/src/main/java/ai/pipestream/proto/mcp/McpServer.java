package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A Model Context Protocol server over an {@link ActionCatalog}: every action becomes an MCP
 * tool (the catalog's manifest already carries the name, tool-use description, and JSON Schema
 * input MCP requires), and a {@link RegistryResources} optionally exposes a schema registry's
 * subjects as MCP resources.
 *
 * <p>The transport is the protocol's stdio framing: one JSON-RPC 2.0 message per line, requests
 * answered in order, notifications consumed silently. {@link #handle(JsonNode)} is the pure
 * message-in/message-out core, so tests and alternative transports (an HTTP mount, a framework
 * adapter) can drive the server without streams. Nothing here is framework-aware; Spring and
 * Quarkus MCP hosts can register the same catalog through their own programmatic APIs.</p>
 */
public final class McpServer {

    private static final Logger LOG = LoggerFactory.getLogger(McpServer.class);

    /** Latest protocol revision this server implements. */
    public static final String PROTOCOL_VERSION = "2025-06-18";

    private static final List<String> SUPPORTED_VERSIONS =
            List.of("2025-06-18", "2025-03-26", "2024-11-05");

    private final ActionCatalog catalog;
    private final RegistryResources resources;
    private final String serverName;
    private final String serverVersion;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * @param catalog   tools; every catalog action is exposed
     * @param resources registry-backed resources, or {@code null} to serve tools only
     */
    public McpServer(ActionCatalog catalog, RegistryResources resources,
                     String serverName, String serverVersion) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.resources = resources;
        this.serverName = Objects.requireNonNull(serverName, "serverName");
        this.serverVersion = Objects.requireNonNull(serverVersion, "serverVersion");
    }

    /**
     * Reads newline-delimited JSON-RPC messages from {@code in} until end of stream, writing
     * one response line per request. Malformed JSON is answered with a parse error rather than
     * terminating the session.
     */
    public void run(InputStream in, OutputStream out) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode message;
            try {
                message = mapper.readTree(line);
            } catch (JsonProcessingException e) {
                write(writer, JsonRpc.error(mapper, null, JsonRpc.PARSE_ERROR, "Parse error"));
                continue;
            }
            handle(message).ifPresent(response -> write(writer, response));
        }
    }

    /**
     * Dispatches one JSON-RPC message. Requests produce a response; notifications and
     * client-side responses produce none.
     */
    public Optional<ObjectNode> handle(JsonNode message) {
        if (!message.isObject()) {
            return Optional.of(JsonRpc.error(mapper, null, JsonRpc.INVALID_REQUEST, "Invalid request"));
        }
        if (!message.has("method")) {
            if (message.has("result") || message.has("error")) {
                // A response to a server-initiated request; this server never sends any.
                return Optional.empty();
            }
            return Optional.of(JsonRpc.error(mapper, message.get("id"), JsonRpc.INVALID_REQUEST,
                    "Invalid request"));
        }
        if (JsonRpc.isNotification(message)) {
            return Optional.empty();
        }
        JsonNode id = message.get("id");
        String method = message.get("method").asText();
        JsonNode params = message.has("params") ? message.get("params") : mapper.createObjectNode();
        try {
            return switch (method) {
                case "initialize" -> Optional.of(JsonRpc.result(mapper, id, initialize(params)));
                case "ping" -> Optional.of(JsonRpc.result(mapper, id, mapper.createObjectNode()));
                case "tools/list" -> Optional.of(JsonRpc.result(mapper, id, listTools()));
                case "tools/call" -> Optional.of(JsonRpc.result(mapper, id, callTool(params)));
                case "resources/list" -> Optional.of(JsonRpc.result(mapper, id, listResources()));
                case "resources/read" -> readResource(params)
                        .map(contents -> JsonRpc.result(mapper, id, contents))
                        .or(() -> Optional.of(JsonRpc.error(mapper, id, JsonRpc.RESOURCE_NOT_FOUND,
                                "Unknown resource: " + params.path("uri").asText())));
                default -> Optional.of(JsonRpc.error(mapper, id, JsonRpc.METHOD_NOT_FOUND,
                        "Method not found: " + method));
            };
        } catch (IllegalArgumentException e) {
            return Optional.of(JsonRpc.error(mapper, id, JsonRpc.INVALID_PARAMS, e.getMessage()));
        } catch (Exception e) {
            // Exception class names and messages can leak paths, targets, or upstream
            // detail; the wire gets a correlation id, the log gets the stack trace.
            String correlationId = java.util.UUID.randomUUID().toString();
            LOG.error("MCP request '{}' failed, correlation id {}", method, correlationId, e);
            return Optional.of(JsonRpc.error(mapper, id, JsonRpc.INTERNAL_ERROR,
                    "Internal error (correlation id " + correlationId + ")"));
        }
    }

    private ObjectNode initialize(JsonNode params) {
        String requested = params.path("protocolVersion").asText(PROTOCOL_VERSION);
        ObjectNode result = mapper.createObjectNode();
        result.put("protocolVersion",
                SUPPORTED_VERSIONS.contains(requested) ? requested : PROTOCOL_VERSION);
        ObjectNode capabilities = result.putObject("capabilities");
        capabilities.putObject("tools");
        if (resources != null) {
            capabilities.putObject("resources");
        }
        ObjectNode serverInfo = result.putObject("serverInfo");
        serverInfo.put("name", serverName);
        serverInfo.put("version", serverVersion);
        return result;
    }

    private ObjectNode listTools() {
        ObjectNode result = mapper.createObjectNode();
        // The catalog manifest entries ({name, description, inputSchema}) are already the
        // MCP tool shape; inputSchema is JSON Schema in both worlds.
        result.set("tools", catalog.list());
        return result;
    }

    private ObjectNode callTool(JsonNode params) {
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
            ObjectNode output = catalog.execute(name, (ObjectNode) arguments);
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

    private ObjectNode listResources() {
        ObjectNode result = mapper.createObjectNode();
        result.set("resources", resources == null
                ? JsonNodeFactory.instance.arrayNode()
                : resources.list(mapper));
        return result;
    }

    private Optional<ObjectNode> readResource(JsonNode params) {
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

    private void write(BufferedWriter writer, ObjectNode response) {
        try {
            writer.write(response.toString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
