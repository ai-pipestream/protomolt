package ai.pipestream.proto.serve;

import ai.pipestream.proto.mcp.McpServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * The MCP streamable HTTP transport over {@link McpServer}: JSON-RPC messages POSTed to one
 * endpoint, answered as {@code application/json}. The server core is stateless, so no session
 * management is required — any MCP client connects with just the URL:
 *
 * <pre>claude mcp add --transport http protomolt http://host:8080/mcp</pre>
 *
 * <p>Server-initiated streams are not used; GET answers 405 as the specification allows.
 * Requests carrying a browser {@code Origin} header from a non-local origin are refused, the
 * specification's DNS-rebinding guard.</p>
 */
public final class McpHttpHandler implements HttpHandler {

    private static final Set<String> LOCAL_ORIGIN_HOSTS = Set.of("localhost", "127.0.0.1", "[::1]");
    private static final String CONTENT_TYPE = "application/json; charset=utf-8";
    /** Same cap as the REST hosts and the registry: nothing reads request bodies unbounded. */
    private static final int MAX_BODY_BYTES = 16 * 1024 * 1024;

    private final McpServer server;
    private final byte[] apiToken;
    private final ObjectMapper mapper = new ObjectMapper();

    public McpHttpHandler(McpServer server) {
        this(server, null);
    }

    /**
     * With a non-null {@code apiToken}, every request must present it as an
     * {@code api_token} header or an {@code authorization} bearer credential; MCP clients
     * pass it with {@code claude mcp add --transport http ... --header "api_token: ..."}.
     */
    public McpHttpHandler(McpServer server, String apiToken) {
        this.server = server;
        this.apiToken = apiToken == null
                ? null
                : apiToken.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (rejectedOrigin(exchange)) {
            write(exchange, 403, error(null, -32000, "Origin not allowed"));
            return;
        }
        if (apiToken != null && !authorized(exchange)) {
            write(exchange, 401, error(null, -32000, "Missing or invalid API token 'api_token'"));
            return;
        }
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            // No server-initiated streams (GET) and no sessions to delete (DELETE).
            exchange.getResponseHeaders().set("allow", "POST");
            write(exchange, 405, error(null, -32000, "Method not allowed"));
            return;
        }
        byte[] body = BoundedBodies.read(exchange.getRequestBody(), MAX_BODY_BYTES);
        if (body == null) {
            write(exchange, 413, error(null, -32000,
                    "Request body exceeds " + MAX_BODY_BYTES + " bytes"));
            return;
        }
        JsonNode message;
        try {
            message = mapper.readTree(body);
        } catch (IOException e) {
            write(exchange, 400, error(null, -32700, "Parse error"));
            return;
        }
        if (message == null || message.isMissingNode()) {
            write(exchange, 400, error(null, -32700, "Parse error"));
            return;
        }
        if (message.isArray()) {
            if (message.isEmpty()) {
                // JSON-RPC 2.0: an empty batch is a single invalid-request error.
                write(exchange, 400, error(null, -32600, "Invalid request: empty batch"));
                return;
            }
            // JSON-RPC batching exists in protocol revision 2025-03-26; answer in kind.
            ArrayNode responses = mapper.createArrayNode();
            for (JsonNode entry : message) {
                server.handle(entry).ifPresent(responses::add);
            }
            if (responses.isEmpty()) {
                accepted(exchange);
            } else {
                write(exchange, 200, responses);
            }
            return;
        }
        Optional<? extends JsonNode> response = server.handle(message);
        if (response.isEmpty()) {
            accepted(exchange);
        } else {
            write(exchange, 200, response.get());
        }
    }

    /** True when the request presents the shared secret (constant-time comparison). */
    private boolean authorized(HttpExchange exchange) {
        String presented = exchange.getRequestHeaders().getFirst("api_token");
        if (presented == null) {
            String authorization = exchange.getRequestHeaders().getFirst("authorization");
            if (authorization != null && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = authorization.substring(7).trim();
            }
        }
        return presented != null && !presented.isBlank()
                && java.security.MessageDigest.isEqual(
                        apiToken, presented.getBytes(StandardCharsets.UTF_8));
    }

    /** True when a browser-sent Origin header points anywhere but this machine. */
    private static boolean rejectedOrigin(HttpExchange exchange) {
        String origin = exchange.getRequestHeaders().getFirst("origin");
        if (origin == null || origin.isBlank()) {
            return false;
        }
        try {
            String host = URI.create(origin.trim()).getHost();
            return host == null
                    || !LOCAL_ORIGIN_HOSTS.contains(host.toLowerCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    private JsonNode error(JsonNode id, int code, String message) {
        var node = mapper.createObjectNode();
        node.put("jsonrpc", "2.0");
        node.set("id", id == null ? mapper.nullNode() : id);
        var error = node.putObject("error");
        error.put("code", code);
        error.put("message", message);
        return node;
    }

    private static void accepted(HttpExchange exchange) throws IOException {
        // A notification (or client response): acknowledged, nothing to send back.
        exchange.sendResponseHeaders(202, -1);
        exchange.close();
    }

    private static void write(HttpExchange exchange, int status, JsonNode body) throws IOException {
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("content-type", CONTENT_TYPE);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
