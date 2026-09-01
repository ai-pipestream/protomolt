package ai.pipestream.proto.serve;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.authz.CallerResolver;
import ai.pipestream.proto.mcp.McpServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Streamable HTTP transport for {@link McpServer}. Each initialize request creates a bounded,
 * stateful MCP session; subsequent requests must carry both the session and negotiated protocol
 * version headers. The handler remains an ordinary JDK {@link HttpHandler}, so each in-flight
 * tool request can be cancelled by a concurrent POST on the same session.
 */
public final class McpHttpHandler implements HttpHandler, AutoCloseable {

    private static final Set<String> LOCAL_ORIGIN_HOSTS = Set.of("localhost", "127.0.0.1", "[::1]");
    private static final String CONTENT_TYPE = "application/json; charset=utf-8";
    private static final String SESSION_HEADER = "Mcp-Session-Id";
    private static final String VERSION_HEADER = "MCP-Protocol-Version";
    private static final int MAX_BODY_BYTES = 16 * 1024 * 1024;
    private static final int MAX_SESSIONS = 256;

    private final McpServer server;
    private final byte[] apiToken;
    private final CallerResolver resolver;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object sessionLock = new Object();
    private final LinkedHashMap<String, McpServer.Session> sessions =
            new LinkedHashMap<>(16, 0.75f, true);

    public McpHttpHandler(McpServer server) {
        this(server, null);
    }

    public McpHttpHandler(McpServer server, String apiToken) {
        this(server, apiToken, null);
    }

    /**
     * With a resolver, a credential a mounted access policy names authenticates as its
     * principal; the caller is pinned to the session it initializes. A non-null resolver
     * requires the operator token.
     */
    public McpHttpHandler(McpServer server, String apiToken,
                          CallerResolver resolver) {
        this.server = java.util.Objects.requireNonNull(server, "server");
        this.apiToken = apiToken == null ? null : apiToken.getBytes(StandardCharsets.UTF_8);
        if (resolver != null && apiToken == null) {
            throw new IllegalArgumentException(
                    "an access-policy resolver requires the operator api token");
        }
        this.resolver = resolver;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (rejectedOrigin(exchange)) {
            write(exchange, 403, error(null, -32000, "Origin not allowed"), null);
            return;
        }
        Caller caller = null;
        if (apiToken != null) {
            caller = callerFor(exchange);
            if (caller == null) {
                write(exchange, 401, error(null, -32000,
                        "Missing or invalid API token 'api_token'"), null);
                return;
            }
        }
        String method = exchange.getRequestMethod();
        if ("DELETE".equalsIgnoreCase(method)) {
            deleteSession(exchange);
            return;
        }
        if (!"POST".equalsIgnoreCase(method)) {
            exchange.getResponseHeaders().set("allow", "POST, DELETE");
            write(exchange, 405, error(null, -32000, "Method not allowed"), null);
            return;
        }
        if (!hasMediaType(exchange, "content-type", "application/json")) {
            write(exchange, 415, error(null, -32000,
                    "Content-Type must be application/json"), null);
            return;
        }
        if (!hasMediaType(exchange, "accept", "application/json")
                || !hasMediaType(exchange, "accept", "text/event-stream")) {
            write(exchange, 406, error(null, -32000,
                    "Accept must include application/json and text/event-stream"), null);
            return;
        }
        byte[] body = BoundedBodies.read(exchange.getRequestBody(), MAX_BODY_BYTES);
        if (body == null) {
            write(exchange, 413, error(null, -32000,
                    "Request body exceeds " + MAX_BODY_BYTES + " bytes"), null);
            return;
        }
        JsonNode message;
        try {
            message = mapper.readTree(body);
        } catch (IOException e) {
            write(exchange, 400, error(null, -32700, "Parse error"), null);
            return;
        }
        if (message == null || message.isMissingNode()) {
            write(exchange, 400, error(null, -32700, "Parse error"), null);
            return;
        }
        if (message.isArray()) {
            handleBatch(exchange, message);
            return;
        }
        if (!message.isObject()) {
            write(exchange, 400, error(null, -32600, "Invalid request"), null);
            return;
        }

        String methodName = message.path("method").asText();
        if ("initialize".equals(methodName)) {
            initialize(exchange, message,
                    caller == null ? Caller.operator() : caller);
            return;
        }

        String sessionId = exchange.getRequestHeaders().getFirst(SESSION_HEADER);
        McpServer.Session session = lookup(sessionId);
        if (session == null) {
            write(exchange, 404, error(message.get("id"), -32001,
                    "MCP session is missing or expired"), null);
            return;
        }
        if (!validVersion(exchange, session)) {
            write(exchange, 400, error(message.get("id"), -32602,
                    "MCP-Protocol-Version must match the negotiated session version"), sessionId);
            return;
        }

        if ("tools/call".equals(methodName) && session.isToolCallReady(message)) {
            Future<Optional<com.fasterxml.jackson.databind.node.ObjectNode>> future;
            try {
                future = session.submit(message);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                write(exchange, 200, error(message.get("id"), -32000, e.getMessage()), sessionId);
                return;
            } catch (IllegalArgumentException e) {
                write(exchange, 200, error(message.get("id"), -32602, e.getMessage()), sessionId);
                return;
            }
            try {
                Optional<ObjectNode> response = future.get();
                if (response.isPresent()) {
                    write(exchange, 200, response.get(), sessionId);
                } else {
                    accepted(exchange, sessionId);
                }
            } catch (CancellationException e) {
                accepted(exchange, sessionId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.cancel(true);
                accepted(exchange, sessionId);
            } catch (ExecutionException e) {
                write(exchange, 500, error(message.get("id"), -32603,
                        "Internal error"), sessionId);
            }
            return;
        }

        Optional<com.fasterxml.jackson.databind.node.ObjectNode> response = session.handle(message);
        if (response.isEmpty()) {
            accepted(exchange, sessionId);
        } else {
            write(exchange, 200, response.get(), sessionId);
        }
    }

    /**
     * JSON-RPC batches remain supported for established sessions. Initialize is deliberately
     * excluded because one HTTP request must create exactly one session header.
     */
    private void handleBatch(HttpExchange exchange, JsonNode batch) throws IOException {
        if (batch.isEmpty()) {
            write(exchange, 400, error(null, -32600, "Invalid request: empty batch"), null);
            return;
        }
        for (JsonNode entry : batch) {
            if (!entry.isObject() || "initialize".equals(entry.path("method").asText())) {
                write(exchange, 400, error(entry.isObject() ? entry.get("id") : null,
                        -32600, "Invalid request"), null);
                return;
            }
        }
        String sessionId = exchange.getRequestHeaders().getFirst(SESSION_HEADER);
        McpServer.Session session = lookup(sessionId);
        if (session == null) {
            write(exchange, 404, error(null, -32001, "MCP session is missing or expired"), null);
            return;
        }
        if (!validVersion(exchange, session)) {
            write(exchange, 400, error(null, -32602,
                    "MCP-Protocol-Version must match the negotiated session version"), sessionId);
            return;
        }
        ArrayNode responses = mapper.createArrayNode();
        for (JsonNode entry : batch) {
            if (session.isToolCallReady(entry)) {
                try {
                    session.submit(entry).get().ifPresent(responses::add);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    accepted(exchange, sessionId);
                    return;
                } catch (CancellationException e) {
                    continue;
                } catch (ExecutionException e) {
                    responses.add(error(entry.get("id"), -32603, "Internal error"));
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    responses.add(error(entry.get("id"), -32000, e.getMessage()));
                } catch (IllegalArgumentException e) {
                    responses.add(error(entry.get("id"), -32602, e.getMessage()));
                }
            } else {
                session.handle(entry).ifPresent(responses::add);
            }
        }
        if (responses.isEmpty()) {
            accepted(exchange, sessionId);
        } else {
            write(exchange, 200, responses, sessionId);
        }
    }

    private void initialize(HttpExchange exchange, JsonNode message,
                            Caller caller) throws IOException {
        if (message.has(SESSION_HEADER) || exchange.getRequestHeaders().getFirst(SESSION_HEADER) != null) {
            write(exchange, 400, error(message.get("id"), -32600,
                    "initialize must not include an MCP session"), null);
            return;
        }
        if (!message.has("id") || message.get("id").isNull()) {
            write(exchange, 400, error(null, -32600, "initialize must be a request"), null);
            return;
        }
        String requested = message.path("params").path("protocolVersion").asText(null);
        String requestHeader = exchange.getRequestHeaders().getFirst(VERSION_HEADER);
        if (requestHeader != null && !McpServer.supportsProtocolVersion(requestHeader)) {
            write(exchange, 400, error(message.get("id"), -32602,
                    "unsupported MCP-Protocol-Version"), null);
            return;
        }
        if (requested != null && !McpServer.supportsProtocolVersion(requested)
                && requestHeader != null) {
            write(exchange, 400, error(message.get("id"), -32602,
                    "unsupported protocol version"), null);
            return;
        }
        McpServer.Session session = server.openSession(caller);
        Optional<com.fasterxml.jackson.databind.node.ObjectNode> response = session.handle(message);
        if (response.isEmpty() || response.get().has("error")) {
            session.close();
            if (response.isPresent()) {
                write(exchange, 200, response.get(), null);
            }
            return;
        }
        String sessionId = UUID.randomUUID().toString();
        putSession(sessionId, session);
        exchange.getResponseHeaders().set(SESSION_HEADER, sessionId);
        write(exchange, 200, response.get(), sessionId);
    }

    private boolean validVersion(HttpExchange exchange, McpServer.Session session) {
        String header = exchange.getRequestHeaders().getFirst(VERSION_HEADER);
        return header != null && header.equals(session.negotiatedProtocolVersion());
    }

    private void deleteSession(HttpExchange exchange) throws IOException {
        String sessionId = exchange.getRequestHeaders().getFirst(SESSION_HEADER);
        McpServer.Session session = lookup(sessionId);
        if (session == null) {
            write(exchange, 404, error(null, -32001, "MCP session is missing or expired"), null);
            return;
        }
        if (!validVersion(exchange, session)) {
            write(exchange, 400, error(null, -32602,
                    "MCP-Protocol-Version must match the negotiated session version"), sessionId);
            return;
        }
        if (removeSession(sessionId) == null) {
            write(exchange, 404, error(null, -32001, "MCP session is missing or expired"), null);
            return;
        }
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private void putSession(String id, McpServer.Session session) {
        McpServer.Session evicted = null;
        synchronized (sessionLock) {
            if (sessions.size() >= MAX_SESSIONS) {
                // Access-ordered, so the first entry is the least-recently-used session.
                evicted = sessions.pollFirstEntry().getValue();
            }
            sessions.put(id, session);
        }
        if (evicted != null) {
            evicted.close();
        }
    }

    private McpServer.Session lookup(String id) {
        if (id == null || id.isBlank()) {
            return null;
        }
        synchronized (sessionLock) {
            return sessions.get(id);
        }
    }

    private McpServer.Session removeSession(String id) {
        McpServer.Session session;
        synchronized (sessionLock) {
            session = sessions.remove(id);
        }
        if (session != null) {
            session.close();
        }
        return session;
    }

    /** Closes every live MCP session when the host shuts down. */
    @Override
    public void close() {
        Map<String, McpServer.Session> closing;
        synchronized (sessionLock) {
            closing = new LinkedHashMap<>(sessions);
            sessions.clear();
        }
        closing.values().forEach(McpServer.Session::close);
    }

    private Caller callerFor(HttpExchange exchange) {
        String presented = exchange.getRequestHeaders().getFirst("api_token");
        if (presented == null) {
            String authorization = exchange.getRequestHeaders().getFirst("authorization");
            if (authorization != null && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = authorization.substring(7).trim();
            }
        }
        if (presented == null || presented.isBlank()) {
            return null;
        }
        if (java.security.MessageDigest.isEqual(
                apiToken, presented.getBytes(StandardCharsets.UTF_8))) {
            return Caller.operator();
        }
        return resolver == null ? null : resolver.resolve(presented).orElse(null);
    }

    private static boolean rejectedOrigin(HttpExchange exchange) {
        String origin = exchange.getRequestHeaders().getFirst("origin");
        if (origin == null || origin.isBlank()) {
            return false;
        }
        try {
            String host = URI.create(origin.trim()).getHost();
            return host == null || !LOCAL_ORIGIN_HOSTS.contains(host.toLowerCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    private static boolean hasMediaType(HttpExchange exchange, String header, String expected) {
        java.util.List<String> values = exchange.getRequestHeaders().get(header);
        if (values == null) {
            return false;
        }
        for (String value : values) {
            for (String item : value.split(",")) {
                String mediaType = item.split(";", 2)[0].trim();
                if (expected.equalsIgnoreCase(mediaType)) {
                    return true;
                }
            }
        }
        return false;
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

    private static void accepted(HttpExchange exchange, String sessionId) throws IOException {
        if (sessionId != null) {
            exchange.getResponseHeaders().set(SESSION_HEADER, sessionId);
        }
        exchange.sendResponseHeaders(202, -1);
        exchange.close();
    }

    private void write(HttpExchange exchange, int status, JsonNode body, String sessionId)
            throws IOException {
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("content-type", CONTENT_TYPE);
        if (sessionId != null) {
            exchange.getResponseHeaders().set(SESSION_HEADER, sessionId);
        }
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
