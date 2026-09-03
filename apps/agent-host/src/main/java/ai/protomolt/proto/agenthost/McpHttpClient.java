package ai.protomolt.proto.agenthost;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Minimal streamable-HTTP MCP client with a pooled HTTP/2 connection. */
final class McpHttpClient implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROTOCOL_VERSION = "2025-06-18";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(40);

    private final URI endpoint;
    private final Supplier<String> bearerToken;
    private final HttpClient http;
    private final AtomicLong ids = new AtomicLong();

    private String sessionId;
    private String negotiatedVersion;

    McpHttpClient(URI endpoint, Supplier<String> bearerToken) {
        this.endpoint = validateEndpoint(endpoint);
        this.bearerToken = Objects.requireNonNull(bearerToken, "bearer token supplier");
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    static McpHttpClient usingTokenEnvironment(URI endpoint, String tokenEnvironment) {
        if (tokenEnvironment != null
                && !tokenEnvironment.matches("[A-Z][A-Z0-9_]{0,127}")) {
            throw new IllegalArgumentException("token environment name is invalid");
        }
        return new McpHttpClient(endpoint, () -> {
            if (tokenEnvironment == null) {
                return null;
            }
            String value = System.getenv(tokenEnvironment);
            if (value == null || value.isBlank()) {
                throw new AgentHostException(
                        "configured MCP token environment is unset");
            }
            return value;
        });
    }

    synchronized ObjectNode callTool(String name, ObjectNode arguments) {
        Objects.requireNonNull(name, "tool name");
        Objects.requireNonNull(arguments, "tool arguments");
        ensureSession();
        ObjectNode params = MAPPER.createObjectNode();
        params.put("name", name);
        params.set("arguments", arguments);
        HttpResponse<String> response = send(request("tools/call", params), true);
        if (response.statusCode() != 200) {
            if (response.statusCode() == 404 || response.statusCode() == 410) {
                forgetSession();
            }
            throw new AgentHostException("MCP tool call returned HTTP "
                    + response.statusCode());
        }
        JsonNode envelope = read(response.body());
        if (envelope.has("error")) {
            throw new AgentHostException("MCP tool call failed: "
                    + envelope.path("error").path("message").asText("unknown error"));
        }
        JsonNode result = envelope.path("result");
        if (result.path("isError").asBoolean()) {
            String detail = result.path("structuredContent").path("message")
                    .asText("tool rejected the command");
            throw new AgentHostException("MCP tool '" + name + "' failed: " + detail);
        }
        JsonNode structured = result.path("structuredContent");
        if (!structured.isObject()) {
            throw new AgentHostException("MCP tool '" + name
                    + "' returned no structured object");
        }
        return (ObjectNode) structured;
    }

    synchronized void reconnect() {
        forgetSession();
        ensureSession();
    }

    private void ensureSession() {
        if (sessionId != null) {
            return;
        }
        ObjectNode params = MAPPER.createObjectNode();
        params.put("protocolVersion", PROTOCOL_VERSION);
        params.putObject("capabilities");
        params.putObject("clientInfo").put("name", "protomolt-agent-host")
                .put("version", "1");
        HttpResponse<String> initialized = send(request("initialize", params), false);
        if (initialized.statusCode() != 200) {
            throw new AgentHostException("MCP initialize returned HTTP "
                    + initialized.statusCode());
        }
        String nextSession = initialized.headers().firstValue("Mcp-Session-Id")
                .orElseThrow(() -> new AgentHostException(
                        "MCP initialize returned no session id"));
        JsonNode envelope = read(initialized.body());
        if (envelope.has("error")) {
            throw new AgentHostException("MCP initialize failed: "
                    + envelope.path("error").path("message").asText("unknown error"));
        }
        String version = envelope.path("result").path("protocolVersion").asText();
        if (version.isBlank()) {
            throw new AgentHostException("MCP initialize returned no protocol version");
        }
        sessionId = nextSession;
        negotiatedVersion = version;
        ObjectNode notification = MAPPER.createObjectNode();
        notification.put("jsonrpc", "2.0");
        notification.put("method", "notifications/initialized");
        HttpResponse<String> ready = send(notification, true);
        if (ready.statusCode() != 202) {
            forgetSession();
            throw new AgentHostException("MCP initialized notification returned HTTP "
                    + ready.statusCode());
        }
    }

    private ObjectNode request(String method, ObjectNode params) {
        ObjectNode request = MAPPER.createObjectNode();
        request.put("jsonrpc", "2.0");
        request.put("id", ids.incrementAndGet());
        request.put("method", method);
        request.set("params", params);
        return request;
    }

    private HttpResponse<String> send(ObjectNode body, boolean includeSession) {
        HttpRequest.Builder request = HttpRequest.newBuilder(endpoint)
                .timeout(REQUEST_TIMEOUT)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
        if (includeSession && sessionId != null) {
            request.header("Mcp-Session-Id", sessionId);
            request.header("MCP-Protocol-Version", negotiatedVersion);
        }
        String token = bearerToken.get();
        if (token != null && !token.isBlank()) {
            request.header("Authorization", "Bearer " + token);
        }
        try {
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AgentHostException("MCP request was interrupted", e);
        } catch (IOException e) {
            forgetSession();
            throw new AgentHostException("MCP endpoint is unavailable", e);
        }
    }

    private static JsonNode read(String body) {
        try {
            return MAPPER.readTree(body);
        } catch (IOException e) {
            throw new AgentHostException("MCP endpoint returned invalid JSON", e);
        }
    }

    private void forgetSession() {
        sessionId = null;
        negotiatedVersion = null;
    }

    private static URI validateEndpoint(URI endpoint) {
        Objects.requireNonNull(endpoint, "MCP endpoint");
        if (!("http".equalsIgnoreCase(endpoint.getScheme())
                || "https".equalsIgnoreCase(endpoint.getScheme()))
                || endpoint.getHost() == null || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("MCP endpoint must be an HTTP(S) URI");
        }
        return endpoint;
    }

    @Override
    public synchronized void close() {
        forgetSession();
        http.close();
    }
}
