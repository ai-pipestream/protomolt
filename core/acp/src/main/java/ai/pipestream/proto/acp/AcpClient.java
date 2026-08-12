package ai.pipestream.proto.acp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * A minimal blocking ACP client for driving an agent the way an IDE does, over in-memory
 * pipes or a launched child process. Calls block on the response future with
 * a timeout; session updates are delivered to the listener registered with
 * {@link #onSessionUpdate}. Everything runs on virtual threads, so blocking here parks rather
 * than pins.
 *
 * <p>The client answers no agent-to-client methods (this agent declares no capabilities that
 * would use them); any such request gets a JSON-RPC method-not-found error.</p>
 */
public final class AcpClient implements AutoCloseable {

    /** How a headless client answers agent permission requests. */
    public enum PermissionPolicy {
        /** Select a reject-once option when present, otherwise cancel the request. */
        REJECT,
        /** Select the only allow-once option; ambiguous choices are cancelled. */
        ALLOW_SINGLE
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(3);

    private final AcpConnection connection;
    private final Process process;
    private volatile Duration requestTimeout = DEFAULT_TIMEOUT;

    private AcpClient(AcpConnection connection, Process process) {
        this.connection = connection;
        this.process = process;
    }

    /** A client over any pair of streams, typically one end of an in-memory pipe in tests. */
    public static AcpClient over(InputStream in, OutputStream out) {
        return new AcpClient(AcpConnection.over(in, out).start(), null);
    }

    /**
     * Launches the agent as a child process and speaks the protocol over its stdio, exactly as
     * an IDE does. The agent's stderr is copied to this process's stderr so agent logs stay
     * visible and the pipe can never fill up and block the agent.
     */
    public static AcpClient launch(String... command) throws IOException {
        Process child = new ProcessBuilder(command).start();
        Thread.ofVirtual().name("acp-agent-stderr").start(() -> {
            try {
                child.getErrorStream().transferTo(System.err);
            } catch (IOException ignored) {
                // The agent exited; nothing more to copy.
            }
        });
        return new AcpClient(
                AcpConnection.over(child.getInputStream(), child.getOutputStream()).start(), child);
    }

    public AcpClient withRequestTimeout(Duration timeout) {
        this.requestTimeout = timeout;
        return this;
    }

    /** The listener receives the {@code session/update} notification's params node. */
    public AcpClient onSessionUpdate(Consumer<JsonNode> listener) {
        connection.onNotification((method, params) -> {
            if ("session/update".equals(method)) {
                listener.accept(params);
            }
        });
        return this;
    }

    /**
     * Answers ACP {@code session/request_permission} requests without an interactive UI.
     * {@link PermissionPolicy#ALLOW_SINGLE} is deliberately conservative: it approves only
     * when the agent supplies exactly one allow-once choice, so an elicitation represented as
     * several allow choices is never answered arbitrarily.
     */
    public AcpClient withPermissionPolicy(PermissionPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("permission policy is required");
        }
        connection.onRequest((method, params) -> {
            if (!"session/request_permission".equals(method)) {
                throw new AcpError(AcpConnection.METHOD_NOT_FOUND,
                        "unknown method: " + method);
            }
            return permissionResponse(params, policy);
        });
        return this;
    }

    /** Negotiates protocol version 1 and returns the agent's initialize result. */
    public JsonNode initialize() {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("protocolVersion", 1);
        ObjectNode capabilities = params.putObject("clientCapabilities");
        ObjectNode fs = capabilities.putObject("fs");
        fs.put("readTextFile", false);
        fs.put("writeTextFile", false);
        capabilities.put("terminal", false);
        return call("initialize", params);
    }

    /** Opens a session rooted at {@code cwd} and returns its session id. */
    public String newSession(String cwd) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("cwd", cwd);
        params.putArray("mcpServers");
        return call("session/new", params).path("sessionId").asText();
    }

    /** Loads a persisted agent session and returns its session id. */
    public String loadSession(String sessionId, String cwd) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("session id is required");
        }
        ObjectNode params = MAPPER.createObjectNode();
        params.put("sessionId", sessionId);
        params.put("cwd", cwd);
        params.putArray("mcpServers");
        return call("session/load", params).path("sessionId").asText(sessionId);
    }

    /** Sends one prompt turn of plain text and returns its result (the stop reason). */
    public JsonNode prompt(String sessionId, String text) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("sessionId", sessionId);
        ObjectNode block = params.putArray("prompt").addObject();
        block.put("type", "text");
        block.put("text", text);
        return call("session/prompt", params);
    }

    private JsonNode call(String method, JsonNode params) {
        try {
            return connection.request(method, params)
                    .get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AcpError(AcpConnection.INTERNAL_ERROR,
                    method + " did not answer within " + requestTimeout);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AcpError error) {
                throw error;
            }
            throw new AcpError(AcpConnection.INTERNAL_ERROR, String.valueOf(e.getCause()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AcpError(AcpConnection.INTERNAL_ERROR, "interrupted waiting for " + method);
        }
    }

    private static JsonNode permissionResponse(JsonNode params, PermissionPolicy policy) {
        java.util.List<JsonNode> options = new java.util.ArrayList<>();
        JsonNode choices = params == null ? null : params.get("options");
        if (choices != null && choices.isArray()) {
            choices.forEach(options::add);
        }
        java.util.List<JsonNode> matching = options.stream()
                .filter(option -> policy == PermissionPolicy.ALLOW_SINGLE
                        ? "allow_once".equals(option.path("kind").asText())
                        : "reject_once".equals(option.path("kind").asText()))
                .toList();
        ObjectNode response = MAPPER.createObjectNode();
        ObjectNode outcome = response.putObject("outcome");
        boolean selectable = policy == PermissionPolicy.ALLOW_SINGLE
                ? matching.size() == 1 : !matching.isEmpty();
        if (selectable && matching.get(0).path("optionId").isTextual()) {
            outcome.put("outcome", "selected");
            outcome.put("optionId", matching.get(0).path("optionId").asText());
        } else {
            outcome.put("outcome", "cancelled");
        }
        return response;
    }

    @Override
    public void close() {
        connection.close();
        if (process != null) {
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }
    }
}
