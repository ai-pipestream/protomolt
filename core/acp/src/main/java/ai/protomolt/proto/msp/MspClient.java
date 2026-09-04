package ai.protomolt.proto.msp;

import ai.protomolt.proto.acp.AcpConnection;
import ai.protomolt.proto.acp.AcpError;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A blocking client for the Muse Session Protocol (MSP), the JSON-RPC session host behind
 * {@code muse serve}: newline-delimited JSON-RPC 2.0 on the host's stdio, the same framing
 * as ACP. The client owns the host process it launched and the pipes to it; MSP v1 has no
 * authentication, so nothing else may ever reach those pipes.
 *
 * <p>The surface is what an unattended driver needs: the handshake, one session started or
 * resumed, one prompt turn at a time that blocks until the turn's durable terminal record
 * lands, the accumulated agent message of that turn, and the session's token accounting.
 * Approvals the host raises are answered from the session's approval mode; a session started
 * with {@code allowAll} raises none, and any that still arrives is approved when an approved
 * choice exists.</p>
 */
public final class MspClient implements AutoCloseable {

    /** Token counters as MSP reports them, counted once under the provider's convention. */
    public record TokenUsage(long promptTokens, long outputTokens, long totalTokens) {
        public static final TokenUsage NONE = new TokenUsage(0, 0, 0);
    }

    /** The outcome of one prompt turn. */
    public record TurnResult(String turnId, String terminal, String text, TokenUsage usage) {
        public boolean completed() {
            return "completed".equals(terminal);
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = Logger.getLogger(MspClient.class.getName());
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration DEFAULT_TURN_TIMEOUT = Duration.ofMinutes(30);

    private final AcpConnection connection;
    private final Process process;
    private final Map<String, PendingTurn> turns = new ConcurrentHashMap<>();
    private volatile Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private volatile Duration turnTimeout = DEFAULT_TURN_TIMEOUT;
    private volatile TokenUsage cumulative = TokenUsage.NONE;
    private volatile JsonNode serverInfo = MAPPER.createObjectNode();

    private MspClient(AcpConnection connection, Process process) {
        this.connection = connection;
        this.process = process;
        connection.onNotification(this::notification);
        connection.onRequest((method, params) -> {
            throw new AcpError(AcpConnection.METHOD_NOT_FOUND, "unknown method: " + method);
        });
    }

    /** A client over any pair of streams, typically one end of an in-memory pipe in tests. */
    public static MspClient over(InputStream in, OutputStream out) {
        return new MspClient(AcpConnection.over(in, out).start(), null);
    }

    /**
     * Launches the session host (normally {@code muse serve ...}) and speaks MSP over its
     * stdio. The host's stderr is not a contract but is where startup failures appear, so it
     * is copied to this process's stderr rather than discarded.
     */
    public static MspClient launch(String... command) throws IOException {
        Process child = new ProcessBuilder(command).start();
        Thread.ofVirtual().name("msp-host-stderr").start(() -> {
            try {
                child.getErrorStream().transferTo(System.err);
            } catch (IOException ignored) {
                // The host exited; nothing more to copy.
            }
        });
        return new MspClient(
                AcpConnection.over(child.getInputStream(), child.getOutputStream()).start(), child);
    }

    public MspClient withRequestTimeout(Duration timeout) {
        this.requestTimeout = Objects.requireNonNull(timeout, "timeout");
        return this;
    }

    /** How long one prompt turn may run before the client gives up waiting for its terminal. */
    public MspClient withTurnTimeout(Duration timeout) {
        this.turnTimeout = Objects.requireNonNull(timeout, "timeout");
        return this;
    }

    /**
     * Performs the handshake: {@code initialize} with the client identity (the name must
     * match {@code [a-z0-9_]+}), then the {@code initialized} notification once the result
     * is in, which is the order the host enforces.
     *
     * @return the host's initialize result (server info, schema fingerprint, muse home)
     */
    public JsonNode initialize(String clientName, String clientVersion) {
        if (clientName == null || !clientName.matches("[a-z0-9_]+")) {
            throw new IllegalArgumentException("MSP client name must match [a-z0-9_]+");
        }
        ObjectNode params = MAPPER.createObjectNode();
        ObjectNode info = params.putObject("clientInfo");
        info.put("name", clientName);
        info.put("version", clientVersion == null ? "0" : clientVersion);
        params.putObject("capabilities");
        JsonNode result = call("initialize", params);
        serverInfo = result;
        connection.notify("initialized", null);
        return result;
    }

    /** The initialize result, empty before the handshake. */
    public JsonNode serverInfo() {
        return serverInfo;
    }

    /**
     * Starts a root session rooted at the workspace with the named approval mode, one of the
     * modes the host already defines ({@code allowAll} for an unattended run).
     *
     * @return the session id the host minted
     */
    public String startSession(String workspaceRoot, String modelId, String approvalMode) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("commandId", Uuid7.nextString());
        if (workspaceRoot != null) {
            params.put("workspaceRoot", workspaceRoot);
        }
        if (modelId != null && !modelId.isBlank()) {
            params.put("modelId", modelId);
        }
        if (approvalMode != null && !approvalMode.isBlank()) {
            params.put("approvalMode", approvalMode);
        }
        JsonNode result = call("session/start", params);
        String sessionId = result.path("session").path("id").asText();
        if (sessionId.isEmpty()) {
            throw new MspError(AcpConnection.INTERNAL_ERROR,
                    "session/start answered without a session id");
        }
        return sessionId;
    }

    /**
     * Resumes a persisted session without replaying its history. Pending server requests
     * (an approval still waiting) are re-issued by the host right after the result and are
     * answered by this client's handlers.
     *
     * @return the session id, as the host reports it
     */
    public String resumeSession(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("session id is required");
        }
        ObjectNode params = MAPPER.createObjectNode();
        params.put("commandId", Uuid7.nextString());
        params.put("sessionId", sessionId);
        params.put("excludeItems", true);
        JsonNode result = call("session/resume", params);
        return result.path("session").path("id").asText(sessionId);
    }

    /**
     * Submits one text prompt and blocks until the turn's terminal record lands. The text of
     * the result is the accumulated agent message of that turn; a turn that ends in the
     * {@code failed} terminal raises {@link MspError} with the host's failure message.
     */
    public TurnResult turn(String sessionId, String text) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("session id is required");
        }
        ObjectNode params = MAPPER.createObjectNode();
        String commandId = Uuid7.nextString();
        params.put("commandId", commandId);
        params.put("sessionId", sessionId);
        ObjectNode part = params.putArray("input").addObject();
        part.put("type", "text");
        part.put("text", text);
        params.put("ifBusy", "queue");
        // A fresh turn's id derives from its command id, so the collector is registered
        // before the ack: an item that completes between the ack and the registration would
        // otherwise be lost.
        PendingTurn pending = new PendingTurn();
        turns.put(commandId, pending);
        String turnId;
        try {
            JsonNode ack = call("turn/start", params);
            turnId = ack.path("turnId").asText();
            if (turnId.isEmpty()) {
                throw new MspError(AcpConnection.INTERNAL_ERROR,
                        "turn/start answered without a turn id");
            }
            if (!turnId.equals(commandId)) {
                turns.put(turnId, pending);
            }
        } catch (RuntimeException e) {
            turns.remove(commandId);
            throw e;
        }
        try {
            JsonNode completed = pending.terminal.get(turnTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
            String terminal = completed.path("terminal").asText();
            TokenUsage usage = usage(completed.path("usage"));
            if ("failed".equals(terminal)) {
                JsonNode error = completed.path("error");
                throw new MspError(MspError.TURN_FAILED, "turn failed: "
                        + error.path("kind").asText("unknown") + ": "
                        + error.path("message").asText(completed.path("reason").asText("")),
                        error.path("retryable").asBoolean(false));
            }
            return new TurnResult(turnId, terminal, pending.text.toString(), usage);
        } catch (TimeoutException e) {
            throw new MspError(AcpConnection.INTERNAL_ERROR,
                    "turn " + turnId + " did not complete within " + turnTimeout);
        } catch (ExecutionException e) {
            throw new MspError(AcpConnection.INTERNAL_ERROR, String.valueOf(e.getCause()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MspError(AcpConnection.INTERNAL_ERROR, "interrupted waiting for the turn");
        } finally {
            turns.remove(commandId);
            turns.remove(turnId);
        }
    }

    /** The session's running counted-once totals, from the last {@code session/tokenUsage}. */
    public TokenUsage cumulativeUsage() {
        return cumulative;
    }

    /** The launched host's exit code, or empty while it runs or when no process was launched. */
    public java.util.OptionalInt exitCode() {
        if (process == null || process.isAlive()) {
            return java.util.OptionalInt.empty();
        }
        return java.util.OptionalInt.of(process.exitValue());
    }

    /**
     * Waits up to the given time for the launched host to exit and returns its exit code, or
     * empty when it is still running (or was never launched). A host that refuses to serve
     * closes its stdout before its process ends, so a caller that saw the stream close
     * waits here before classifying the exit.
     */
    public java.util.OptionalInt awaitExit(Duration wait) {
        if (process == null) {
            return java.util.OptionalInt.empty();
        }
        try {
            if (!process.waitFor(wait.toMillis(), TimeUnit.MILLISECONDS)) {
                return java.util.OptionalInt.empty();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return java.util.OptionalInt.empty();
        }
        return java.util.OptionalInt.of(process.exitValue());
    }

    private void notification(String method, JsonNode params) {
        switch (method) {
            case "item/completed" -> {
                JsonNode item = params.path("item");
                if ("agentMessage".equals(item.path("kind").asText())) {
                    PendingTurn pending = turns.get(item.path("turnId").asText());
                    if (pending != null) {
                        pending.text.setLength(0);
                        pending.text.append(item.path("text").asText());
                    }
                }
            }
            case "turn/completed" -> {
                PendingTurn pending = turns.get(params.path("turnId").asText());
                if (pending != null) {
                    pending.terminal.complete(params);
                }
            }
            case "session/tokenUsage" -> {
                JsonNode totals = params.path("cumulative");
                if (totals.isObject()) {
                    cumulative = new TokenUsage(totals.path("promptTokens").asLong(),
                            totals.path("outputTokens").asLong(),
                            totals.path("totalTokens").asLong());
                }
            }
            case "approval/requested" -> Thread.ofVirtual().name("msp-approval")
                    .start(() -> decide(params));
            default -> {
                // Item deltas, view gaps, and the rest of the feed are not needed by a
                // driver that reads completed items only.
            }
        }
    }

    /**
     * Answers an approval the host raised while unattended: the first choice whose decision
     * approves, else the first choice at all. The requirement reference is echoed verbatim,
     * which is what guards a stale decision against a later approval stage.
     */
    private void decide(JsonNode request) {
        JsonNode choices = request.path("availableChoices");
        JsonNode chosen = null;
        for (JsonNode choice : choices) {
            if (choice.path("decision").asText().startsWith("approved")) {
                chosen = choice;
                break;
            }
        }
        if (chosen == null && choices.isArray() && !choices.isEmpty()) {
            chosen = choices.get(0);
        }
        if (chosen == null) {
            LOG.warning(() -> "msp: approval " + request.path("approvalId").asText()
                    + " offered no choices; leaving it pending");
            return;
        }
        ObjectNode params = MAPPER.createObjectNode();
        params.put("commandId", Uuid7.nextString());
        params.put("sessionId", request.path("sessionId").asText());
        params.put("approvalId", request.path("approvalId").asText());
        params.put("choiceId", chosen.path("choiceId").asText());
        params.set("requirementId", request.path("currentRequirementId"));
        try {
            call("approval/decide", params);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, e, () -> "msp: approval/decide failed for "
                    + request.path("approvalId").asText());
        }
    }

    private static TokenUsage usage(JsonNode usage) {
        if (!usage.isObject()) {
            return TokenUsage.NONE;
        }
        long input = usage.path("inputTokens").asLong();
        long output = usage.path("outputTokens").asLong();
        return new TokenUsage(input, output, input + output);
    }

    private JsonNode call(String method, JsonNode params) {
        try {
            return connection.request(method, params)
                    .get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new MspError(AcpConnection.INTERNAL_ERROR,
                    method + " did not answer within " + requestTimeout);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AcpError error) {
                // The host's error table marks overloaded (-32001) and backpressured
                // (-32031) as the retryable codes.
                throw new MspError(error.code(), error.getMessage(),
                        error.code() == -32001 || error.code() == -32031);
            }
            throw new MspError(AcpConnection.INTERNAL_ERROR, String.valueOf(e.getCause()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MspError(AcpConnection.INTERNAL_ERROR, "interrupted waiting for " + method);
        }
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

    private static final class PendingTurn {
        private final StringBuilder text = new StringBuilder();
        private final CompletableFuture<JsonNode> terminal = new CompletableFuture<>();
    }
}
