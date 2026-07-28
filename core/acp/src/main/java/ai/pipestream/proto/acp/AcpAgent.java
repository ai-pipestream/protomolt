package ai.pipestream.proto.acp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

/**
 * The ACP agent runtime: answers {@code initialize}, {@code session/new}, and
 * {@code session/prompt} over an {@link AcpConnection} and streams prompt output back as
 * {@code session/update} notifications. What a prompt turn does is the {@link PromptHandler}
 * the agent is built with; the runtime owns only the protocol. The agent declares no file,
 * terminal, or permission capabilities; it is read-only. One bad prompt answers with a
 * JSON-RPC error or an error chunk and the session keeps going.
 *
 * <p>{@link #start()} serves in the background while {@link #run()} blocks until the peer
 * closes the stream, which is how an agent process's {@code main} serves stdio.</p>
 */
public final class AcpAgent implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AcpConnection connection;
    private final PromptHandler handler;

    private AcpAgent(AcpConnection connection, PromptHandler handler) {
        this.connection = connection;
        this.handler = handler;
    }

    /**
     * Builds an agent over any pair of streams.
     *
     * @param in the stream client messages are read from (stdin in production)
     * @param out the stream responses and notifications are written to (stdout in production)
     * @param handler what one prompt turn does
     * @return the agent, ready to {@link #start()} or {@link #run()}
     */
    public static AcpAgent over(InputStream in, OutputStream out, PromptHandler handler) {
        AcpAgent agent = new AcpAgent(AcpConnection.over(in, out), handler);
        agent.connection.onRequest(agent::handle);
        return agent;
    }

    /** Starts serving: the read loop and per-request virtual threads run from here. */
    public void start() {
        connection.start();
    }

    /** Starts serving and blocks until the client closes the stream. */
    public void run() {
        start();
        try {
            connection.awaitEnd();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        connection.close();
    }

    private JsonNode handle(String method, JsonNode params) {
        return switch (method) {
            case "initialize" -> initializeResult();
            case "session/new" -> newSessionResult();
            case "session/prompt" -> prompt(params);
            default -> throw new AcpError(AcpConnection.METHOD_NOT_FOUND, "unknown method: " + method);
        };
    }

    /**
     * The handshake the IDE negotiates against: protocol version 1, no load-session, MCP,
     * prompt, or auth capabilities beyond the defaults, and no authentication methods. Field
     * names and shape match what real clients (Zed, JetBrains) read from the wire.
     */
    private static JsonNode initializeResult() {
        ObjectNode result = MAPPER.createObjectNode();
        result.put("protocolVersion", 1);
        ObjectNode capabilities = result.putObject("agentCapabilities");
        capabilities.put("loadSession", false);
        ObjectNode mcp = capabilities.putObject("mcpCapabilities");
        mcp.put("http", false);
        mcp.put("sse", false);
        ObjectNode prompt = capabilities.putObject("promptCapabilities");
        prompt.put("audio", false);
        prompt.put("embeddedContext", false);
        prompt.put("image", false);
        result.putArray("authMethods");
        return result;
    }

    private static JsonNode newSessionResult() {
        ObjectNode result = MAPPER.createObjectNode();
        result.put("sessionId", UUID.randomUUID().toString());
        return result;
    }

    private JsonNode prompt(JsonNode params) {
        String sessionId = params != null ? params.path("sessionId").asText() : "";
        String text = promptText(params);
        PromptContext context = new PromptContext() {
            @Override
            public void sendMessage(String chunk) {
                sendUpdate(sessionId, "agent_message_chunk", chunk);
            }

            @Override
            public void sendThought(String thought) {
                sendUpdate(sessionId, "agent_thought_chunk", thought);
            }
        };
        handler.run(text, context);
        ObjectNode result = MAPPER.createObjectNode();
        result.put("stopReason", "end_turn");
        return result;
    }

    /** The user's prompt is its text content blocks joined by newlines; other types are ignored. */
    private static String promptText(JsonNode params) {
        StringBuilder text = new StringBuilder();
        for (JsonNode block : params != null ? params.path("prompt") : MAPPER.createArrayNode()) {
            if ("text".equals(block.path("type").asText())) {
                if (text.length() > 0) {
                    text.append('\n');
                }
                text.append(block.path("text").asText());
            }
        }
        return text.toString();
    }

    private void sendUpdate(String sessionId, String kind, String text) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("sessionId", sessionId);
        ObjectNode update = params.putObject("update");
        update.put("sessionUpdate", kind);
        ObjectNode content = update.putObject("content");
        content.put("type", "text");
        content.put("text", text);
        connection.notify("session/update", params);
    }
}
