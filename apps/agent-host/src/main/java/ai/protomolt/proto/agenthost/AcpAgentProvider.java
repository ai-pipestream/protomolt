package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.acp.AcpClient;
import ai.protomolt.proto.acp.AcpError;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** A long-lived ACP child process, used by Kimi Code. */
final class AcpAgentProvider implements AgentProvider {

    private static final int MAX_RESPONSE_CHARS = 256 * 1024;
    /** JSON-RPC invalid params, the code Kimi uses for a lost provider session. */
    private static final int INVALID_PARAMS = -32602;
    /** Anchored error shapes observed from Kimi for a lost provider session. */
    private static final String UNKNOWN_SESSION_PREFIX = "Unknown sessionId: ";
    private static final String UNKNOWN_SESSION_NESTED_PREFIX =
            "Invalid params: Unknown sessionId: ";

    private final Path workspace;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String name;
    /** The name as it reads in a message: "Kimi", "Cursor". */
    private final String label;
    private final List<String> command;
    private final Duration timeout;
    private final AcpClient.PermissionPolicy permissionPolicy;
    private final List<String> messageChunks = new ArrayList<>();

    private AcpClient client;
    private String sessionId;
    private int responseChars;
    private boolean responseTooLarge;

    /** The Kimi provider: {@code kimi acp} with the default session handling. */
    AcpAgentProvider(Path workspace, String savedSessionId, List<String> command,
                     Duration timeout, AcpClient.PermissionPolicy permissionPolicy) {
        this("kimi", workspace, savedSessionId, command, timeout, permissionPolicy);
    }

    /**
     * An ACP provider under its own name. Cursor ({@code agent acp}) advertises the
     * {@code cursor_login} authentication method, which the provider runs after the
     * handshake, and raises blocking extension requests that an unattended host answers
     * without a person: a multiple-choice question is skipped with a reason that points the
     * agent at the coordinator, and a plan is accepted.
     */
    AcpAgentProvider(String name, Path workspace, String savedSessionId, List<String> command,
                     Duration timeout, AcpClient.PermissionPolicy permissionPolicy) {
        this.name = name;
        this.label = name.isEmpty() ? name
                : Character.toUpperCase(name.charAt(0)) + name.substring(1);
        this.workspace = workspace.toAbsolutePath().normalize();
        this.sessionId = savedSessionId == null ? "" : savedSessionId;
        this.command = List.copyOf(command);
        this.timeout = timeout;
        this.permissionPolicy = permissionPolicy;
        connect();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public synchronized String sessionId() {
        return sessionId;
    }

    @Override
    public String prompt(String prompt) {
        resetResponse();
        try {
            client.prompt(sessionId, prompt);
        } catch (AcpError first) {
            closeClient();
            connect();
            resetResponse();
            try {
                client.prompt(sessionId, prompt);
            } catch (AcpError second) {
                throw new AgentHostException(label + " ACP prompt failed after reconnect", second);
            }
        }
        return completedResponse();
    }

    private void connect() {
        if (command.isEmpty()) {
            throw new AgentHostException(label + " ACP command is empty");
        }
        try {
            client = AcpClient.launch(command.toArray(String[]::new))
                    .withRequestTimeout(timeout)
                    .withPermissionPolicy(permissionPolicy)
                    .withExtensionHandler(AcpAgentProvider::extension)
                    .onSessionUpdate(this::sessionUpdate);
            JsonNode initialized = client.initialize();
            authenticate(initialized);
            boolean canLoad = initialized.path("agentCapabilities")
                    .path("loadSession").asBoolean();
            if (!sessionId.isBlank() && canLoad) {
                sessionId = loadOrFreshSession();
            } else {
                sessionId = client.newSession(workspace.toString());
            }
        } catch (IOException | RuntimeException e) {
            closeClient();
            throw new AgentHostException("could not start " + label + " ACP", e);
        }
    }

    /**
     * Runs the agent's advertised authentication method when there is exactly one, or the
     * Cursor login when it is among several. The credential itself is never here: Cursor
     * reads its CLI login or {@code CURSOR_API_KEY} from the environment.
     */
    private void authenticate(JsonNode initialized) {
        JsonNode methods = initialized.path("authMethods");
        String methodId = null;
        if (AcpClient.advertisesAuthMethod(initialized, "cursor_login")) {
            methodId = "cursor_login";
        } else if (methods.isArray() && methods.size() == 1) {
            methodId = methods.get(0).path("id").asText(null);
        }
        if (methodId != null && !methodId.isBlank()) {
            client.authenticate(methodId);
        }
    }

    /**
     * Cursor's blocking extension requests, answered without a person. A question the agent
     * needs answered belongs to the coordinator, so it is skipped with that direction; a
     * plan is accepted so the agent proceeds to the work the packet asked for.
     */
    static JsonNode extension(String method, JsonNode params) {
        ObjectNode response = MAPPER.createObjectNode();
        ObjectNode outcome = response.putObject("outcome");
        switch (method) {
            case "cursor/ask_question" -> {
                outcome.put("outcome", "skipped");
                outcome.put("reason", "unattended host: put the question to the coordinator"
                        + " with delegation-message and continue with the rest of the packet");
                return response;
            }
            case "cursor/create_plan" -> {
                outcome.put("outcome", "accepted");
                return response;
            }
            default -> {
                return null;
            }
        }
    }

    /**
     * Reloads the saved provider session, or starts a fresh one when the agent explicitly
     * reports the saved id as unknown, which is how a reset session store (a container
     * restart) surfaces. Only that report classifies as a lost provider session; every
     * other load failure propagates. The ProtoMolt cursor, pending commands, and identity
     * live in {@link AgentHostState} and are untouched here; {@code AgentHost} persists the
     * replacement session id on its next sync.
     */
    private String loadOrFreshSession() {
        try {
            return client.loadSession(sessionId, workspace.toString());
        } catch (AcpError e) {
            if (!isUnknownSession(e, sessionId)) {
                throw e;
            }
            return client.newSession(workspace.toString());
        }
    }

    /**
     * Only the explicit lost-session report classifies as a provider-session mismatch: the
     * JSON-RPC invalid-params code carrying a message that equals one of the anchored Kimi
     * forms "Unknown sessionId: <id>" or "Invalid params: Unknown sessionId: <id>" with
     * the exact session id that session/load attempted. Another code, a different or
     * missing session id, a prefixed unrelated sentence, or text that merely mentions the
     * phrase is a normal load failure and propagates.
     */
    private static boolean isUnknownSession(AcpError error, String sessionId) {
        if (error.code() != INVALID_PARAMS) {
            return false;
        }
        String message = error.getMessage();
        return message != null
                && (message.equals(UNKNOWN_SESSION_PREFIX + sessionId)
                || message.equals(UNKNOWN_SESSION_NESTED_PREFIX + sessionId));
    }

    private synchronized void sessionUpdate(JsonNode params) {
        JsonNode update = params.path("update");
        if ("agent_message_chunk".equals(update.path("sessionUpdate").asText())) {
            JsonNode text = update.path("content").path("text");
            if (text.isTextual()) {
                String chunk = text.asText();
                responseChars += chunk.length();
                if (responseChars <= MAX_RESPONSE_CHARS) {
                    messageChunks.add(chunk);
                } else {
                    responseTooLarge = true;
                }
            }
        }
    }

    private synchronized void resetResponse() {
        messageChunks.clear();
        responseChars = 0;
        responseTooLarge = false;
    }

    private synchronized String completedResponse() {
        if (responseTooLarge) {
            throw new AgentHostException(label + " ACP response exceeds the 256 KiB limit");
        }
        String response = String.join("", messageChunks).trim();
        if (response.isEmpty()) {
            throw new AgentHostException(label + " ACP returned no agent message");
        }
        return response;
    }

    private void closeClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public synchronized void close() {
        closeClient();
    }
}
