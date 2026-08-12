package ai.pipestream.proto.agenthost;

import ai.pipestream.proto.acp.AcpClient;
import ai.pipestream.proto.acp.AcpError;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** A long-lived ACP child process, used by Kimi Code. */
final class AcpAgentProvider implements AgentProvider {

    private static final int MAX_RESPONSE_CHARS = 256 * 1024;

    private final Path workspace;
    private final List<String> command;
    private final Duration timeout;
    private final AcpClient.PermissionPolicy permissionPolicy;
    private final List<String> messageChunks = new ArrayList<>();

    private AcpClient client;
    private String sessionId;
    private int responseChars;
    private boolean responseTooLarge;

    AcpAgentProvider(Path workspace, String savedSessionId, List<String> command,
                     Duration timeout, AcpClient.PermissionPolicy permissionPolicy) {
        this.workspace = workspace.toAbsolutePath().normalize();
        this.sessionId = savedSessionId == null ? "" : savedSessionId;
        this.command = List.copyOf(command);
        this.timeout = timeout;
        this.permissionPolicy = permissionPolicy;
        connect();
    }

    @Override
    public String name() {
        return "kimi";
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
                throw new AgentHostException("Kimi ACP prompt failed after reconnect", second);
            }
        }
        return completedResponse();
    }

    private void connect() {
        if (command.isEmpty()) {
            throw new AgentHostException("Kimi ACP command is empty");
        }
        try {
            client = AcpClient.launch(command.toArray(String[]::new))
                    .withRequestTimeout(timeout)
                    .withPermissionPolicy(permissionPolicy)
                    .onSessionUpdate(this::sessionUpdate);
            JsonNode initialized = client.initialize();
            boolean canLoad = initialized.path("agentCapabilities")
                    .path("loadSession").asBoolean();
            if (!sessionId.isBlank() && canLoad) {
                sessionId = client.loadSession(sessionId, workspace.toString());
            } else {
                sessionId = client.newSession(workspace.toString());
            }
        } catch (IOException | RuntimeException e) {
            closeClient();
            throw new AgentHostException("could not start Kimi ACP", e);
        }
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
            throw new AgentHostException("Kimi ACP response exceeds the 256 KiB limit");
        }
        String response = String.join("", messageChunks).trim();
        if (response.isEmpty()) {
            throw new AgentHostException("Kimi ACP returned no agent message");
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
