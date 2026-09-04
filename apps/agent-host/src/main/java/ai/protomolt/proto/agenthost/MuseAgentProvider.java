package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.msp.MspClient;
import ai.protomolt.proto.msp.MspError;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Meta's Muse Code driven as one long-lived {@code muse serve} session host over MSP. The
 * host owns the process and its pipes (MSP has no authentication of its own), starts one
 * session in the workspace with the {@code allowAll} approval mode so no tool call waits for
 * a person, resumes that session across host restarts, and settles tokens from what the
 * host reports per turn.
 *
 * <p>Exit code 5 from {@code muse serve} means the installed build has no SDK surface and
 * will not serve; that is reported once and not retried. Sandbox posture is fixed by the
 * launch flags and is the operator's choice; Muse's default sandbox has no network and a
 * read-only home, which a Gradle or git workflow cannot run inside.</p>
 */
final class MuseAgentProvider implements AgentProvider {

    static final String CLIENT_NAME = "protomolt_agent_host";
    private static final String APPROVAL_MODE = "allowAll";

    private final Path workspace;
    private final List<String> command;
    private final String model;
    private final Duration timeout;

    private MspClient client;
    private String sessionId;
    private Usage usage = new Usage(0, 0);

    MuseAgentProvider(Path workspace, String savedSessionId, List<String> command,
                      String model, Duration timeout) {
        this.workspace = workspace.toAbsolutePath().normalize();
        this.sessionId = savedSessionId == null ? "" : savedSessionId;
        this.command = List.copyOf(command);
        this.model = model;
        this.timeout = timeout;
        connect();
    }

    @Override
    public String name() {
        return "muse";
    }

    @Override
    public synchronized String sessionId() {
        return sessionId;
    }

    @Override
    public synchronized Optional<Usage> usage() {
        return Optional.of(usage);
    }

    @Override
    public synchronized String prompt(String prompt) {
        MspClient.TurnResult result;
        try {
            result = client.turn(sessionId, prompt);
        } catch (MspError first) {
            if (first.code() == MspError.TURN_FAILED) {
                throw new AgentHostException("Muse " + first.getMessage(), first);
            }
            // The connection itself failed: relaunch the host, resume the session, and give
            // the same prompt one more chance.
            closeClient();
            connect();
            try {
                result = client.turn(sessionId, prompt);
            } catch (MspError second) {
                throw new AgentHostException("Muse turn failed after reconnect", second);
            }
        }
        usage = usage.plus(result.usage().promptTokens(), result.usage().outputTokens());
        String text = result.text().trim();
        if (text.isEmpty()) {
            throw new AgentHostException("Muse returned no agent message");
        }
        return text;
    }

    private void connect() {
        if (command.isEmpty()) {
            throw new AgentHostException("Muse command is empty");
        }
        try {
            client = MspClient.launch(command.toArray(String[]::new))
                    .withRequestTimeout(Duration.ofMinutes(1))
                    .withTurnTimeout(timeout);
            client.initialize(CLIENT_NAME, "1");
        } catch (IOException | RuntimeException e) {
            int exit = client == null ? -1 : client.awaitExit(Duration.ofSeconds(5)).orElse(-1);
            closeClient();
            if (exit == 5) {
                throw new AgentHostException("muse serve exited with 5: this build has no SDK"
                        + " surface and will not serve MSP; nothing to retry", e);
            }
            throw new AgentHostException("could not start muse serve"
                    + (exit >= 0 ? " (exit " + exit + ")" : ""), e);
        }
        try {
            sessionId = sessionId.isBlank() ? freshSession() : resumeOrFreshSession();
        } catch (RuntimeException e) {
            closeClient();
            throw new AgentHostException("could not open the Muse session", e);
        }
    }

    private String freshSession() {
        return client.startSession(workspace.toString(), model, APPROVAL_MODE);
    }

    /**
     * Resumes the saved session, or starts a fresh one when the host no longer has it (an
     * ephemeral host, or a session store that was reset). Only a non-retryable refusal
     * classifies as a lost session; a retryable one, such as an overloaded host, propagates
     * so the reconnect path can try again.
     */
    private String resumeOrFreshSession() {
        try {
            return client.resumeSession(sessionId);
        } catch (MspError e) {
            if (e.retryable()) {
                throw e;
            }
            System.err.println("agent-host: muse serve could not resume session " + sessionId
                    + " (" + e.getMessage() + "); starting a fresh session");
            return freshSession();
        }
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
