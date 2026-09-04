package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.acp.AcpConnection;
import ai.protomolt.proto.acp.AcpError;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The Muse provider against a stand-in {@code muse serve} speaking MSP on stdio. */
class MuseAgentProviderTest {

    @TempDir
    Path temporary;

    @Test
    void startsASessionRunsATurnAndSumsUsage() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        try (MuseAgentProvider provider = new MuseAgentProvider(workspace, "", fake("serve"),
                "muse-spark-1.3", Duration.ofSeconds(30))) {
            assertThat(provider.sessionId()).isEqualTo("ses-fresh-1");
            assertThat(provider.prompt("event packet"))
                    .isEqualTo("{\"handledEventCursors\":[],\"commands\":[{\"tool\":"
                            + "\"host-ack\",\"arguments\":{\"reason\":\"ses-fresh-1\"}}]}");
            assertThat(provider.prompt("second packet")).contains("ses-fresh-1");
            assertThat(provider.usage()).contains(new AgentProvider.Usage(24, 10));
        }
    }

    @Test
    void resumesAKnownSessionAndReplacesAnUnknownOne() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("resume-workspace"));
        try (MuseAgentProvider provider = new MuseAgentProvider(workspace, "ses-known",
                fake("serve"), null, Duration.ofSeconds(30))) {
            assertThat(provider.sessionId()).isEqualTo("ses-known");
            assertThat(provider.prompt("packet")).contains("ses-known");
        }
        try (MuseAgentProvider provider = new MuseAgentProvider(workspace, "ses-gone",
                fake("serve"), null, Duration.ofSeconds(30))) {
            assertThat(provider.sessionId()).isEqualTo("ses-fresh-1");
        }
    }

    @Test
    void aFailedTurnCarriesTheHostsReason() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("failing-workspace"));
        try (MuseAgentProvider provider = new MuseAgentProvider(workspace, "", fake("serve"),
                null, Duration.ofSeconds(30))) {
            assertThatThrownBy(() -> provider.prompt("please fail"))
                    .isInstanceOf(AgentHostException.class)
                    .hasMessageContaining("turn failed")
                    .hasMessageContaining("provider refused the request");
        }
    }

    @Test
    void exitFiveMeansTheBuildCannotServeAndIsNotRetried() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("exit5-workspace"));
        assertThatThrownBy(() -> new MuseAgentProvider(workspace, "", fake("exit5"), null,
                Duration.ofSeconds(30)))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("exited with 5")
                .hasMessageContaining("no SDK surface");
    }

    private static List<String> fake(String mode) {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return List.of(java, "-cp", System.getProperty("java.class.path"),
                FakeMuseMain.class.getName(), mode);
    }

    /**
     * A stand-in {@code muse serve}: the handshake, session start and resume, and a turn
     * whose agent message is a host command batch naming the session. A prompt containing
     * "fail" ends in the failed terminal. Mode "exit5" exits with the code that means the
     * build has no SDK surface.
     */
    public static final class FakeMuseMain {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        public static void main(String[] args) throws Exception {
            if (args.length > 0 && "exit5".equals(args[0])) {
                System.err.println("muse serve: SDK surface unavailable");
                System.exit(5);
            }
            AcpConnection connection = AcpConnection.over(System.in, System.out);
            int[] fresh = {0};
            connection.onRequest((method, params) -> switch (method) {
                case "initialize" -> {
                    if (!params.path("clientInfo").path("name").asText().matches("[a-z0-9_]+")) {
                        throw new AcpError(-32602, "invalid client name");
                    }
                    ObjectNode result = MAPPER.createObjectNode();
                    result.putObject("serverInfo").put("name", "muse").put("version", "1.0.3");
                    result.put("sessionDurability", "ephemeral");
                    yield result;
                }
                case "session/start" -> {
                    requireCommandId(params);
                    if (!"allowAll".equals(params.path("approvalMode").asText())) {
                        throw new AcpError(-32602, "unattended runs select allowAll");
                    }
                    fresh[0]++;
                    yield session("ses-fresh-" + fresh[0]);
                }
                case "session/resume" -> {
                    requireCommandId(params);
                    String id = params.path("sessionId").asText();
                    if (!"ses-known".equals(id)) {
                        throw new AcpError(-32040, "session not found: " + id);
                    }
                    ObjectNode result = session(id);
                    result.putArray("pendingRequests");
                    result.putObject("history").put("mode", "none");
                    yield result;
                }
                case "turn/start" -> {
                    requireCommandId(params);
                    String turnId = params.path("commandId").asText();
                    String sessionId = params.path("sessionId").asText();
                    String text = params.path("input").get(0).path("text").asText();
                    ObjectNode ack = MAPPER.createObjectNode();
                    ack.put("commandId", turnId);
                    ack.put("turnId", turnId);
                    ack.put("disposition", "started");
                    ack.put("startedNewTurn", true);
                    ack.put("status", "accepted");
                    Thread.ofVirtual().start(() -> finishTurn(connection, sessionId, turnId,
                            text.contains("fail")));
                    yield ack;
                }
                default -> throw new AcpError(-32601, "unknown method: " + method);
            });
            connection.start();
            connection.awaitEnd();
        }

        private static void finishTurn(AcpConnection connection, String sessionId,
                                       String turnId, boolean fail) {
            ObjectNode completed = MAPPER.createObjectNode();
            completed.put("sessionId", sessionId);
            completed.put("turnId", turnId);
            completed.put("viewCursor", "v2");
            completed.putObject("sourceRange");
            if (fail) {
                completed.put("terminal", "failed");
                ObjectNode error = completed.putObject("error");
                error.put("kind", "providerRejected");
                error.put("message", "provider refused the request");
                error.put("retryable", false);
                connection.notify("turn/completed", completed);
                return;
            }
            ObjectNode item = MAPPER.createObjectNode();
            item.put("sessionId", sessionId);
            item.put("viewCursor", "v1");
            item.putObject("sourceRange");
            ObjectNode message = item.putObject("item");
            message.put("itemId", turnId + "-message");
            message.put("kind", "agentMessage");
            message.put("status", "completed");
            message.put("turnId", turnId);
            message.put("text", "{\"handledEventCursors\":[],\"commands\":[{\"tool\":"
                    + "\"host-ack\",\"arguments\":{\"reason\":\"" + sessionId + "\"}}]}");
            connection.notify("item/completed", item);
            ObjectNode usage = MAPPER.createObjectNode();
            usage.put("sessionId", sessionId);
            usage.put("turnId", turnId);
            usage.put("promptTokens", 12);
            usage.put("totalTokens", 17);
            usage.putObject("cumulative").put("promptTokens", 12).put("outputTokens", 5)
                    .put("totalTokens", 17);
            connection.notify("session/tokenUsage", usage);
            completed.put("terminal", "completed");
            completed.putObject("usage").put("inputTokens", 12).put("outputTokens", 5);
            connection.notify("turn/completed", completed);
        }

        private static ObjectNode session(String id) {
            ObjectNode result = MAPPER.createObjectNode();
            ObjectNode session = result.putObject("session");
            session.put("id", id);
            session.put("status", "idle");
            session.putNull("activeTurnId");
            result.put("viewCursor", "v0");
            return result;
        }

        private static void requireCommandId(JsonNode params) {
            if (!params.path("commandId").asText().matches(
                    "[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")) {
                throw new AcpError(-32602, "commandId must be a UUIDv7");
            }
        }
    }
}
