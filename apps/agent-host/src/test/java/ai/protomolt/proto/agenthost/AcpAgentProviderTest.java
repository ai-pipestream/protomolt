package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.acp.AcpAgent;
import ai.protomolt.proto.acp.AcpClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AcpAgentProviderTest {

    @TempDir
    Path temporary;

    @Test
    void collectsAgentMessageChunksFromARealAcpChildProcess() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        List<String> command = fakeAgent(FakeAcpMain.class);
        try (AcpAgentProvider provider = new AcpAgentProvider(workspace, "", command,
                Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT)) {
            assertThat(provider.sessionId()).isNotBlank();
            assertThat(provider.prompt("event packet"))
                    .isEqualTo("{\"handledEventCursors\":[],\"commands\":[{\"tool\":"
                            + "\"host-ack\",\"arguments\":{\"reason\":\"observed\"}}]}");
        }
    }

    @Test
    void rebindsAnUnknownSavedSessionWithAFreshProviderSession() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("rebind-workspace"));
        List<String> command = fakeAgent(FakeLoadableAcpMain.class, "recoverable");
        try (AcpAgentProvider provider = new AcpAgentProvider(workspace, "session-stale-gone",
                command, Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT)) {
            assertThat(provider.sessionId()).isEqualTo("session-fresh-0001");
            assertThat(provider.prompt("event packet"))
                    .isEqualTo("session:session-fresh-0001");
        }
    }

    @Test
    void rebindsThePrefixedInvalidParamsReport() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("prefixed-workspace"));
        List<String> command = fakeAgent(FakeLoadableAcpMain.class, "prefixed");
        try (AcpAgentProvider provider = new AcpAgentProvider(workspace, "session-stale-gone",
                command, Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT)) {
            assertThat(provider.sessionId()).isEqualTo("session-fresh-0001");
            assertThat(provider.prompt("event packet"))
                    .isEqualTo("session:session-fresh-0001");
        }
    }

    @Test
    void keepsALoadableSavedSessionWithoutRebinding() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("reload-workspace"));
        List<String> command = fakeAgent(FakeLoadableAcpMain.class, "recoverable");
        try (AcpAgentProvider provider = new AcpAgentProvider(workspace, "session-persisted",
                command, Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT)) {
            assertThat(provider.sessionId()).isEqualTo("session-persisted");
            assertThat(provider.prompt("event packet"))
                    .isEqualTo("session:session-persisted");
        }
    }

    @Test
    void doesNotRecoverFromOtherLoadFailures() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("fatal-workspace"));
        List<String> command = fakeAgent(FakeLoadableAcpMain.class, "fatal");
        assertThatThrownBy(() -> new AcpAgentProvider(workspace, "session-stale-gone",
                command, Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("could not start Kimi ACP")
                .cause().hasMessageContaining("session store corrupted");
    }

    @Test
    void doesNotRecoverWhenTheReportCarriesAnotherCode() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("wrong-code-workspace"));
        List<String> command = fakeAgent(FakeLoadableAcpMain.class, "wrong-code");
        assertThatThrownBy(() -> new AcpAgentProvider(workspace, "session-stale-gone",
                command, Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("could not start Kimi ACP")
                .cause().hasMessageContaining("Unknown sessionId: session-stale-gone");
    }

    @Test
    void doesNotRecoverWhenTheReportNamesAnotherSessionId() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("wrong-id-workspace"));
        List<String> command = fakeAgent(FakeLoadableAcpMain.class, "wrong-id");
        assertThatThrownBy(() -> new AcpAgentProvider(workspace, "session-stale-gone",
                command, Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("could not start Kimi ACP")
                .cause().hasMessageContaining("Unknown sessionId: session-other-owner");
    }

    @Test
    void doesNotRecoverWhenTheReportOmitsTheSessionId() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("empty-suffix-workspace"));
        List<String> command = fakeAgent(FakeLoadableAcpMain.class, "empty-suffix");
        assertThatThrownBy(() -> new AcpAgentProvider(workspace, "session-stale-gone",
                command, Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("could not start Kimi ACP")
                .cause().hasMessageContaining("Unknown sessionId:");
    }

    @Test
    void doesNotRecoverWhenThePhraseIsOnlyMentioned() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("incidental-workspace"));
        List<String> command = fakeAgent(FakeLoadableAcpMain.class, "incidental");
        assertThatThrownBy(() -> new AcpAgentProvider(workspace, "session-stale-gone",
                command, Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("could not start Kimi ACP")
                .cause().hasMessageContaining("session/load failed");
    }

    private static List<String> fakeAgent(Class<?> main, String... args) {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = new java.util.ArrayList<>(List.of(java, "-cp",
                System.getProperty("java.class.path"), main.getName()));
        command.addAll(List.of(args));
        return List.copyOf(command);
    }

    public static final class FakeAcpMain {
        public static void main(String[] args) {
            AcpAgent.over(System.in, System.out, (prompt, context) -> {
                context.sendMessage("{\"handledEventCursors\":[],\"commands\":[");
                context.sendMessage("{\"tool\":\"host-ack\",\"arguments\":"
                        + "{\"reason\":\"observed\"}}]}");
            }).run();
        }
    }

    /**
     * A minimal load-capable ACP agent speaking newline-delimited JSON-RPC on stdio. The
     * first argument selects how an unknown session id is rejected by session/load:
     * "recoverable" and "prefixed" are the two observed Kimi lost-session reports,
     * "wrong-code", "wrong-id", "empty-suffix", and "incidental" are near-miss shapes
     * that must not classify, and any other mode answers loads with a generic store failure.
     * The id "session-persisted" and every id issued by session/new stay loadable, so a
     * rebind is observable as a session id change.
     */
    public static final class FakeLoadableAcpMain {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        public static void main(String[] args) throws Exception {
            String mode = args.length > 0 ? args[0] : "fatal";
            Set<String> loadable = new HashSet<>(Set.of("session-persisted"));
            int fresh = 0;
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                ObjectNode message = (ObjectNode) MAPPER.readTree(line);
                String method = message.path("method").asText();
                JsonNode id = message.get("id");
                JsonNode params = message.get("params");
                switch (method) {
                    case "initialize" -> write(result(id, initializeResult()));
                    case "session/new" -> {
                        fresh++;
                        String sessionId = String.format("session-fresh-%04d", fresh);
                        loadable.add(sessionId);
                        ObjectNode created = MAPPER.createObjectNode();
                        created.put("sessionId", sessionId);
                        write(result(id, created));
                    }
                    case "session/load" -> {
                        String requested = params.path("sessionId").asText();
                        if (loadable.contains(requested)) {
                            ObjectNode loaded = MAPPER.createObjectNode();
                            loaded.put("sessionId", requested);
                            write(result(id, loaded));
                        } else {
                            write(loadError(mode, id, requested));
                        }
                    }
                    case "session/prompt" -> {
                        String sessionId = params.path("sessionId").asText();
                        write(messageChunk(sessionId, "session:" + sessionId));
                        ObjectNode stopped = MAPPER.createObjectNode();
                        stopped.put("stopReason", "end_turn");
                        write(result(id, stopped));
                    }
                    default -> write(error(id, -32601, "unknown method: " + method));
                }
            }
        }

        private static ObjectNode initializeResult() {
            ObjectNode result = MAPPER.createObjectNode();
            result.put("protocolVersion", 1);
            result.putObject("agentCapabilities").put("loadSession", true);
            return result;
        }

        /** The load failure for the scenario under test, mirroring observed Kimi shapes. */
        private static ObjectNode loadError(String mode, JsonNode id, String requested) {
            return switch (mode) {
                case "recoverable" -> error(id, -32602, "Unknown sessionId: " + requested);
                case "prefixed" -> error(id, -32602,
                        "Invalid params: Unknown sessionId: " + requested);
                case "wrong-code" -> error(id, -32603, "Unknown sessionId: " + requested);
                case "wrong-id" -> error(id, -32602, "Unknown sessionId: session-other-owner");
                case "empty-suffix" -> error(id, -32602, "Unknown sessionId:");
                case "incidental" -> error(id, -32602,
                        "session/load failed after the peer log mentioned Unknown sessionId: "
                                + requested);
                default -> error(id, -32603, "session store corrupted");
            };
        }

        private static ObjectNode result(JsonNode id, JsonNode payload) {
            ObjectNode response = MAPPER.createObjectNode();
            response.put("jsonrpc", "2.0");
            response.set("id", id);
            response.set("result", payload);
            return response;
        }

        private static ObjectNode error(JsonNode id, int code, String message) {
            ObjectNode response = MAPPER.createObjectNode();
            response.put("jsonrpc", "2.0");
            response.set("id", id);
            ObjectNode error = response.putObject("error");
            error.put("code", code);
            error.put("message", message);
            return response;
        }

        private static ObjectNode messageChunk(String sessionId, String text) {
            ObjectNode notification = MAPPER.createObjectNode();
            notification.put("jsonrpc", "2.0");
            notification.put("method", "session/update");
            ObjectNode params = notification.putObject("params");
            params.put("sessionId", sessionId);
            ObjectNode update = params.putObject("update");
            update.put("sessionUpdate", "agent_message_chunk");
            ObjectNode content = update.putObject("content");
            content.put("type", "text");
            content.put("text", text);
            return notification;
        }

        private static void write(ObjectNode message) throws Exception {
            System.out.write(MAPPER.writeValueAsBytes(message));
            System.out.write('\n');
            System.out.flush();
        }
    }
}
