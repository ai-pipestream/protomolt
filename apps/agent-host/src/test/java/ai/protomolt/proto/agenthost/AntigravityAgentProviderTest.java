package ai.protomolt.proto.agenthost;

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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The Antigravity provider against a stand-in {@code agy} that speaks its NDJSON pipe. */
class AntigravityAgentProviderTest {

    @TempDir
    Path temporary;

    @Test
    void startsThenResumesTheConversationAndSumsUsage() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        Path state = temporary.resolve("state/host.json");
        Files.createDirectories(state.getParent());

        try (AntigravityAgentProvider provider = new AntigravityAgentProvider(workspace, state,
                "", AgentRole.WORKER, fake("ok"), "gemini-3.7-flash-high",
                Duration.ofSeconds(30))) {
            assertThat(provider.prompt("first packet")).contains("\"reason\":\"new\"");
            assertThat(provider.sessionId()).isEqualTo(FakeAgyMain.CONVERSATION);
            assertThat(provider.prompt("second packet")).contains("\"reason\":\"resumed\"");
            assertThat(provider.usage()).contains(new AgentProvider.Usage(200, 40));
        }
        assertThat(Files.readString(state.resolveSibling("host.json.schema.json")))
                .contains("handledEventCursors", "delegation-candidate");
    }

    @Test
    void aNonZeroExitSurfacesTheStderrTail() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("failing-workspace"));
        Path state = temporary.resolve("failing/host.json");
        Files.createDirectories(state.getParent());
        try (AntigravityAgentProvider provider = new AntigravityAgentProvider(workspace, state,
                "", AgentRole.WORKER, fake("fail"), null, Duration.ofSeconds(30))) {
            assertThatThrownBy(() -> provider.prompt("packet"))
                    .isInstanceOf(AgentHostException.class)
                    .hasMessageContaining("exited with 1")
                    .hasMessageContaining("quota exhausted");
        }
    }

    @Test
    void aStreamWithoutAResultIsAFailure() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("silent-workspace"));
        Path state = temporary.resolve("silent/host.json");
        Files.createDirectories(state.getParent());
        try (AntigravityAgentProvider provider = new AntigravityAgentProvider(workspace, state,
                "", AgentRole.WORKER, fake("silent"), null, Duration.ofSeconds(30))) {
            assertThatThrownBy(() -> provider.prompt("packet"))
                    .isInstanceOf(AgentHostException.class)
                    .hasMessageContaining("without a result");
        }
    }

    private static List<String> fake(String mode) {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return List.of(java, "-cp", System.getProperty("java.class.path"),
                FakeAgyMain.class.getName(), mode);
    }

    /**
     * A stand-in for {@code agy} in stream-json mode: checks the flags the provider must
     * pass, reads the one user line from stdin, and answers with the documented event
     * shapes. Mode "ok" answers with a schema-shaped structured output, "fail" exits 1 with
     * a message on stderr, and "silent" ends the stream without a result.
     */
    public static final class FakeAgyMain {
        static final String CONVERSATION = "9ec58bfd-4d67-4f5e-83a5-9d907e9c6b1f";

        public static void main(String[] args) throws Exception {
            List<String> arguments = new ArrayList<>(List.of(args));
            String mode = arguments.remove(0);
            require(arguments, "--input-format", "stream-json");
            require(arguments, "--output-format", "stream-json");
            if (!arguments.contains("--dangerously-skip-permissions")) {
                System.exit(11);
            }
            int schema = arguments.indexOf("--json-schema");
            if (schema < 0 || !Files.isRegularFile(Path.of(arguments.get(schema + 1)))) {
                System.exit(12);
            }
            int conversation = arguments.indexOf("--conversation");
            boolean resumed = conversation >= 0;
            if (resumed && !CONVERSATION.equals(arguments.get(conversation + 1))) {
                System.exit(13);
            }
            ObjectMapper mapper = new ObjectMapper();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(System.in, StandardCharsets.UTF_8));
            JsonNode user = mapper.readTree(in.readLine());
            if (!"user".equals(user.path("event").asText())
                    || user.path("message").path("content").asText().isBlank()) {
                System.exit(14);
            }
            if ("fail".equals(mode)) {
                System.err.println("agy: quota exhausted for this account");
                System.exit(1);
            }
            ObjectNode init = mapper.createObjectNode();
            init.put("event", "init");
            init.put("conversation_id", CONVERSATION);
            init.putObject("init").put("permission_mode", "always-proceed");
            System.out.println(init);
            ObjectNode step = mapper.createObjectNode();
            step.put("event", "step_update");
            step.putObject("step_update").put("conversation_id", CONVERSATION)
                    .put("step_type", "agent_response").put("state", "DONE");
            System.out.println(step);
            if ("silent".equals(mode)) {
                return;
            }
            ObjectNode output = mapper.createObjectNode();
            output.putArray("handledEventCursors");
            ObjectNode command = output.putArray("commands").addObject();
            command.put("tool", "host-ack");
            command.putObject("arguments").put("reason", resumed ? "resumed" : "new");
            ObjectNode result = mapper.createObjectNode();
            result.put("event", "result");
            ObjectNode body = result.putObject("result");
            body.put("conversation_id", CONVERSATION);
            body.put("status", "SUCCESS");
            body.put("response", output.toString());
            body.set("structured_output", output);
            body.putObject("usage").put("input_tokens", 100).put("output_tokens", 20)
                    .put("total_tokens", 120);
            System.out.println(result);
        }

        private static void require(List<String> arguments, String flag, String value) {
            int index = arguments.indexOf(flag);
            if (index < 0 || !value.equals(arguments.get(index + 1))) {
                System.exit(10);
            }
        }
    }
}
