package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CodexAgentProviderTest {

    @TempDir
    Path temporary;

    @Test
    void startsThenResumesTheSameThreadThroughRealChildProcesses() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        Path state = temporary.resolve("state/host.json");
        Files.createDirectories(state.getParent());
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> executable = List.of(java, "-cp", System.getProperty("java.class.path"),
                FakeCodexMain.class.getName());

        try (CodexAgentProvider provider = new CodexAgentProvider(workspace, state, "",
                AgentRole.COORDINATOR, executable, null, Duration.ofSeconds(30))) {
            assertThat(provider.prompt("first packet")).contains("new-thread");
            assertThat(provider.sessionId()).isEqualTo(FakeCodexMain.THREAD);
            assertThat(provider.prompt("second packet")).contains("resumed-thread");
        }
        assertThat(Files.readString(state.resolveSibling("host.json.schema.json")))
                .contains("handledEventCursors", "delegation-review");
    }

    public static final class FakeCodexMain {
        static final String THREAD = "11111111-2222-4333-8444-555555555555";

        public static void main(String[] args) throws Exception {
            List<String> arguments = List.of(args);
            int outputIndex = arguments.indexOf("--output-last-message");
            if (outputIndex < 0 || !arguments.contains("--output-schema")) {
                System.exit(9);
            }
            boolean resumed = arguments.contains("resume");
            if ((!resumed && (!arguments.contains("--approve-for-me")
                    || arguments.contains("--sandbox")))
                    || (resumed && arguments.contains("--approve-for-me"))) {
                System.exit(10);
            }
            Path output = Path.of(arguments.get(outputIndex + 1));
            new String(System.in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            String reason = resumed ? "resumed-thread" : "new-thread";
            ObjectMapper mapper = new ObjectMapper();
            var response = mapper.createObjectNode();
            response.putArray("handledEventCursors");
            var command = response.putArray("commands").addObject();
            command.put("tool", "host-ack");
            command.putObject("arguments").put("reason", reason);
            Files.writeString(output, response.toString());
            System.out.println("{\"type\":\"thread.started\",\"thread_id\":\""
                    + THREAD + "\"}");
        }
    }
}
