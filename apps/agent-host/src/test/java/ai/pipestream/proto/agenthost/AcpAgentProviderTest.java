package ai.pipestream.proto.agenthost;

import ai.pipestream.proto.acp.AcpAgent;
import ai.pipestream.proto.acp.AcpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AcpAgentProviderTest {

    @TempDir
    Path temporary;

    @Test
    void collectsAgentMessageChunksFromARealAcpChildProcess() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = List.of(java, "-cp", System.getProperty("java.class.path"),
                FakeAcpMain.class.getName());
        try (AcpAgentProvider provider = new AcpAgentProvider(workspace, "", command,
                Duration.ofSeconds(30), AcpClient.PermissionPolicy.REJECT)) {
            assertThat(provider.sessionId()).isNotBlank();
            assertThat(provider.prompt("event packet"))
                    .isEqualTo("{\"handledEventCursors\":[],\"commands\":[{\"tool\":"
                            + "\"host-ack\",\"arguments\":{\"reason\":\"observed\"}}]}");
        }
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
}
