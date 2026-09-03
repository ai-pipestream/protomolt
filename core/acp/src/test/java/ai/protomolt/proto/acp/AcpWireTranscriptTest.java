package ai.protomolt.proto.acp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replays the golden transcript under {@code src/test/resources/acp}: the exact bytes the
 * exchange with the third-party SDK produced (cleaned of its duplicated discriminator
 * keys), cross-checked against the ACP spec. The prompt handler stands in for the application
 * and answers with the payloads the transcript carries, so every byte the runtime writes
 * around them (the framing, the id echo, the {@code session/update} envelope) must equal the
 * golden line: the transport stays message-faithful to what real IDEs (Zed, JetBrains) parse.
 */
class AcpWireTranscriptTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void theWireMatchesTheGoldenTranscript() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(), AcpWireTranscriptTest::scripted);
        agent.start();
        try (JsonPipe wire = JsonPipe.over(ends[0].in(), ends[0].out())) {
            List<String> golden;
            try (InputStream resource = getClass().getResourceAsStream("/acp/wire-transcript.ndjson")) {
                assertThat(resource).as("golden transcript on the test classpath").isNotNull();
                golden = new String(resource.readAllBytes(), StandardCharsets.UTF_8).lines().toList();
            }
            String sessionId = null;
            for (String line : golden) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith(">> ")) {
                    String message = line.substring(3);
                    wire.send(sessionId == null ? message : message.replace("$SESSION", sessionId));
                } else if (line.startsWith("<< ")) {
                    String actual = wire.take();
                    if (sessionId == null && actual.contains("sessionId")) {
                        sessionId = MAPPER.readTree(actual).path("result").path("sessionId").asText();
                        assertThat(sessionId).isNotBlank();
                    }
                    JsonNode expected = MAPPER.readTree(
                            line.substring(3).replace("$SESSION", sessionId == null ? "" : sessionId));
                    assertThat(MAPPER.readTree(actual)).isEqualTo(expected);
                }
            }
        } finally {
            agent.close();
        }
    }

    /** Answers with the payloads the golden transcript carries for each prompt line. */
    private static void scripted(String text, PromptContext context) {
        if (text.startsWith("compile ")) {
            context.sendThought("running compile");
            context.sendMessage("{\n"
                    + "  \"ok\" : true,\n"
                    + "  \"files\" : [ \"p/m.proto\" ],\n"
                    + "  \"descriptorSetBase64\" :"
                    + " \"CicKCXAvbS5wcm90bxIBcCIPCgFNEgoKAmlkGAEgASgJYgZwcm90bzM=\"\n"
                    + "}");
        } else {
            context.sendMessage("Unknown verb '" + text + "'. Try 'list' to see them.");
        }
    }
}
