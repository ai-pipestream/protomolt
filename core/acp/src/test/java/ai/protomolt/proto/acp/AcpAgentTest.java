package ai.protomolt.proto.acp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The agent runtime over in-memory pipes: the initialize handshake shape, session id minting,
 * prompt text extraction, error answers when the handler fails, and the method-not-found
 * default. The golden-transcript test pins the exact wire bytes; this class pins the behavior
 * the {@link PromptHandler} and the client see.
 */
class AcpAgentTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void initializeAdvertisesProtocolOneAndNoCapabilities() {
        TestPipes.End[] ends = TestPipes.pair();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out());
                AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(), (text, context) -> {
                })) {
            agent.start();

            JsonNode result = client.initialize();

            assertThat(result.path("protocolVersion").asInt()).isEqualTo(1);
            JsonNode capabilities = result.path("agentCapabilities");
            assertThat(capabilities.path("loadSession").asBoolean()).isFalse();
            assertThat(capabilities.path("mcpCapabilities").path("http").asBoolean()).isFalse();
            assertThat(capabilities.path("mcpCapabilities").path("sse").asBoolean()).isFalse();
            JsonNode prompt = capabilities.path("promptCapabilities");
            assertThat(prompt.path("audio").asBoolean()).isFalse();
            assertThat(prompt.path("embeddedContext").asBoolean()).isFalse();
            assertThat(prompt.path("image").asBoolean()).isFalse();
            assertThat(result.path("authMethods").isArray()).isTrue();
            assertThat(result.path("authMethods")).isEmpty();
        }
    }

    @Test
    void everySessionGetsAFreshId() {
        TestPipes.End[] ends = TestPipes.pair();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out());
                AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(), (text, context) -> {
                })) {
            agent.start();

            String first = client.newSession("/a");
            String second = client.newSession("/b");

            assertThat(first).isNotBlank();
            assertThat(second).isNotBlank();
            assertThat(first).isNotEqualTo(second);
        }
    }

    @Test
    void unknownMethodAnswersMethodNotFound() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(), (text, context) -> {
        });
        agent.start();
        try (JsonPipe wire = JsonPipe.over(ends[0].in(), ends[0].out())) {
            wire.send("{\"jsonrpc\":\"2.0\",\"id\":\"9\",\"method\":\"agent/bogus\"}");

            JsonNode error = MAPPER.readTree(wire.take()).path("error");
            assertThat(error.path("code").asInt()).isEqualTo(AcpConnection.METHOD_NOT_FOUND);
            assertThat(error.path("message").asText()).contains("agent/bogus");
        } finally {
            agent.close();
        }
    }

    @Test
    void promptTextJoinsTextBlocksAndIgnoresOtherTypes() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        BlockingQueue<String> seen = new ArrayBlockingQueue<>(2);
        AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(),
                (text, context) -> seen.add(text));
        agent.start();
        try (JsonPipe wire = JsonPipe.over(ends[0].in(), ends[0].out())) {
            wire.send("{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"method\":\"session/prompt\",\"params\":"
                    + "{\"sessionId\":\"s1\",\"prompt\":["
                    + "{\"type\":\"text\",\"text\":\"first\"},"
                    + "{\"type\":\"image\",\"data\":\"aXN0bw==\"},"
                    + "{\"type\":\"text\",\"text\":\"second\"}]}}");

            JsonNode result = MAPPER.readTree(wire.take()).path("result");
            assertThat(result.path("stopReason").asText()).isEqualTo("end_turn");
            String text = seen.poll(30, TimeUnit.SECONDS);
            assertThat(text).isEqualTo("first\nsecond");

            // A prompt with no params at all still runs the turn, with empty text.
            wire.send("{\"jsonrpc\":\"2.0\",\"id\":\"2\",\"method\":\"session/prompt\"}");
            result = MAPPER.readTree(wire.take()).path("result");
            assertThat(result.path("stopReason").asText()).isEqualTo("end_turn");
            assertThat(seen.poll(30, TimeUnit.SECONDS)).isEmpty();
        } finally {
            agent.close();
        }
    }

    @Test
    void handlerFailureAnswersInternalErrorAndTheSessionSurvives() {
        TestPipes.End[] ends = TestPipes.pair();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out());
                AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(), (text, context) -> {
                    if ("boom".equals(text)) {
                        throw new IllegalStateException("boom");
                    }
                    context.sendMessage("ok: " + text);
                })) {
            agent.start();
            String sessionId = client.newSession("/workspace");

            assertThatThrownBy(() -> client.prompt(sessionId, "boom"))
                    .isInstanceOfSatisfying(AcpError.class, error -> {
                        assertThat(error.code()).isEqualTo(AcpConnection.INTERNAL_ERROR);
                        assertThat(error.getMessage()).contains("boom");
                    });

            JsonNode recovered = client.prompt(sessionId, "fine");
            assertThat(recovered.path("stopReason").asText()).isEqualTo("end_turn");
        }
    }

    @Test
    void handlerAcpErrorKeepsItsCodeAndMessage() {
        TestPipes.End[] ends = TestPipes.pair();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out());
                AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(), (text, context) -> {
                    throw new AcpError(-32042, "custom failure");
                })) {
            agent.start();
            String sessionId = client.newSession("/workspace");

            assertThatThrownBy(() -> client.prompt(sessionId, "anything"))
                    .isInstanceOfSatisfying(AcpError.class, error -> {
                        assertThat(error.code()).isEqualTo(-32042);
                        assertThat(error.getMessage()).isEqualTo("custom failure");
                    });
        }
    }

    @Test
    void promptStreamsThoughtAndMessageChunksWithTheSessionId() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        List<JsonNode> updates = new CopyOnWriteArrayList<>();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())
                .onSessionUpdate(updates::add);
                AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(), (text, context) -> {
                    context.sendThought("thinking");
                    context.sendMessage("answer");
                })) {
            agent.start();
            String sessionId = client.newSession("/workspace");

            JsonNode result = client.prompt(sessionId, "hi");

            assertThat(result.path("stopReason").asText()).isEqualTo("end_turn");
            assertThat(updates).hasSize(2);
            assertThat(updates).allSatisfy(params ->
                    assertThat(params.path("sessionId").asText()).isEqualTo(sessionId));
            assertThat(updates.get(0).path("update").path("sessionUpdate").asText())
                    .isEqualTo("agent_thought_chunk");
            assertThat(updates.get(0).path("update").path("content").path("text").asText())
                    .isEqualTo("thinking");
            assertThat(updates.get(1).path("update").path("sessionUpdate").asText())
                    .isEqualTo("agent_message_chunk");
            assertThat(updates.get(1).path("update").path("content").path("text").asText())
                    .isEqualTo("answer");
        }
    }
}
