package ai.protomolt.proto.acp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Connection behavior over in-memory pipes: the request/response exchange, error answers for
 * unknown methods and malformed JSON, chunk ordering against the prompt result, and pending
 * requests failing when the peer goes away. The asserts that can block wait with a bound far
 * above any honest run; the bound only has to catch a real hang.
 */
class AcpConnectionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void promptTurnStreamsChunksInOrderBeforeTheResult() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        // Appended on the client's reader thread, read by the test thread.
        List<String> chunks = new CopyOnWriteArrayList<>();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())
                .onSessionUpdate(params -> {
                    JsonNode update = params.path("update");
                    chunks.add(update.path("sessionUpdate").asText()
                            + "|" + update.path("content").path("text").asText());
                });
                AcpAgent agent = AcpAgent.over(ends[1].in(), ends[1].out(), (text, context) -> {
                    context.sendThought("thinking about " + text);
                    context.sendMessage("echo: " + text);
                })) {
            agent.start();
            JsonNode init = client.initialize();
            assertThat(init.path("protocolVersion").asInt()).isEqualTo(1);
            String sessionId = client.newSession("/workspace");
            assertThat(sessionId).isNotBlank();

            JsonNode result = client.prompt(sessionId, "hello");

            assertThat(result.path("stopReason").asText()).isEqualTo("end_turn");
            // Notifications run inline on the read loop, so both chunks were delivered before
            // the prompt response completed the client's future.
            assertThat(chunks).containsExactly(
                    "agent_thought_chunk|thinking about hello",
                    "agent_message_chunk|echo: hello");
        }
    }

    @Test
    void unknownMethodAnswersMethodNotFound() {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        AcpConnection client = AcpConnection.over(ends[0].in(), ends[0].out()).start();
        try {
            CompletableFuture<JsonNode> response = client.request("bogus/method", null);
            assertThatThrownBy(() -> response.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(AcpError.class, error ->
                            assertThat(error.code()).isEqualTo(AcpConnection.METHOD_NOT_FOUND));
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void malformedJsonAnswersParseError() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        try (JsonPipe wire = JsonPipe.over(ends[0].in(), ends[0].out())) {
            wire.send("{not json");
            JsonNode error = MAPPER.readTree(wire.take()).path("error");
            assertThat(error.path("code").asInt()).isEqualTo(AcpConnection.PARSE_ERROR);

            // Valid JSON but not an object is a parse-level rejection too.
            wire.send("[1,2]");
            error = MAPPER.readTree(wire.take()).path("error");
            assertThat(error.path("code").asInt()).isEqualTo(AcpConnection.PARSE_ERROR);
        } finally {
            server.close();
        }
    }

    @Test
    void peerCloseFailsPendingRequests() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection client = AcpConnection.over(ends[0].in(), ends[0].out()).start();
        try {
            CompletableFuture<JsonNode> response = client.request("never/answered", null);
            // The peer's write side closes: the client's read loop hits EOF and fails every
            // pending request instead of leaving it parked forever.
            ends[1].out().close();
            assertThatThrownBy(() -> response.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(EOFException.class);
        } finally {
            client.close();
        }
    }
}
