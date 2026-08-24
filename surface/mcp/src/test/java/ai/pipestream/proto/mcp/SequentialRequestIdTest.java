package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A request id only has to be unique among the requests in flight. A client that waits for
 * one tool call to answer and then reuses the id for the next is behaving correctly, so the
 * session must have released the id by the time it answers.
 *
 * <p>The window is narrow and timing-dependent, which is what makes it worth a test: a
 * completed task wakes whoever waits on it before it runs its own cleanup, so a caller
 * quick enough to arrive in between is refused as a duplicate.
 */
class SequentialRequestIdTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ObjectNode request(int id, String method, ObjectNode params) {
        ObjectNode message = MAPPER.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", id);
        message.put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        return message;
    }

    private static ObjectNode params(String key, String value) {
        return MAPPER.createObjectNode().put(key, value);
    }

    private static McpServer.Session readySession() {
        McpServer server = new McpServer(ActionCatalog.defaults(ActionContext.create()), null,
                "protomolt-test", "0.0-test");
        McpServer.Session session = server.openSession();
        session.handle(request(1, "initialize", params("protocolVersion", "2025-06-18")));
        ObjectNode initialized = MAPPER.createObjectNode();
        initialized.put("jsonrpc", "2.0");
        initialized.put("method", "notifications/initialized");
        session.handle(initialized);
        return session;
    }

    @Test
    void anIdIsFreeAgainAsSoonAsItsCallHasAnswered() throws Exception {
        McpServer.Session session = readySession();
        ObjectNode arguments = MAPPER.createObjectNode();
        arguments.put("name", "list-types");
        arguments.set("arguments", MAPPER.createObjectNode());

        // The same id, one call after another, each awaited before the next is sent.
        // Two hundred rounds because the caller has to arrive inside the cleanup window.
        for (int round = 0; round < 200; round++) {
            Optional<ObjectNode> answer =
                    session.submit(request(7, "tools/call", arguments.deepCopy())).get();

            assertThat(answer).as("round %s produced no answer", round).isPresent();
            assertThat(answer.orElseThrow().path("error").isMissingNode())
                    .as("round %s: %s", round, answer.orElseThrow())
                    .isTrue();
        }
    }
}
