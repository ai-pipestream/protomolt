package ai.protomolt.proto.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A session opened for a scoped caller serves that caller's catalog view for its whole
 * lifetime: {@code tools/list} is filtered, {@code tools/call} outside the scope is an
 * {@code isError} result naming the missing scope, and the metadata counts the filtered
 * manifest.
 */
class McpScopeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ObjectNode request(int id, String method, ObjectNode params) {
        ObjectNode message = MAPPER.createObjectNode()
                .put("jsonrpc", "2.0").put("id", id).put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        return message;
    }

    private static McpServer.Session ready(Caller caller) {
        McpServer server = new McpServer(
                ActionCatalog.defaults(ActionContext.create()), null, "test", "0");
        McpServer.Session session = server.openSession(caller);
        ObjectNode initialize = MAPPER.createObjectNode();
        initialize.put("protocolVersion", "2025-06-18");
        session.handle(request(1, "initialize", initialize)).orElseThrow();
        session.handle(MAPPER.createObjectNode()
                .put("jsonrpc", "2.0").put("method", "notifications/initialized"));
        return session;
    }

    @Test
    void theSessionServesTheCallersFilteredCatalog() {
        McpServer.Session session =
                ready(Caller.scoped("ci-reader", Set.of(Scopes.SCHEMA_READ)));
        ObjectNode listed = session.handle(request(2, "tools/list", null)).orElseThrow();
        assertThat(listed.path("result").path("tools")).hasSize(17);
        assertThat(listed.path("result").path("_meta")
                .path("ai.pipestream.protomolt/toolCount").asInt()).isEqualTo(17);
    }

    @Test
    void aCallerWithoutTheScopeSeesNothingAndCallsNothing() {
        McpServer.Session session =
                ready(Caller.scoped("runner", Set.of(Scopes.WORKFLOW_RUN)));
        ObjectNode listed = session.handle(request(2, "tools/list", null)).orElseThrow();
        assertThat(listed.path("result").path("tools")).isEmpty();

        ObjectNode params = MAPPER.createObjectNode().put("name", "list-types");
        params.putObject("arguments");
        Optional<ObjectNode> called = session.handle(request(3, "tools/call", params));
        ObjectNode result = called.orElseThrow().path("result").deepCopy();
        assertThat(result.path("isError").asBoolean()).isTrue();
        assertThat(result.path("content").toString())
                .contains("permission-denied").contains(Scopes.SCHEMA_READ).contains("runner");
    }

    @Test
    void theStdioSessionKeepsProcessAuthority() {
        McpServer server = new McpServer(
                ActionCatalog.defaults(ActionContext.create()), null, "test", "0");
        McpServer.Session session = server.openSession();
        ObjectNode initialize = MAPPER.createObjectNode();
        initialize.put("protocolVersion", "2025-06-18");
        ObjectNode result = session.handle(request(1, "initialize", initialize)).orElseThrow();
        assertThat(result.path("result").path("_meta")
                .path("ai.pipestream.protomolt/toolCount").asInt()).isEqualTo(17);
    }
}
