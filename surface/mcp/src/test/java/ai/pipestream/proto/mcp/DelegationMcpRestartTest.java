package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.delegation.AdmissionPolicy;
import ai.pipestream.proto.delegation.CandidateReviewer;
import ai.pipestream.proto.delegation.DelegationActions;
import ai.pipestream.proto.delegation.DelegationBridge;
import ai.pipestream.proto.delegation.DelegationReducer;
import ai.pipestream.proto.delegation.InMemoryTranscriptRepository;
import ai.pipestream.proto.delegation.InProcessDelegationCoordinator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The live delegation MCP surface across a server restart over a durable transcript:
 * the worker re-registers through the same verb, resumes its sequence scopes, a
 * watch resumes from the pre-restart cursor with no lost or duplicated frames, and
 * the task runs to acceptance on the restored coordinator.
 */
class DelegationMcpRestartTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "kimi-worker";

    private int requestIds;

    @Test
    void theSurfaceReconnectsToARestoredCoordinator() throws Exception {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();

        String taskId;
        long savedCursor;
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            DelegationBridge bridge = new DelegationBridge(coordinator);
            McpServer server = server(bridge);
            McpServer.Session kimi = openSession(server);
            McpServer.Session codex = openSession(server);

            ObjectNode firstRegister = call(kimi, "delegation-worker-register",
                    MAPPER.createObjectNode().put("workerId", WORKER).put("provider", "kimi"));
            assertThat(firstRegister.path("admitted").asBoolean()).isTrue();
            ObjectNode offered = call(codex, "delegation-offer", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("leaseSeconds", 300)
                    .set("spec", MAPPER.createObjectNode()
                            .put("objective", "Implement the bounded change and prove it")
                            .set("requiredChecks", MAPPER.createArrayNode()
                                    .add(MAPPER.createObjectNode()
                                            .put("name", "unit-tests")
                                            .put("description", "the tests pass")))));
            taskId = offered.path("taskId").asText();
            call(kimi, "delegation-accept", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", taskId).put("attempt", 1));
            call(kimi, "delegation-progress", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", taskId).put("attempt", 1)
                    .put("message", "mapped the envelope"));

            ObjectNode drained = call(kimi, "delegation-watch", MAPPER.createObjectNode()
                    .put("afterCursor", 0).put("timeoutMs", 1_000));
            savedCursor = lastCursor(drained);
            kimi.close();
            codex.close();
            bridge.close();
        }

        try (InProcessDelegationCoordinator restored = coordinator(repository)) {
            DelegationBridge bridge = new DelegationBridge(restored);
            McpServer server = server(bridge);
            McpServer.Session kimi = openSession(server);
            McpServer.Session codex = openSession(server);

            // The same worker re-registers through the same verb and is admitted.
            ObjectNode registered = call(kimi, "delegation-worker-register",
                    MAPPER.createObjectNode().put("workerId", WORKER)
                            .put("provider", "kimi"));
            assertThat(registered.path("admitted").asBoolean()).isTrue();

            // The watch resumes from the pre-restart cursor: exactly the re-hello and
            // the admission, nothing earlier, nothing twice.
            ObjectNode resumed = call(kimi, "delegation-watch", MAPPER.createObjectNode()
                    .put("afterCursor", savedCursor).put("timeoutMs", 5_000));
            assertThat(resumed.path("timedOut").asBoolean()).isFalse();
            List<Long> cursors = new ArrayList<>();
            resumed.path("events").forEach(event -> cursors.add(event.path("cursor").asLong()));
            assertThat(cursors).containsExactly(savedCursor + 1, savedCursor + 2);
            assertThat(payloadKinds(resumed)).containsExactly("hello", "admission");

            // The attempt scope resumes: the next progress note is progress_seq 2.
            ObjectNode progress = call(kimi, "delegation-progress", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", taskId).put("attempt", 1)
                    .put("message", "wired the stream"));
            assertThat(progress.path("progressSeq").asInt()).isEqualTo(2);

            // The task completes on the restored coordinator.
            call(kimi, "delegation-candidate", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", taskId)
                    .set("candidate", candidate()));
            call(codex, "delegation-review", MAPPER.createObjectNode()
                    .put("taskId", taskId).put("decision", "REVIEW_DECISION_ACCEPT")
                    .put("verdict", "acceptance checks verified"));
            assertThat(restored.state().clean())
                    .as(restored.state().findings().toString()).isTrue();
            assertThat(restored.state().tasks().get(taskId).phase())
                    .isEqualTo(DelegationReducer.Phase.ACCEPTED);

            // The bounded resources read the restored truth.
            ObjectNode tasks = readResource(codex, DelegationResources.TASKS_URI);
            JsonNode row = tasks.path("tasks").get(0);
            assertThat(row.path("taskId").asText()).isEqualTo(taskId);
            assertThat(row.path("phase").asText()).isEqualTo("ACCEPTED");

            kimi.close();
            codex.close();
            bridge.close();
        }
    }

    private static InProcessDelegationCoordinator coordinator(
            InMemoryTranscriptRepository repository) {
        return new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), java.time.Clock.systemUTC(), repository);
    }

    private static McpServer server(DelegationBridge bridge) {
        ActionCatalog catalog = DelegationActions.register(
                ActionCatalog.defaults(ActionContext.create()), bridge);
        return new McpServer(catalog, CompositeResources.of(new DelegationResources(bridge)),
                "protomolt-test", "0.0-test");
    }

    private McpServer.Session openSession(McpServer server) {
        McpServer.Session session = server.openSession();
        ObjectNode initialize = MAPPER.createObjectNode();
        initialize.put("jsonrpc", "2.0");
        initialize.put("id", ++requestIds);
        initialize.put("method", "initialize");
        initialize.putObject("params").put("protocolVersion", McpServer.PROTOCOL_VERSION);
        assertThat(session.handle(initialize)).isPresent();
        ObjectNode initialized = MAPPER.createObjectNode();
        initialized.put("jsonrpc", "2.0");
        initialized.put("method", "notifications/initialized");
        assertThat(session.handle(initialized)).isEmpty();
        return session;
    }

    /** Drives a tool call through the session and returns its structured content. */
    private ObjectNode call(McpServer.Session session, String tool, ObjectNode arguments) {
        Optional<ObjectNode> response = session.handle(toolCall(tool, arguments));
        assertThat(response).isPresent();
        ObjectNode result = (ObjectNode) response.get().path("result");
        assertThat(result.path("isError").asBoolean())
                .as("tool %s failed: %s", tool, result.path("structuredContent"))
                .isFalse();
        return (ObjectNode) result.path("structuredContent");
    }

    private ObjectNode toolCall(String tool, ObjectNode arguments) {
        ObjectNode message = MAPPER.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", ++requestIds);
        message.put("method", "tools/call");
        ObjectNode params = message.putObject("params");
        params.put("name", tool);
        params.set("arguments", arguments);
        return message;
    }

    private ObjectNode readResource(McpServer.Session session, String uri) throws Exception {
        ObjectNode message = MAPPER.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", ++requestIds);
        message.put("method", "resources/read");
        message.putObject("params").put("uri", uri);
        Optional<ObjectNode> response = session.handle(message);
        assertThat(response).isPresent();
        String text = response.get().path("result").path("contents").get(0)
                .path("text").asText();
        return (ObjectNode) MAPPER.readTree(text);
    }

    private static long lastCursor(ObjectNode watchResult) {
        ArrayNode events = (ArrayNode) watchResult.path("events");
        assertThat(events).isNotEmpty();
        return events.get(events.size() - 1).path("cursor").asLong();
    }

    /** The payload kinds of a watch result's frames, in wire order. */
    private static List<String> payloadKinds(ObjectNode watchResult) {
        List<String> kinds = new ArrayList<>();
        for (JsonNode event : watchResult.path("events")) {
            JsonNode entry = event.path("entry");
            JsonNode frame = entry.path("workerFrame").isObject()
                    ? entry.path("workerFrame") : entry.path("coordinatorFrame");
            frame.fieldNames().forEachRemaining(field -> {
                if (!List.of("frameId", "taskId", "seq", "sentAt").contains(field)) {
                    kinds.add(field);
                }
            });
        }
        return kinds;
    }

    private static ObjectNode candidate() {
        ObjectNode candidate = MAPPER.createObjectNode()
                .put("attempt", 1)
                .put("revision", 1)
                .put("summary", "implemented and proven");
        candidate.set("evidence", MAPPER.createArrayNode()
                .add(MAPPER.createObjectNode()
                        .put("checkName", "unit-tests")
                        .put("verdict", "CHECK_VERDICT_PASSED")
                        .put("ranAt", "2026-08-12T00:00:00Z")
                        .put("detail", "312 tests, 0 failures")));
        candidate.set("commits", MAPPER.createArrayNode()
                .add(MAPPER.createObjectNode()
                        .put("repository", "git.rokkon.com/ai-pipestream/protomolt")
                        .put("commit", "a".repeat(40))
                        .put("subject", "delegation: the bounded change")));
        return candidate;
    }
}
