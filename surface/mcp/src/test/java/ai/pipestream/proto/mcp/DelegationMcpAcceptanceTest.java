package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.delegation.DelegationActions;
import ai.pipestream.proto.delegation.DelegationBridge;
import ai.pipestream.proto.delegation.DelegationReducer;
import ai.pipestream.proto.delegation.InProcessDelegationCoordinator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The live delegation MCP surface, role-played end to end by two independent MCP
 * sessions against one in-process coordinator: Kimi drives the worker session, Codex
 * drives the coordinator session. Covers discovery, offer, accept, structured
 * messaging both ways, progress, checkpoint, candidate review with one revision, and
 * acceptance, then proves a worker MCP session can disconnect and resume its watch
 * from the last cursor with no lost or duplicated frames.
 */
class DelegationMcpAcceptanceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "kimi-worker";

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;
    private McpServer server;
    private int requestIds;

    @BeforeEach
    void setUp() {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
        ActionCatalog catalog = DelegationActions.register(
                ActionCatalog.defaults(ActionContext.create()), bridge);
        server = new McpServer(catalog,
                CompositeResources.of(new DelegationResources(bridge)),
                "protomolt-test", "0.0-test");
    }

    @AfterEach
    void tearDown() {
        bridge.close();
        coordinator.close();
    }

    @Test
    void kimiAndCodexCompleteAReviewedTaskOverTwoMcpSessions() throws Exception {
        McpServer.Session kimi = openSession();
        McpServer.Session codex = openSession();

        // Kimi registers; Codex discovers the worker.
        ObjectNode registered = call(kimi, "delegation-worker-register",
                MAPPER.createObjectNode()
                        .put("workerId", WORKER)
                        .put("provider", "kimi")
                        .put("model", "kimi-k2")
                        .set("capabilities", MAPPER.createArrayNode()
                                .add(MAPPER.createObjectNode()
                                        .put("name", "java-build")
                                        .put("description", "implements and tests Java"))));
        assertThat(registered.path("admitted").asBoolean()).isTrue();
        assertThat(registered.path("sessionId").asText()).isNotBlank();

        ObjectNode workers = call(codex, "delegation-worker-list", MAPPER.createObjectNode());
        assertThat(workers.path("workers").findValuesAsText("workerId")).contains(WORKER);

        // The first watch drains the registration frames that already exist.
        ObjectNode initialWatch = call(kimi, "delegation-watch", MAPPER.createObjectNode()
                .put("afterCursor", 0).put("timeoutMs", 1_000));
        assertThat(payloadKinds(initialWatch)).contains("hello", "admission");
        long kimiCursor = lastCursor(initialWatch);

        // Now the watch genuinely pends on a virtual thread: Codex's offer is what
        // releases it, with the offer event.
        Future<Optional<ObjectNode>> pendingWatch = submitToolCall(kimi,
                "delegation-watch", MAPPER.createObjectNode()
                        .put("afterCursor", kimiCursor)
                        .put("timeoutMs", 10_000));

        ObjectNode spec = MAPPER.createObjectNode()
                .put("objective", "Implement the bounded change and prove it")
                .set("requiredChecks", MAPPER.createArrayNode()
                        .add(MAPPER.createObjectNode()
                                .put("name", "unit-tests")
                                .put("description", "the module tests pass")));
        ObjectNode offered = call(codex, "delegation-offer",
                MAPPER.createObjectNode()
                        .put("workerId", WORKER)
                        .put("leaseSeconds", 300)
                        .set("spec", spec));
        String taskId = offered.path("taskId").asText();
        assertThat(offered.path("offer").path("attempt").asInt()).isEqualTo(1);

        ObjectNode firstWatch = awaitTool(pendingWatch);
        assertThat(firstWatch.path("timedOut").asBoolean()).isFalse();
        kimiCursor = lastCursor(firstWatch);
        assertThat(payloadKinds(firstWatch)).containsExactly("offer");

        // Kimi accepts the lease.
        ObjectNode accepted = call(kimi, "delegation-accept", MAPPER.createObjectNode()
                .put("workerId", WORKER).put("taskId", taskId).put("attempt", 1));
        assertThat(accepted.path("ok").asBoolean()).isTrue();

        // Structured updates in both directions: guidance, a question, its answer.
        ObjectNode guidance = call(codex, "delegation-message", MAPPER.createObjectNode()
                .put("taskId", taskId)
                .put("sender", "coordinator")
                .put("recipient", WORKER)
                .put("kind", "TASK_MESSAGE_KIND_GUIDANCE")
                .put("text", "keep the change inside the mapper"));
        String guidanceId = guidance.path("message").path("messageId").asText();

        ObjectNode guidanceSeen = call(kimi, "delegation-watch", MAPPER.createObjectNode()
                .put("afterCursor", kimiCursor).put("taskId", taskId)
                .put("timeoutMs", 5_000));
        kimiCursor = lastCursor(guidanceSeen);
        assertThat(payloadKinds(guidanceSeen)).contains("accept", "taskMessage");

        ObjectNode question = call(kimi, "delegation-message", MAPPER.createObjectNode()
                .put("taskId", taskId)
                .put("sender", WORKER)
                .put("kind", "TASK_MESSAGE_KIND_QUESTION")
                .put("text", "may the reducer stay untouched?")
                .put("replyTo", guidanceId));
        String questionId = question.path("message").path("messageId").asText();

        ObjectNode answer = call(codex, "delegation-message", MAPPER.createObjectNode()
                .put("taskId", taskId)
                .put("sender", "coordinator")
                .put("recipient", WORKER)
                .put("kind", "TASK_MESSAGE_KIND_ANSWER")
                .put("text", "yes; lifecycle logic stays put")
                .put("replyTo", questionId));
        assertThat(answer.path("message").path("replyTo").asText()).isEqualTo(questionId);

        // Kimi publishes progress and a checkpoint, then the first candidate.
        call(kimi, "delegation-progress", MAPPER.createObjectNode()
                .put("workerId", WORKER).put("taskId", taskId).put("attempt", 1)
                .put("message", "mapped the envelope"));
        ObjectNode checkpointed = call(kimi, "delegation-checkpoint", MAPPER.createObjectNode()
                .put("workerId", WORKER).put("taskId", taskId).put("attempt", 1)
                .put("resumeToken", "envelope-mapped").put("note", "halfway"));
        assertThat(checkpointed.path("checkpointSeq").asInt()).isEqualTo(1);
        call(kimi, "delegation-candidate", MAPPER.createObjectNode()
                .put("workerId", WORKER).put("taskId", taskId)
                .set("candidate", candidate(1, 1, "first revision")));

        // Codex sees the candidate and asks for a revision.
        ObjectNode candidateSeen = call(codex, "delegation-watch", MAPPER.createObjectNode()
                .put("afterCursor", 0).put("taskId", taskId).put("timeoutMs", 5_000));
        assertThat(payloadKinds(candidateSeen)).contains("completion");
        long codexCursor = lastCursor(candidateSeen);

        // Kimi's MCP session drops here. The worker stream outlives it.
        long cursorBeforeDrop = kimiCursor;
        kimi.close();

        call(codex, "delegation-review", MAPPER.createObjectNode()
                .put("taskId", taskId)
                .put("decision", "revise")
                .put("feedback", "prove the edge case too")
                .set("failedChecks", MAPPER.createArrayNode().add("unit-tests")));

        // Kimi reconnects with a fresh MCP session and resumes from the saved cursor:
        // exactly the frames after it arrive, the revision request among them, and a
        // drained watch proves nothing was duplicated.
        McpServer.Session kimiReconnected = openSession();
        ObjectNode resumed = call(kimiReconnected, "delegation-watch", MAPPER.createObjectNode()
                .put("afterCursor", cursorBeforeDrop).put("timeoutMs", 5_000));
        assertThat(resumed.path("timedOut").asBoolean()).isFalse();
        List<Long> cursors = new ArrayList<>();
        resumed.path("events").forEach(event -> cursors.add(event.path("cursor").asLong()));
        assertThat(cursors).allMatch(cursor -> cursor > cursorBeforeDrop);
        assertThat(cursors).isSorted();
        assertThat(payloadKinds(resumed)).contains("revisionRequested");
        // Frames at or before the saved cursor are never redelivered.
        assertThat(payloadKinds(resumed)).doesNotContain("offer", "accept");
        long resumedCursor = lastCursor(resumed);

        ObjectNode drained = call(kimiReconnected, "delegation-watch", MAPPER.createObjectNode()
                .put("afterCursor", resumedCursor).put("timeoutMs", 50));
        assertThat(drained.path("timedOut").asBoolean()).isTrue();
        assertThat(drained.path("events")).isEmpty();

        // Kimi answers the revision with the next candidate revision; Codex accepts.
        call(kimiReconnected, "delegation-candidate", MAPPER.createObjectNode()
                .put("workerId", WORKER).put("taskId", taskId)
                .set("candidate", candidate(1, 2, "revised with the edge case proven")));

        ObjectNode revisionSeen = call(codex, "delegation-watch", MAPPER.createObjectNode()
                .put("afterCursor", codexCursor).put("taskId", taskId)
                .put("timeoutMs", 5_000));
        assertThat(payloadKinds(revisionSeen)).contains("revisionRequested", "completion");

        call(codex, "delegation-review", MAPPER.createObjectNode()
                .put("taskId", taskId)
                .put("decision", "accept")
                .put("verdict", "acceptance checks verified"));

        // The transcript is the truth: the reducer stays clean and the task is accepted.
        ObjectNode transcript = call(codex, "delegation-transcript", MAPPER.createObjectNode()
                .put("taskId", taskId).put("maxEntries", 500));
        assertThat(transcript.path("events").size()).isGreaterThanOrEqualTo(11);
        assertThat(coordinator.state().clean())
                .as(coordinator.state().findings().toString()).isTrue();
        assertThat(coordinator.state().tasks().get(taskId).phase())
                .isEqualTo(DelegationReducer.Phase.ACCEPTED);
        assertThat(coordinator.state().tasks().get(taskId).candidateRevision()).isEqualTo(2);

        // The bounded resources tell the same story without tool calls.
        ObjectNode workersResource = readResource(codex, DelegationResources.WORKERS_URI);
        assertThat(workersResource.path("workers").findValuesAsText("workerId"))
                .contains(WORKER);
        ObjectNode tasksResource = readResource(codex, DelegationResources.TASKS_URI);
        JsonNode taskRow = tasksResource.path("tasks").get(0);
        assertThat(taskRow.path("taskId").asText()).isEqualTo(taskId);
        assertThat(taskRow.path("phase").asText()).isEqualTo("ACCEPTED");
        ObjectNode transcriptResource = readResource(codex,
                DelegationResources.TASKS_URI + "/" + taskId + "/transcript");
        assertThat(transcriptResource.path("entries").size())
                .isEqualTo(transcript.path("events").size());

        kimiReconnected.close();
        codex.close();
    }

    private McpServer.Session openSession() {
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

    /** Submits a long-polling tool call on the session's virtual-thread executor. */
    private Future<Optional<ObjectNode>> submitToolCall(McpServer.Session session,
                                                        String tool, ObjectNode arguments) {
        return session.submit(toolCall(tool, arguments));
    }

    private ObjectNode awaitTool(Future<Optional<ObjectNode>> future) throws Exception {
        Optional<ObjectNode> response = future.get(15, TimeUnit.SECONDS);
        assertThat(response).isPresent();
        ObjectNode result = (ObjectNode) response.get().path("result");
        assertThat(result.path("isError").asBoolean()).isFalse();
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
            List<String> payload = new ArrayList<>();
            frame.fieldNames().forEachRemaining(field -> {
                if (!List.of("frameId", "taskId", "seq", "sentAt").contains(field)) {
                    payload.add(field);
                }
            });
            kinds.addAll(payload);
        }
        return kinds;
    }

    private static ObjectNode candidate(int attempt, int revision, String summary) {
        ObjectNode candidate = MAPPER.createObjectNode()
                .put("attempt", attempt)
                .put("revision", revision)
                .put("summary", summary);
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
