package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.serve.ProtoMoltServe;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Restarts from a saved command position and commits the event cursor only after the batch. */
class AgentHostPendingBatchRecoveryTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "pending-restart-worker";
    private static final String TASK = "8e8b7c26-8401-4a66-9c6d-f50c16d38bd1";

    @TempDir
    Path temporary;

    @Test
    void restartSkipsThePersistedCommandPositionAndAdvancesCursorAfterTheRemainingCommand()
            throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        Path statePath = temporary.resolve("state/worker.json");
        AgentHostStateStore store = new AgentHostStateStore(statePath);

        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0));
             McpHttpClient coordinator = new McpHttpClient(
                     URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp"),
                     () -> null)) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            AgentHost registration = host(endpoint, statePath, workspace);
            registration.connect();

            ObjectNode spec = MAPPER.createObjectNode().put("objective", "resume the pending batch");
            spec.putArray("requiredChecks").addObject()
                    .put("name", "resume-check").put("description", "check progress");
            coordinator.callTool("delegation-offer", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", TASK)
                    .put("leaseSeconds", 300).set("spec", spec));
            coordinator.callTool("delegation-accept", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", TASK).put("attempt", 1));

            ObjectNode firstCommand = progress("command-one-already-recorded");
            ObjectNode firstResult = coordinator.callTool("delegation-progress", firstCommand);
            assertThat(firstResult.path("progressSeq").asInt()).isEqualTo(1);
            long targetCursor = coordinator.callTool("delegation-watch", MAPPER.createObjectNode()
                    .put("afterCursor", 0).put("timeoutMs", 0).put("maxEvents", 64))
                    .path("cursor").asLong();
            assertThat(targetCursor).isPositive();

            // Model a crash after command 1's remote success and after nextCommand=1 was
            // durably saved: restart must skip command 1. The other crash window, after a
            // remote success but before that local save, can replay the command; this test
            // deliberately makes no exactly-once claim about that ambiguous outcome.
            AgentHostState saved = store.loadOrCreate(WORKER, AgentRole.WORKER, "fixture", workspace)
                    .withPending(new AgentHostState.PendingTurn(targetCursor, List.of(targetCursor),
                            List.of(new AgentTurn.Command("delegation-progress", firstCommand),
                                    new AgentTurn.Command("delegation-progress",
                                            progress("command-two-after-restart"))),
                            1, false));
            store.save(saved);
            registration.close();

            AgentHostState restored = store.loadOrCreate(
                    WORKER, AgentRole.WORKER, "fixture", workspace);
            assertThat(restored.cursor()).isZero();
            assertThat(restored.pending().nextCommand()).isEqualTo(1);

            try (AgentHost resumed = host(endpoint, statePath, workspace)) {
                assertThat(resumed.pollOnce()).isTrue();
                assertThat(resumed.state().pending()).isNull();
                assertThat(resumed.state().cursor()).isEqualTo(targetCursor);
            }

            ObjectNode transcript = coordinator.callTool("delegation-transcript",
                    MAPPER.createObjectNode().put("taskId", TASK).put("maxEntries", 100));
            List<String> progress = new java.util.ArrayList<>();
            for (JsonNode event : transcript.path("events")) {
                JsonNode frame = event.path("entry").path("workerFrame");
                if (frame.has("progress")) {
                    progress.add(frame.path("progress").path("message").asText());
                }
            }
            assertThat(progress).containsExactly(
                    "command-one-already-recorded", "command-two-after-restart");
        }
    }

    private static ObjectNode progress(String message) {
        return MAPPER.createObjectNode().put("workerId", WORKER).put("taskId", TASK)
                .put("attempt", 1).put("message", message);
    }

    private static AgentHost host(URI endpoint, Path statePath, Path workspace) {
        AgentHostStateStore store = new AgentHostStateStore(statePath);
        AgentHostState state = store.loadOrCreate(WORKER, AgentRole.WORKER, "fixture", workspace);
        return new AgentHost(new AgentHost.Config(AgentRole.WORKER, WORKER, null,
                workspace.toAbsolutePath(), Duration.ofMillis(50), 64, false),
                new McpHttpClient(endpoint, () -> null), new FixtureProvider(), store, state);
    }

    private static final class FixtureProvider implements AgentProvider {
        @Override public String name() { return "fixture"; }
        @Override public String sessionId() { return "pending-batch-session"; }
        @Override public String prompt(String prompt) {
            throw new AssertionError("saved commands should resume without another model turn");
        }
        @Override public void close() { }
    }
}
