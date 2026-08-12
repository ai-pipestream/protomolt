package ai.pipestream.proto.agenthost;

import ai.pipestream.proto.serve.ProtoMoltServe;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Two persistent model stand-ins completing one reviewed task over the real HTTP surface. */
class AgentHostAcceptanceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "kimi-worker";
    private static final String TASK = "11111111-1111-4111-8111-111111111111";

    @TempDir
    Path temporary;

    @Test
    void codexAndKimiExchangeStructuredTurnsAndResumeFromDurableHostState()
            throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        Path workerState = temporary.resolve("state/kimi.json");
        Path coordinatorState = temporary.resolve("state/codex.json");
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            AgentHost worker = host(endpoint, AgentRole.WORKER, WORKER, "kimi",
                    workerState, workspace, new ScriptedProvider("kimi", "kimi-session"));
            AgentHost coordinator = host(endpoint, AgentRole.COORDINATOR,
                    "codex-coordinator", "codex", coordinatorState, workspace,
                    new ScriptedProvider("codex", "codex-thread"));
            try (worker; coordinator) {
                worker.connect();
                coordinator.bootstrap("Delegate the bounded implementation to Kimi and review it");
                assertThat(worker.pollOnce()).isTrue();
                long savedCursor = worker.state().cursor();
                assertThat(worker.state().providerSessionId()).isEqualTo("kimi-session");
                worker.close();

                assertThat(coordinator.pollOnce()).isTrue();

                AgentHost resumed = host(endpoint, AgentRole.WORKER, WORKER, "kimi",
                        workerState, workspace,
                        new ScriptedProvider("kimi", "kimi-session"));
                try (resumed) {
                    assertThat(resumed.state().cursor()).isEqualTo(savedCursor);
                    assertThat(resumed.pollOnce()).isTrue();
                    assertThat(resumed.state().cursor()).isGreaterThan(savedCursor);
                    assertThat(resumed.state().providerSessionId())
                            .isEqualTo("kimi-session");
                }

                try (McpHttpClient inspector = new McpHttpClient(endpoint, () -> null)) {
                    ObjectNode transcript = inspector.callTool("delegation-transcript",
                            MAPPER.createObjectNode().put("taskId", TASK)
                                    .put("maxEntries", 100));
                    assertThat(payloadKinds(transcript)).contains(
                            "offer", "accept", "taskMessage", "progress", "checkpoint",
                            "completion", "accepted");
                }
            }
        }
    }

    private AgentHost host(URI endpoint, AgentRole role, String identity, String providerName,
                           Path statePath, Path workspace, AgentProvider provider) {
        AgentHostStateStore store = new AgentHostStateStore(statePath);
        AgentHostState state = store.loadOrCreate(identity, role, providerName, workspace);
        return new AgentHost(new AgentHost.Config(role, identity, null,
                workspace.toAbsolutePath(), Duration.ofMillis(100), 64),
                new McpHttpClient(endpoint, () -> null), provider, store, state);
    }

    private static List<String> payloadKinds(ObjectNode transcript) {
        List<String> kinds = new ArrayList<>();
        for (JsonNode event : transcript.path("events")) {
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

    private static final class ScriptedProvider implements AgentProvider {
        private final String name;
        private final String session;

        private ScriptedProvider(String name, String session) {
            this.name = name;
            this.session = session;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String sessionId() {
            return session;
        }

        @Override
        public String prompt(String prompt) {
            try {
                ObjectNode packet = (ObjectNode) MAPPER.readTree(
                        prompt.substring(prompt.lastIndexOf("Packet:\n") + 8));
                List<Long> cursors = new ArrayList<>();
                packet.path("events").forEach(
                        event -> cursors.add(event.path("cursor").asLong()));
                return "kimi".equals(name)
                        ? workerReply(packet, cursors).toString()
                        : coordinatorReply(packet, cursors).toString();
            } catch (Exception e) {
                throw new AgentHostException("scripted provider could not read packet", e);
            }
        }

        private static ObjectNode workerReply(ObjectNode packet, List<Long> cursors) {
            ArrayNode commands = MAPPER.createArrayNode();
            if (hasPayload(packet, "offer")) {
                commands.add(command("delegation-accept", MAPPER.createObjectNode()
                        .put("taskId", TASK).put("attempt", 1)));
                commands.add(command("delegation-message", MAPPER.createObjectNode()
                        .put("taskId", TASK)
                        .put("kind", "TASK_MESSAGE_KIND_QUESTION")
                        .put("text", "May I keep the reducer unchanged?")));
                commands.add(command("delegation-progress", MAPPER.createObjectNode()
                        .put("taskId", TASK).put("attempt", 1)
                        .put("message", "implementation and tests are complete")));
                commands.add(command("delegation-checkpoint", MAPPER.createObjectNode()
                        .put("taskId", TASK).put("attempt", 1)
                        .put("resumeToken", "tests-green").put("note", "ready for review")));
                ObjectNode candidate = MAPPER.createObjectNode()
                        .put("attempt", 1).put("revision", 1)
                        .put("summary", "bounded implementation complete");
                candidate.putArray("evidence").addObject()
                        .put("checkName", "unit-tests")
                        .put("verdict", "CHECK_VERDICT_PASSED")
                        .put("ranAt", "2026-08-12T00:00:00Z")
                        .put("detail", "focused tests pass");
                candidate.putArray("commits").addObject()
                        .put("repository", "git.rokkon.com/ai-pipestream/protomolt")
                        .put("commit", "a".repeat(40))
                        .put("subject", "agent host acceptance fixture");
                commands.add(command("delegation-candidate", MAPPER.createObjectNode()
                        .put("taskId", TASK).set("candidate", candidate)));
            } else {
                commands.add(command("host-ack", MAPPER.createObjectNode()
                        .put("reason", "coordinator result observed")));
            }
            return reply(cursors, commands);
        }

        private static ObjectNode coordinatorReply(ObjectNode packet, List<Long> cursors) {
            ArrayNode commands = MAPPER.createArrayNode();
            if ("bootstrap".equals(packet.path("kind").asText())) {
                ObjectNode spec = MAPPER.createObjectNode()
                        .put("objective", packet.path("objective").asText());
                spec.putArray("requiredChecks").addObject()
                        .put("name", "unit-tests")
                        .put("description", "focused unit tests pass");
                commands.add(command("delegation-offer", MAPPER.createObjectNode()
                        .put("workerId", WORKER).put("taskId", TASK)
                        .put("leaseSeconds", 300).set("spec", spec)));
            } else {
                if (hasPayload(packet, "taskMessage")) {
                    commands.add(command("delegation-message", MAPPER.createObjectNode()
                            .put("taskId", TASK).put("recipient", WORKER)
                            .put("kind", "TASK_MESSAGE_KIND_ANSWER")
                            .put("text", "Yes, keep the lifecycle reducer unchanged")));
                }
                if (hasPayload(packet, "completion")) {
                    commands.add(command("delegation-review", MAPPER.createObjectNode()
                            .put("taskId", TASK).put("decision", "accept")
                            .put("verdict", "required check and commit verified")));
                }
                if (commands.isEmpty()) {
                    commands.add(command("host-ack", MAPPER.createObjectNode()
                            .put("reason", "worker update observed")));
                }
            }
            return reply(cursors, commands);
        }

        private static boolean hasPayload(ObjectNode packet, String name) {
            for (JsonNode event : packet.path("events")) {
                JsonNode entry = event.path("entry");
                if (entry.path("workerFrame").has(name)
                        || entry.path("coordinatorFrame").has(name)) {
                    return true;
                }
            }
            return false;
        }

        private static ObjectNode command(String tool, ObjectNode arguments) {
            ObjectNode command = MAPPER.createObjectNode();
            command.put("tool", tool);
            command.set("arguments", arguments);
            return command;
        }

        private static ObjectNode reply(List<Long> cursors, ArrayNode commands) {
            ObjectNode reply = MAPPER.createObjectNode();
            ArrayNode handled = reply.putArray("handledEventCursors");
            cursors.forEach(handled::add);
            reply.set("commands", commands);
            return reply;
        }

        @Override
        public void close() {
        }
    }
}
