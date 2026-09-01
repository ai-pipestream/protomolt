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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void acpPromptCarriesExactWorkerArgumentContract() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("prompt-workspace"));
        Path workerState = temporary.resolve("prompt-state/kimi.json");
        CapturingProvider provider = new CapturingProvider();
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0));
             AgentHost worker = host(
                     URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp"),
                     AgentRole.WORKER, WORKER, "kimi", workerState, workspace, provider);
             McpHttpClient coordinator = new McpHttpClient(
                     URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp"),
                     () -> null)) {
            worker.connect();
            ObjectNode spec = MAPPER.createObjectNode().put("objective", "inspect contract");
            spec.putArray("requiredChecks").addObject().put("name", "unit-tests");
            coordinator.callTool("delegation-offer", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", TASK)
                    .put("leaseSeconds", 300).set("spec", spec));

            assertThat(worker.pollOnce()).isTrue();
            assertThat(provider.prompt)
                    .contains("delegation-progress={taskId,attempt,message}")
                    .contains("delegation-checkpoint={taskId,attempt,resumeToken,note}")
                    .contains("evidence:[{checkName,verdict,ranAt,detail}]")
                    .contains("Use exactly these field names and no others");
        }
    }

    @Test
    void workerCannotAcknowledgeAwayAnOffer() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("required-workspace"));
        Path workerState = temporary.resolve("required-state/kimi.json");
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0));
             AgentHost worker = host(
                     URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp"),
                     AgentRole.WORKER, WORKER, "kimi", workerState, workspace,
                     new AckOnlyProvider());
             McpHttpClient coordinator = new McpHttpClient(
                     URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp"),
                     () -> null)) {
            worker.connect();
            ObjectNode spec = MAPPER.createObjectNode().put("objective", "do the work");
            spec.putArray("requiredChecks").addObject().put("name", "unit-tests");
            coordinator.callTool("delegation-offer", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", TASK)
                    .put("leaseSeconds", 300).set("spec", spec));

            assertThatThrownBy(worker::pollOnce)
                    .isInstanceOf(AgentHostException.class)
                    .hasMessageContaining("task offer requires delegation-accept")
                    .hasMessageContaining(TASK);
            assertThat(worker.state().cursor()).isZero();
            assertThat(worker.state().pending()).isNull();
        }
    }

    @Test
    void workerCannotAcknowledgeAwayCoordinatorGuidance() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("guidance-workspace"));
        Path workerState = temporary.resolve("guidance-state/kimi.json");
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0));
             AgentHost worker = host(
                     URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp"),
                     AgentRole.WORKER, WORKER, "kimi", workerState, workspace,
                     new AcceptThenAckProvider());
             McpHttpClient coordinator = new McpHttpClient(
                     URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp"),
                     () -> null)) {
            worker.connect();
            ObjectNode spec = MAPPER.createObjectNode().put("objective", "do the work");
            spec.putArray("requiredChecks").addObject().put("name", "unit-tests");
            coordinator.callTool("delegation-offer", MAPPER.createObjectNode()
                    .put("workerId", WORKER).put("taskId", TASK)
                    .put("leaseSeconds", 300).set("spec", spec));
            assertThat(worker.pollOnce()).isTrue();
            long acceptedCursor = worker.state().cursor();
            coordinator.callTool("delegation-message", MAPPER.createObjectNode()
                    .put("taskId", TASK).put("sender", "coordinator")
                    .put("recipient", WORKER)
                    .put("kind", "TASK_MESSAGE_KIND_GUIDANCE")
                    .put("text", "continue after restart"));

            assertThatThrownBy(worker::pollOnce)
                    .isInstanceOf(AgentHostException.class)
                    .hasMessageContaining("guidance cannot be acknowledged")
                    .hasMessageContaining(TASK);
            assertThat(worker.state().cursor()).isEqualTo(acceptedCursor);
            assertThat(worker.state().pending()).isNull();
        }
    }

    @Test
    void reboundProviderSessionKeepsTheCursorAndSkipsProcessedEvents() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("rebind-workspace"));
        Path workerState = temporary.resolve("rebind-state/kimi.json");
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            AcceptThenProgressProvider first =
                    new AcceptThenProgressProvider("kimi-session-old");
            AgentHost worker = host(endpoint, AgentRole.WORKER, WORKER, "kimi",
                    workerState, workspace, first);
            try (worker; McpHttpClient coordinator = new McpHttpClient(endpoint,
                    () -> null)) {
                worker.connect();
                ObjectNode spec = MAPPER.createObjectNode().put("objective", "do the work");
                spec.putArray("requiredChecks").addObject().put("name", "unit-tests");
                coordinator.callTool("delegation-offer", MAPPER.createObjectNode()
                        .put("workerId", WORKER).put("taskId", TASK)
                        .put("leaseSeconds", 300).set("spec", spec));
                assertThat(worker.pollOnce()).isTrue();
                assertThat(first.prompts).isEqualTo(1);
                long savedCursor = worker.state().cursor();
                assertThat(savedCursor).isPositive();
                assertThat(worker.state().providerSessionId()).isEqualTo("kimi-session-old");
                worker.close();

                // A restart rebinds a fresh Kimi provider session; the durable ProtoMolt
                // state survives with only the provider session id replaced.
                AcceptThenProgressProvider rebound =
                        new AcceptThenProgressProvider("kimi-session-fresh");
                AgentHost resumed = host(endpoint, AgentRole.WORKER, WORKER, "kimi",
                        workerState, workspace, rebound);
                try (resumed) {
                    assertThat(resumed.state().cursor()).isEqualTo(savedCursor);
                    assertThat(resumed.state().providerSessionId())
                            .isEqualTo("kimi-session-fresh");

                    // The retained cursor keeps the processed offer out of later batches:
                    // the only newer event is the worker's own accept, which is not
                    // relevant to the worker, so no model turn runs and the offer is
                    // never processed twice.
                    assertThat(resumed.pollOnce()).isTrue();
                    assertThat(rebound.prompts).isZero();

                    coordinator.callTool("delegation-message", MAPPER.createObjectNode()
                            .put("taskId", TASK).put("sender", "coordinator")
                            .put("recipient", WORKER)
                            .put("kind", "TASK_MESSAGE_KIND_GUIDANCE")
                            .put("text", "continue with the bounded implementation"));
                    assertThat(resumed.pollOnce()).isTrue();
                    assertThat(rebound.prompts).isEqualTo(1);
                    assertThat(resumed.state().cursor()).isGreaterThan(savedCursor);

                    ObjectNode transcript = coordinator.callTool("delegation-transcript",
                            MAPPER.createObjectNode().put("taskId", TASK)
                                    .put("maxEntries", 100));
                    assertThat(payloadKinds(transcript).stream()
                            .filter("accept"::equals).count()).isEqualTo(1);
                }
            }
        }
    }

    @Test
    void coordinatorCannotAcknowledgeAwayAnAccept() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("step-workspace"));
        Path coordinatorState = temporary.resolve("step-state/coordinator.json");
        AckCountingProvider provider = new AckCountingProvider();
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            AgentHost coordinator = host(endpoint, AgentRole.COORDINATOR,
                    "codex-coordinator", "codex", coordinatorState, workspace, provider);
            try (coordinator; McpHttpClient workerSide = new McpHttpClient(endpoint,
                    () -> null)) {
                registerOfferAndAccept(workerSide);

                assertThatThrownBy(coordinator::pollOnce)
                        .isInstanceOf(AgentHostException.class)
                        .hasMessageContaining("guidance")
                        .hasMessageContaining("cancellation")
                        .hasMessageContaining(TASK);
                assertThat(provider.prompts).isEqualTo(2);
                assertThat(provider.lastPrompt)
                        .contains("Your previous response was rejected");
                assertThat(coordinator.state().cursor()).isZero();
                assertThat(coordinator.state().pending()).isNull();

                AgentHostState reloaded = new AgentHostStateStore(coordinatorState)
                        .loadOrCreate("codex-coordinator", AgentRole.COORDINATOR,
                                "codex", workspace);
                assertThat(reloaded.cursor()).isZero();
                assertThat(reloaded.pending()).isNull();
            }
        }
    }

    @Test
    void coordinatorCannotAcknowledgeAwayACompletionCandidate() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("review-workspace"));
        Path coordinatorState = temporary.resolve("review-state/coordinator.json");
        GuidanceThenAckProvider provider = new GuidanceThenAckProvider();
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            AgentHost coordinator = host(endpoint, AgentRole.COORDINATOR,
                    "codex-coordinator", "codex", coordinatorState, workspace, provider);
            try (coordinator; McpHttpClient workerSide = new McpHttpClient(endpoint,
                    () -> null)) {
                registerOfferAndAccept(workerSide);

                assertThat(coordinator.pollOnce()).isTrue();
                long acceptedCursor = coordinator.state().cursor();
                int promptsAfterAccept = provider.prompts;

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
                workerSide.callTool("delegation-candidate", MAPPER.createObjectNode()
                        .put("workerId", WORKER).put("taskId", TASK)
                        .set("candidate", candidate));

                assertThatThrownBy(coordinator::pollOnce)
                        .isInstanceOf(AgentHostException.class)
                        .hasMessageContaining("delegation-review")
                        .hasMessageContaining(TASK);
                assertThat(provider.prompts).isEqualTo(promptsAfterAccept + 2);
                assertThat(provider.lastPrompt)
                        .contains("Your previous response was rejected");
                assertThat(coordinator.state().cursor()).isEqualTo(acceptedCursor);
                assertThat(coordinator.state().pending()).isNull();

                AgentHostState reloaded = new AgentHostStateStore(coordinatorState)
                        .loadOrCreate("codex-coordinator", AgentRole.COORDINATOR,
                                "codex", workspace);
                assertThat(reloaded.cursor()).isEqualTo(acceptedCursor);
                assertThat(reloaded.pending()).isNull();
            }
        }
    }

    private static void registerOfferAndAccept(McpHttpClient workerSide) {
        ObjectNode registration = MAPPER.createObjectNode()
                .put("workerId", WORKER).put("provider", "kimi");
        registration.putArray("capabilities").addObject()
                .put("name", "structured-delegation")
                .put("description", "Consumes event batches and emits commands");
        workerSide.callTool("delegation-worker-register", registration);
        ObjectNode spec = MAPPER.createObjectNode().put("objective", "do the work");
        spec.putArray("requiredChecks").addObject().put("name", "unit-tests");
        workerSide.callTool("delegation-offer", MAPPER.createObjectNode()
                .put("workerId", WORKER).put("taskId", TASK)
                .put("leaseSeconds", 300).set("spec", spec));
        workerSide.callTool("delegation-accept", MAPPER.createObjectNode()
                .put("workerId", WORKER).put("taskId", TASK).put("attempt", 1));
    }

    @Test
    void aRebuiltCoordinatorIsRejoinedOnlyWhenTranscriptLossIsDeclared() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("rebuilt-workspace"));
        Path workerState = temporary.resolve("rebuilt-state/kimi.json");

        // A coordinator this worker has consumed events from, so its cursor is a real
        // position in a real transcript.
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            try (AgentHost worker = host(endpoint, AgentRole.WORKER, WORKER, "kimi",
                    workerState, workspace, new CapturingProvider());
                 McpHttpClient coordinator = new McpHttpClient(endpoint, () -> null)) {
                worker.connect();
                ObjectNode spec = MAPPER.createObjectNode().put("objective", "inspect contract");
                spec.putArray("requiredChecks").addObject().put("name", "unit-tests");
                coordinator.callTool("delegation-offer", MAPPER.createObjectNode()
                        .put("workerId", WORKER).put("taskId", TASK)
                        .put("leaseSeconds", 300).set("spec", spec));
                assertThat(worker.pollOnce()).isTrue();
                assertThat(worker.state().cursor()).isPositive();
            }
        }

        // A second coordinator with none of that history, which is what a redeploy over
        // empty volumes leaves behind.
        try (ProtoMoltServe rebuilt = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + rebuilt.httpPort() + "/mcp");
            try (AgentHost refusing = host(endpoint, AgentRole.WORKER, WORKER, "kimi",
                    workerState, workspace, new CapturingProvider())) {
                assertThatThrownBy(refusing::connect)
                        .isInstanceOf(AgentHostException.class)
                        .hasMessageContaining("no longer knows worker")
                        .hasMessageContaining("--reset-on-transcript-loss");
                assertThat(refusing.state().cursor())
                        .as("a refusal must not quietly move the position").isPositive();
            }

            try (AgentHost rejoining = host(endpoint, AgentRole.WORKER, WORKER, "kimi",
                    workerState, workspace, new CapturingProvider(), true);
                 McpHttpClient coordinator = new McpHttpClient(endpoint, () -> null)) {
                rejoining.connect();

                assertThat(rejoining.state().cursor()).isZero();
                assertThat(rejoining.state().pending()).isNull();
                assertThat(coordinator.callTool("delegation-worker-list",
                        MAPPER.createObjectNode()).path("workers"))
                        .anySatisfy(worker -> assertThat(worker.path("workerId").asText())
                                .isEqualTo(WORKER));
            }
        }
    }

    private AgentHost host(URI endpoint, AgentRole role, String identity, String providerName,
                           Path statePath, Path workspace, AgentProvider provider) {
        return host(endpoint, role, identity, providerName, statePath, workspace, provider,
                false);
    }

    private AgentHost host(URI endpoint, AgentRole role, String identity, String providerName,
                           Path statePath, Path workspace, AgentProvider provider,
                           boolean resetOnTranscriptLoss) {
        AgentHostStateStore store = new AgentHostStateStore(statePath);
        AgentHostState state = store.loadOrCreate(identity, role, providerName, workspace);
        return new AgentHost(new AgentHost.Config(role, identity, null,
                workspace.toAbsolutePath(), Duration.ofMillis(100), 64, resetOnTranscriptLoss),
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
                            .put("taskId", TASK).put("decision", "REVIEW_DECISION_ACCEPT")
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

    private static final class CapturingProvider implements AgentProvider {
        private String prompt;

        @Override
        public String name() {
            return "kimi";
        }

        @Override
        public String sessionId() {
            return "capturing-session";
        }

        @Override
        public String prompt(String value) {
            prompt = value;
            try {
                JsonNode packet = MAPPER.readTree(
                        value.substring(value.lastIndexOf("Packet:\n") + 8));
                long cursor = packet.path("events").get(0).path("cursor").asLong();
                return "{\"handledEventCursors\":[" + cursor + "],\"commands\":[{"
                        + "\"tool\":\"delegation-accept\",\"arguments\":{"
                        + "\"taskId\":\"" + TASK + "\",\"attempt\":1}}]}";
            } catch (Exception e) {
                throw new AgentHostException("capturing provider could not read packet", e);
            }
        }

        @Override
        public void close() {
        }
    }

    private static final class AckOnlyProvider implements AgentProvider {
        @Override
        public String name() {
            return "kimi";
        }

        @Override
        public String sessionId() {
            return "ack-only-session";
        }

        @Override
        public String prompt(String value) {
            try {
                JsonNode packet = MAPPER.readTree(
                        value.substring(value.lastIndexOf("Packet:\n") + 8));
                long cursor = packet.path("events").get(0).path("cursor").asLong();
                return "{\"handledEventCursors\":[" + cursor + "],\"commands\":[{"
                        + "\"tool\":\"host-ack\",\"arguments\":{"
                        + "\"reason\":\"noted\"}}]}";
            } catch (Exception e) {
                throw new AgentHostException("ack-only provider could not read packet", e);
            }
        }

        @Override
        public void close() {
        }
    }

    private static final class AckCountingProvider implements AgentProvider {
        private int prompts;
        private String lastPrompt;

        @Override
        public String name() {
            return "codex";
        }

        @Override
        public String sessionId() {
            return "ack-counting-session";
        }

        @Override
        public String prompt(String value) {
            prompts++;
            lastPrompt = value;
            try {
                ObjectNode packet = (ObjectNode) MAPPER.readTree(
                        value.substring(value.lastIndexOf("Packet:\n") + 8));
                StringBuilder cursors = new StringBuilder();
                for (JsonNode event : packet.path("events")) {
                    if (cursors.length() > 0) {
                        cursors.append(',');
                    }
                    cursors.append(event.path("cursor").asLong());
                }
                return "{\"handledEventCursors\":[" + cursors + "],\"commands\":[{"
                        + "\"tool\":\"host-ack\",\"arguments\":{"
                        + "\"reason\":\"worker update observed\"}}]}";
            } catch (Exception e) {
                throw new AgentHostException(
                        "ack-counting provider could not read packet", e);
            }
        }

        @Override
        public void close() {
        }
    }

    private static final class GuidanceThenAckProvider implements AgentProvider {
        private int prompts;
        private String lastPrompt;

        @Override
        public String name() {
            return "codex";
        }

        @Override
        public String sessionId() {
            return "guidance-then-ack-session";
        }

        @Override
        public String prompt(String value) {
            prompts++;
            lastPrompt = value;
            try {
                ObjectNode packet = (ObjectNode) MAPPER.readTree(
                        value.substring(value.lastIndexOf("Packet:\n") + 8));
                StringBuilder cursors = new StringBuilder();
                for (JsonNode event : packet.path("events")) {
                    if (cursors.length() > 0) {
                        cursors.append(',');
                    }
                    cursors.append(event.path("cursor").asLong());
                }
                if (ScriptedProvider.hasPayload(packet, "completion")) {
                    return "{\"handledEventCursors\":[" + cursors + "],\"commands\":[{"
                            + "\"tool\":\"host-ack\",\"arguments\":{"
                            + "\"reason\":\"worker update observed\"}}]}";
                }
                return "{\"handledEventCursors\":[" + cursors + "],\"commands\":[{"
                        + "\"tool\":\"delegation-message\",\"arguments\":{"
                        + "\"taskId\":\"" + TASK + "\",\"recipient\":\"" + WORKER + "\","
                        + "\"kind\":\"TASK_MESSAGE_KIND_GUIDANCE\","
                        + "\"text\":\"continue with the bounded implementation\"}}]}";
            } catch (Exception e) {
                throw new AgentHostException(
                        "guidance-then-ack provider could not read packet", e);
            }
        }

        @Override
        public void close() {
        }
    }

    private static final class AcceptThenProgressProvider implements AgentProvider {
        private final String session;
        private int prompts;

        private AcceptThenProgressProvider(String session) {
            this.session = session;
        }

        @Override
        public String name() {
            return "kimi";
        }

        @Override
        public String sessionId() {
            return session;
        }

        @Override
        public String prompt(String value) {
            prompts++;
            try {
                ObjectNode packet = (ObjectNode) MAPPER.readTree(
                        value.substring(value.lastIndexOf("Packet:\n") + 8));
                StringBuilder cursors = new StringBuilder();
                for (JsonNode event : packet.path("events")) {
                    if (cursors.length() > 0) {
                        cursors.append(',');
                    }
                    cursors.append(event.path("cursor").asLong());
                }
                if (ScriptedProvider.hasPayload(packet, "offer")) {
                    return "{\"handledEventCursors\":[" + cursors + "],\"commands\":[{"
                            + "\"tool\":\"delegation-accept\",\"arguments\":{"
                            + "\"taskId\":\"" + TASK + "\",\"attempt\":1}}]}";
                }
                return "{\"handledEventCursors\":[" + cursors + "],\"commands\":[{"
                        + "\"tool\":\"delegation-progress\",\"arguments\":{"
                        + "\"taskId\":\"" + TASK + "\",\"attempt\":1,"
                        + "\"message\":\"continued on the rebound session\"}}]}";
            } catch (Exception e) {
                throw new AgentHostException(
                        "accept-then-progress provider could not read packet", e);
            }
        }

        @Override
        public void close() {
        }
    }

    private static final class AcceptThenAckProvider implements AgentProvider {
        @Override
        public String name() {
            return "kimi";
        }

        @Override
        public String sessionId() {
            return "accept-then-ack-session";
        }

        @Override
        public String prompt(String value) {
            try {
                ObjectNode packet = (ObjectNode) MAPPER.readTree(
                        value.substring(value.lastIndexOf("Packet:\n") + 8));
                long cursor = packet.path("events").get(0).path("cursor").asLong();
                if (ScriptedProvider.hasPayload(packet, "offer")) {
                    return "{\"handledEventCursors\":[" + cursor + "],\"commands\":[{"
                            + "\"tool\":\"delegation-accept\",\"arguments\":{"
                            + "\"taskId\":\"" + TASK + "\",\"attempt\":1}}]}";
                }
                return "{\"handledEventCursors\":[" + cursor + "],\"commands\":[{"
                        + "\"tool\":\"host-ack\",\"arguments\":{"
                        + "\"reason\":\"noted\"}}]}";
            } catch (Exception e) {
                throw new AgentHostException(
                        "accept-then-ack provider could not read packet", e);
            }
        }

        @Override
        public void close() {
        }
    }
}
