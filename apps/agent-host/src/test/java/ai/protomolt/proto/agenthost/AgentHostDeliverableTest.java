package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.serve.ProtoMoltServe;
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

/**
 * A worker host and a coordinator exchange a task with a deliverable contract over a live
 * coordinator: the worker sees the contract as a type name and a schema, not descriptor
 * bytes; its candidate's deliverable is read with the contract's type and reaches the
 * coordinator; the contract survives a host restart from saved state.
 */
class AgentHostDeliverableTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "kimi-worker";
    private static final String TASK = "22222222-2222-4222-8222-222222222222";

    @TempDir
    Path temporary;

    @Test
    void theWorkerSeesTheContractAndItsDeliverableReachesTheCoordinator() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        Path statePath = temporary.resolve("state/kimi.json");
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0));
             McpHttpClient coordinator = new McpHttpClient(
                     URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp"),
                     () -> null)) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            ContractProvider provider = new ContractProvider();
            AgentHost worker = host(endpoint, statePath, workspace, provider);
            try (worker) {
                worker.connect();
                offer(coordinator);
                assertThat(worker.pollOnce()).isTrue();
            }
            JsonNode offer = provider.offers.get(0);
            JsonNode contract = offer.path("spec").path("contract");
            assertThat(contract.path("typeName").asText()).isEqualTo(DeliverableFixture.TYPE_NAME);
            assertThat(contract.path("schema").isObject())
                    .as("schema parsed into an object for the model").isTrue();
            assertThat(contract.path("schema").toString()).contains("headline");
            assertThat(contract.has("descriptorSet"))
                    .as("descriptor bytes are not shown to the model").isFalse();
            assertThat(provider.lastPrompt).contains(DeliverableFixture.TYPE_NAME);
            assertThat(provider.schemas).isNotEmpty();
            assertThat(provider.schemas.get(provider.schemas.size() - 1).toString())
                    .contains(DeliverableFixture.TYPE_URL);

            ObjectNode transcript = coordinator.callTool("delegation-transcript",
                    MAPPER.createObjectNode().put("taskId", TASK).put("maxEntries", 100));
            List<String> kinds = new ArrayList<>();
            for (JsonNode event : transcript.path("events")) {
                JsonNode frame = event.path("entry").path("workerFrame");
                frame.fieldNames().forEachRemaining(kinds::add);
            }
            assertThat(kinds).contains("accept", "completion");
            String recorded = transcript.toString();
            assertThat(recorded).contains(DeliverableFixture.TYPE_URL)
                    .contains("eight chars or more");

            AgentHostStateStore store = new AgentHostStateStore(statePath);
            AgentHostState saved = store.loadOrCreate(WORKER, AgentRole.WORKER, "kimi",
                    workspace);
            assertThat(saved.contracts()).containsKey(TASK);

            AgentHost resumed = host(endpoint, statePath, workspace, new ContractProvider());
            try (resumed) {
                assertThat(resumed.contracts()).containsKey(TASK);
            }
        }
    }

    private void offer(McpHttpClient coordinator) {
        ObjectNode spec = MAPPER.createObjectNode();
        spec.put("objective", "Write the review report as the contract's message");
        spec.putArray("requiredChecks").addObject().put("name", "report-written");
        ObjectNode contract = spec.putObject("contract");
        contract.put("descriptorSet", DeliverableFixture.descriptorSetBase64());
        contract.put("typeName", DeliverableFixture.TYPE_NAME);
        ObjectNode arguments = MAPPER.createObjectNode();
        arguments.put("workerId", WORKER);
        arguments.put("taskId", TASK);
        arguments.put("leaseSeconds", 600);
        arguments.set("spec", spec);
        ObjectNode reply = coordinator.callTool("delegation-offer", arguments);
        assertThat(reply.path("ok").asBoolean()).as(reply.toString()).isTrue();
    }

    private AgentHost host(URI endpoint, Path statePath, Path workspace,
                           AgentProvider provider) {
        AgentHostStateStore store = new AgentHostStateStore(statePath);
        AgentHostState state = store.loadOrCreate(WORKER, AgentRole.WORKER, "kimi", workspace);
        return new AgentHost(new AgentHost.Config(AgentRole.WORKER, WORKER, null,
                workspace.toAbsolutePath(), Duration.ofMillis(100), 64, false),
                new McpHttpClient(endpoint, () -> null), provider, store, state);
    }

    /** Accepts the offer and submits a deliverable of the contract's type. */
    private static final class ContractProvider implements AgentProvider {
        private final List<JsonNode> offers = new ArrayList<>();
        private final List<ObjectNode> schemas = new ArrayList<>();
        private String lastPrompt = "";

        @Override
        public String name() {
            return "kimi";
        }

        @Override
        public String sessionId() {
            return "kimi-session";
        }

        @Override
        public void outputSchema(ObjectNode schema) {
            schemas.add(schema.deepCopy());
        }

        @Override
        public String prompt(String prompt) {
            lastPrompt = prompt;
            try {
                ObjectNode packet = (ObjectNode) MAPPER.readTree(
                        prompt.substring(prompt.lastIndexOf("Packet:\n") + 8));
                List<Long> cursors = new ArrayList<>();
                ArrayNode commands = MAPPER.createArrayNode();
                for (JsonNode event : packet.path("events")) {
                    cursors.add(event.path("cursor").asLong());
                    JsonNode offer = event.path("entry").path("coordinatorFrame").path("offer");
                    if (offer.isObject()) {
                        offers.add(offer);
                        commands.add(command("delegation-accept", MAPPER.createObjectNode()
                                .put("taskId", TASK).put("attempt", 1)));
                        ObjectNode candidate = MAPPER.createObjectNode();
                        candidate.put("attempt", 1).put("revision", 1)
                                .put("summary", "the review report is written");
                        candidate.putArray("evidence").addObject()
                                .put("checkName", "report-written")
                                .put("verdict", "CHECK_VERDICT_PASSED")
                                .put("ranAt", "2026-09-04T08:00:00Z");
                        candidate.putArray("commits").addObject()
                                .put("repository", "git.rokkon.com/x/y")
                                .put("commit", "b185c5996b3868377b92682a65661e3f66769316");
                        ObjectNode result = candidate.putObject("result");
                        result.put("@type", DeliverableFixture.TYPE_URL);
                        result.put("headline", "eight chars or more");
                        result.put("findings", 2);
                        ObjectNode arguments = MAPPER.createObjectNode();
                        arguments.put("taskId", TASK);
                        arguments.set("candidate", candidate);
                        commands.add(command("delegation-candidate", arguments));
                    }
                }
                if (commands.isEmpty()) {
                    commands.add(command("host-ack",
                            MAPPER.createObjectNode().put("reason", "nothing to do")));
                }
                ObjectNode reply = MAPPER.createObjectNode();
                ArrayNode handled = reply.putArray("handledEventCursors");
                cursors.forEach(handled::add);
                reply.set("commands", commands);
                return reply.toString();
            } catch (Exception e) {
                throw new AgentHostException("contract provider could not read packet", e);
            }
        }

        private static ObjectNode command(String tool, ObjectNode arguments) {
            ObjectNode command = MAPPER.createObjectNode();
            command.put("tool", tool);
            command.set("arguments", arguments);
            return command;
        }

        @Override
        public void close() {
        }
    }
}
