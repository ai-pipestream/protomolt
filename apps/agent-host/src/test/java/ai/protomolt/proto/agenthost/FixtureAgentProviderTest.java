package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.samples.starter.v1.CoordinationReport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixtureAgentProviderTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TASK = UUID.randomUUID().toString();
    @TempDir Path workspace;

    @Test void supportedOfferStoresValidatedReportAndClaimsOnlyTheFixtureCheck()
            throws Exception {
        DeliverableContract contract = contract();
        FixtureAgentProvider provider = new FixtureAgentProvider(workspace);
        provider.deliverableContracts(Map.of(TASK, contract));

        String reply = provider.prompt("events\nPacket:\n" + packet(offer(true)));
        AgentTurn turn = AgentTurn.parse(reply, AgentRole.WORKER, List.of(11L),
                "fixture-worker", Map.of(TASK, contract));

        assertThat(turn.commands()).extracting(AgentTurn.Command::tool)
                .containsExactly("delegation-accept", "delegation-candidate");
        JsonNode candidate = JSON.readTree(reply).path("commands").get(1)
                .path("arguments").path("candidate");
        assertThat(candidate.path("evidence").get(0).path("checkName").asText())
                .isEqualTo(FixtureAgentProvider.CHECK);
        assertThat(candidate.path("evidence").get(0).path("detail").asText())
                .contains("Fixture-only").contains("no external tests");
        assertThat(candidate.path("commits").isMissingNode()).isTrue();
        String digest = candidate.path("artifacts").get(0).path("sha256").asText();
        byte[] bytes = Files.readAllBytes(workspace.resolve("artifacts").resolve(digest));
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(bytes))).isEqualTo(digest);
        CoordinationReport report = CoordinationReport.parseFrom(bytes);
        assertThat(report.getFindingCount()).isEqualTo(report.getFindingsCount()).isEqualTo(1);
    }

    @Test void unsupportedOfferRejectsWithoutCreatingAnArtifact() throws Exception {
        FixtureAgentProvider provider = new FixtureAgentProvider(workspace);
        provider.deliverableContracts(Map.of(TASK, contract()));

        String reply = provider.prompt("events\nPacket:\n" + packet(offer(false)));
        AgentTurn turn = AgentTurn.parse(reply, AgentRole.WORKER, List.of(11L),
                "fixture-worker", Map.of(TASK, contract()));

        assertThat(turn.commands()).extracting(AgentTurn.Command::tool)
                .containsExactly("delegation-reject");
        JsonNode rejection = JSON.readTree(reply).path("commands").get(0).path("arguments");
        assertThat(rejection.path("attempt").asInt()).isEqualTo(1);
        assertThat(rejection.path("reason").asText()).contains("objective");
        assertThat(Files.exists(workspace.resolve("artifacts"))).isFalse();
    }

    @Test void fixtureCliIsWorkerOnlyAndKeepsEventBatchesWithinTheCommandLimit() {
        String[] worker = {"--endpoint", "http://localhost:8080/mcp", "--role", "worker",
                "--identity", "fixture-worker", "--provider", "fixture", "--workspace",
                workspace.toString(), "--state", workspace.resolve("host.json").toString()};
        assertThat(AgentHostMain.Options.parse(worker).maxEvents()).isEqualTo(8);
        String[] coordinator = worker.clone();
        coordinator[3] = "coordinator";
        assertThatThrownBy(() -> AgentHostMain.Options.parse(coordinator))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only the worker role");
    }

    private static String packet(ObjectNode frame) {
        ObjectNode packet = JSON.createObjectNode().put("role", "worker");
        ArrayNode events = packet.putArray("events");
        ObjectNode event = events.addObject().put("cursor", 11).put("taskId", TASK);
        event.putObject("entry").set("coordinatorFrame", frame);
        return packet.toString();
    }

    private static ObjectNode offer(boolean supported) {
        ObjectNode spec = JSON.createObjectNode()
                .put("objective", supported ? "Produce a coordination report for this task"
                        : "Execute production deployment");
        spec.putArray("requiredChecks").addObject()
                .put("name", FixtureAgentProvider.CHECK);
        return JSON.createObjectNode().set("offer", JSON.createObjectNode()
                .put("attempt", 1).set("spec", spec));
    }

    private static DeliverableContract contract() {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        add(CoordinationReport.getDescriptor().getFile(), files);
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder();
        files.values().forEach(file -> set.addFile(file.toProto()));
        return DeliverableContract.newBuilder().setDescriptorSet(set.build().toByteString())
                .setTypeName(FixtureAgentProvider.TYPE).build();
    }

    private static void add(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> add(dependency, files));
        files.put(file.getName(), file);
    }
}
