package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentTurnTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void workerMustAcknowledgeEveryCursorAndCannotSpoofItsIdentity() {
        String response = """
                {"handledEventCursors":[4,7],"commands":[{"tool":"delegation-message",
                 "arguments":{"taskId":"11111111-1111-1111-1111-111111111111",
                 "kind":"TASK_MESSAGE_KIND_QUESTION","text":"scope?"}}]}
                """;
        AgentTurn turn = AgentTurn.parse(response, AgentRole.WORKER, List.of(4L, 7L),
                "kimi-worker");
        ObjectNode arguments = turn.commands().get(0).arguments();
        assertThat(arguments.path("sender").asText()).isEqualTo("kimi-worker");
        assertThat(arguments.path("recipient").asText()).isEqualTo("coordinator");

        assertThatThrownBy(() -> AgentTurn.parse(response, AgentRole.WORKER,
                List.of(4L), "kimi-worker"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("exactly match");
    }

    @Test
    void roleToolAllowlistAndUnknownTopLevelFieldsAreClosed() {
        assertThatThrownBy(() -> AgentTurn.parse("""
                {"handledEventCursors":[1],"commands":[{"tool":"delegation-review",
                 "arguments":{}}]}
                """, AgentRole.WORKER, List.of(1L), "kimi"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("not allowed");
        assertThatThrownBy(() -> AgentTurn.parse("""
                {"handledEventCursors":[],"commands":[{"tool":"host-ack",
                 "arguments":{}}],"explanation":"trust me"}
                """, AgentRole.COORDINATOR, List.of(), "codex"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("unknown fields");
    }

    @Test
    void coordinatorMessageIdentityIsPinned() {
        ObjectNode response = MAPPER.createObjectNode();
        response.putArray("handledEventCursors").add(3);
        ObjectNode command = response.putArray("commands").addObject();
        command.put("tool", "delegation-message");
        command.putObject("arguments").put("sender", "worker-spoof");
        assertThatThrownBy(() -> AgentTurn.parse(response.toString(),
                AgentRole.COORDINATOR, List.of(3L), "codex"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("cannot override sender");
    }

    @Test
    void outputSchemaUsesOnlyRoleTools() {
        String schema = AgentTurn.outputSchema(AgentRole.COORDINATOR).toString();
        assertThat(schema).contains("delegation-review", "delegation-offer")
                .doesNotContain("delegation-candidate", "delegation-progress");
    }

    @Test
    void outputSchemasCloseAndRequireEveryObjectProperty() {
        for (AgentRole role : AgentRole.values()) {
            List<String> findings = new ArrayList<>();
            assertStrictObjects(AgentTurn.outputSchema(role), "$", findings);
            assertThat(findings).as("%s schema findings", role).isEmpty();
        }
    }

    private static void assertStrictObjects(JsonNode node, String path,
                                            List<String> findings) {
        if (node.isObject()) {
            if ("object".equals(node.path("type").asText())) {
                if (!node.path("additionalProperties").isBoolean()
                        || node.path("additionalProperties").asBoolean()) {
                    findings.add(path + " is not closed");
                }
                List<String> propertyNames = new ArrayList<>();
                node.path("properties").fieldNames().forEachRemaining(propertyNames::add);
                List<String> required = new ArrayList<>();
                node.path("required").forEach(entry -> required.add(entry.asText()));
                if (!required.equals(propertyNames)) {
                    findings.add(path + " does not require every property");
                }
            }
            node.properties().forEach(entry -> assertStrictObjects(entry.getValue(),
                    path + "." + entry.getKey(), findings));
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                assertStrictObjects(node.get(i), path + "[" + i + "]", findings);
            }
        }
    }
}
