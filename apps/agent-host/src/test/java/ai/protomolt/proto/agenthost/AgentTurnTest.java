package ai.protomolt.proto.agenthost;

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
                 "arguments":{"taskId":"11111111-1111-4111-8111-111111111111",
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

    /**
     * Kimi narrates between its tool calls and the ACP provider joins every message
     * chunk of the turn, so the text reaching the parser is prose, then the JSON object,
     * sometimes inside a Markdown fence. The turn is the last complete JSON object in the
     * reply; the narration around it is not a reason to reject the turn.
     */
    @Test
    void narrationAroundTheJsonObjectIsIgnored() {
        String object = """
                {"handledEventCursors":[11],"commands":[{"tool":"delegation-accept",
                 "arguments":{"taskId":"11111111-1111-4111-8111-111111111111","attempt":1}}]}
                """.strip();
        String narrated = "I'll remove the block from buf.yaml now.\n"
                + "Running buf breaking {against origin/main} and buf lint... both clean.\n"
                + "Committed b185c599.\n\n```json\n" + object + "\n```\n"
                + "Let me know if you want anything else.";
        AgentTurn turn = AgentTurn.parse(narrated, AgentRole.WORKER, List.of(11L),
                "kimi-worker");
        assertThat(turn.handledEventCursors()).containsExactly(11L);
        assertThat(turn.commands()).hasSize(1);
        assertThat(turn.commands().get(0).tool()).isEqualTo("delegation-accept");

        String twoObjects = "First I considered {\"handledEventCursors\":[11],\"commands\":[]}"
                + " but the real answer is:\n" + object;
        assertThat(AgentTurn.parse(twoObjects, AgentRole.WORKER, List.of(11L), "kimi-worker")
                .commands()).hasSize(1);

        assertThatThrownBy(() -> AgentTurn.parse("Done. The block is gone and buf is clean.",
                AgentRole.WORKER, List.of(11L), "kimi-worker"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("not JSON");
    }

    /**
     * The first live Kimi worker closed its candidate reply with one brace too few, ten
     * times in a row, so no complete object existed to extract and every repair turn got
     * the same "not JSON". A reply whose only defect is closers missing at its very end is
     * completed; a reply that is unbalanced anywhere else is still rejected, and the
     * rejection names the parse position so the repair prompt says what to fix.
     */
    @Test
    void missingTrailingClosersAreCompletedAndOtherSyntaxErrorsAreLocated() {
        String complete = """
                {"handledEventCursors":[13],"commands":[{"tool":"delegation-progress",
                 "arguments":{"taskId":"11111111-1111-4111-8111-111111111111","attempt":1,
                 "message":"checks pass"}}]}
                """.strip();
        String oneBraceShort = complete.substring(0, complete.length() - 1);
        AgentTurn turn = AgentTurn.parse(oneBraceShort, AgentRole.WORKER, List.of(13L),
                "kimi-worker");
        assertThat(turn.commands().get(0).tool()).isEqualTo("delegation-progress");

        String twoShort = complete.substring(0, complete.length() - 3);
        assertThat(AgentTurn.parse(twoShort, AgentRole.WORKER, List.of(13L), "kimi-worker")
                .commands()).hasSize(1);

        String brokenInside = complete.replace("\"attempt\":1,", "\"attempt\":1");
        assertThatThrownBy(() -> AgentTurn.parse(brokenInside, AgentRole.WORKER,
                List.of(13L), "kimi-worker"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("not JSON")
                .hasMessageContaining("column");
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

    @Test
    void ordinaryJsonProvidersCannotBypassTheAdvertisedCommandSchema() {
        assertThatThrownBy(() -> AgentTurn.parse("""
                {"handledEventCursors":[4],"commands":[{
                  "tool":"delegation-progress",
                  "arguments":{
                    "taskId":"11111111-1111-4111-8111-111111111111",
                    "message":"working"
                  }}]}
                """, AgentRole.WORKER, List.of(4L), "kimi-worker"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("command 0 delegation-progress:")
                .hasMessageContaining("attempt field is required");

        assertThatThrownBy(() -> AgentTurn.parse("""
                {"handledEventCursors":[4],"commands":[{
                  "tool":"delegation-progress",
                  "arguments":{
                    "taskId":"11111111-1111-4111-8111-111111111111",
                    "attempt":1,
                    "message":"working",
                    "unreviewed":true
                  }}]}
                """, AgentRole.WORKER, List.of(4L), "kimi-worker"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("unknown field 'unreviewed'");

        AgentTurn valid = AgentTurn.parse("""
                {"handledEventCursors":[4],"commands":[{
                  "tool":"delegation-progress",
                  "arguments":{
                    "taskId":"11111111-1111-4111-8111-111111111111",
                    "attempt":1,
                    "message":"working"
                  }}]}
                """, AgentRole.WORKER, List.of(4L), "kimi-worker");
        assertThat(valid.commands().getFirst().arguments().path("workerId").asText())
                .isEqualTo("kimi-worker");
    }

    /**
     * The bounds a command is held to are the request message's own. A task id that is not
     * a uuid is refused in the words of the rule the proto states, not in the words of a
     * schema kept alongside it.
     */
    @Test
    void aTaskIdThatIsNotAUuidIsRefusedInTheProtoRulesWords() {
        assertThatThrownBy(() -> AgentTurn.parse("""
                {"handledEventCursors":[4],"commands":[{
                  "tool":"delegation-accept",
                  "arguments":{"taskId":"task-1","attempt":1}}]}
                """, AgentRole.WORKER, List.of(4L), "kimi-worker"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("command 0 delegation-accept:")
                .hasMessageContaining("taskId value must be a valid UUID");
    }

    /**
     * A candidate that references neither a commit nor an artifact breaks a rule the
     * candidate message states about itself as a whole, which only running the message's
     * own CEL can catch.
     */
    @Test
    void aCandidateWithNoCommitOrArtifactIsRefusedByTheMessageRule() {
        assertThatThrownBy(() -> AgentTurn.parse("""
                {"handledEventCursors":[4],"commands":[{
                  "tool":"delegation-candidate",
                  "arguments":{
                    "taskId":"11111111-1111-4111-8111-111111111111",
                    "candidate":{
                      "attempt":1,"revision":1,"summary":"done",
                      "evidence":[{"checkName":"unit-tests",
                        "verdict":"CHECK_VERDICT_PASSED",
                        "ranAt":"2026-09-01T00:00:00Z","detail":"the suite passed"}]
                    }}}]}
                """, AgentRole.WORKER, List.of(4L), "kimi-worker"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("a completion candidate must reference at least one "
                        + "commit or artifact");
    }

    /**
     * A member no field of the request message declares is refused wherever it sits, and
     * the refusal names the command that carried it so the repair turn knows which one.
     */
    @Test
    void aMemberNoRequestMessageDeclaresIsRefused() {
        assertThatThrownBy(() -> AgentTurn.parse("""
                {"handledEventCursors":[4],"commands":[{
                  "tool":"delegation-candidate",
                  "arguments":{
                    "taskId":"11111111-1111-4111-8111-111111111111",
                    "candidate":{
                      "attempt":1,"revision":1,"summary":"done","confidence":"high",
                      "evidence":[{"checkName":"unit-tests",
                        "verdict":"CHECK_VERDICT_PASSED",
                        "ranAt":"2026-09-01T00:00:00Z"}],
                      "commits":[{"repository":"example/repo",
                        "commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "subject":"the change"}]
                    }}}]}
                """, AgentRole.WORKER, List.of(4L), "kimi-worker"))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining(
                        "command 0 delegation-candidate contains unknown field 'confidence'");
    }

    /**
     * The evidence structure the model is offered is the one the proto declares, down to
     * the members no hand-written mirror carried: check evidence references its own
     * artifacts.
     */
    @Test
    void theRenderedSchemaCarriesTheDescriptorsEvidenceStructure() {
        String schema = AgentTurn.outputSchema(AgentRole.WORKER).toString();
        assertThat(schema).contains("CheckEvidence").contains("checkName")
                .contains("artifacts").contains("sha256");
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
