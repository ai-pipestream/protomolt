package ai.protomolt.proto.msp;

import ai.protomolt.proto.acp.AcpConnection;
import ai.protomolt.proto.acp.AcpError;
import ai.protomolt.proto.acp.TestPipes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The MSP client against a stand-in session host on the other end of an in-memory pipe. */
class MspClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void handshakeSessionAndTurnCollectTheAgentMessageAndUsage() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection host = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        List<String> notifications = new CopyOnWriteArrayList<>();
        host.onNotification((method, params) -> notifications.add(method));
        host.onRequest((method, params) -> switch (method) {
            case "initialize" -> {
                assertThat(params.path("clientInfo").path("name").asText())
                        .isEqualTo("protomolt_test");
                yield MAPPER.createObjectNode().put("museHome", "/home/x/.local/share/muse");
            }
            case "session/start" -> {
                assertThat(params.path("workspaceRoot").asText()).isEqualTo("/work/repo");
                assertThat(params.path("modelId").asText()).isEqualTo("muse-spark-1.3");
                assertThat(params.path("approvalMode").asText()).isEqualTo("allowAll");
                assertThat(params.path("commandId").asText()).hasSize(36);
                yield session("ses-1");
            }
            case "turn/start" -> {
                String turnId = params.path("commandId").asText();
                assertThat(params.path("input").get(0).path("text").asText())
                        .isEqualTo("hello");
                assertThat(params.path("ifBusy").asText()).isEqualTo("queue");
                host.notify("item/completed", agentMessage("ses-1", turnId, "partial"));
                host.notify("item/completed", agentMessage("ses-1", turnId, "final answer"));
                host.notify("session/tokenUsage", tokenUsage("ses-1", turnId, 40, 8));
                host.notify("turn/completed", completed("ses-1", turnId, 40, 8));
                yield ack(turnId);
            }
            default -> throw new AcpError(-32601, "unknown method: " + method);
        });
        try (MspClient client = MspClient.over(ends[0].in(), ends[0].out())
                .withRequestTimeout(Duration.ofSeconds(30))) {
            assertThat(client.initialize("protomolt_test", "1").path("museHome").asText())
                    .isEqualTo("/home/x/.local/share/muse");
            assertThat(client.startSession("/work/repo", "muse-spark-1.3", "allowAll"))
                    .isEqualTo("ses-1");
            MspClient.TurnResult result = client.turn("ses-1", "hello");
            assertThat(result.completed()).isTrue();
            assertThat(result.text()).isEqualTo("final answer");
            assertThat(result.usage()).isEqualTo(new MspClient.TokenUsage(40, 8, 48));
            assertThat(client.cumulativeUsage()).isEqualTo(new MspClient.TokenUsage(40, 8, 48));
            assertThat(notifications).contains("initialized");
        } finally {
            host.close();
        }
    }

    @Test
    void aFailedTurnRaisesTheHostsReasonWithItsRetryFlag() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection host = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        host.onRequest((method, params) -> switch (method) {
            case "initialize" -> MAPPER.createObjectNode();
            case "session/start" -> session("ses-2");
            case "turn/start" -> {
                String turnId = params.path("commandId").asText();
                ObjectNode failed = completed("ses-2", turnId, 0, 0);
                failed.put("terminal", "failed");
                failed.putObject("error").put("kind", "overloaded")
                        .put("message", "provider is busy").put("retryable", true);
                host.notify("turn/completed", failed);
                yield ack(turnId);
            }
            default -> throw new AcpError(-32601, "unknown method: " + method);
        });
        try (MspClient client = MspClient.over(ends[0].in(), ends[0].out())) {
            client.initialize("protomolt_test", "1");
            String session = client.startSession(null, null, null);
            assertThatThrownBy(() -> client.turn(session, "work"))
                    .isInstanceOfSatisfying(MspError.class, error -> {
                        assertThat(error.code()).isEqualTo(MspError.TURN_FAILED);
                        assertThat(error.retryable()).isTrue();
                        assertThat(error.getMessage())
                                .contains("overloaded").contains("provider is busy");
                    });
        } finally {
            host.close();
        }
    }

    @Test
    void anApprovalRaisedMidTurnIsDecidedWithTheApprovedChoice() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection host = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        CompletableFuture<JsonNode> decision = new CompletableFuture<>();
        host.onRequest((method, params) -> switch (method) {
            case "initialize" -> MAPPER.createObjectNode();
            case "session/start" -> session("ses-3");
            case "turn/start" -> {
                String turnId = params.path("commandId").asText();
                ObjectNode approval = MAPPER.createObjectNode();
                approval.put("sessionId", "ses-3");
                approval.put("approvalId", "apr-1");
                approval.put("turnId", turnId);
                approval.putObject("currentRequirementId").put("approvalId", "apr-1")
                        .put("sourceIndex", 4);
                approval.putArray("availableChoices")
                        .addObject().put("choiceId", "deny").put("decision", "denied")
                        .put("label", "Deny").put("scope", "once");
                approval.withArray("availableChoices")
                        .addObject().put("choiceId", "allow").put("decision", "approved")
                        .put("label", "Allow").put("scope", "once");
                host.notify("approval/requested", approval);
                yield ack(turnId);
            }
            case "approval/decide" -> {
                decision.complete(params);
                host.notify("item/completed", agentMessage("ses-3",
                        params.path("commandId").asText(), "unused"));
                yield MAPPER.createObjectNode().put("status", "accepted").put("terminal", true);
            }
            default -> throw new AcpError(-32601, "unknown method: " + method);
        });
        try (MspClient client = MspClient.over(ends[0].in(), ends[0].out())
                .withTurnTimeout(Duration.ofSeconds(1))) {
            client.initialize("protomolt_test", "1");
            String session = client.startSession(null, null, "allowAll");
            // The turn itself never completes here; the approval is what is under test.
            assertThatThrownBy(() -> client.turn(session, "write a file"))
                    .isInstanceOf(MspError.class).hasMessageContaining("did not complete");
            JsonNode decided = decision.get(30, TimeUnit.SECONDS);
            assertThat(decided.path("approvalId").asText()).isEqualTo("apr-1");
            assertThat(decided.path("choiceId").asText()).isEqualTo("allow");
            assertThat(decided.path("sessionId").asText()).isEqualTo("ses-3");
            assertThat(decided.path("requirementId").path("sourceIndex").asInt()).isEqualTo(4);
            assertThat(decided.path("commandId").asText()).hasSize(36);
        } finally {
            host.close();
        }
    }

    @Test
    void resumeReturnsTheSessionTheHostReports() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection host = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        host.onRequest((method, params) -> switch (method) {
            case "initialize" -> MAPPER.createObjectNode();
            case "session/resume" -> {
                assertThat(params.path("sessionId").asText()).isEqualTo("ses-old");
                assertThat(params.path("excludeItems").asBoolean()).isTrue();
                ObjectNode result = session("ses-old");
                result.putArray("pendingRequests");
                result.putObject("history").put("mode", "none");
                yield result;
            }
            default -> throw new AcpError(-32601, "unknown method: " + method);
        });
        try (MspClient client = MspClient.over(ends[0].in(), ends[0].out())) {
            client.initialize("protomolt_test", "1");
            assertThat(client.resumeSession("ses-old")).isEqualTo("ses-old");
        } finally {
            host.close();
        }
    }

    @Test
    void theClientNameMustBeAMachineIdentifier() {
        TestPipes.End[] ends = TestPipes.pair();
        try (MspClient client = MspClient.over(ends[0].in(), ends[0].out())) {
            assertThatThrownBy(() -> client.initialize("protomolt-host", "1"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void uuid7IsVersionSevenAndTimeOrdered() {
        String first = Uuid7.nextString();
        String second = Uuid7.nextString();
        assertThat(first).matches("[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}");
        assertThat(second.substring(0, 12).compareTo(first.substring(0, 12)))
                .isGreaterThanOrEqualTo(0);
    }

    private static ObjectNode session(String id) {
        ObjectNode result = MAPPER.createObjectNode();
        result.putObject("session").put("id", id).put("status", "idle").putNull("activeTurnId");
        result.put("viewCursor", "v0");
        return result;
    }

    private static ObjectNode ack(String turnId) {
        ObjectNode ack = MAPPER.createObjectNode();
        ack.put("commandId", turnId);
        ack.put("turnId", turnId);
        ack.put("disposition", "started");
        ack.put("startedNewTurn", true);
        ack.put("status", "accepted");
        return ack;
    }

    private static ObjectNode agentMessage(String sessionId, String turnId, String text) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("sessionId", sessionId);
        params.put("viewCursor", "v1");
        params.putObject("sourceRange");
        params.putObject("item").put("itemId", turnId + "-m").put("kind", "agentMessage")
                .put("status", "completed").put("turnId", turnId).put("text", text);
        return params;
    }

    private static ObjectNode tokenUsage(String sessionId, String turnId, long prompt,
                                         long output) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("sessionId", sessionId);
        params.put("turnId", turnId);
        params.put("promptTokens", prompt);
        params.put("totalTokens", prompt + output);
        params.putObject("cumulative").put("promptTokens", prompt).put("outputTokens", output)
                .put("totalTokens", prompt + output);
        return params;
    }

    private static ObjectNode completed(String sessionId, String turnId, long input,
                                        long output) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("sessionId", sessionId);
        params.put("turnId", turnId);
        params.put("terminal", "completed");
        params.put("viewCursor", "v2");
        params.putObject("sourceRange");
        params.putObject("usage").put("inputTokens", input).put("outputTokens", output);
        return params;
    }
}
