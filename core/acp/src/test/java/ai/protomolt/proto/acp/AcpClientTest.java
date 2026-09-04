package ai.protomolt.proto.acp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The blocking client against raw {@link AcpConnection} peers: the request timeout, error
 * propagation from the peer, and the session-update listener's method filter.
 */
class AcpClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void requestThatNeverGetsAnAnswerTimesOut() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        CountDownLatch release = new CountDownLatch(1);
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        server.onRequest((method, params) -> {
            release.await(); // a wedged peer: accepts the request, never answers
            return null;
        });
        AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())
                .withRequestTimeout(Duration.ofMillis(300));
        try {
            assertThatThrownBy(client::initialize)
                    .isInstanceOfSatisfying(AcpError.class, error -> {
                        assertThat(error.code()).isEqualTo(AcpConnection.INTERNAL_ERROR);
                        assertThat(error.getMessage()).contains("initialize")
                                .contains("did not answer within");
                    });
        } finally {
            client.close();
            // Unblock the handler before closing: ExecutorService.close() waits for it.
            release.countDown();
            server.close();
        }
    }

    @Test
    void peerErrorArrivesWithItsCodeAndMessage() {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        server.onRequest((method, params) -> {
            throw new AcpError(-32001, "nope");
        });
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())) {
            assertThatThrownBy(client::initialize)
                    .isInstanceOfSatisfying(AcpError.class, error -> {
                        assertThat(error.code()).isEqualTo(-32001);
                        assertThat(error.getMessage()).isEqualTo("nope");
                    });
        } finally {
            server.close();
        }
    }

    @Test
    void bareErrorObjectDefaultsToInternalError() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        // A peer that answers with a bare error object: no code, no message. The client must
        // still surface an AcpError rather than a null-code surprise.
        try (JsonPipe wire = JsonPipe.over(ends[1].in(), ends[1].out());
                AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())) {
            Thread responder = Thread.ofVirtual().start(() -> {
                try {
                    String id = MAPPER.readTree(wire.take()).path("id").asText();
                    wire.send("{\"jsonrpc\":\"2.0\",\"id\":\"" + id + "\",\"error\":{}}");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            assertThatThrownBy(client::initialize)
                    .isInstanceOfSatisfying(AcpError.class, error -> {
                        assertThat(error.code()).isEqualTo(AcpConnection.INTERNAL_ERROR);
                        assertThat(error.getMessage()).isEqualTo("unknown error");
                    });
            responder.join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    @Test
    void onlySessionUpdateNotificationsReachTheListener() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch updated = new CountDownLatch(1);
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())
                .onSessionUpdate(params -> {
                    received.add(params.path("marker").asText());
                    updated.countDown();
                })) {
            ObjectNode other = MAPPER.createObjectNode();
            other.put("marker", "other");
            server.notify("other/event", other);
            // Notifications run inline in wire order, so once the session/update below is
            // delivered, the other/event before it was already filtered out.
            ObjectNode session = MAPPER.createObjectNode();
            session.put("marker", "session");
            server.notify("session/update", session);

            assertThat(updated.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(received).containsExactly("session");
        } finally {
            server.close();
        }
    }

    @Test
    void loadsAnExistingSessionWithItsWorkspace() {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        server.onRequest((method, params) -> {
            assertThat(method).isEqualTo("session/load");
            assertThat(params.path("sessionId").asText()).isEqualTo("saved-session");
            assertThat(params.path("cwd").asText()).isEqualTo("/work/project");
            assertThat(params.path("mcpServers")).isEmpty();
            return MAPPER.createObjectNode().put("sessionId", "saved-session");
        });
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())) {
            assertThat(client.loadSession("saved-session", "/work/project"))
                    .isEqualTo("saved-session");
        } finally {
            server.close();
        }
    }

    @Test
    void headlessPermissionPolicyApprovesOnlyOneUnambiguousChoice() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())
                .withPermissionPolicy(AcpClient.PermissionPolicy.ALLOW_SINGLE)) {
            ObjectNode params = MAPPER.createObjectNode();
            params.putArray("options")
                    .add(option("allow", "allow_once"))
                    .add(option("reject", "reject_once"));
            assertThat(server.request("session/request_permission", params)
                    .get(30, TimeUnit.SECONDS).path("outcome").path("optionId").asText())
                    .isEqualTo("allow");

            params.withArray("options").add(option("another", "allow_once"));
            assertThat(server.request("session/request_permission", params)
                    .get(30, TimeUnit.SECONDS).path("outcome").path("outcome").asText())
                    .isEqualTo("cancelled");
        } finally {
            server.close();
        }
    }

    private static ObjectNode option(String id, String kind) {
        return MAPPER.createObjectNode().put("optionId", id).put("kind", kind)
                .put("name", id);
    }

    @Test
    void authenticateSendsTheAdvertisedMethodId() {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        server.onRequest((method, params) -> {
            assertThat(method).isEqualTo("authenticate");
            assertThat(params.path("methodId").asText()).isEqualTo("cursor_login");
            return MAPPER.createObjectNode();
        });
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())) {
            assertThat(client.authenticate("cursor_login").isObject()).isTrue();
        } finally {
            server.close();
        }
    }

    @Test
    void advertisedAuthMethodsAreReadFromTheInitializeResult() {
        ObjectNode initialized = MAPPER.createObjectNode();
        initialized.putArray("authMethods").addObject().put("id", "cursor_login");
        assertThat(AcpClient.advertisesAuthMethod(initialized, "cursor_login")).isTrue();
        assertThat(AcpClient.advertisesAuthMethod(initialized, "other")).isFalse();
        assertThat(AcpClient.advertisesAuthMethod(MAPPER.createObjectNode(), "cursor_login"))
                .isFalse();
    }

    @Test
    void extensionHandlerAnswersAgentRequestsThePolicyDoesNotCover() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())
                .withPermissionPolicy(AcpClient.PermissionPolicy.REJECT)
                .withExtensionHandler((method, params) -> "cursor/create_plan".equals(method)
                        ? MAPPER.createObjectNode().put("answered",
                                params.path("toolCallId").asText())
                        : null)) {
            ObjectNode plan = MAPPER.createObjectNode().put("toolCallId", "call-7");
            assertThat(server.request("cursor/create_plan", plan).get(30, TimeUnit.SECONDS)
                    .path("answered").asText()).isEqualTo("call-7");
            assertThatThrownBy(() -> server.request("cursor/unknown", plan)
                    .get(30, TimeUnit.SECONDS))
                    .cause().isInstanceOf(AcpError.class)
                    .hasMessageContaining("unknown method: cursor/unknown");
        } finally {
            server.close();
        }
    }

    @Test
    void permissionOptionKindsMatchWithHyphensToo() throws Exception {
        TestPipes.End[] ends = TestPipes.pair();
        AcpConnection server = AcpConnection.over(ends[1].in(), ends[1].out()).start();
        try (AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())
                .withPermissionPolicy(AcpClient.PermissionPolicy.ALLOW_SINGLE)) {
            ObjectNode request = MAPPER.createObjectNode();
            ObjectNode allow = request.putArray("options").addObject();
            allow.put("kind", "allow-once");
            allow.put("optionId", "yes");
            JsonNode outcome = server.request("session/request_permission", request)
                    .get(30, TimeUnit.SECONDS).path("outcome");
            assertThat(outcome.path("outcome").asText()).isEqualTo("selected");
            assertThat(outcome.path("optionId").asText()).isEqualTo("yes");
        } finally {
            server.close();
        }
    }
}
