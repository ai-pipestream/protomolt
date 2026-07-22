package ai.pipestream.proto.acp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.StreamEmitter;
import ai.pipestream.proto.actions.StreamingAction;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import com.agentclientprotocol.sdk.agent.AcpSyncAgent;
import com.agentclientprotocol.sdk.client.AcpClient;
import com.agentclientprotocol.sdk.client.AcpSyncClient;
import com.agentclientprotocol.sdk.spec.AcpSchema;
import com.agentclientprotocol.sdk.test.InMemoryTransportPair;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the catalog agent through the ACP protocol in memory: initialize, open a session,
 * prompt with console lines, and collect the streamed message chunks — the same exchange an
 * IDE would run over stdio.
 */
class ProtoMoltAcpAgentTest {

    /**
     * Appended by the SDK's session-update thread, read and reset by the test thread, so it is
     * synchronized rather than a {@link StringBuilder}. This closes a real data race; it is not
     * the cause of the hang described on {@link #catalogVerbsRunThroughTheProtocol()}, which
     * still reproduced after the change.
     */
    private final StringBuffer chunks = new StringBuffer();

    /**
     * Above the SDK's 30s default, since these tests assert protocol behaviour rather than
     * latency and the machine may be contended. Raising it does not prevent the hang described
     * on {@link #catalogVerbsRunThroughTheProtocol()} — that call blocks for whatever bound is
     * set — but it keeps an honestly-slow run from being reported as a failure.
     */
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(3);

    private AcpSyncClient clientOver(InMemoryTransportPair pair) {
        return AcpClient.sync(pair.clientTransport())
                .requestTimeout(REQUEST_TIMEOUT)
                .sessionUpdateConsumer(notification -> {
                    if (notification.update() instanceof AcpSchema.AgentMessageChunk message
                            && message.content() instanceof AcpSchema.TextContent text) {
                        chunks.append(text.text());
                    }
                })
                .build();
    }

    /**
     * Tagged {@code acp-protocol} and excluded from the default build.
     *
     * <p>On a saturated machine {@link AcpSyncClient#prompt} has been seen never returning: the
     * call blocks until its request timeout instead of failing an assertion. What provokes it is
     * issuing more than one prompt on a session — the single-prompt test in this class has never
     * been observed hanging, while both multi-prompt tests have, on different prompts and
     * different verbs. So it is not the {@code compile} verb and not this agent; it is the SDK's
     * reactive client under contention, and it reproduces unchanged on the code as it stood
     * before this suite was reworked.
     *
     * <p>Run with {@code ./gradlew :protomolt-acp:acpProtocolTest}.
     */
    @Tag("acp-protocol")
    @Test
    void catalogVerbsRunThroughTheProtocol() throws Exception {
        InMemoryTransportPair pair = InMemoryTransportPair.create();
        AcpSyncAgent agent = ProtoMoltAcpAgent.buildAgent(
                pair.agentTransport(), ProtoMoltCatalog.full(ActionContext.create()));
        agent.start();
        try (AcpSyncClient client = clientOver(pair)) {
            client.initialize();
            var session = client.newSession(new AcpSchema.NewSessionRequest("/workspace", List.of()));

            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent("list"))));
            assertThat(chunks.toString()).contains("compile").contains("eval-cel");

            chunks.setLength(0);
            String compileLine = "compile {\"sources\":{\"p/m.proto\":"
                    + "\"syntax = \\\"proto3\\\"; package p; message M { string id = 1; }\"}}";
            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent(compileLine))));
            assertThat(chunks.toString()).contains("\"ok\" : true");

            chunks.setLength(0);
            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent("nope"))));
            assertThat(chunks.toString()).contains("Unknown verb 'nope'");
        }
    }

    /**
     * Verb input that is valid JSON but not an object used to be cast straight to
     * {@code ObjectNode}, so the IDE user saw a raw {@link ClassCastException} naming Jackson's
     * internal node classes; input that is not JSON at all reached the same cast. Both are
     * reported in the caller's terms now, and neither ends the session.
     *
     * <p>Both cases share one agent and session deliberately: each agent costs a transport pair,
     * and neither case needs its own. Proving the session survives requires a further prompt
     * after the bad one, which is what puts this in the {@code acp-protocol} lane — see
     * {@link #catalogVerbsRunThroughTheProtocol()}.
     */
    @Tag("acp-protocol")
    @Test
    void malformedVerbInputIsReportedInTheCallersTermsAndTheSessionSurvives() throws Exception {
        InMemoryTransportPair pair = InMemoryTransportPair.create();
        AcpSyncAgent agent = ProtoMoltAcpAgent.buildAgent(
                pair.agentTransport(), ProtoMoltCatalog.full(ActionContext.create()));
        agent.start();
        try (AcpSyncClient client = clientOver(pair)) {
            client.initialize();
            var session = client.newSession(new AcpSchema.NewSessionRequest("/workspace", List.of()));

            // Valid JSON, wrong shape: named by shape, not by Jackson's node classes.
            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent("compile [1,2,3]"))));
            assertThat(chunks.toString())
                    .contains("input must be a JSON object")
                    .contains("array")
                    .doesNotContain("ClassCastException")
                    .doesNotContain("ObjectNode");

            // Not JSON at all.
            chunks.setLength(0);
            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent("compile {not json"))));
            assertThat(chunks.toString()).contains("input is not JSON");

            // The session keeps going after both.
            chunks.setLength(0);
            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent("list"))));
            assertThat(chunks.toString()).contains("compile");
        }
    }

    @Test
    void streamingVerbChunksEachEmission() throws Exception {
        InMemoryTransportPair pair = InMemoryTransportPair.create();
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create());
        catalog.register(new StreamingAction() {
            @Override
            public String name() {
                return "tick-stream";
            }

            @Override
            public String description() {
                return "emits three ticks";
            }

            @Override
            public ObjectNode inputSchema() {
                return JsonNodeFactory.instance.objectNode();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                ObjectNode out = JsonNodeFactory.instance.objectNode();
                out.put("ticks", 3);
                return out;
            }

            @Override
            public void executeStreaming(ObjectNode input, ActionContext context,
                    StreamEmitter emitter) {
                for (int i = 1; i <= 3; i++) {
                    ObjectNode tick = JsonNodeFactory.instance.objectNode();
                    tick.put("tick", i);
                    emitter.emit(tick);
                }
            }
        });
        AcpSyncAgent agent = ProtoMoltAcpAgent.buildAgent(pair.agentTransport(), catalog);
        agent.start();
        try (AcpSyncClient client = clientOver(pair)) {
            client.initialize();
            var session = client.newSession(new AcpSchema.NewSessionRequest("/workspace", List.of()));
            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent("tick-stream"))));
            assertThat(chunks.toString())
                    .contains("\"tick\" : 1")
                    .contains("\"tick\" : 2")
                    .contains("\"tick\" : 3");
        }
    }
}
