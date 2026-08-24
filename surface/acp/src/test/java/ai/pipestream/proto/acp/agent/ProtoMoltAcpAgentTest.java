package ai.pipestream.proto.acp.agent;

import ai.pipestream.proto.acp.AcpAgent;
import ai.pipestream.proto.acp.AcpClient;
import ai.pipestream.proto.acp.TestPipes;
import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.JsonStreamEmitter;
import ai.pipestream.proto.actions.JsonStreamingAction;
import ai.pipestream.proto.actions.StreamEmitter;
import ai.pipestream.proto.actions.StreamingAction;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Struct;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the catalog agent through the ACP protocol over in-memory pipes: initialize, open a
 * session, prompt with console lines, and collect the streamed session/update chunks, the same
 * exchange an IDE runs over stdio. The client and agent are the first-party virtual-thread
 * implementation in {@code protomolt-acp}; no SDK, no reactive runtime.
 */
class ProtoMoltAcpAgentTest {

    // Appended by the client's notification listener and read by the test thread; the listener
    // runs on the client's reader virtual thread, so this is synchronized rather than a
    // StringBuilder.
    private final StringBuffer chunks = new StringBuffer();

    // These tests assert protocol behaviour, not latency; the bound only has to catch a real
    // hang, so it sits far above any honest run even on a contended machine.
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(3);

    private record Harness(AcpClient client, AcpAgent agent) implements AutoCloseable {
        @Override
        public void close() {
            client.close();
            agent.close();
        }
    }

    private Harness harness(ActionCatalog catalog) {
        TestPipes.End[] ends = TestPipes.pair();
        AcpAgent agent = ProtoMoltAcpAgent.buildAgent(ends[1].in(), ends[1].out(), catalog);
        agent.start();
        AcpClient client = AcpClient.over(ends[0].in(), ends[0].out())
                .withRequestTimeout(REQUEST_TIMEOUT)
                .onSessionUpdate(params -> {
                    JsonNode update = params.path("update");
                    if ("agent_message_chunk".equals(update.path("sessionUpdate").asText())
                            && "text".equals(update.path("content").path("type").asText())) {
                        chunks.append(update.path("content").path("text").asText());
                    }
                });
        return new Harness(client, agent);
    }

    /**
     * Tagged {@code acp-protocol} and excluded from the default build: the compile prompt
     * shells out to protoc, which is slower than anything the default test task runs. Several
     * prompts share one session deliberately: a session that dies after one prompt is exactly
     * the failure this lane exists to catch.
     *
     * <p>Run with {@code ./gradlew :protomolt-acp-agent:acpProtocolTest}.</p>
     */
    @Tag("acp-protocol")
    @Test
    void catalogVerbsRunThroughTheProtocol() {
        try (Harness harness = harness(ProtoMoltCatalog.full(ActionContext.create()))) {
            AcpClient client = harness.client();
            JsonNode init = client.initialize();
            assertThat(init.path("protocolVersion").asInt()).isEqualTo(1);
            String sessionId = client.newSession("/workspace");
            assertThat(sessionId).isNotBlank();

            JsonNode response = client.prompt(sessionId, "list");
            assertThat(response.path("stopReason").asText()).isEqualTo("end_turn");
            assertThat(chunks.toString()).contains("compile").contains("eval-cel");

            chunks.setLength(0);
            String compileLine = "compile {\"sources\":{\"p/m.proto\":"
                    + "\"syntax = \\\"proto3\\\"; package p; message M { string id = 1; }\"}}";
            response = client.prompt(sessionId, compileLine);
            assertThat(response.path("stopReason").asText()).isEqualTo("end_turn");
            assertThat(chunks.toString()).contains("\"ok\" : true");

            chunks.setLength(0);
            response = client.prompt(sessionId, "nope");
            assertThat(response.path("stopReason").asText()).isEqualTo("end_turn");
            assertThat(chunks.toString()).contains("Unknown verb 'nope'");
        }
    }

    /**
     * Verb input that is valid JSON but not an object used to be cast straight to
     * {@code ObjectNode}, so the IDE user saw a raw {@link ClassCastException} naming Jackson's
     * internal node classes; input that is not JSON at all reached the same cast. Both are
     * reported in the caller's terms now, and neither ends the session.
     *
     * <p>Both cases share one agent and session deliberately: each agent costs a pipe pair,
     * and neither case needs its own. Proving the session survives requires a further prompt
     * after the bad one, which is what puts this in the {@code acp-protocol} lane.</p>
     */
    @Tag("acp-protocol")
    @Test
    void malformedVerbInputIsReportedInTheCallersTermsAndTheSessionSurvives() {
        try (Harness harness = harness(ProtoMoltCatalog.full(ActionContext.create()))) {
            AcpClient client = harness.client();
            client.initialize();
            String sessionId = client.newSession("/workspace");

            // Valid JSON, wrong shape: named by shape, not by Jackson's node classes.
            client.prompt(sessionId, "compile [1,2,3]");
            assertThat(chunks.toString())
                    .contains("input must be a JSON object")
                    .contains("array")
                    .doesNotContain("ClassCastException")
                    .doesNotContain("ObjectNode");

            // Not JSON at all.
            chunks.setLength(0);
            client.prompt(sessionId, "compile {not json");
            assertThat(chunks.toString()).contains("input is not JSON");

            // The session keeps going after both.
            chunks.setLength(0);
            client.prompt(sessionId, "list");
            assertThat(chunks.toString()).contains("compile");
        }
    }

    @Test
    void streamingVerbChunksEachEmission() {
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create());
        catalog.register(new JsonStreamingAction() {
            @Override
            public String name() {
                return "tick-stream";
            }

            @Override
            public String description() {
                return "emits three ticks";
            }

            @Override
            public Descriptor requestType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public Descriptor responseType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                ObjectNode out = JsonNodeFactory.instance.objectNode();
                out.put("ticks", 3);
                return out;
            }

            @Override
            public void executeStreaming(ObjectNode input, ActionContext context,
                    JsonStreamEmitter emitter) throws ActionException {
                for (int i = 1; i <= 3; i++) {
                    ObjectNode tick = JsonNodeFactory.instance.objectNode();
                    tick.put("tick", i);
                    emitter.emit(tick);
                }
            }
        });
        try (Harness harness = harness(catalog)) {
            AcpClient client = harness.client();
            client.initialize();
            String sessionId = client.newSession("/workspace");
            client.prompt(sessionId, "tick-stream");
            assertThat(chunks.toString())
                    .contains("\"tick\" : 1")
                    .contains("\"tick\" : 2")
                    .contains("\"tick\" : 3");
        }
    }
}
