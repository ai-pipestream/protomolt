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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the catalog agent through the ACP protocol in memory: initialize, open a session,
 * prompt with console lines, and collect the streamed message chunks — the same exchange an
 * IDE would run over stdio.
 */
class ProtoMoltAcpAgentTest {

    private final StringBuilder chunks = new StringBuilder();

    private AcpSyncClient clientOver(InMemoryTransportPair pair) {
        return AcpClient.sync(pair.clientTransport())
                .sessionUpdateConsumer(notification -> {
                    if (notification.update() instanceof AcpSchema.AgentMessageChunk message
                            && message.content() instanceof AcpSchema.TextContent text) {
                        chunks.append(text.text());
                    }
                })
                .build();
    }

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
