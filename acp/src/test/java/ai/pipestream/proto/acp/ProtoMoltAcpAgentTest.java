package ai.pipestream.proto.acp;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import com.agentclientprotocol.sdk.agent.AcpSyncAgent;
import com.agentclientprotocol.sdk.client.AcpClient;
import com.agentclientprotocol.sdk.client.AcpSyncClient;
import com.agentclientprotocol.sdk.spec.AcpSchema;
import com.agentclientprotocol.sdk.test.InMemoryTransportPair;
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
}
