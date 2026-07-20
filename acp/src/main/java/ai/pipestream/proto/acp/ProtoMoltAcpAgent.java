package ai.pipestream.proto.acp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import com.agentclientprotocol.sdk.agent.AcpAgent;
import com.agentclientprotocol.sdk.agent.AcpSyncAgent;
import com.agentclientprotocol.sdk.agent.SyncPromptContext;
import com.agentclientprotocol.sdk.agent.transport.StdioAcpAgentTransport;
import com.agentclientprotocol.sdk.spec.AcpAgentTransport;
import com.agentclientprotocol.sdk.spec.AcpSchema;

import java.util.List;
import java.util.UUID;

/**
 * The ProtoMolt action catalog as an Agent Client Protocol agent. An ACP-capable IDE
 * (JetBrains AI chat, Zed) launches this process and drives it over stdio: each session
 * is a console where a prompt of the form {@code <verb> <json>} runs the catalog verb and
 * the JSON result streams back as message chunks. The agent declares no file, terminal, or
 * permission capabilities; it is read-only. {@code main} only wires the stdio transport;
 * {@link #buildAgent} takes transport and catalog as arguments so tests drive the agent
 * in-memory.
 */
public final class ProtoMoltAcpAgent {

    public static void main(String[] args) {
        AcpSyncAgent agent = buildAgent(
                new StdioAcpAgentTransport(), ProtoMoltCatalog.full(ActionContext.create()));
        agent.run();
    }

    /**
     * Builds the catalog agent over any transport.
     *
     * @param transport the ACP transport (stdio in production, in-memory in tests)
     * @param catalog the action catalog to expose
     * @return the agent, ready to {@code start()} or {@code run()}
     */
    public static AcpSyncAgent buildAgent(AcpAgentTransport transport, ActionCatalog catalog) {
        CatalogLineRunner runner = new CatalogLineRunner(catalog);
        return AcpAgent.sync(transport)
                .initializeHandler(request -> new AcpSchema.InitializeResponse(
                        1, new AcpSchema.AgentCapabilities(), List.of()))
                .newSessionHandler(request -> new AcpSchema.NewSessionResponse(
                        UUID.randomUUID().toString(), null, null))
                .promptHandler((request, context) -> {
                    runner.run(promptText(request), context);
                    return new AcpSchema.PromptResponse(AcpSchema.StopReason.END_TURN);
                })
                .build();
    }

    private static String promptText(AcpSchema.PromptRequest request) {
        StringBuilder text = new StringBuilder();
        for (AcpSchema.ContentBlock block : request.prompt()) {
            if (block instanceof AcpSchema.TextContent textContent) {
                if (text.length() > 0) {
                    text.append('\n');
                }
                text.append(textContent.text());
            }
        }
        return text.toString();
    }

    private ProtoMoltAcpAgent() {
    }
}
