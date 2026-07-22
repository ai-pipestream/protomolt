package ai.pipestream.proto.acp;

import com.agentclientprotocol.sdk.client.AcpClient;
import com.agentclientprotocol.sdk.client.AcpSyncClient;
import com.agentclientprotocol.sdk.client.transport.AgentParameters;
import com.agentclientprotocol.sdk.client.transport.StdioAcpClientTransport;
import com.agentclientprotocol.sdk.spec.AcpSchema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Drives a ProtoMolt ACP agent over stdio and prints a transcript, as a runnable proof that the
 * agent answers the protocol — including when the agent is a container. It is the same exchange an
 * IDE runs: initialize, open a session, send one prompt, read the streamed reply.
 *
 * <p>The agent command is the program arguments, defaulting to the published ACP container:
 * {@code docker run -i --rm protomolt-acp:local}. Pass a different command to point it elsewhere,
 * e.g. {@code AcpSmoke java -cp ... ai.pipestream.proto.acp.ProtoMoltAcpAgent} to drive the agent
 * in-process without a container. Invoked by the {@code :protomolt-acp:acpSmoke} Gradle task and by
 * {@code scripts/docker-smoke.sh}.
 *
 * <p>One prompt only, deliberately: multiple prompts on one session can wedge the SDK's client on a
 * loaded machine (see {@link ProtoMoltAcpAgentTest}), and a single {@code list} is enough to prove
 * the agent is live and serving the verb catalog.
 */
public final class AcpSmoke {

    private AcpSmoke() {
    }

    public static void main(String[] args) throws Exception {
        List<String> command = args.length > 0
                ? List.of(args)
                : List.of("docker", "run", "-i", "--rm", "protomolt-acp:local");

        StringBuilder transcript = new StringBuilder();
        AgentParameters parameters = AgentParameters.builder(command.get(0))
                .args(command.subList(1, command.size()).toArray(String[]::new))
                .build();

        System.out.println("acp-smoke: launching agent: " + String.join(" ", command));

        try (AcpSyncClient client = AcpClient.sync(new StdioAcpClientTransport(parameters))
                .requestTimeout(Duration.ofMinutes(3))
                .sessionUpdateConsumer(notification -> {
                    if (notification.update() instanceof AcpSchema.AgentMessageChunk message
                            && message.content() instanceof AcpSchema.TextContent text) {
                        transcript.append(text.text());
                    }
                })
                .build()) {

            AcpSchema.InitializeResponse init = client.initialize();
            System.out.println("acp-smoke: initialized, protocol version " + init.protocolVersion());

            var session = client.newSession(new AcpSchema.NewSessionRequest("/workspace", List.of()));
            System.out.println("acp-smoke: session " + session.sessionId());

            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent("list"))));

            String verbs = transcript.toString();
            System.out.println("acp-smoke: 'list' returned " + verbs.lines().count() + " lines");
            System.out.println("----------------------------------------");
            System.out.print(verbs);
            System.out.println("----------------------------------------");

            List<String> expected = new ArrayList<>(List.of("compile", "validate-message", "eval-cel"));
            expected.removeIf(verbs::contains);
            if (!expected.isEmpty()) {
                System.err.println("acp-smoke: FAILED — catalog is missing expected verbs: " + expected);
                System.exit(1);
            }
            System.out.println("acp-smoke: OK — the agent answered ACP and served its verb catalog");
        }
    }
}
