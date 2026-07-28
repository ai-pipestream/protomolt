package ai.pipestream.proto.acp;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Drives a ProtoMolt ACP agent over stdio and prints a transcript, as a runnable proof that the
 * agent answers the protocol, including when the agent is a container. It is the same exchange
 * an IDE runs: initialize, open a session, send one prompt, read the streamed reply.
 *
 * <p>The agent command is the program arguments, defaulting to the published ACP container:
 * {@code docker run -i --rm protomolt-acp:local}. Pass a different command to point it
 * elsewhere, e.g. {@code AcpSmoke java -cp ... ai.pipestream.proto.acp.ProtoMoltAcpAgent} to
 * drive the agent as a plain child process without a container. Invoked by the
 * {@code :protomolt-acp:acpSmoke} Gradle task and by {@code scripts/docker-smoke.sh}.</p>
 */
public final class AcpSmoke {

    private AcpSmoke() {
    }

    public static void main(String[] args) throws Exception {
        List<String> command = args.length > 0
                ? List.of(args)
                : List.of("docker", "run", "-i", "--rm", "protomolt-acp:local");

        StringBuffer transcript = new StringBuffer();

        System.out.println("acp-smoke: launching agent: " + String.join(" ", command));

        try (AcpClient client = AcpClient.launch(command.toArray(String[]::new))
                .withRequestTimeout(Duration.ofMinutes(3))
                .onSessionUpdate(params -> {
                    JsonNode update = params.path("update");
                    if ("agent_message_chunk".equals(update.path("sessionUpdate").asText())) {
                        transcript.append(update.path("content").path("text").asText());
                    }
                })) {

            JsonNode init = client.initialize();
            System.out.println("acp-smoke: initialized, protocol version "
                    + init.path("protocolVersion").asInt());

            String sessionId = client.newSession("/workspace");
            System.out.println("acp-smoke: session " + sessionId);

            client.prompt(sessionId, "list");

            String verbs = transcript.toString();
            System.out.println("acp-smoke: 'list' returned " + verbs.lines().count() + " lines");
            System.out.println("----------------------------------------");
            System.out.print(verbs);
            System.out.println("----------------------------------------");

            List<String> expected = new ArrayList<>(List.of("compile", "validate-message", "eval-cel"));
            expected.removeIf(verbs::contains);
            if (!expected.isEmpty()) {
                System.err.println("acp-smoke: FAILED: catalog is missing expected verbs: " + expected);
                System.exit(1);
            }
            System.out.println("acp-smoke: OK: the agent answered ACP and served its verb catalog");
        }
    }
}
