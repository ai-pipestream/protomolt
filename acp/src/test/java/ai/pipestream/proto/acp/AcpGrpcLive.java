package ai.pipestream.proto.acp;

import com.agentclientprotocol.sdk.client.AcpClient;
import com.agentclientprotocol.sdk.client.AcpSyncClient;
import com.agentclientprotocol.sdk.client.transport.AgentParameters;
import com.agentclientprotocol.sdk.client.transport.StdioAcpClientTransport;
import com.agentclientprotocol.sdk.spec.AcpSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.List;

/**
 * A live self-hosting proof: through the ACP agent, reflect ProtoMolt's own running gRPC service
 * and then invoke one of its methods, over gRPC, and print the real answer. It is the toolkit
 * describing and calling itself, driven the way an IDE drives it.
 *
 * <p>Two verbs run: {@code reflect} fetches the server's descriptor set over gRPC server
 * reflection, and {@code grpc-invoke} calls a method with that descriptor set as its schema. Each
 * runs in a fresh agent process with a single prompt — issuing more than one prompt on one session
 * can wedge the SDK client under load (see {@link ProtoMoltAcpAgentTest}), and this keeps every
 * session to one prompt.
 *
 * <p>Arguments (via the {@code :protomolt-acp:acpGrpcLive} task's {@code -P} properties):
 * {@code -Pagent} the command that launches the agent (default the container on the compose
 * network), {@code -Ptarget} the gRPC target (default {@code serve:9090}), {@code -Pmethod} the
 * fully qualified {@code Service/Method} (default {@code ProtoMoltService/ListTypes}).
 */
public final class AcpGrpcLive {

    private static final ObjectMapper JSON = new ObjectMapper();

    private AcpGrpcLive() {
    }

    public static void main(String[] args) throws Exception {
        String agentCommand = args.length > 0 && !args[0].isBlank()
                ? args[0]
                : "docker run -i --rm --network protomolt_default protomolt-acp:local";
        String target = args.length > 1 && !args[1].isBlank() ? args[1] : "serve:9090";
        String method = args.length > 2 && !args[2].isBlank()
                ? args[2]
                : "ai.pipestream.protomolt.v1.ProtoMoltService/ListTypes";
        String[] command = agentCommand.trim().split("\\s+");

        System.out.println("acp-grpc-live: agent   = " + agentCommand);
        System.out.println("acp-grpc-live: gRPC    = " + target);
        System.out.println("acp-grpc-live: method  = " + method);

        // 1) Discover the running service by reflection, over gRPC.
        String reflectLine = "reflect " + JSON.writeValueAsString(obj("target", target));
        JsonNode reflected = JSON.readTree(runOnePrompt(command, reflectLine));
        if (!reflected.path("ok").asBoolean(false)) {
            fail("reflect failed: " + reflected.path("error").asText(reflected.toString()));
        }
        List<String> services = new java.util.ArrayList<>();
        reflected.path("services").forEach(s -> services.add(s.asText()));
        System.out.println("acp-grpc-live: reflected " + reflected.path("fileCount").asInt()
                + " files, services: " + services);
        String descriptorSet = reflected.path("descriptorSetBase64").asText();
        if (descriptorSet.isBlank()) {
            fail("reflect returned no descriptor set");
        }

        // 2) Call one of its methods, over gRPC, with the reflected schema.
        ObjectNode invoke = JSON.createObjectNode();
        invoke.put("target", target);
        invoke.put("method", method);
        invoke.putObject("schema").put("descriptorSetBase64", descriptorSet);
        invoke.putObject("request");
        String invokeLine = "grpc-invoke " + JSON.writeValueAsString(invoke);

        JsonNode result = JSON.readTree(runOnePrompt(command, invokeLine));
        if (!result.path("ok").asBoolean(true) && result.has("ok")) {
            fail("grpc-invoke failed: " + result.path("error").asText(result.toString()));
        }
        System.out.println("acp-grpc-live: " + method + " answered:");
        System.out.println("----------------------------------------");
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        System.out.println("----------------------------------------");

        boolean describesItself = services.stream()
                .anyMatch(s -> s.contains("ProtoMoltService"));
        if (!describesItself) {
            fail("the reflected services did not include ProtoMoltService: " + services);
        }
        System.out.println("acp-grpc-live: OK — the ACP agent reflected and invoked our own gRPC service");
    }

    /**
     * Launches a fresh agent, runs a single prompt, and returns the concatenated message text —
     * which for a verb line is the verb's JSON result.
     */
    private static String runOnePrompt(String[] command, String promptLine) throws Exception {
        StringBuilder chunks = new StringBuilder();
        AgentParameters parameters = AgentParameters.builder(command[0])
                .args(java.util.Arrays.copyOfRange(command, 1, command.length))
                .build();
        try (AcpSyncClient client = AcpClient.sync(new StdioAcpClientTransport(parameters))
                .requestTimeout(Duration.ofMinutes(3))
                .sessionUpdateConsumer(notification -> {
                    if (notification.update() instanceof AcpSchema.AgentMessageChunk message
                            && message.content() instanceof AcpSchema.TextContent text) {
                        chunks.append(text.text());
                    }
                })
                .build()) {
            client.initialize();
            var session = client.newSession(new AcpSchema.NewSessionRequest("/workspace", List.of()));
            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent(promptLine))));
        }
        return chunks.toString();
    }

    private static ObjectNode obj(String key, String value) {
        ObjectNode node = JSON.createObjectNode();
        node.put(key, value);
        return node;
    }

    private static void fail(String message) {
        System.err.println("acp-grpc-live: FAILED — " + message);
        System.exit(1);
    }
}
