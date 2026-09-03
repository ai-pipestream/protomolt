package ai.protomolt.proto.acp.agent;

import ai.protomolt.proto.acp.AcpClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.List;

/**
 * A live self-hosting proof: through the ACP agent, reflect ProtoMolt's own running gRPC
 * service and then invoke one of its methods, over gRPC, and print the real answer. It is the
 * toolkit describing and calling itself, driven the way an IDE drives it.
 *
 * <p>Two verbs run on one session of one agent process: {@code reflect} fetches the server's
 * descriptor set over gRPC server reflection, and {@code grpc-invoke} calls a method with that
 * descriptor set as its schema.</p>
 *
 * <p>Arguments (via the {@code :protomolt-acp-agent:acpGrpcLive} task's {@code -P} properties):
 * {@code -Pagent} the command that launches the agent (default the container on the compose
 * network), {@code -Ptarget} the gRPC target (default {@code serve:9090}), {@code -Pmethod}
 * the fully qualified {@code Service/Method} (default {@code ProtoMoltService/ListTypes}).</p>
 */
public final class AcpGrpcLive {

    private static final ObjectMapper JSON = new ObjectMapper();

    private AcpGrpcLive() {
    }

    public static void main(String[] args) throws Exception {
        String agentCommand = args.length > 0 && !args[0].isBlank()
                ? args[0]
                : "docker run -i --rm --network protomolt_default protomolt-acp-agent:local";
        String target = args.length > 1 && !args[1].isBlank() ? args[1] : "serve:9090";
        String method = args.length > 2 && !args[2].isBlank()
                ? args[2]
                : "ai.pipestream.proto.grpc.service.v1.ProtoMoltService/ListTypes";
        String[] command = agentCommand.trim().split("\\s+");

        System.out.println("acp-grpc-live: agent   = " + agentCommand);
        System.out.println("acp-grpc-live: gRPC    = " + target);
        System.out.println("acp-grpc-live: method  = " + method);

        // Chunks of the current prompt turn; reset before each prompt. Written by the client's
        // notification listener on its reader thread, so synchronized.
        StringBuffer chunks = new StringBuffer();
        try (AcpClient client = AcpClient.launch(command)
                .withRequestTimeout(Duration.ofMinutes(3))
                .onSessionUpdate(params -> {
                    JsonNode update = params.path("update");
                    if ("agent_message_chunk".equals(update.path("sessionUpdate").asText())) {
                        chunks.append(update.path("content").path("text").asText());
                    }
                })) {
            client.initialize();
            String sessionId = client.newSession("/workspace");

            // 1) Discover the running service by reflection, over gRPC.
            ObjectNode reflect = JSON.createObjectNode();
            reflect.put("target", target);
            client.prompt(sessionId, "reflect " + JSON.writeValueAsString(reflect));
            JsonNode reflected = JSON.readTree(chunks.toString());
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
            chunks.setLength(0);
            ObjectNode invoke = JSON.createObjectNode();
            invoke.put("target", target);
            invoke.put("method", method);
            invoke.putObject("schema").put("descriptorSetBase64", descriptorSet);
            invoke.putObject("request");
            client.prompt(sessionId, "grpc-invoke " + JSON.writeValueAsString(invoke));

            JsonNode result = JSON.readTree(chunks.toString());
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
            System.out.println("acp-grpc-live: OK: the ACP agent reflected and invoked our own gRPC service");
        }
    }

    private static void fail(String message) {
        System.err.println("acp-grpc-live: FAILED: " + message);
        System.exit(1);
    }
}
