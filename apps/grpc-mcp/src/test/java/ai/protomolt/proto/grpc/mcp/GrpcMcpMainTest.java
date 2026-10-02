package ai.protomolt.proto.grpc.mcp;

import ai.protomolt.proto.composer.Composer;
import ai.protomolt.proto.composer.ComposerException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcMcpMainTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void reflectsAndInvokesARealEndpointThroughTheSpiAssembly() throws Exception {
        var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .addService(new HealthStatusManager().getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance()).build().start();
        try {
            String target = "127.0.0.1:" + server.getPort();
            Map<String, String> env = Map.of("PROTOMOLT_GRPC_TARGET", target,
                    "PROTOMOLT_GRPC_TRANSPORT", "plaintext");
            var listed = exchange(env, mapper.createObjectNode().put("method", "tools/list"));
            assertThat(listed.path("result").path("tools").findValuesAsText("name"))
                    .containsExactly("reflect", "grpc-invoke");
            var reflected = exchange(env, tool("reflect", mapper.createObjectNode()
                    .put("target", target).put("tls", false)));
            assertThat(reflected.path("result").path("isError").asBoolean()).isFalse();
            var schema = reflected.path("result").path("structuredContent");
            assertThat(schema.path("ok").asBoolean()).isTrue();
            assertThat(schema.path("services").toString()).contains("grpc.health.v1.Health");
            var arguments = mapper.createObjectNode().put("target", target)
                    .put("tls", false).put("method", "grpc.health.v1.Health/Check");
            arguments.putObject("schema").put("descriptorSetBase64",
                    schema.path("descriptorSetBase64").asText());
            arguments.putObject("request");
            var invoked = exchange(env, tool("grpc-invoke", arguments));
            var result = invoked.path("result").path("structuredContent");
            assertThat(result.path("ok").asBoolean()).as(invoked.toString()).isTrue();
            assertThat(result.path("responses").get(0).path("status").asText()).isEqualTo("SERVING");

            arguments.put("target", "127.0.0.2:" + server.getPort());
            var refused = exchange(env, tool("grpc-invoke", arguments));
            assertThat(refused.path("result").path("isError").asBoolean()).isTrue();
            assertThat(refused.toString()).contains("host is not allowed");
            arguments.put("target", target).put("tls", true);
            assertThat(exchange(env, tool("grpc-invoke", arguments)).toString())
                    .contains("TLS outbound channels are disabled");
            arguments.put("tls", false).put("deadlineMs", -1);
            assertThat(exchange(env, tool("grpc-invoke", arguments))
                    .path("result").path("isError").asBoolean()).isTrue();
        } finally {
            server.shutdownNow();
            assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void requiresConfigurationAndAnInstalledModuleBeforeWritingProtocolOutput() {
        for (var env : List.of(Map.<String, String>of(),
                Map.of("PROTOMOLT_GRPC_TARGET", "localhost:9090"),
                Map.of("PROTOMOLT_GRPC_TARGET", "localhost:9090", "PROTOMOLT_GRPC_TRANSPORT", "guess"),
                Map.of("PROTOMOLT_GRPC_TARGET", "not a target", "PROTOMOLT_GRPC_TRANSPORT", "tls"))) {
            var output = new ByteArrayOutputStream();
            assertThatThrownBy(() -> GrpcMcpMain.run(env, new ByteArrayInputStream(new byte[0]), output))
                    .isInstanceOf(ComposerException.class);
            assertThat(output.size()).isZero();
        }
        assertThatThrownBy(() -> Composer.emptyBuilder().build().boot(List.of("grpc-invoke")))
                .isInstanceOf(ComposerException.class).hasMessageContaining("grpc-invoke");
    }

    private ObjectNode tool(String name, ObjectNode arguments) {
        var request = mapper.createObjectNode().put("method", "tools/call");
        request.putObject("params").put("name", name).set("arguments", arguments);
        return request;
    }

    private JsonNode exchange(Map<String, String> env, ObjectNode request) throws Exception {
        request.put("jsonrpc", "2.0").put("id", 2);
        String messages = """
                {"jsonrpc":"2.0","id":1,"method":"initialize"}
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """ + request + "\n";
        var output = new ByteArrayOutputStream();
        GrpcMcpMain.run(env, new ByteArrayInputStream(messages.getBytes(StandardCharsets.UTF_8)), output);
        var lines = output.toString(StandardCharsets.UTF_8).lines().toList();
        assertThat(lines).hasSize(2);
        return mapper.readTree(lines.get(1));
    }
}
