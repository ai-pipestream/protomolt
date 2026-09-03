package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.serve.ProtoMoltServe;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class McpHttpClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void bearerAuthenticationWorksAndFailuresNeverEchoTokenMaterial() {
        String token = "secret-that-must-not-appear";
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0, token))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            try (McpHttpClient accepted = new McpHttpClient(endpoint, () -> token)) {
                assertThat(accepted.callTool("delegation-worker-list",
                        MAPPER.createObjectNode()).path("ok").asBoolean()).isTrue();
            }
            try (McpHttpClient refused = new McpHttpClient(endpoint, () -> token + "-wrong")) {
                assertThatThrownBy(() -> refused.callTool("delegation-worker-list",
                        MAPPER.createObjectNode()))
                        .isInstanceOf(AgentHostException.class)
                        .hasMessageContaining("HTTP 401")
                        .hasMessageNotContaining(token);
            }
        }
    }

    @Test
    void endpointMustBeHttpWithoutAFragment() {
        assertThatThrownBy(() -> new McpHttpClient(
                URI.create("file:///tmp/socket"), () -> null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new McpHttpClient(
                URI.create("https://example.test/mcp#fragment"), () -> null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
