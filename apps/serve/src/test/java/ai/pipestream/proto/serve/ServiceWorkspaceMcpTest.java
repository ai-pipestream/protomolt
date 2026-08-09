package ai.pipestream.proto.serve;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ServiceWorkspaceMcpTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path workspace;

    @Test
    void agentRegistersThisServerAndInspectsItAfterRestartWithoutDescriptorCopying()
            throws Exception {
        try (HttpClient http = HttpClient.newHttpClient()) {
            int firstGrpcPort;
            try (ProtoMoltServe first = start()) {
                firstGrpcPort = first.grpcPort();
                ObjectNode arguments = MAPPER.createObjectNode();
                ObjectNode profile = arguments.putObject("profile");
                profile.put("name", "protomolt-self");
                profile.put("description", "ProtoMolt reflected through its own MCP surface");
                ObjectNode endpoint = profile.putArray("endpoints").addObject();
                endpoint.put("name", "local");
                endpoint.put("host", "127.0.0.1");
                endpoint.put("port", firstGrpcPort);
                endpoint.put("transport", "TRANSPORT_PLAINTEXT");

                JsonNode registered = tool(http, first.httpPort(), "service-register", arguments);
                assertThat(registered.path("ok").asBoolean()).isTrue();
                assertThat(registered.has("descriptorSetBase64")).isFalse();
                assertThat(registered.path("profile").has("descriptorSetBase64")).isFalse();
                assertThat(registered.path("services").findValuesAsText("name"))
                        .contains("ai.pipestream.protomolt.v1.ProtoMoltService", "ServiceInspect");

                JsonNode resources = rpc(http, first.httpPort(), "resources/list",
                        MAPPER.createObjectNode()).path("result").path("resources");
                assertThat(resources.findValuesAsText("uri"))
                        .contains("protomolt://services/protomolt-self");
            }

            // The endpoint from the first process is gone. Inspection must still work from the
            // persisted profile and content-addressed descriptor artifact.
            try (ProtoMoltServe restarted = start()) {
                ObjectNode inspect = MAPPER.createObjectNode().put("name", "protomolt-self");
                JsonNode persisted = tool(http, restarted.httpPort(), "service-inspect", inspect);
                assertThat(persisted.path("profile").path("endpoints").get(0)
                        .path("port").asInt()).isEqualTo(firstGrpcPort);
                assertThat(persisted.path("services").findValuesAsText("fullName"))
                        .contains("ai.pipestream.protomolt.v1.ProtoMoltService/ServiceRegister");
                assertThat(persisted.has("descriptorSetBase64")).isFalse();
                assertThat(persisted.path("profile").has("descriptorSetBase64")).isFalse();
            }
        }
    }

    private ProtoMoltServe start() {
        return ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, null, false, null, null, List.of(), workspace));
    }

    private static JsonNode tool(HttpClient http, int port, String name, ObjectNode arguments)
            throws Exception {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("name", name);
        params.set("arguments", arguments);
        return rpc(http, port, "tools/call", params)
                .path("result").path("structuredContent");
    }

    private static JsonNode rpc(HttpClient http, int port, String method, ObjectNode params)
            throws Exception {
        ObjectNode request = MAPPER.createObjectNode();
        request.put("jsonrpc", "2.0");
        request.put("id", 1);
        request.put("method", method);
        request.set("params", params);
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/mcp"))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(request.toString()))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return MAPPER.readTree(response.body());
    }
}
