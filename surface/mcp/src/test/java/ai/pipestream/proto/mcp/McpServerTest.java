package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Struct;
import static org.assertj.core.api.Assertions.assertThat;

class McpServerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private McpServer server;

    @Test
    void retainsTheOriginalRegistryResourcesConstructorForBinaryCompatibility() throws Exception {
        assertThat(McpServer.class.getConstructor(ActionCatalog.class, RegistryResources.class,
                String.class, String.class)).isNotNull();
    }

    @BeforeEach
    void setUp() {
        server = new McpServer(ActionCatalog.defaults(ActionContext.create()), null,
                "protomolt-test", "0.0-test");
    }

    private ObjectNode request(int id, String method, ObjectNode params) {
        ObjectNode message = mapper.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", id);
        message.put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        return message;
    }

    private ObjectNode respond(ObjectNode message) {
        Optional<ObjectNode> response = server.handle(message);
        assertThat(response).isPresent();
        return response.get();
    }

    @Test
    void initializeNegotiatesKnownProtocolVersion() {
        ObjectNode params = mapper.createObjectNode();
        params.put("protocolVersion", "2025-03-26");
        JsonNode result = respond(request(1, "initialize", params)).get("result");
        assertThat(result.get("protocolVersion").asText()).isEqualTo("2025-03-26");
        assertThat(result.get("serverInfo").get("name").asText()).isEqualTo("protomolt-test");
        assertThat(result.get("capabilities").has("tools")).isTrue();
        assertThat(result.get("capabilities").has("resources")).isTrue();
        assertThat(result.path("_meta").path("ai.pipestream.protomolt/workspace").asText())
                .isEqualTo(WorkspaceResources.URI);
    }

    @Test
    void initializeFallsBackToLatestOnUnknownVersion() {
        ObjectNode params = mapper.createObjectNode();
        params.put("protocolVersion", "1999-01-01");
        JsonNode result = respond(request(1, "initialize", params)).get("result");
        assertThat(result.get("protocolVersion").asText()).isEqualTo(McpServer.PROTOCOL_VERSION);
    }

    @Test
    void initializeIncludesSelfContainedDefaultWorkflowInstructions() {
        JsonNode instructions = respond(request(1, "initialize", null))
                .get("result").get("instructions");

        assertThat(instructions.asText()).isEqualTo(McpServer.DEFAULT_INSTRUCTIONS);
        assertThat(instructions.asText().length()).isLessThanOrEqualTo(512);
        assertThat(instructions.asText()).contains("protomolt://workspace", "service-register",
                "service-inspect", "service-invoke",
                "reflect", "grpc-invoke", "suggest-mappings", "compile-workflow",
                "record-workflow-run", "replay-workflow", "promote-workflow");
    }

    @Test
    void initializeUsesConfiguredInstructions() {
        McpServer configured = new McpServer(ActionCatalog.defaults(ActionContext.create()), null,
                "configured", "1", "Use the supplied workflow.");

        JsonNode result = configured.handle(request(1, "initialize", null)).orElseThrow()
                .get("result");

        assertThat(result.get("instructions").asText()).isEqualTo("Use the supplied workflow.");
    }

    @Test
    void initializeCanOmitInstructionsWhenConfiguredEmpty() {
        McpServer configured = new McpServer(ActionCatalog.defaults(ActionContext.create()), null,
                "configured", "1", "");

        JsonNode result = configured.handle(request(1, "initialize", null)).orElseThrow()
                .get("result");

        assertThat(result.has("instructions")).isFalse();
    }

    @Test
    void initializedNotificationProducesNoResponse() {
        ObjectNode notification = mapper.createObjectNode();
        notification.put("jsonrpc", "2.0");
        notification.put("method", "notifications/initialized");
        assertThat(server.handle(notification)).isEmpty();
    }

    /**
     * A request object with no {@code method} was dropped without a reply, leaving the client
     * waiting on an id that never comes back.
     */
    @Test
    void anObjectWithoutAMethodIsAnInvalidRequest() {
        ObjectNode message = mapper.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", 11);

        JsonNode response = respond(message);
        assertThat(response.get("id").asInt()).isEqualTo(11);
        assertThat(response.get("error").get("code").asInt()).isEqualTo(JsonRpc.INVALID_REQUEST);
    }

    @Test
    void aClientResponseProducesNoResponse() {
        ObjectNode message = mapper.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", 12);
        message.putObject("result");
        assertThat(server.handle(message)).isEmpty();
    }

    @Test
    void aNonObjectMessageIsAnInvalidRequest() {
        JsonNode response = server.handle(mapper.createArrayNode()).orElseThrow();
        assertThat(response.get("error").get("code").asInt()).isEqualTo(JsonRpc.INVALID_REQUEST);
    }

    @Test
    void pingReturnsEmptyResult() {
        JsonNode response = respond(request(7, "ping", null));
        assertThat(response.get("id").asInt()).isEqualTo(7);
        assertThat(response.get("result").isObject()).isTrue();
    }

    @Test
    void toolsListExposesEveryCatalogActionWithInputSchema() {
        JsonNode result = respond(request(2, "tools/list", null)).get("result");
        JsonNode tools = result.get("tools");
        assertThat(tools.size()).isEqualTo(17);
        for (JsonNode tool : tools) {
            assertThat(tool.get("name").asText()).isNotEmpty();
            assertThat(tool.get("description").asText()).isNotEmpty();
            assertThat(tool.get("inputSchema").isObject()).isTrue();
        }
        assertThat(tools.findValuesAsText("name"))
                .contains("compile", "check-compat", "eval-cel", "list-types");
        assertThat(result.path("_meta").path("ai.pipestream.protomolt/toolCount").asInt())
                .isEqualTo(tools.size());
        assertThat(result.path("_meta")
                .path("ai.pipestream.protomolt/toolCatalogFingerprint").asText())
                .matches("sha256:[0-9a-f]{64}");
    }

    @Test
    void toolsCallCompilesInlineSources() {
        ObjectNode params = mapper.createObjectNode();
        params.put("name", "compile");
        ObjectNode arguments = params.putObject("arguments");
        ObjectNode sources = arguments.putObject("sources");
        sources.put("greeter.proto", """
                syntax = "proto3";
                package demo;
                message Greeting { string text = 1; }
                """);
        JsonNode result = respond(request(3, "tools/call", params)).get("result");
        assertThat(result.get("isError").asBoolean()).isFalse();
        assertThat(result.get("structuredContent").get("ok").asBoolean()).isTrue();
        assertThat(result.get("structuredContent").get("files").get(0).asText())
                .isEqualTo("greeter.proto");
        assertThat(result.get("structuredContent").get("descriptorSetBase64").asText()).isNotEmpty();
        // The text content mirrors the structured payload for clients without structured support.
        assertThat(result.get("content").get(0).get("type").asText()).isEqualTo("text");
        assertThat(result.get("content").get(0).get("text").asText()).contains("greeter.proto");
    }

    @Test
    void toolsCallSurfacesActionErrorsAsToolErrors() {
        ObjectNode params = mapper.createObjectNode();
        params.put("name", "no-such-verb");
        params.putObject("arguments");
        JsonNode result = respond(request(4, "tools/call", params)).get("result");
        assertThat(result.get("isError").asBoolean()).isTrue();
        assertThat(result.get("structuredContent").get("error").asText()).isEqualTo("unknown-action");
    }

    @Test
    void toolsCallWithoutNameIsInvalidParams() {
        JsonNode response = respond(request(5, "tools/call", mapper.createObjectNode()));
        assertThat(response.get("error").get("code").asInt()).isEqualTo(-32602);
    }

    @Test
    void unknownMethodIsMethodNotFound() {
        JsonNode response = respond(request(6, "prompts/list", null));
        assertThat(response.get("error").get("code").asInt()).isEqualTo(-32601);
    }

    @Test
    void resourcesListAlwaysContainsTheWorkspaceBootstrap() {
        JsonNode result = respond(request(8, "resources/list", null)).get("result");
        assertThat(result.get("resources").findValuesAsText("uri"))
                .containsExactly(WorkspaceResources.URI);
    }

    @Test
    void workspaceResourceMatchesInitializeAndToolsListMetadata() throws Exception {
        JsonNode initialized = respond(request(81, "initialize", null)).path("result");
        JsonNode tools = respond(request(82, "tools/list", null)).path("result");
        ObjectNode params = mapper.createObjectNode().put("uri", WorkspaceResources.URI);
        JsonNode read = respond(request(83, "resources/read", params)).path("result");
        JsonNode workspace = mapper.readTree(
                read.path("contents").get(0).path("text").asText());

        String expected = tools.path("_meta")
                .path("ai.pipestream.protomolt/toolCatalogFingerprint").asText();
        assertThat(initialized.path("_meta")
                .path("ai.pipestream.protomolt/toolCatalogFingerprint").asText())
                .isEqualTo(expected);
        assertThat(workspace.path("toolCatalog").path("fingerprint").asText())
                .isEqualTo(expected);
        assertThat(workspace.path("toolCatalog").path("count").asInt())
                .isEqualTo(tools.path("tools").size());
    }

    @Test
    void resourceTemplateListingIsImplementedWithoutOptionalProviders() {
        JsonNode result = respond(request(84, "resources/templates/list", null)).path("result");

        assertThat(result.path("resourceTemplates")).isEmpty();
    }

    @Test
    void resourcesReadWithoutRegistryIsResourceNotFound() {
        ObjectNode params = mapper.createObjectNode();
        params.put("uri", "protomolt://registry/subjects");
        JsonNode response = respond(request(9, "resources/read", params));
        assertThat(response.get("error").get("code").asInt()).isEqualTo(-32002);
    }

    @Test
    void internalErrorsNeverEchoExceptionDetail() {
        ActionCatalog exploding = ActionCatalog.defaults(ActionContext.create())
                .replace(new ai.pipestream.proto.actions.JsonAction() {
                    @Override
                    public String name() {
                        return "compile";
                    }

                    @Override
                    public String description() {
                        return "Blows up with sensitive detail.";
                    }

                    @Override
                    public Descriptor requestType() {
                        // Struct accepts any JSON object, so a fixture is not constrained by a
                        // contract it is not testing.
                        return Struct.getDescriptor();
                    }

                    @Override
                    public Descriptor responseType() {
                        // Struct accepts any JSON object, so a fixture is not constrained by a
                        // contract it is not testing.
                        return Struct.getDescriptor();
                    }

                    @Override
                    public ObjectNode execute(ObjectNode input,
                                              ActionContext context) {
                        throw new IllegalStateException(
                                "/secret/host/path credential=hunter2");
                    }
                });
        McpServer boom = new McpServer(exploding, null, "protomolt-test", "0.0-test");

        ObjectNode params = mapper.createObjectNode();
        params.put("name", "compile");
        params.set("arguments", mapper.createObjectNode());
        ObjectNode response = boom.handle(request(42, "tools/call", params)).orElseThrow();

        JsonNode error = response.get("error");
        assertThat(error.get("code").asInt()).isEqualTo(-32603);
        assertThat(error.get("message").asText())
                .contains("correlation id")
                .doesNotContain("hunter2")
                .doesNotContain("secret")
                .doesNotContain("IllegalStateException");
    }
}
