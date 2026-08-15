package ai.pipestream.proto.serve;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.RegistryWorkflowVersionRepository;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The Phase 2 acceptance path through the actual streamable-HTTP MCP surface. */
class WorkflowWorkbenchMcpTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROTO = """
            syntax = "proto3";
            package workbench.test;
            message Text { string text = 1; }
            message Tokens { repeated int64 ids = 1; }
            message EmbeddingRequest { repeated int64 ids = 1; }
            message Embedding { repeated double vector = 1; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Embedder { rpc Embed(EmbeddingRequest) returns (Embedding); }
            """;

    @TempDir
    Path directory;

    private FileDescriptor file;
    private String descriptorSet;
    private Server tokenizer;
    private Server embedder;

    @BeforeEach
    void startServices() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("workbench/test/workbench.proto", PROTO, "test").build());
        file = compiled.descriptorFor("workbench/test/workbench.proto").orElseThrow();
        descriptorSet = Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray());

        var tokenizerService = file.findServiceByName("Tokenizer");
        var tokenize = DynamicGrpcCalls.methodDescriptor(
                tokenizerService.findMethodByName("Tokenize"));
        Descriptor tokens = file.findMessageTypeByName("Tokens");
        tokenizer = ServerBuilder.forPort(0)
                .addService(ServerServiceDefinition.builder(tokenizerService.getFullName())
                        .addMethod(tokenize, ServerCalls.asyncUnaryCall((request, output) -> {
                            String text = (String) request.getField(
                                    request.getDescriptorForType().findFieldByName("text"));
                            DynamicMessage.Builder response = DynamicMessage.newBuilder(tokens);
                            for (char value : text.toCharArray()) {
                                response.addRepeatedField(tokens.findFieldByName("ids"),
                                        (long) value);
                            }
                            output.onNext(response.build());
                            output.onCompleted();
                        })).build())
                .build().start();

        var embedderService = file.findServiceByName("Embedder");
        var embed = DynamicGrpcCalls.methodDescriptor(embedderService.findMethodByName("Embed"));
        Descriptor embedding = file.findMessageTypeByName("Embedding");
        embedder = ServerBuilder.forPort(0)
                .addService(ServerServiceDefinition.builder(embedderService.getFullName())
                        .addMethod(embed, ServerCalls.asyncUnaryCall((request, output) -> {
                            int count = request.getRepeatedFieldCount(
                                    request.getDescriptorForType().findFieldByName("ids"));
                            DynamicMessage response = DynamicMessage.newBuilder(embedding)
                                    .addRepeatedField(embedding.findFieldByName("vector"),
                                            count / 10.0)
                                    .build();
                            output.onNext(response);
                            output.onCompleted();
                        })).build())
                .build().start();
    }

    @AfterEach
    void stopServices() {
        if (tokenizer != null) {
            tokenizer.shutdownNow();
        }
        if (embedder != null) {
            embedder.shutdownNow();
        }
    }

    @Test
    void agentCompilesRecordsReplaysAndPromotesTwoLiveServices() throws Exception {
        Path registry = directory.resolve("registry.git");
        Path workflows = directory.resolve("workflows");
        String fingerprint;
        try (ProtoMoltServe serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, registry, 0, null, false, null, null, List.of(),
                null, null, workflows));
             McpClient mcp = new McpClient(serve.httpPort())) {
            assertThat(mcp.instructions()).contains("compile-workflow", "record-workflow-run",
                    "replay-workflow", "promote-workflow");
            assertThat(mcp.tools()).contains("suggest-mappings", "compile-workflow",
                    "record-workflow-run", "replay-workflow", "promote-workflow");

            ObjectNode suggestions = MAPPER.createObjectNode();
            ObjectNode source = suggestions.putArray("sources").addObject();
            source.put("name", "tokens");
            source.set("schema", schema());
            source.put("type", "workbench.test.Tokens");
            ObjectNode target = suggestions.putObject("target");
            target.set("schema", schema());
            target.put("type", "workbench.test.EmbeddingRequest");
            assertThat(mcp.tool("suggest-mappings", suggestions).path("candidates")
                    .findValuesAsText("rule")).containsExactly("ids=tokens.ids");

            ObjectNode workflow = workflow();
            ObjectNode compile = MAPPER.createObjectNode();
            compile.set("workflow", workflow);
            JsonNode compiled = mcp.tool("compile-workflow", compile);
            fingerprint = compiled.path("workflowFingerprint").asText();
            assertThat(fingerprint).hasSize(64);

            ObjectNode record = MAPPER.createObjectNode();
            record.set("workflow", workflow);
            record.putObject("input").put("text", "hello");
            record.put("runId", "run-1");
            JsonNode evidence = mcp.tool("record-workflow-run", record);
            assertThat(evidence.path("ok").asBoolean()).isTrue();
            assertThat(evidence.path("evidence").path("steps")).hasSize(2);
            assertThat(evidence.path("evidence").path("workflowFingerprint").asText())
                    .isEqualTo(fingerprint);

            ObjectNode replay = MAPPER.createObjectNode();
            replay.set("workflow", compiled.path("workflow"));
            replay.put("runId", "run-1");
            replay.set("schema", schema());
            JsonNode replayed = mcp.tool("replay-workflow", replay);
            assertThat(replayed.path("ok").asBoolean()).isTrue();
            assertThat(replayed.path("steps")).hasSize(2);

            ObjectNode promote = MAPPER.createObjectNode();
            promote.set("workflow", compiled.path("workflow"));
            promote.put("version", "v1");
            JsonNode promoted = mcp.tool("promote-workflow", promote);
            assertThat(promoted.path("promoted").asBoolean()).isTrue();
            assertThat(promoted.path("versionedWorkflow").path("workflowFingerprint").asText())
                    .isEqualTo(fingerprint);
        }

        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(registry).build()) {
            assertThat(new RegistryWorkflowVersionRepository(store).find("workbench", "v1"))
                    .isPresent().get().extracting(version -> version.getWorkflowFingerprint())
                    .isEqualTo(fingerprint);
        }
        assertThat(directory.resolve("workflows/runs/run-1.pb")).isRegularFile();
    }

    private ObjectNode schema() {
        return MAPPER.createObjectNode().put("descriptorSetBase64", descriptorSet);
    }

    private ObjectNode workflow() {
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", "workbench");
        workflow.set("schema", schema());
        workflow.put("inputType", "workbench.test.Text");
        workflow.put("deadlineMs", 10_000);
        var steps = workflow.putArray("steps");
        ObjectNode tokenize = steps.addObject();
        tokenize.put("name", "tokenize");
        tokenize.put("target", "127.0.0.1:" + tokenizer.getPort());
        tokenize.put("method", "workbench.test.Tokenizer/Tokenize");
        tokenize.putArray("rules").add("text=input.text");
        ObjectNode embed = steps.addObject();
        embed.put("name", "embed");
        embed.put("target", "127.0.0.1:" + embedder.getPort());
        embed.put("method", "workbench.test.Embedder/Embed");
        embed.putArray("rules").add("ids=tokenize.ids");
        return workflow;
    }

    private final class McpClient implements AutoCloseable {
        private final HttpClient http = HttpClient.newHttpClient();
        private final URI endpoint;
        private final String session;
        private final String version;
        private final String instructions;

        private McpClient(int port) throws Exception {
            endpoint = URI.create("http://127.0.0.1:" + port + "/mcp");
            ObjectNode params = MAPPER.createObjectNode();
            params.put("protocolVersion", "2025-06-18");
            params.putObject("capabilities");
            params.putObject("clientInfo").put("name", "workflow-acceptance").put("version", "0");
            HttpResponse<String> response = send(null, null, request(99, "initialize", params));
            assertThat(response.statusCode()).isEqualTo(200);
            session = response.headers().firstValue("Mcp-Session-Id").orElseThrow();
            JsonNode initialized = MAPPER.readTree(response.body()).path("result");
            version = initialized.path("protocolVersion").asText();
            instructions = initialized.path("instructions").asText();
            assertThat(send(session, version,
                    notification("notifications/initialized")).statusCode()).isEqualTo(202);
        }

        private String instructions() {
            return instructions;
        }

        private List<String> tools() throws Exception {
            return MAPPER.readTree(send(session, version,
                            request(1, "tools/list", MAPPER.createObjectNode())).body())
                    .path("result").path("tools").findValuesAsText("name");
        }

        private JsonNode tool(String name, ObjectNode arguments) throws Exception {
            ObjectNode params = MAPPER.createObjectNode();
            params.put("name", name);
            params.set("arguments", arguments);
            JsonNode result = MAPPER.readTree(send(session, version,
                    request(2, "tools/call", params)).body()).path("result");
            assertThat(result.path("isError").asBoolean())
                    .as(() -> result.path("structuredContent").toString()).isFalse();
            return result.path("structuredContent");
        }

        private HttpResponse<String> send(String sessionId, String protocolVersion,
                                          ObjectNode body) throws Exception {
            HttpRequest.Builder request = HttpRequest.newBuilder(endpoint)
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
            if (sessionId != null) {
                request.header("Mcp-Session-Id", sessionId);
            }
            if (protocolVersion != null) {
                request.header("MCP-Protocol-Version", protocolVersion);
            }
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }

        private ObjectNode request(int id, String method, ObjectNode params) {
            ObjectNode request = notification(method);
            request.put("id", id);
            request.set("params", params);
            return request;
        }

        private ObjectNode notification(String method) {
            return MAPPER.createObjectNode().put("jsonrpc", "2.0").put("method", method);
        }

        @Override
        public void close() {
            // The server owns the MCP session lifecycle; closing the server releases it.
        }
    }
}
