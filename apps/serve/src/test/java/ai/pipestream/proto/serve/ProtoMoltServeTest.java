package ai.pipestream.proto.serve;

import ai.pipestream.proto.grpc.invoke.ReflectionClient;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.v1.DescribeModelRequest;
import ai.pipestream.proto.inference.v1.ModelEntry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boots the whole one-process server and exercises every surface: JSON/REST calls, the
 * OpenAPI document, Swagger UI assets, and gRPC reflection — the same verbs everywhere.
 */
class ProtoMoltServeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;

    @BeforeAll
    static void start() {
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0));
        http = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + serve.httpPort();
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base + path))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void compileWorksOverRest() throws Exception {
        HttpResponse<String> response = post("/grpc-json/ProtoMoltService/Compile", """
                {"sources": {"shop/v1/order.proto":
                  "syntax = \\"proto3\\";\\npackage shop.v1;\\nmessage Order { string id = 1; }"}}
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = MAPPER.readTree(response.body());
        assertThat(body.path("ok").asBoolean()).isTrue();
        assertThat(body.path("descriptorSetBase64").asText()).isNotEmpty();
    }

    @Test
    void evalCelWorksOverRest() throws Exception {
        HttpResponse<String> response = post("/grpc-json/ProtoMoltService/EvalCel", """
                {"schema": {"sources": {"shop/v1/order.proto":
                   "syntax = \\"proto3\\";\\npackage shop.v1;\\nmessage Order { int32 qty = 1; }"}},
                 "message": {"qty": 7},
                 "expression": "input.qty * 3"}
                """);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = MAPPER.readTree(response.body());
        assertThat(body.path("result").asInt()).isEqualTo(21);
        assertThat(body.path("resultType").asText()).isEqualTo("int");
    }

    @Test
    void actionFailuresAreClientErrorsOverRest() throws Exception {
        HttpResponse<String> response = post("/grpc-json/ProtoMoltService/ExtractMetadata", """
                {"schema": {"type": "no.such.Type"}}
                """);
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("unknown-type");
    }

    @Test
    void jobsVerbsAnswerUnavailableWithoutAStore() throws Exception {
        HttpResponse<String> response = post("/grpc-json/ProtoMoltService/GetJob", """
                {"jobId": "00000000-0000-0000-0000-000000000000"}
                """);
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("unavailable").contains("jobs");
    }

    @Test
    void openApiDocumentsEveryVerb() throws Exception {
        HttpResponse<String> response = get("/openapi.json");
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode paths = MAPPER.readTree(response.body()).path("paths");
        assertThat(paths.size()).isEqualTo(40);
        assertThat(paths.has("/grpc-json/ProtoMoltService/GrpcInvoke")).isTrue();
        assertThat(paths.has("/grpc-json/ProtoMoltService/GenerateStubs")).isTrue();
        assertThat(paths.has("/grpc-json/ProtoMoltService/SubmitChain")).isTrue();
        assertThat(paths.has("/grpc-json/ProtoMoltService/CompleteStep")).isTrue();
        assertThat(paths.has("/grpc-json/ProtoMoltService/ServiceRegister")).isTrue();
        assertThat(paths.has("/grpc-json/ProtoMoltService/ServiceInspect")).isTrue();
        assertThat(paths.has("/grpc-json/ProtoMoltService/RecordRecipeRun")).isTrue();
    }

    @Test
    void swaggerUiServesTheConsole() throws Exception {
        HttpResponse<String> index = get("/docs");
        assertThat(index.statusCode()).isEqualTo(200);
        assertThat(index.headers().firstValue("content-type").orElse("")).contains("text/html");
        assertThat(index.body()).contains("SwaggerUIBundle").contains("/openapi.json");

        HttpResponse<String> bundle = get("/docs/swagger-ui-bundle.js");
        assertThat(bundle.statusCode()).isEqualTo(200);
        HttpResponse<String> css = get("/docs/swagger-ui.css");
        assertThat(css.statusCode()).isEqualTo(200);

        HttpResponse<String> traversal = get("/docs/..%2fescape");
        assertThat(traversal.statusCode()).isEqualTo(404);
    }

    @Test
    void grpcSurfaceReflectsFromTheSameProcess() throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", serve.grpcPort())
                .usePlaintext()
                .build();
        try {
            ReflectionClient.Result discovered = ReflectionClient.discover(channel, 10_000);
            assertThat(discovered.services())
                    .contains("ai.pipestream.protomolt.v1.ProtoMoltService");
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void healthIsUp() throws Exception {
        HttpResponse<String> response = get("/health");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("UP");
    }

    @Test
    void parsesOutboundGrpcPolicyOptionsBeforeStartingTheServer() {
        ProtoMoltServe.Options options = ProtoMoltServe.Options.parse(new String[]{
                "--grpc-allowed-schemes", "dns",
                "--grpc-allowed-hosts", "*.example.com",
                "--grpc-allowed-ports", "443",
                "--grpc-allow-plaintext", "false",
                "--grpc-allow-tls", "true",
                "--grpc-max-deadline-ms", "5000",
                "--grpc-max-active-channels", "3"});

        assertThat(options.outboundPolicy().validateTarget("api.example.com:443", true)
                .scheme()).isEqualTo("dns");
        options.outboundPolicy().validateDeadline(5_000);
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                options.outboundPolicy().validateTarget("api.example.com:443", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("plaintext");
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                options.outboundPolicy().validateTarget("other.example.net:443", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host");
    }

    @Test
    void parsesStableMeshIdentityOptions() {
        ProtoMoltServe.Options options = ProtoMoltServe.Options.parse(new String[]{
                "--mesh-cluster-id", "protomolt",
                "--mesh-cluster-name", "ProtoMolt private mesh",
                "--mesh-trust-domain", "taild24b1c.ts.net",
                "--mesh-created-at", "2026-08-14T00:00:00Z"});

        assertThat(options.meshCluster().clusterId()).isEqualTo("protomolt");
        assertThat(options.meshCluster().displayName()).isEqualTo("ProtoMolt private mesh");
        assertThat(options.meshCluster().createdAt())
                .isEqualTo(java.time.Instant.parse("2026-08-14T00:00:00Z"));
    }

    @Test
    void inferenceModelSpecAdvertisesStructuredOutputCapability() {
        InferenceEngines engines = ProtoMoltServe.inferenceEngines(java.util.List.of(
                "judge|openai|https://models.example.test|backend|machine:krick|"
                        + "streaming,structured-output"));

        ModelEntry entry = engines.describe(DescribeModelRequest.newBuilder()
                .setModel("judge").build()).getEntry();
        assertThat(entry.getCapabilities().getStreaming()).isTrue();
        assertThat(entry.getCapabilities().getStructuredOutput()).isTrue();
        assertThat(entry.getLabelsMap()).containsEntry("machine", "krick");
    }

    @Test
    void inferenceModelSpecRejectsUnknownCapability() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                ProtoMoltServe.inferenceEngines(java.util.List.of(
                        "judge|openai|https://models.example.test|||telepathy")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("telepathy");
    }

    @Test
    void inferenceModelSpecParsesCredentialReference() {
        InferenceEngines engines = ProtoMoltServe.inferenceEngines(java.util.List.of(
                "judge|openai|https://models.example.test|backend||structured-output|"
                        + "env:OPENAI_TOKEN"));

        ModelEntry entry = engines.describe(DescribeModelRequest.newBuilder()
                .setModel("judge").build()).getEntry();
        assertThat(entry.getCredentialRef()).isEqualTo("env:OPENAI_TOKEN");
        assertThat(entry.getCapabilities().getStructuredOutput()).isTrue();
    }

    @Test
    void inferenceModelSpecWithoutCredentialReferenceLeavesItEmpty() {
        InferenceEngines engines = ProtoMoltServe.inferenceEngines(java.util.List.of(
                "judge|openai|https://models.example.test|backend|machine:krick|streaming"));

        ModelEntry entry = engines.describe(DescribeModelRequest.newBuilder()
                .setModel("judge").build()).getEntry();
        assertThat(entry.getCredentialRef()).isEmpty();
    }

    @Test
    void inferenceModelSpecRejectsMalformedCredentialReference() {
        String credentialRef = "not-a-valid-reference";
        IllegalArgumentException failure = org.assertj.core.api.Assertions.catchThrowableOfType(() ->
                ProtoMoltServe.inferenceEngines(java.util.List.of(
                        "judge|openai|https://models.example.test||||" + credentialRef)),
                IllegalArgumentException.class);

        assertThat(failure).hasMessageContaining("credential");
        assertThat(failure.getMessage().contains(credentialRef)).isFalse();
    }
}
