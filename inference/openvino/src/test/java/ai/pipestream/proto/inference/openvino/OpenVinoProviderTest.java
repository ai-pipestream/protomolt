package ai.pipestream.proto.inference.openvino;

import ai.pipestream.proto.inference.spi.ChunkObserver;
import ai.pipestream.proto.inference.spi.CredentialResolutionException;
import ai.pipestream.proto.inference.spi.InferenceException;
import ai.pipestream.proto.inference.v1.ChatTurn;
import ai.pipestream.proto.inference.v1.FinishReason;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStreamRequest;
import ai.pipestream.proto.inference.v1.GenerateStreamResponse;
import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import ai.pipestream.proto.inference.v1.Role;
import ai.pipestream.proto.inference.v1.StructuredOutputConstraint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenVinoProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;
    private final AtomicReference<String> lastBody = new AtomicReference<>();
    private final AtomicReference<String> lastAuthorization = new AtomicReference<>();
    private volatile int status = 200;
    private volatile String responseBody = "";

    private final OpenVinoProvider provider = new OpenVinoProvider(Duration.ofSeconds(5));

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v3/chat/completions", exchange -> {
            lastAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            lastBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private ModelEntry model() {
        return ModelEntry.newBuilder()
                .setId("judge")
                .setProvider("openvino")
                .setEndpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .setBackendModel("OpenVINO/gemma-4-31B-it-int4-ov")
                .build();
    }

    private static GenerateRequest request(String model) {
        return GenerateRequest.newBuilder()
                .setModel(model)
                .setTemperature(0.2)
                .addMessages(ChatTurn.newBuilder()
                        .setRole(Role.ROLE_SYSTEM).setContent("You are a judge."))
                .addMessages(ChatTurn.newBuilder()
                        .setRole(Role.ROLE_USER).setContent("Verdict?"))
                .build();
    }

    @Test
    void unaryMapsTextFinishReasonAndUsage() {
        responseBody = """
                {"id":"chat-1","model":"OpenVINO/gemma-4-31B-it-int4-ov",
                 "choices":[{"index":0,"message":{"role":"assistant","content":"VALID"},
                             "finish_reason":"stop"}],
                 "usage":{"prompt_tokens":8031,"completion_tokens":12,"total_tokens":8043}}
                """;
        GenerateResponse response = provider.generate(model(), request("judge"));
        assertThat(response.getText()).isEqualTo("VALID");
        assertThat(response.getProvider()).isEqualTo("openvino");
        assertThat(response.getModel()).isEqualTo("judge");
        assertThat(response.getFinishReason()).isEqualTo(FinishReason.FINISH_REASON_STOP);
        assertThat(response.getUsage().getPromptTokens()).isEqualTo(8031);
        assertThat(response.getUsage().getCompletionTokens()).isEqualTo(12);
        assertThat(lastBody.get()).contains("\"model\":\"OpenVINO/gemma-4-31B-it-int4-ov\"")
                .contains("\"role\":\"system\"")
                .contains("\"temperature\":0.2")
                .doesNotContain("top_p")
                .doesNotContain("max_tokens");
    }

    @Test
    void unarySendsExactStrictResponseFormatWithoutCatalogMetadata() throws Exception {
        responseBody = """
                {"choices":[{"message":{"content":"{\\"verdict\\":\\"VALID\\"}"},
                              "finish_reason":"stop"}],
                 "usage":{"prompt_tokens":10,"completion_tokens":4}}
                """;
        ModelEntry structuredModel = ModelEntry.newBuilder(model())
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true))
                .putLabels("credentialRef", "env:OVMS_TOKEN")
                .build();
        StructuredOutputConstraint constraint = StructuredOutputConstraint.newBuilder()
                .setName("court_Verdict")
                .setJsonSchema("""
                        {"type":"object","properties":{"verdict":{"type":"string"}},
                         "required":["verdict"],"additionalProperties":false}
                        """)
                .build();

        provider.generate(structuredModel, GenerateRequest.newBuilder(request("judge"))
                .setStructuredOutput(constraint).build());

        JsonNode body = MAPPER.readTree(lastBody.get());
        JsonNode responseFormat = body.path("response_format");
        assertThat(responseFormat.path("type").asText()).isEqualTo("json_schema");
        assertThat(responseFormat.path("json_schema").path("name").asText())
                .isEqualTo("court_Verdict");
        assertThat(responseFormat.path("json_schema").path("strict").asBoolean()).isTrue();
        assertThat(responseFormat.path("json_schema").path("schema"))
                .isEqualTo(MAPPER.readTree(constraint.getJsonSchema()));
        assertThat(lastBody.get()).doesNotContain("credentialRef", "OVMS_TOKEN")
                .doesNotContain(structuredModel.getEndpoint());
    }

    @Test
    void structuredOutputRejectsIncapableModelBeforeHttp() {
        GenerateRequest structured = GenerateRequest.newBuilder(request("judge"))
                .setStructuredOutput(StructuredOutputConstraint.newBuilder()
                        .setName("court_Verdict")
                        .setJsonSchema("{\"type\":\"object\"}"))
                .build();

        assertThatThrownBy(() -> provider.generate(model(), structured))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("structured-output capability");
        assertThat(lastBody.get()).isNull();
    }

    @Test
    void structuredOutputRejectsMalformedSchemaBeforeHttpWithoutEchoingIt() {
        ModelEntry structuredModel = ModelEntry.newBuilder(model())
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true))
                .build();
        GenerateRequest structured = GenerateRequest.newBuilder(request("judge"))
                .setStructuredOutput(StructuredOutputConstraint.newBuilder()
                        .setName("court_Verdict")
                        .setJsonSchema("{SECRET_NOT_JSON"))
                .build();

        assertThatThrownBy(() -> provider.generate(structuredModel, structured))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("constraint")
                .hasMessageNotContaining("SECRET_NOT_JSON");
        assertThat(lastBody.get()).isNull();
    }

    @Test
    void httpErrorFailsLoud() {
        status = 500;
        responseBody = "boom";
        assertThatThrownBy(() -> provider.generate(model(), request("judge")))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("HTTP 500");
    }

    @Test
    void malformedJsonFailsLoud() {
        responseBody = "not json";
        assertThatThrownBy(() -> provider.generate(model(), request("judge")))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("malformed");
    }

    @Test
    void unreachableEndpointFailsLoud() {
        ModelEntry dead = ModelEntry.newBuilder(model())
                .setEndpoint("http://127.0.0.1:1").build();
        assertThatThrownBy(() -> provider.generate(dead, request("judge")))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("judge");
    }

    @Test
    void streamingDeliversDeltasThenFinalUsage() {
        status = 200;
        // HttpServer will send this as one body; the provider parses SSE lines.
        responseBody = "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n"
                + "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"VAL\"}}]}\n\n"
                + "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ID\"}}]}\n\n"
                + "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],"
                + "\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"total_tokens\":12}}\n\n"
                + "data: [DONE]\n\n";
        List<String> deltas = new ArrayList<>();
        List<GenerateStreamResponse> finals = new ArrayList<>();
        AtomicReference<InferenceException> error = new AtomicReference<>();
        AtomicReference<Boolean> completed = new AtomicReference<>(false);

        GenerateStreamRequest streamRequest = GenerateStreamRequest.newBuilder()
                .setModel("judge")
                .addMessages(ChatTurn.newBuilder().setRole(Role.ROLE_USER).setContent("go"))
                .build();
        provider.generateStream(model(), streamRequest, new ChunkObserver() {
            @Override
            public void onNext(GenerateStreamResponse chunk) {
                if (chunk.getLast()) {
                    finals.add(chunk);
                } else {
                    deltas.add(chunk.getTextDelta());
                }
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }

            @Override
            public void onError(InferenceException e) {
                error.set(e);
            }
        });

        assertThat(error.get()).isNull();
        assertThat(completed.get()).isTrue();
        assertThat(deltas).containsExactly("VAL", "ID");
        assertThat(finals).hasSize(1);
        assertThat(finals.get(0).getFinishReason()).isEqualTo(FinishReason.FINISH_REASON_STOP);
        assertThat(finals.get(0).getUsage().getPromptTokens()).isEqualTo(10);
        assertThat(lastBody.get()).contains("\"stream\":true").contains("include_usage");
    }

    @Test
    void unarySendsTheResolvedBearerCredentialAndKeepsItOutOfTheBody() {
        responseBody = """
                {"choices":[{"message":{"content":"VALID"},"finish_reason":"stop"}],
                 "usage":{"prompt_tokens":1,"completion_tokens":1}}
                """;
        OpenVinoProvider authenticated = new OpenVinoProvider(Duration.ofSeconds(5),
                ref -> "ovms-secret-token");
        ModelEntry authedModel = ModelEntry.newBuilder(model())
                .setCredentialRef("env:OVMS_TOKEN").build();

        authenticated.generate(authedModel, request("judge"));

        assertThat(lastAuthorization.get()).isEqualTo("Bearer ovms-secret-token");
        assertThat(lastBody.get())
                .doesNotContain("ovms-secret-token")
                .doesNotContain("env:OVMS_TOKEN")
                .doesNotContain("credential_ref");
    }

    @Test
    void streamingSendsTheResolvedBearerCredential() {
        responseBody = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"}}]}\n\n"
                + "data: [DONE]\n\n";
        OpenVinoProvider authenticated = new OpenVinoProvider(Duration.ofSeconds(5),
                ref -> "ovms-secret-token");
        ModelEntry authedModel = ModelEntry.newBuilder(model())
                .setCredentialRef("env:OVMS_TOKEN").build();
        List<String> deltas = new ArrayList<>();

        authenticated.generateStream(authedModel, GenerateStreamRequest.newBuilder()
                        .setModel("judge")
                        .addMessages(ChatTurn.newBuilder()
                                .setRole(Role.ROLE_USER).setContent("go"))
                        .build(),
                new ChunkObserver() {
                    @Override
                    public void onNext(GenerateStreamResponse chunk) {
                        if (!chunk.getLast()) {
                            deltas.add(chunk.getTextDelta());
                        }
                    }

                    @Override
                    public void onComplete() {
                    }

                    @Override
                    public void onError(InferenceException e) {
                        throw new AssertionError(e);
                    }
                });

        assertThat(deltas).containsExactly("OK");
        assertThat(lastAuthorization.get()).isEqualTo("Bearer ovms-secret-token");
        assertThat(lastBody.get())
                .doesNotContain("ovms-secret-token")
                .doesNotContain("env:OVMS_TOKEN");
    }

    @Test
    void unresolvableCredentialFailsBeforeAnyHttpRequest() {
        OpenVinoProvider failing = new OpenVinoProvider(Duration.ofSeconds(5), ref -> {
            throw new CredentialResolutionException("credential reference does not resolve: "
                    + "the referenced environment variable is unset or empty");
        });
        ModelEntry authedModel = ModelEntry.newBuilder(model())
                .setCredentialRef("env:OVMS_UNSET").build();

        assertThatThrownBy(() -> failing.generate(authedModel, request("judge")))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageNotContaining("env:OVMS_UNSET");
        assertThatThrownBy(() -> failing.generateStream(authedModel,
                GenerateStreamRequest.newBuilder().setModel("judge").build(),
                new ChunkObserver() {
                    @Override
                    public void onNext(GenerateStreamResponse chunk) {
                    }

                    @Override
                    public void onComplete() {
                    }

                    @Override
                    public void onError(InferenceException e) {
                    }
                }))
                .isInstanceOf(CredentialResolutionException.class);
        assertThat(lastBody.get()).isNull();
        assertThat(lastAuthorization.get()).isNull();
    }

    @Test
    void noCredentialReferenceSendsNoAuthorizationHeader() {
        responseBody = """
                {"choices":[{"message":{"content":"VALID"},"finish_reason":"stop"}],
                 "usage":{"prompt_tokens":1,"completion_tokens":1}}
                """;

        provider.generate(model(), request("judge"));

        assertThat(lastAuthorization.get()).isNull();
    }
}
