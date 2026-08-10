package ai.pipestream.proto.inference.openai;

import ai.pipestream.proto.inference.v1.ChatTurn;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class OpenAiCompatProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;
    private final AtomicReference<String> lastBody = new AtomicReference<>();

    private final OpenAiCompatProvider provider = new OpenAiCompatProvider(Duration.ofSeconds(5));

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            lastBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = """
                    {"id":"chat-1","model":"gpt-oss:20b",
                     "choices":[{"index":0,"message":{"role":"assistant","content":"GUILTY"},
                                 "finish_reason":"stop"}],
                     "usage":{"prompt_tokens":42,"completion_tokens":3,"total_tokens":45}}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
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

    @Test
    void speaksV1AndStampsOpenaiProvenance() {
        ModelEntry model = ModelEntry.newBuilder()
                .setId("gpt-oss-20b-plaintiff")
                .setProvider("openai")
                .setEndpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .setBackendModel("gpt-oss:20b")
                .build();
        GenerateResponse response = provider.generate(model, GenerateRequest.newBuilder()
                .setModel("gpt-oss-20b-plaintiff")
                .addMessages(ChatTurn.newBuilder().setRole(Role.ROLE_USER).setContent("verdict?"))
                .build());
        assertThat(response.getText()).isEqualTo("GUILTY");
        assertThat(response.getProvider()).isEqualTo("openai");
        assertThat(response.getUsage().getPromptTokens()).isEqualTo(42);
        assertThat(lastBody.get()).contains("\"model\":\"gpt-oss:20b\"");
    }

    @Test
    void speaksV1WithTheSameStrictStructuredOutputEnvelope() throws Exception {
        ModelEntry model = ModelEntry.newBuilder()
                .setId("gpt-oss-20b-plaintiff")
                .setProvider("openai")
                .setEndpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .setBackendModel("gpt-oss:20b")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true))
                .putLabels("credentialRef", "env:OPENAI_TOKEN")
                .build();
        StructuredOutputConstraint constraint = StructuredOutputConstraint.newBuilder()
                .setName("court_Verdict")
                .setJsonSchema("{\"type\":\"object\",\"additionalProperties\":false}")
                .build();

        provider.generate(model, GenerateRequest.newBuilder()
                .setModel(model.getId())
                .addMessages(ChatTurn.newBuilder()
                        .setRole(Role.ROLE_USER).setContent("verdict?"))
                .setStructuredOutput(constraint)
                .build());

        JsonNode body = MAPPER.readTree(lastBody.get());
        assertThat(body.at("/response_format/type").asText()).isEqualTo("json_schema");
        assertThat(body.at("/response_format/json_schema/name").asText())
                .isEqualTo("court_Verdict");
        assertThat(body.at("/response_format/json_schema/strict").asBoolean()).isTrue();
        assertThat(body.at("/response_format/json_schema/schema"))
                .isEqualTo(MAPPER.readTree(constraint.getJsonSchema()));
        assertThat(lastBody.get()).doesNotContain("credentialRef", "OPENAI_TOKEN");
    }
}
