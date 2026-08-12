package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Deterministic in-process HTTP coverage for the OpenAI-compatible provider. */
class OpenAiAgentProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String MODEL = "meta-models/Muse-Glimmer-30B";
    private static final String SECRET_BODY = "internal-trace-3f9c-secret-detail";

    @TempDir
    Path temporary;

    private HttpServer server;
    private final List<JsonNode> requests = new ArrayList<>();
    private final List<String> authorizations = new ArrayList<>();
    private final AtomicInteger status = new AtomicInteger(200);
    private volatile String responseBody = completion(
            "{\"handledEventCursors\":[1],\"commands\":[]}");

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void promptPostsModelAndMessagesWithoutBearerCredential() throws Exception {
        start();
        try (OpenAiAgentProvider provider = provider()) {
            String reply = provider.prompt("first packet");
            assertThat(reply).contains("handledEventCursors");
        }
        assertThat(requests).hasSize(1);
        JsonNode body = requests.get(0);
        assertThat(body.path("model").asText()).isEqualTo(MODEL);
        assertThat(body.path("response_format").path("type").asText())
                .isEqualTo("json_object");
        assertThat(body.path("messages")).hasSize(1);
        assertThat(body.path("messages").path(0).path("role").asText()).isEqualTo("user");
        assertThat(body.path("messages").path(0).path("content").asText())
                .isEqualTo("first packet");
        assertThat(authorizations).containsOnly((String) null);
        assertThat(providerName()).isEqualTo("openai");
    }

    @Test
    void successfulTurnsAccumulateBoundedHistoryForRepairAttempts() throws Exception {
        start();
        try (OpenAiAgentProvider provider = provider()) {
            provider.prompt("first packet");
            responseBody = completion("not json at all");
            String reply = provider.prompt(
                    "repair packet with the rejection appended");
            assertThat(reply).isEqualTo("not json at all");
        }
        assertThat(requests).hasSize(2);
        JsonNode messages = requests.get(1).path("messages");
        assertThat(messages).hasSize(3);
        assertThat(messages.path(0).path("role").asText()).isEqualTo("user");
        assertThat(messages.path(1).path("role").asText()).isEqualTo("assistant");
        assertThat(messages.path(2).path("content").asText())
                .contains("repair packet");
    }

    @Test
    void nonSuccessStatusRedactsTheEndpointResponseBody() throws Exception {
        status.set(500);
        responseBody = SECRET_BODY;
        start();
        try (OpenAiAgentProvider provider = provider()) {
            assertThatThrownBy(() -> provider.prompt("packet"))
                    .isInstanceOf(AgentHostException.class)
                    .hasMessageContaining("500")
                    .hasMessageNotContaining(SECRET_BODY);
        }
    }

    @Test
    void invalidJsonIsRejectedWithoutEchoingTheBody() throws Exception {
        responseBody = SECRET_BODY;
        start();
        try (OpenAiAgentProvider provider = provider()) {
            assertThatThrownBy(() -> provider.prompt("packet"))
                    .isInstanceOf(AgentHostException.class)
                    .hasMessageContaining("invalid JSON")
                    .hasMessageNotContaining(SECRET_BODY);
        }
    }

    @Test
    void missingMessageContentIsRejectedWithoutEchoingTheBody() throws Exception {
        responseBody = "{\"choices\":[],\"trace\":\"" + SECRET_BODY + "\"}";
        start();
        try (OpenAiAgentProvider provider = provider()) {
            assertThatThrownBy(() -> provider.prompt("packet"))
                    .isInstanceOf(AgentHostException.class)
                    .hasMessageContaining("no message content")
                    .hasMessageNotContaining(SECRET_BODY);
        }
    }

    @Test
    void endpointAndModelValidationIsStrict() {
        try (OpenAiAgentProvider ignored = new OpenAiAgentProvider(
                URI.create("http://127.0.0.1:8011/v1"), MODEL, "", Duration.ofMinutes(5))) {
            assertThat(ignored.sessionId()).startsWith("openai-");
        }
        assertThatThrownBy(() -> new OpenAiAgentProvider(
                URI.create("ftp://127.0.0.1/v1"), MODEL, "", Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OpenAiAgentProvider(
                URI.create("http://user:pass@127.0.0.1/v1"), MODEL, "",
                Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OpenAiAgentProvider(
                URI.create("http://127.0.0.1/v1"), "  ", "", Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cliRequiresEndpointAndModelOnlyForTheOpenAiProvider() {
        String[] base = {"--endpoint", "https://protomolt.rokkon.com/mcp",
                "--role", "worker", "--identity", "glimmer-worker",
                "--provider", "openai", "--workspace", temporary.toString(),
                "--state", temporary.resolve("glimmer.json").toString()};
        AgentHostMain.Options parsed = AgentHostMain.Options.parse(with(base,
                "--provider-endpoint", "http://glimmer-vllm:8011/v1",
                "--model", MODEL));
        assertThat(parsed.providerEndpoint())
                .isEqualTo(URI.create("http://glimmer-vllm:8011/v1"));
        assertThat(parsed.model()).isEqualTo(MODEL);
        assertThatThrownBy(() -> AgentHostMain.Options.parse(base))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--provider-endpoint");
        String[] noModel = with(base, "--provider-endpoint",
                "http://glimmer-vllm:8011/v1");
        assertThatThrownBy(() -> AgentHostMain.Options.parse(noModel))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--model");
        String[] kimi = {"--endpoint", "https://protomolt.rokkon.com/mcp",
                "--role", "worker", "--identity", "kimi-worker",
                "--provider", "kimi", "--workspace", temporary.toString(),
                "--state", temporary.resolve("kimi.json").toString(),
                "--provider-endpoint", "http://glimmer-vllm:8011/v1"};
        assertThatThrownBy(() -> AgentHostMain.Options.parse(kimi))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("valid only with --provider openai");
    }

    private String providerName() {
        try (OpenAiAgentProvider provider = provider()) {
            return provider.name();
        }
    }

    private OpenAiAgentProvider provider() {
        return new OpenAiAgentProvider(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/v1"),
                MODEL, "", Duration.ofMinutes(5));
    }

    private void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            requests.add(MAPPER.readTree(exchange.getRequestBody()));
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("content-type", "application/json");
            exchange.sendResponseHeaders(status.get(), body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
    }

    private static String completion(String content) {
        return "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\""
                + content.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}}]}";
    }

    private static String[] with(String[] base, String... extra) {
        String[] combined = new String[base.length + extra.length];
        System.arraycopy(base, 0, combined, 0, base.length);
        System.arraycopy(extra, 0, combined, base.length, extra.length);
        return combined;
    }
}
