package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * OpenAI-compatible chat-completions provider for a local sidecar model. The client sends
 * no bearer credential and no arbitrary headers, requests a JSON object response so the
 * closed {@link AgentTurn} command schema is preserved by host-side validation, and keeps
 * a bounded in-memory conversation so one repair turn sees the rejected answer. Endpoint
 * response bodies are never copied into exceptions.
 */
final class OpenAiAgentProvider implements AgentProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_RESPONSE_CHARS = 1024 * 1024;
    private static final int MAX_HISTORY_MESSAGES = 64;
    private static final String MODEL_PATTERN = "[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}";

    private final URI chatCompletions;
    private final String model;
    private final Duration timeout;
    private final HttpClient http;
    private final List<ObjectNode> history = new ArrayList<>();

    private String sessionId;

    OpenAiAgentProvider(URI endpoint, String model, String savedSessionId,
                        Duration timeout) {
        this.chatCompletions = chatCompletions(validateEndpoint(endpoint));
        this.model = validateModel(model);
        this.timeout = Objects.requireNonNull(timeout, "turn timeout");
        this.sessionId = savedSessionId == null || savedSessionId.isBlank()
                ? "openai-" + UUID.randomUUID() : savedSessionId;
        this.http = HttpClient.newBuilder()
                // OpenAI-compatible local sidecars commonly expose HTTP/1.1 only.
                // Java's cleartext HTTP/2 upgrade is rejected by Intel vLLM before
                // the JSON request reaches the chat-completions handler.
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public String name() {
        return "openai";
    }

    @Override
    public synchronized String sessionId() {
        return sessionId;
    }

    @Override
    public synchronized String prompt(String prompt) {
        ObjectNode user = message("user", prompt);
        ArrayNode messages = MAPPER.createArrayNode();
        history.forEach(messages::add);
        messages.add(user);
        ObjectNode body = MAPPER.createObjectNode();
        body.put("model", model);
        body.set("messages", messages);
        body.put("stream", false);
        body.putObject("response_format").put("type", "json_object");
        HttpRequest request = HttpRequest.newBuilder(chatCompletions)
                .timeout(timeout)
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AgentHostException("chat completions request was interrupted", e);
        } catch (IOException e) {
            throw new AgentHostException("chat completions endpoint is unavailable", e);
        }
        if (response.statusCode() != 200) {
            // The body is endpoint-controlled and can echo request or model detail.
            throw new AgentHostException("chat completions endpoint returned HTTP "
                    + response.statusCode());
        }
        String responseBody = response.body();
        if (responseBody == null || responseBody.length() > MAX_RESPONSE_CHARS) {
            throw new AgentHostException(
                    "chat completions response exceeded the bounded size");
        }
        String content = readContent(responseBody);
        history.add(user);
        history.add(message("assistant", content));
        while (history.size() > MAX_HISTORY_MESSAGES) {
            history.remove(0);
        }
        return content.trim();
    }

    private static String readContent(String responseBody) {
        JsonNode parsed;
        try {
            parsed = MAPPER.readTree(responseBody);
        } catch (IOException e) {
            throw new AgentHostException(
                    "chat completions endpoint returned invalid JSON", e);
        }
        String content = parsed.path("choices").path(0).path("message")
                .path("content").asText("");
        if (content.isBlank()) {
            throw new AgentHostException(
                    "chat completions endpoint returned no message content");
        }
        return content;
    }

    private static ObjectNode message(String role, String content) {
        ObjectNode message = MAPPER.createObjectNode();
        message.put("role", role);
        message.put("content", content);
        return message;
    }

    private static URI chatCompletions(URI endpoint) {
        String path = endpoint.getPath() == null ? "" : endpoint.getPath();
        if (path.endsWith("/chat/completions")) {
            return endpoint;
        }
        String base = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return endpoint.resolve(base + "/chat/completions");
    }

    private static URI validateEndpoint(URI endpoint) {
        Objects.requireNonNull(endpoint, "provider endpoint");
        if (!("http".equalsIgnoreCase(endpoint.getScheme())
                || "https".equalsIgnoreCase(endpoint.getScheme()))
                || endpoint.getHost() == null || endpoint.getUserInfo() != null
                || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("provider endpoint must be an HTTP(S) URI "
                    + "without user info or a fragment");
        }
        return endpoint;
    }

    private static String validateModel(String model) {
        if (model == null || !model.matches(MODEL_PATTERN)) {
            throw new IllegalArgumentException("provider model must be a bounded "
                    + "identifier such as meta-models/Muse-Glimmer-30B");
        }
        return model;
    }

    @Override
    public synchronized void close() {
        history.clear();
        http.close();
    }
}
