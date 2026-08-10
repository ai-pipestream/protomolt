package ai.pipestream.proto.inference.openvino;

import ai.pipestream.proto.inference.spi.ChunkObserver;
import ai.pipestream.proto.inference.spi.InferenceException;
import ai.pipestream.proto.inference.v1.ChatTurn;
import ai.pipestream.proto.inference.v1.FinishReason;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStreamRequest;
import ai.pipestream.proto.inference.v1.GenerateStreamResponse;
import ai.pipestream.proto.inference.v1.ModelEntry;
import ai.pipestream.proto.inference.v1.Role;
import ai.pipestream.proto.inference.v1.StructuredOutputConstraint;
import ai.pipestream.proto.inference.v1.Usage;
import ai.pipestream.proto.prompt.ResponseFormatShaper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * The shared OpenAI-compatible chat-completions transport: the wire logic
 * every REST provider profile uses — OVMS on {@code /v3}, and Ollama, vLLM,
 * and llama.cpp (the NVIDIA and edge-box lanes) on {@code /v1}.
 *
 * <p>Jackson exists only at this wire edge: protobuf messages come in, an
 * OpenAI JSON body goes out, and the JSON response is mapped straight back to
 * protobuf. Unset sampling knobs are omitted from the wire body so the
 * backend applies its own defaults (a proto3 zero is not a value).</p>
 *
 * <p>Every failure is an {@link InferenceException} naming the model id and
 * endpoint. Instances are stateless and thread-safe.</p>
 */
public final class OpenAiChatTransport {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private final String providerId;
    private final String chatPath;
    private final HttpClient client;
    private final Duration requestTimeout;

    /**
     * Creates the transport for one provider profile.
     *
     * @param providerId the id stamped on responses' provenance (e.g. "openvino", "openai")
     * @param chatPath the chat-completions path (e.g. "/v3/chat/completions" for OVMS,
     *     "/v1/chat/completions" for Ollama, vLLM, llama.cpp)
     * @param requestTimeout the timeout for one generation call
     */
    public OpenAiChatTransport(String providerId, String chatPath, Duration requestTimeout) {
        this.providerId = providerId;
        this.chatPath = chatPath;
        this.requestTimeout = requestTimeout;
        this.client = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
    }

    /**
     * Executes one unary generation against the model's backend.
     *
     * @param model the catalog entry (endpoint, backend model name)
     * @param request the typed request
     * @return the completed generation with provenance filled
     */
    public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
        ObjectNode body = chatBody(model, request.getMessagesList(), request.getTemperature(),
                request.getTopP(), request.getMaxOutputTokens(), false,
                request.hasStructuredOutput() ? request.getStructuredOutput() : null);
        JsonNode response = post(model, body, false);
        JsonNode choice = firstChoice(model, response);
        return GenerateResponse.newBuilder()
                .setText(choice.path("message").path("content").asText())
                .setModel(request.getModel())
                .setProvider(providerId)
                .setFinishReason(finishReason(choice.path("finish_reason").asText()))
                .setUsage(usage(response.path("usage")))
                .build();
    }

    /**
     * Executes one streaming generation against the model's backend.
     *
     * @param model the catalog entry (endpoint, backend model name)
     * @param request the typed request
     * @param observer the chunk sink
     */
    public void generateStream(ModelEntry model, GenerateStreamRequest request, ChunkObserver observer) {
        ObjectNode body = chatBody(model, request.getMessagesList(), request.getTemperature(),
                request.getTopP(), request.getMaxOutputTokens(), true, null);
        HttpRequest httpRequest = HttpRequest.newBuilder(chatUri(model))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();
        HttpResponse<java.io.InputStream> response;
        try {
            response = client.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        } catch (IOException e) {
            throw new InferenceException("stream to " + model.getId() + " at "
                    + model.getEndpoint() + " failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InferenceException("stream to " + model.getId() + " interrupted", e);
        }
        if (response.statusCode() != 200) {
            throw new InferenceException("model " + model.getId() + " at " + model.getEndpoint()
                    + " answered HTTP " + response.statusCode());
        }
        streamEvents(model, response, observer);
    }

    /** Parses the SSE stream and pushes chunks; the terminal event completes the observer. */
    private void streamEvents(ModelEntry model, HttpResponse<java.io.InputStream> response,
                              ChunkObserver observer) {
        Usage finalUsage = Usage.getDefaultInstance();
        FinishReason finalReason = FinishReason.FINISH_REASON_UNSPECIFIED;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("data:")) {
                    continue;
                }
                String payload = line.substring(5).trim();
                if (payload.equals("[DONE]")) {
                    break;
                }
                JsonNode event;
                try {
                    event = MAPPER.readTree(payload);
                } catch (IOException e) {
                    throw new InferenceException("model " + model.getId()
                            + " sent a malformed stream event: " + abbreviate(payload), e);
                }
                if (event.has("usage") && !event.path("usage").isNull()) {
                    finalUsage = usage(event.path("usage"));
                }
                JsonNode choice = event.path("choices").path(0);
                if (choice.isMissingNode()) {
                    continue;
                }
                String delta = choice.path("delta").path("content").asText("");
                String reason = choice.path("finish_reason").isTextual()
                        ? choice.path("finish_reason").asText() : null;
                if (reason != null) {
                    finalReason = finishReason(reason);
                }
                if (!delta.isEmpty()) {
                    observer.onNext(GenerateStreamResponse.newBuilder()
                            .setTextDelta(delta)
                            .setLast(false)
                            .build());
                }
            }
        } catch (IOException e) {
            observer.onError(new InferenceException("stream from " + model.getId() + " at "
                    + model.getEndpoint() + " broke: " + e.getMessage(), e));
            return;
        }
        observer.onNext(GenerateStreamResponse.newBuilder()
                .setLast(true)
                .setFinishReason(finalReason)
                .setUsage(finalUsage)
                .build());
        observer.onComplete();
    }

    /** Builds the OpenAI chat body, omitting unset knobs so backend defaults apply. */
    private ObjectNode chatBody(ModelEntry model, List<ChatTurn> messages, double temperature,
                                double topP, int maxOutputTokens, boolean stream,
                                StructuredOutputConstraint structuredOutput) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("model", model.getBackendModel().isEmpty() ? model.getId() : model.getBackendModel());
        ArrayNode turns = body.putArray("messages");
        for (ChatTurn turn : messages) {
            ObjectNode node = turns.addObject();
            node.put("role", wireRole(turn.getRole()));
            node.put("content", turn.getContent());
        }
        if (temperature > 0) {
            body.put("temperature", temperature);
        }
        if (topP > 0) {
            body.put("top_p", topP);
        }
        if (maxOutputTokens > 0) {
            body.put("max_tokens", maxOutputTokens);
        }
        if (structuredOutput != null) {
            addStructuredOutput(model, body, structuredOutput);
        }
        if (stream) {
            body.put("stream", true);
            ObjectNode streamOptions = body.putObject("stream_options");
            streamOptions.put("include_usage", true);
        }
        return body;
    }

    /** Adds the strict response-format envelope after capability and schema checks. */
    private static void addStructuredOutput(ModelEntry model, ObjectNode body,
                                            StructuredOutputConstraint constraint) {
        if (!model.getCapabilities().getStructuredOutput()) {
            throw new InferenceException("model '" + model.getId()
                    + "' does not declare the structured-output capability");
        }
        String envelope;
        try {
            envelope = ResponseFormatShaper.jsonSchemaEnvelope(
                    constraint.getName(), constraint.getJsonSchema(), true);
        } catch (IllegalArgumentException e) {
            throw new InferenceException("structured-output constraint for model '"
                    + model.getId() + "' is invalid: " + e.getMessage(), e);
        }
        try {
            body.set("response_format", MAPPER.readTree(envelope));
        } catch (IOException e) {
            throw new IllegalStateException("response-format shaper emitted invalid JSON", e);
        }
    }

    private JsonNode post(ModelEntry model, ObjectNode body, boolean stream) {
        HttpRequest httpRequest = HttpRequest.newBuilder(chatUri(model))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();
        HttpResponse<String> response;
        try {
            response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new InferenceException("call to " + model.getId() + " at " + model.getEndpoint()
                    + " failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InferenceException("call to " + model.getId() + " interrupted", e);
        }
        if (response.statusCode() != 200) {
            throw new InferenceException("model " + model.getId() + " at " + model.getEndpoint()
                    + " answered HTTP " + response.statusCode() + ": " + abbreviate(response.body()));
        }
        try {
            return MAPPER.readTree(response.body());
        } catch (IOException e) {
            throw new InferenceException("model " + model.getId() + " at " + model.getEndpoint()
                    + " answered with malformed JSON: " + abbreviate(response.body()), e);
        }
    }

    private JsonNode firstChoice(ModelEntry model, JsonNode response) {
        JsonNode choice = response.path("choices").path(0);
        if (choice.isMissingNode()) {
            throw new InferenceException("model " + model.getId() + " at " + model.getEndpoint()
                    + " answered with no choices: " + abbreviate(response.toString()));
        }
        return choice;
    }

    private URI chatUri(ModelEntry model) {
        String base = model.getEndpoint();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return URI.create(base + chatPath);
    }

    private static String wireRole(Role role) {
        return switch (role) {
            case ROLE_SYSTEM -> "system";
            case ROLE_USER -> "user";
            case ROLE_ASSISTANT -> "assistant";
            case ROLE_TOOL -> "tool";
            default -> throw new InferenceException("cannot map role to the wire: " + role);
        };
    }

    private static FinishReason finishReason(String reason) {
        return switch (reason) {
            case "stop" -> FinishReason.FINISH_REASON_STOP;
            case "length" -> FinishReason.FINISH_REASON_LENGTH;
            case null, default -> FinishReason.FINISH_REASON_ABORTED;
        };
    }

    private static Usage usage(JsonNode node) {
        return Usage.newBuilder()
                .setPromptTokens(node.path("prompt_tokens").asLong())
                .setCompletionTokens(node.path("completion_tokens").asLong())
                .build();
    }

    private static String abbreviate(String text) {
        if (text == null) {
            return "";
        }
        return text.length() > 200 ? text.substring(0, 197) + "..." : text;
    }
}
