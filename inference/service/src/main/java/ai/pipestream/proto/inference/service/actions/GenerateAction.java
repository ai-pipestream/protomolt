package ai.pipestream.proto.inference.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.InferenceException;
import ai.pipestream.proto.inference.v1.ChatTurn;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.Role;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@code inference-generate} verb: one unary generation against a catalog
 * model, resolved through the inference SPI. The typed request is built from
 * the envelope and validated against the contract's declared validate.v1
 * rules before any provider is touched. Unknown models and provider failures
 * come back as {@code ok:false} with the verbatim error, so an agent reading
 * the envelope can repair and retry.
 * <p>
 * A null facade means inference is not configured on this server; every call
 * then answers {@code unavailable}.
 */
public final class GenerateAction implements ProtoAction {

    private final InferenceEngines engines;

    /**
     * @param engines the inference facade, or null when inference is not configured
     */
    public GenerateAction(InferenceEngines engines) {
        this.engines = engines;
    }

    @Override
    public String name() {
        return "inference-generate";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Runs one chat generation against a model in the server's inference catalog "
                + "(inference-list-models shows the registry). Returns the text, token usage, "
                + "and provenance: the provider, model id, and model version that produced it.";
    }

    @Override
    public ObjectNode inputSchema() {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode schema = factory.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("model")
                .put("type", "string")
                .put("description", "The catalog id of the model to run.");
        ObjectNode messages = properties.putObject("messages");
        messages.put("type", "array");
        messages.put("description", "The conversation, oldest first.");
        ObjectNode turn = messages.putObject("items");
        turn.put("type", "object");
        ObjectNode turnProps = turn.putObject("properties");
        turnProps.putObject("role")
                .put("type", "string")
                .put("enum", factory.arrayNode().add("system").add("user").add("assistant").add("tool"));
        turnProps.putObject("content").put("type", "string");
        turn.putArray("required").add("role").add("content");
        properties.putObject("temperature")
                .put("type", "number")
                .put("description", "Sampling temperature; omit for the provider default.");
        properties.putObject("topP")
                .put("type", "number")
                .put("description", "Nucleus sampling mass; omit for the provider default.");
        properties.putObject("maxOutputTokens")
                .put("type", "integer")
                .put("description", "Hard cap on generated tokens; omit for the provider default.");
        properties.putObject("enableThinking")
                .put("type", "boolean")
                .put("description", "Requests the model's deliberation mode when it has one.");
        schema.putArray("required").add("model").add("messages");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        InferenceActionSupport.requireEngines(engines);
        GenerateRequest.Builder request = GenerateRequest.newBuilder()
                .setModel(InferenceActionSupport.requireString(input, "model"));

        JsonNode messages = input.get("messages");
        if (messages == null || !messages.isArray() || messages.isEmpty()) {
            throw InferenceActionSupport.invalidInput(
                    "'messages' must be a non-empty array of {role, content}");
        }
        for (JsonNode turnNode : messages) {
            if (!turnNode.isObject()) {
                throw InferenceActionSupport.invalidInput("each message must be an object {role, content}");
            }
            String roleText = InferenceActionSupport.requireString((ObjectNode) turnNode, "role");
            String content = InferenceActionSupport.requireString((ObjectNode) turnNode, "content");
            request.addMessages(ChatTurn.newBuilder()
                    .setRole(role(roleText))
                    .setContent(content));
        }
        if (input.hasNonNull("temperature")) {
            request.setTemperature(input.get("temperature").asDouble());
        }
        if (input.hasNonNull("topP")) {
            request.setTopP(input.get("topP").asDouble());
        }
        if (input.hasNonNull("maxOutputTokens")) {
            request.setMaxOutputTokens(input.get("maxOutputTokens").asInt());
        }
        if (input.hasNonNull("enableThinking")) {
            request.setEnableThinking(input.get("enableThinking").asBoolean());
        }

        GenerateRequest typed = request.build();
        try {
            ValidationResult.validate(typed).throwIfInvalid();
        } catch (ValidationResult.ValidationException e) {
            throw InferenceActionSupport.invalidInput(
                    "request violates its declared rules: " + e.result().violations());
        }

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        try {
            GenerateResponse response = engines.generate(typed);
            result.put("ok", true);
            result.put("text", response.getText());
            result.put("model", response.getModel());
            result.put("provider", response.getProvider());
            if (!response.getModelVersion().isEmpty()) {
                result.put("modelVersion", response.getModelVersion());
            }
            result.put("finishReason", finishReason(response.getFinishReason()));
            ObjectNode usage = result.putObject("usage");
            usage.put("promptTokens", response.getUsage().getPromptTokens());
            usage.put("completionTokens", response.getUsage().getCompletionTokens());
        } catch (InferenceException e) {
            result.put("ok", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    private static Role role(String text) throws ActionException {
        return switch (text) {
            case "system" -> Role.ROLE_SYSTEM;
            case "user" -> Role.ROLE_USER;
            case "assistant" -> Role.ROLE_ASSISTANT;
            case "tool" -> Role.ROLE_TOOL;
            default -> throw InferenceActionSupport.invalidInput(
                    "unknown role '" + text + "' (system, user, assistant, tool)");
        };
    }

    private static String finishReason(ai.pipestream.proto.inference.v1.FinishReason reason) {
        return switch (reason) {
            case FINISH_REASON_STOP -> "stop";
            case FINISH_REASON_LENGTH -> "length";
            case FINISH_REASON_ABORTED -> "aborted";
            default -> "unspecified";
        };
    }
}
