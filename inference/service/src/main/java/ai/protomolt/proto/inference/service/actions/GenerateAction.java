package ai.protomolt.proto.inference.service.actions;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.spi.InferenceException;
import ai.protomolt.proto.inference.v1.ChatTurn;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.Role;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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
    public Descriptor requestType() {
        return CatalogContract.request("InferenceGenerateRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("InferenceGenerateResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        InferenceActionSupport.requireEngines(engines);
        // The request message declares the model required and bounds every sampling
        // knob, and its own min_items rule refuses an empty turn list, so the shape
        // checks that used to live here are the contract's.
        GenerateRequest.Builder request = GenerateRequest.newBuilder()
                .setModel(Fields.string(input, "model"))
                .setTemperature(Fields.decimal(input, "temperature"))
                .setTopP(Fields.decimal(input, "topP"))
                .setMaxOutputTokens(Fields.integer(input, "maxOutputTokens"))
                .setEnableThinking(Fields.flag(input, "enableThinking"));
        for (Message turn : Fields.<Message>list(input, "messages")) {
            request.addMessages(ChatTurn.newBuilder()
                    .setRole(role(Fields.string(turn, "role")))
                    .setContent(Fields.string(turn, "content")));
        }

        GenerateRequest typed = request.build();
        try {
            ValidationResult.validate(typed).throwIfInvalid();
        } catch (ValidationResult.ValidationException e) {
            throw InferenceActionSupport.invalidInput(
                    "request violates its declared rules: " + e.result().violations());
        }

        try {
            GenerateResponse response = engines.generate(typed);
            Reply result = Reply.of(responseType())
                    .set("ok", true)
                    .set("text", response.getText())
                    .set("model", response.getModel())
                    .set("provider", response.getProvider())
                    .set("modelVersion", response.getModelVersion())
                    .set("finishReason", finishReason(response.getFinishReason()));
            result.nest("usage")
                    .set("promptTokens", response.getUsage().getPromptTokens())
                    .set("completionTokens", response.getUsage().getCompletionTokens())
                    .build();
            return result.build();
        } catch (InferenceException e) {
            return Reply.of(responseType())
                    .set("ok", false).set("error", e.getMessage()).build();
        }
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

    private static String finishReason(ai.protomolt.proto.inference.v1.FinishReason reason) {
        return switch (reason) {
            case FINISH_REASON_STOP -> "stop";
            case FINISH_REASON_LENGTH -> "length";
            case FINISH_REASON_ABORTED -> "aborted";
            default -> "unspecified";
        };
    }
}
