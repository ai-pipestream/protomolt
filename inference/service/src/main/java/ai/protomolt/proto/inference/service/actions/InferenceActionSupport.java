package ai.protomolt.proto.inference.service.actions;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.v1.ModelCapabilities;
import ai.protomolt.proto.inference.v1.ModelEntry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The shared plumbing of the inference verbs: the null-engines "unavailable"
 * gate (a server without {@code --inference-model} answers every inference
 * verb the same way), envelope validation, and the {@link ModelEntry} → JSON
 * mapping for the list/describe verbs. JSON keys are the proto3 JSON
 * (camelCase) forms of the protomolt_service.proto messages, so one envelope
 * serves gRPC, REST, and MCP identically.
 */
final class InferenceActionSupport {

    /** The message every inference verb answers when no catalog is mounted. */
    static final String UNAVAILABLE_MESSAGE = "inference is not configured on this server "
            + "(start protomolt-serve with one or more --inference-model flags)";

    private InferenceActionSupport() {
    }

    /** The null-engines gate: no catalog means every call answers "unavailable". */
    static InferenceEngines requireEngines(InferenceEngines engines) throws ActionException {
        if (engines == null) {
            throw new ActionException("unavailable", UNAVAILABLE_MESSAGE);
        }
        return engines;
    }

    static ActionException invalidInput(String message) {
        return new ActionException("invalid-input", message);
    }

    static String requireString(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            throw invalidInput("Missing required string field '" + field + "'");
        }
        if (!node.isTextual()) {
            throw invalidInput("Field '" + field + "' must be a string");
        }
        return node.asText();
    }

    /** Returns {@code null} when absent; rejects present non-string values. */
    static String optionalString(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isTextual()) {
            throw invalidInput("Field '" + field + "' must be a string");
        }
        return node.asText();
    }

    /** Renders one catalog entry as its proto3 JSON (camelCase) envelope. */
    /**
     * Writes one catalogued model into {@code entry}, which is the contract's own shape for
     * it rather than the engine's.
     */
    static void writeEntry(Reply entry, ModelEntry model) {
        entry.set("id", model.getId())
                .set("provider", model.getProvider())
                .set("endpoint", model.getEndpoint())
                .set("backendModel", model.getBackendModel());
        ModelCapabilities capabilities = model.getCapabilities();
        entry.nest("capabilities")
                .set("maxContextTokens", capabilities.getMaxContextTokens())
                .set("maxOutputTokens", capabilities.getMaxOutputTokens())
                .set("streaming", capabilities.getStreaming())
                .set("thinking", capabilities.getThinking())
                .set("structuredOutput", capabilities.getStructuredOutput())
                .addAll("modalities", capabilities.getModalitiesList())
                .build();
        model.getLabelsMap().forEach((key, value) ->
                entry.append("labels").set("key", key).set("value", value).build());
        entry.build();
    }
}
