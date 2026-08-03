package ai.pipestream.proto.inference.service.actions;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
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
    static ObjectNode entryJson(ModelEntry entry) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("id", entry.getId());
        node.put("provider", entry.getProvider());
        node.put("endpoint", entry.getEndpoint());
        if (!entry.getBackendModel().isEmpty()) {
            node.put("backendModel", entry.getBackendModel());
        }
        ModelCapabilities capabilities = entry.getCapabilities();
        ObjectNode caps = node.putObject("capabilities");
        caps.put("maxContextTokens", capabilities.getMaxContextTokens());
        caps.put("maxOutputTokens", capabilities.getMaxOutputTokens());
        caps.put("streaming", capabilities.getStreaming());
        caps.put("thinking", capabilities.getThinking());
        capabilities.getModalitiesList().forEach(caps.putArray("modalities")::add);
        if (!entry.getLabelsMap().isEmpty()) {
            ObjectNode labels = node.putObject("labels");
            entry.getLabelsMap().forEach(labels::put);
        }
        return node;
    }
}
