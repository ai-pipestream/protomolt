package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * Shared JSON assembly for the delegation catalog actions: envelope validation, proto3
 * JSON parsing and rendering, and the stable error codes the tools report.
 */
final class DelegationActionJson {

    private static final String IDENTITY_PATTERN = "[A-Za-z0-9][A-Za-z0-9._-]{0,127}";
    private static final String UUID_PATTERN =
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

    private DelegationActionJson() {
    }

    /** The 2020-12 envelope skeleton every delegation action schema starts from. */
    static ObjectNode schema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }

    static String text(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node != null && node.isTextual() && !node.asText().isBlank()) {
            return node.asText();
        }
        throw invalid("'" + field + "' must be a non-empty string", "/" + field);
    }

    static String optionalText(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual() && !node.asText().isBlank()) {
            return node.asText();
        }
        throw invalid("'" + field + "' must be a non-empty string", "/" + field);
    }

    /** A required worker-identity-shaped string. */
    static String identity(ObjectNode input, String field) throws ActionException {
        String value = text(input, field);
        if (!value.matches(IDENTITY_PATTERN)) {
            throw invalid("'" + field + "' must be a path-safe identity", "/" + field);
        }
        return value;
    }

    /** A required uuid-shaped string. */
    static String uuid(ObjectNode input, String field) throws ActionException {
        String value = text(input, field);
        if (!value.matches(UUID_PATTERN)) {
            throw invalid("'" + field + "' must be a uuid", "/" + field);
        }
        return value;
    }

    /** An optional uuid-shaped string. */
    static String optionalUuid(ObjectNode input, String field) throws ActionException {
        String value = optionalText(input, field);
        if (value == null) {
            return null;
        }
        if (!value.matches(UUID_PATTERN)) {
            throw invalid("'" + field + "' must be a uuid", "/" + field);
        }
        return value;
    }

    static ObjectNode object(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node instanceof ObjectNode object) {
            return object;
        }
        throw invalid("'" + field + "' must be an object", "/" + field);
    }

    /** A bounded integer with a default when absent. */
    static int boundedInt(ObjectNode input, String field, int fallback, int min, int max)
            throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return fallback;
        }
        if (!node.isIntegralNumber()) {
            throw invalid("'" + field + "' must be an integer", "/" + field);
        }
        long value = node.asLong();
        if (value < min || value > max) {
            throw invalid("'" + field + "' must be between " + min + " and " + max,
                    "/" + field);
        }
        return (int) value;
    }

    /** A bounded non-negative long with a default when absent. */
    static long boundedLong(ObjectNode input, String field, long fallback, long max)
            throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return fallback;
        }
        if (!node.isIntegralNumber()) {
            throw invalid("'" + field + "' must be an integer", "/" + field);
        }
        long value = node.asLong();
        if (value < 0 || value > max) {
            throw invalid("'" + field + "' must be between 0 and " + max, "/" + field);
        }
        return value;
    }

    /** Parses a proto3 JSON object into a concrete message. */
    static <B extends Message.Builder> Message parse(ObjectNode node, B builder,
                                                     String pointer) throws ActionException {
        try {
            JsonFormat.parser().merge(node.toString(), builder);
            return builder.build();
        } catch (Exception e) {
            throw invalid("Invalid protobuf JSON: " + e.getMessage(), pointer);
        }
    }

    /** Renders a protobuf message as a Jackson tree via canonical proto3 JSON. */
    static ObjectNode render(Message message, ActionContext context) throws ActionException {
        try {
            JsonNode node = context.objectMapper().readTree(
                    JsonFormat.printer().omittingInsignificantWhitespace().print(message));
            if (node instanceof ObjectNode object) {
                return object;
            }
            throw new IllegalStateException("protobuf JSON was not an object");
        } catch (JsonProcessingException e) {
            throw new ActionException("render-failed", "Failed to render protobuf JSON", null);
        } catch (Exception e) {
            throw new ActionException("render-failed", e.getMessage(), null);
        }
    }

    static ActionException invalid(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message, details);
    }

    /** The named worker has no live bridge session. */
    static ActionException unknownWorker(String workerId) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("workerId", workerId);
        return new ActionException("unknown-worker",
                "Worker '" + workerId + "' is not registered; call delegation-worker-register",
                details);
    }

    /** The worker's delegation stream already failed; re-register to continue. */
    static ActionException streamFailed(String workerId, String detail) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("workerId", workerId);
        return new ActionException("worker-stream-failed",
                "Worker stream for '" + workerId + "' failed: " + detail, details);
    }

    /** The coordinator refused the operation (unknown task, stale phase, bad frame). */
    static ActionException rejected(String detail) {
        return new ActionException("delegation-rejected", detail);
    }
}
