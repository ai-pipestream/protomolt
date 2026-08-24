package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

final class WorkflowActionJson {

    private static final String IDENTITY_PATTERN = "[A-Za-z0-9][A-Za-z0-9._-]{0,127}";

    private WorkflowActionJson() {
    }

    static ObjectNode schema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }

    /** A required string on a request, refusing a blank one by name. */
    static String text(Message input, String field) throws ActionException {
        String value = Fields.string(input, field);
        if (value.isBlank()) {
            throw invalid("'" + field + "' must be a non-empty string", "/" + field);
        }
        return value;
    }

    /** An optional string on a request; blank means the caller said nothing. */
    static String optionalText(Message input, String field) {
        String value = Fields.string(input, field);
        return value.isBlank() ? null : value;
    }

    /** A required workflow identity, held to the same name rules the store enforces. */
    static String identity(Message input, String field) throws ActionException {
        return checkedName(text(input, field), field);
    }

    /** An optional workflow identity. */
    static String optionalIdentity(Message input, String field) throws ActionException {
        String value = optionalText(input, field);
        return value == null ? null : checkedName(value, field);
    }

    private static String checkedName(String value, String field) throws ActionException {
        try {
            WorkflowValidation.validateName(value, field);
            return value;
        } catch (IllegalArgumentException e) {
            throw invalid(e.getMessage(), "/" + field);
        }
    }

    /**
     * A {@code map<string, string>} of base64 payloads, decoded. Null when the caller sent
     * none, which is distinct from sending an empty one only in that both mean the same.
     */
    static java.util.Map<String, byte[]> base64Map(Message input, String field)
            throws ActionException {
        java.util.Map<String, String> encoded = Fields.map(input, field);
        if (encoded.isEmpty()) {
            return null;
        }
        java.util.Map<String, byte[]> values = new java.util.HashMap<>();
        for (var entry : encoded.entrySet()) {
            try {
                values.put(entry.getKey(),
                        java.util.Base64.getDecoder().decode(entry.getValue()));
            } catch (IllegalArgumentException e) {
                throw invalid("'" + field + "' value is not valid base64",
                        "/" + field + "/" + entry.getKey());
            }
        }
        return values;
    }

    static ObjectNode object(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node instanceof ObjectNode object) {
            return object;
        }
        throw invalid("'" + field + "' must be an object", "/" + field);
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

    static String identity(ObjectNode input, String field) throws ActionException {
        String value = text(input, field);
        try {
            WorkflowValidation.validateName(value, field);
            return value;
        } catch (IllegalArgumentException e) {
            throw invalid(e.getMessage(), "/" + field);
        }
    }

    static String optionalIdentity(ObjectNode input, String field) throws ActionException {
        String value = optionalText(input, field);
        if (value == null) {
            return null;
        }
        try {
            WorkflowValidation.validateName(value, field);
            return value;
        } catch (IllegalArgumentException e) {
            throw invalid(e.getMessage(), "/" + field);
        }
    }

    static ObjectNode identitySchema(ObjectNode properties, String field) {
        return properties.putObject(field).put("type", "string")
                .put("pattern", IDENTITY_PATTERN).put("maxLength", 128);
    }

    static <B extends Message.Builder> Message parse(ObjectNode node, B builder, String pointer)
            throws ActionException {
        try {
            JsonFormat.parser().merge(node.toString(), builder);
            return builder.build();
        } catch (Exception e) {
            throw invalid("Invalid protobuf JSON: " + e.getMessage(), pointer);
        }
    }

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

    static java.util.Map<String, byte[]> base64Map(ObjectNode input, String field)
            throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!(node instanceof ObjectNode object)) {
            throw invalid("'" + field + "' must be an object of base64 strings",
                    "/" + field);
        }
        java.util.Map<String, byte[]> values = new java.util.HashMap<>();
        var fields = object.fields();
        while (fields.hasNext()) {
            var entry = fields.next();
            if (!entry.getValue().isTextual()) {
                throw invalid("'" + field + "' values must be base64 strings",
                        "/" + field + "/" + entry.getKey());
            }
            try {
                values.put(entry.getKey(), java.util.Base64.getDecoder()
                        .decode(entry.getValue().asText()));
            } catch (IllegalArgumentException e) {
                throw invalid("'" + field + "' value is not valid base64",
                        "/" + field + "/" + entry.getKey());
            }
        }
        return values;
    }

    static ActionException invalid(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message, details);
    }

    static ActionException unavailable(String capability, String remedy) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("capability", capability);
        details.put("remedy", remedy);
        return new ActionException("unavailable", capability + " is not configured; " + remedy,
                details);
    }
}
