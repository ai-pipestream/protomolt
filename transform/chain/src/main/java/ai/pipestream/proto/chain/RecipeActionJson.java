package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

final class RecipeActionJson {

    private static final String IDENTITY_PATTERN = "[A-Za-z0-9][A-Za-z0-9._-]{0,127}";

    private RecipeActionJson() {
    }

    static ObjectNode schema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
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
            RecipeValidation.validateName(value, field);
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
            RecipeValidation.validateName(value, field);
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
