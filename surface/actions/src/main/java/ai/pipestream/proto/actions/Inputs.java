package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Hand-rolled minimal envelope validation: required fields and basic JSON types. Violations
 * surface as {@code invalid-input} with the offending JSON pointer in {@code details.pointer}.
 */
final class Inputs {

    private Inputs() {
    }

    static ActionException invalidInput(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message + " (at '" + pointer + "')", details);
    }

    /** The input envelope itself must be a JSON object. */
    static ObjectNode requireEnvelope(ObjectNode input) throws ActionException {
        if (input == null) {
            throw invalidInput("Input must be a JSON object", "");
        }
        return input;
    }

    static ObjectNode requireObject(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            throw invalidInput("Missing required object field '" + field + "'", "/" + field);
        }
        if (!node.isObject()) {
            throw invalidInput("Field '" + field + "' must be a JSON object", "/" + field);
        }
        return (ObjectNode) node;
    }

    static ObjectNode optionalObject(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isObject()) {
            throw invalidInput("Field '" + field + "' must be a JSON object", "/" + field);
        }
        return (ObjectNode) node;
    }

    static String requireString(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            throw invalidInput("Missing required string field '" + field + "'", "/" + field);
        }
        if (!node.isTextual()) {
            throw invalidInput("Field '" + field + "' must be a string", "/" + field);
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
            throw invalidInput("Field '" + field + "' must be a string", "/" + field);
        }
        return node.asText();
    }

    static boolean optionalBoolean(ObjectNode input, String field, boolean defaultValue)
            throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        if (!node.isBoolean()) {
            throw invalidInput("Field '" + field + "' must be a boolean", "/" + field);
        }
        return node.asBoolean();
    }

    /** Returns {@code null} when absent; rejects present non-array values. */
    static ArrayNode optionalArray(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isArray()) {
            throw invalidInput("Field '" + field + "' must be an array", "/" + field);
        }
        return (ArrayNode) node;
    }

    /** Every element of {@code array} as a string; rejects non-string elements. */
    static List<String> stringElements(ArrayNode array, String pointer) throws ActionException {
        List<String> values = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            JsonNode element = array.get(i);
            if (!element.isTextual()) {
                throw invalidInput("Array elements must be strings", pointer + "/" + i);
            }
            values.add(element.asText());
        }
        return values;
    }
}
