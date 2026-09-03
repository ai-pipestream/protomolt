package ai.protomolt.proto.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.UncheckedIOException;
import java.util.List;

/**
 * JSON export support for MappingHelper.
 * Converts schema and field information to JSON for frontend consumption.
 */
public class MappingHelperJsonSupport {

    private final ObjectMapper objectMapper;

    /**
     * Creates a mapping helper json support.
     */
    public MappingHelperJsonSupport() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Exports field information as JSON.
     *
     * @param fields List of field information
     * @return JSON string
     */
    public String fieldsToJson(List<MappingHelper.FieldInfo> fields) {
        try {
            ArrayNode array = objectMapper.createArrayNode();
            for (MappingHelper.FieldInfo field : fields) {
                ObjectNode node = objectMapper.createObjectNode();
                node.put("path", field.path);
                node.put("name", field.name);
                node.put("type", field.type);
                node.put("repeated", field.repeated);
                node.put("required", field.required);
                node.put("hasDefault", field.hasDefault);
                node.put("depth", field.depth);
                array.add(node);
            }
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(array);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to serialize fields to JSON", e);
        }
    }

    /**
     * Exports schema tree as JSON.
     *
     * @param schema The schema root node
     * @return JSON string
     */
    public String schemaToJson(MappingHelper.SchemaNode schema) {
        try {
            ObjectNode node = schemaNodeToJson(schema);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to serialize schema to JSON", e);
        }
    }

    /**
     * Exports field suggestions as JSON.
     *
     * @param suggestions List of field suggestions
     * @return JSON string
     */
    public String suggestionsToJson(List<MappingHelper.FieldSuggestion> suggestions) {
        try {
            ArrayNode array = objectMapper.createArrayNode();
            for (MappingHelper.FieldSuggestion suggestion : suggestions) {
                ObjectNode node = objectMapper.createObjectNode();
                node.put("fieldPath", suggestion.fieldPath);
                node.put("score", suggestion.score);
                array.add(node);
            }
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(array);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to serialize suggestions to JSON", e);
        }
    }

    /**
     * Exports validation result as JSON.
     *
     * @param result Validation result
     * @return JSON string
     */
    public String validationToJson(MappingHelper.ValidationResult result) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("valid", result.isValid);
            node.put("message", result.message);
            node.put("level", result.level.name());
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to serialize validation to JSON", e);
        }
    }

    private ObjectNode schemaNodeToJson(MappingHelper.SchemaNode node) {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("name", node.name);
        json.put("path", node.path);
        json.put("type", node.type);
        json.put("repeated", node.repeated);

        if (!node.children.isEmpty()) {
            ArrayNode children = objectMapper.createArrayNode();
            for (MappingHelper.SchemaNode child : node.children) {
                children.add(schemaNodeToJson(child));
            }
            json.set("children", children);
        }

        return json;
    }
}
