package ai.pipestream.proto.prompt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * Wraps a generated JSON Schema document into the structured-output envelope LLM
 * serving stacks expect (OpenAI / vLLM {@code response_format} shape). The schema is
 * opaque decoder configuration — this class changes nothing about it, only packages it.
 */
public final class ResponseFormatShaper {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ResponseFormatShaper() {
    }

    /**
     * Produces the {@code {"type":"json_schema","json_schema":{...}}} envelope for a
     * JSON Schema document.
     *
     * @param name the schema name the envelope reports (typically the message full name)
     * @param jsonSchema the JSON Schema document as text (e.g. from
     *     {@link ai.pipestream.proto.jsonschema.ProtoJsonSchemaGenerator#generateJson})
     * @param strict whether the backend should reject output that does not validate
     */
    public static String jsonSchemaEnvelope(String name, String jsonSchema, boolean strict) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(jsonSchema, "jsonSchema");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        JsonNode schema;
        try {
            schema = MAPPER.readTree(jsonSchema);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "jsonSchema is not valid JSON: " + e.getOriginalMessage(), e);
        }
        ObjectNode inner = MAPPER.createObjectNode();
        inner.put("name", name);
        inner.put("strict", strict);
        inner.set("schema", schema);
        ObjectNode root = MAPPER.createObjectNode();
        root.put("type", "json_schema");
        root.set("json_schema", inner);
        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize the response-format envelope", e);
        }
    }
}
