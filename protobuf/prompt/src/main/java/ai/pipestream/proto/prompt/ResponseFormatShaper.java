package ai.pipestream.proto.prompt;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Wraps a generated JSON Schema document into the structured-output envelope LLM
 * serving stacks expect (OpenAI / vLLM {@code response_format} shape). The schema is
 * opaque decoder configuration — this class changes nothing about it, only packages it.
 */
public final class ResponseFormatShaper {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern PROVIDER_SAFE_NAME = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private ResponseFormatShaper() {
    }

    /**
     * Produces the {@code {"type":"json_schema","json_schema":{...}}} envelope for a
     * JSON Schema document.
     *
     * @param name the provider-safe schema name the envelope reports
     * @param jsonSchema the JSON Schema document as text (e.g. from
     *     {@link ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator#generateJson})
     * @param strict whether the backend should reject output that does not validate
     */
    public static String jsonSchemaEnvelope(String name, String jsonSchema, boolean strict) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(jsonSchema, "jsonSchema");
        if (!PROVIDER_SAFE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "name must be provider-safe: 1-64 ASCII letters, digits, '_' or '-'");
        }
        JsonNode schema;
        try (JsonParser parser = MAPPER.createParser(jsonSchema)) {
            schema = MAPPER.readTree(parser);
            if (schema == null || !schema.isObject() || parser.nextToken() != null) {
                throw new IllegalArgumentException(
                        "jsonSchema must be a single JSON object");
            }
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "jsonSchema is not valid JSON: " + e.getOriginalMessage(), e);
        } catch (IOException e) {
            throw new IllegalArgumentException("jsonSchema could not be read", e);
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
