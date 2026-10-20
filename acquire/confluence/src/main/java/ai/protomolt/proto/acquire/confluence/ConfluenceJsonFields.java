package ai.protomolt.proto.acquire.confluence;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Null-tolerant string field reads over the Confluence REST JSON, shared by
 * {@link ConfluenceMapper} and its body and link helpers. A field that is
 * missing, null, or not a JSON string reads as the empty string, so the
 * matching proto field stays unset.
 */
final class ConfluenceJsonFields {

    private ConfluenceJsonFields() {
    }

    /** True when {@code field} is a JSON string with at least one character. */
    static boolean hasText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() && !value.asText().isEmpty();
    }

    /** The string value of {@code field}, or the empty string when it is not a JSON string. */
    static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.asText() : "";
    }
}
