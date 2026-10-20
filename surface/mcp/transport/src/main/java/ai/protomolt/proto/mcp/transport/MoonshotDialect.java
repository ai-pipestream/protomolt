package ai.protomolt.proto.mcp.transport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Locale;
import java.util.function.Consumer;

/**
 * Package-private Moonshot / Kimi provider dialect detection and tool-schema union sanitization
 * for {@link McpServer}.
 */
final class MoonshotDialect {

    private MoonshotDialect() {
    }

    /** Determines whether the given MCP client info name represents a Moonshot/Kimi client. */
    static boolean isMoonshotClient(String clientName) {
        if (clientName == null || clientName.isBlank()) {
            return false;
        }
        String normalized = clientName.toLowerCase(Locale.ROOT);
        return normalized.contains("kimi") || normalized.contains("moonshot");
    }

    /**
     * Sanitizes a tool manifest for strict function-calling validators like Moonshot Flavored
     * JSON Schema (MFJS). Strips parent {@code type} (and numeric/string bounds) when {@code anyOf}
     * or {@code oneOf} is present, ensuring variant subschemas carry their own {@code type}.
     * The input is not modified; returns a sanitized deep copy, or {@code null} for {@code null}.
     */
    static ArrayNode sanitizeForMoonshot(ArrayNode manifest) {
        if (manifest == null) {
            return null;
        }
        ArrayNode copy = manifest.deepCopy();
        for (JsonNode tool : copy) {
            if (tool.isObject() && tool.has("inputSchema")) {
                sanitizeSchemaForMoonshot(tool.get("inputSchema"));
            }
        }
        return copy;
    }

    /**
     * Applies the Moonshot schema sanitization in place: at every object carrying an
     * {@code anyOf} or {@code oneOf} union, removes the parent {@code type} (pushing it into
     * variant subschemas that lack one when it is a single textual type) and drops the
     * {@code pattern}, {@code minimum}, {@code maximum}, {@code exclusiveMinimum}, and
     * {@code exclusiveMaximum} keywords the strict validator refuses beside a union.
     */
    static void sanitizeSchemaForMoonshot(JsonNode node) {
        if (node == null) {
            return;
        }
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            boolean hasAnyOf = obj.has("anyOf");
            boolean hasOneOf = obj.has("oneOf");
            if (hasAnyOf || hasOneOf) {
                JsonNode parentType = obj.remove("type");
                if (parentType != null && parentType.isTextual()) {
                    String typeStr = parentType.asText();
                    Consumer<JsonNode> ensureType = branch -> {
                        if (branch.isObject() && !branch.has("type")) {
                            ((ObjectNode) branch).put("type", typeStr);
                        }
                    };
                    if (hasAnyOf && obj.get("anyOf").isArray()) {
                        obj.get("anyOf").forEach(ensureType);
                    }
                    if (hasOneOf && obj.get("oneOf").isArray()) {
                        obj.get("oneOf").forEach(ensureType);
                    }
                }
                obj.remove("pattern");
                obj.remove("minimum");
                obj.remove("maximum");
                obj.remove("exclusiveMinimum");
                obj.remove("exclusiveMaximum");
            }
            obj.properties().forEach(entry -> sanitizeSchemaForMoonshot(entry.getValue()));
        } else if (node.isArray()) {
            node.forEach(MoonshotDialect::sanitizeSchemaForMoonshot);
        }
    }
}
