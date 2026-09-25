package ai.protomolt.proto.http.openapi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Lowers shared JSON Schema validation keywords to OpenAPI 3.0 Schema Objects. */
final class OpenApiValidation {
    private OpenApiValidation() { }

    static void merge(Map<String, Object> schema, Map<String, Object> validation) {
        Map<String, Object> overlay = convert(validation);
        if (overlay.isEmpty()) return;
        // OpenAPI 3.0 ignores siblings of a Reference Object.
        if (schema.containsKey("$ref")) {
            Object ref = schema.remove("$ref");
            append(schema, Map.of("$ref", ref));
        }
        for (var entry : overlay.entrySet()) {
            if (!schema.containsKey(entry.getKey())) {
                schema.put(entry.getKey(), entry.getValue());
            } else if (!schema.get(entry.getKey()).equals(entry.getValue())) {
                append(schema, Map.of(entry.getKey(), entry.getValue()));
            }
        }
    }

    private static Map<String, Object> convert(Map<String, Object> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            switch (key) {
                case "const" -> mergeKeyword(result, "enum", List.of(value));
                case "exclusiveMinimum", "exclusiveMaximum" -> {
                    String bound = key.equals("exclusiveMinimum") ? "minimum" : "maximum";
                    // Keep conjunctions if inclusive and exclusive bounds both exist.
                    if (source.containsKey(bound)) append(result, Map.of(bound, value, key, true));
                    else {
                        result.put(bound, value);
                        result.put(key, true);
                    }
                }
                // OAS 3.0 has no propertyNames. Retain the rule for documentation,
                // explicitly outside the client-enforced Schema Object vocabulary.
                case "propertyNames" -> result.put("x-protomolt-property-names", value);
                default -> {
                    if (key.startsWith("x-")) result.put(key, value);
                    else mergeKeyword(result, key, convertValue(value));
                }
            }
        });
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Object convertValue(Object value) {
        if (value instanceof Map<?, ?> map) return convert((Map<String, Object>) map);
        if (value instanceof List<?> list) return list.stream().map(OpenApiValidation::convertValue).toList();
        return value;
    }

    private static void mergeKeyword(Map<String, Object> schema, String key, Object value) {
        if (schema.containsKey(key)) append(schema, Map.of(key, value));
        else schema.put(key, value);
    }

    @SuppressWarnings("unchecked")
    private static void append(Map<String, Object> schema, Object rule) {
        List<Object> rules = new ArrayList<>((List<Object>) schema.getOrDefault("allOf", List.of()));
        rules.add(rule);
        schema.put("allOf", rules);
    }
}
