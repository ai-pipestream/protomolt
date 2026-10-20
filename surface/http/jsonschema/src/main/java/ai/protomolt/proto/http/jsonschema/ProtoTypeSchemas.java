package ai.protomolt.proto.http.jsonschema;

import ai.protomolt.proto.validate.model.CelConstraint;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Package-private canonical proto3 JSON type schemas and map-composition helpers for
 * {@link ProtoJsonSchemaGenerator}.
 *
 * <p>Handles scalar, enum, 64-bit integer, and well-known protobuf message encodings before
 * validation constraints are overlaid, as well as conjunction-safe schema merging and
 * {@code x-protomolt-cel} rule attachment.
 */
final class ProtoTypeSchemas {

    static final String CEL_KEYWORD = "x-protomolt-cel";

    private ProtoTypeSchemas() {
    }

    /** Schema for the field's canonical proto3 JSON encoding, before constraints. */
    static Map<String, Object> baseSchema(
            FieldDescriptor field, boolean validationOnly, Consumer<Descriptor> enqueue) {
        if (validationOnly) {
            return new LinkedHashMap<>();
        }
        return switch (field.getType()) {
            case INT32, SINT32, SFIXED32 -> schemaOf("type", "integer");
            case UINT32, FIXED32 -> schemaOf("type", "integer", "minimum", 0L);
            // Proto3 JSON encodes 64-bit ints as strings and accepts both forms.
            case INT64, SINT64, SFIXED64 -> longSchema("^-?[0-9]+$", false);
            case UINT64, FIXED64 -> longSchema("^[0-9]+$", true);
            case FLOAT, DOUBLE -> schemaOf("type", "number");
            case BOOL -> schemaOf("type", "boolean");
            case STRING -> schemaOf("type", "string");
            case BYTES -> schemaOf("type", "string", "contentEncoding", "base64");
            case ENUM -> enumBase(field.getEnumType());
            case MESSAGE, GROUP -> messageRef(field.getMessageType(), enqueue);
        };
    }

    static Map<String, Object> longSchema(String pattern, boolean unsigned) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", List.of("integer", "string"));
        schema.put("pattern", pattern);
        if (unsigned) {
            schema.put("minimum", 0L);
        }
        return schema;
    }

    /** Proto3 JSON accepts an enum as its declared name or as a bare number. */
    static Map<String, Object> enumBase(EnumDescriptor type) {
        List<String> names = type.getValues().stream()
                .map(EnumValueDescriptor::getName)
                .toList();
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("anyOf", List.of(
                schemaOf("type", "string", "enum", names),
                schemaOf("type", "integer")));
        return schema;
    }

    static Map<String, Object> messageRef(Descriptor type, Consumer<Descriptor> enqueue) {
        return switch (type.getFullName()) {
            case "google.protobuf.Timestamp" ->
                    schemaOf("type", "string", "format", "date-time");
            case "google.protobuf.Duration" ->
                    schemaOf("type", "string", "pattern", "^-?[0-9]+(\\.[0-9]{1,9})?s$");
            case "google.protobuf.Struct" -> schemaOf("type", "object");
            case "google.protobuf.Value" -> new LinkedHashMap<>();
            case "google.protobuf.ListValue" -> schemaOf("type", "array");
            case "google.protobuf.FieldMask", "google.protobuf.StringValue" ->
                    schemaOf("type", "string");
            case "google.protobuf.BytesValue" ->
                    schemaOf("type", "string", "contentEncoding", "base64");
            case "google.protobuf.BoolValue" -> schemaOf("type", "boolean");
            case "google.protobuf.Int32Value" -> schemaOf("type", "integer");
            case "google.protobuf.UInt32Value" ->
                    schemaOf("type", "integer", "minimum", 0L);
            case "google.protobuf.Int64Value" -> longSchema("^-?[0-9]+$", false);
            case "google.protobuf.UInt64Value" -> longSchema("^[0-9]+$", true);
            case "google.protobuf.FloatValue", "google.protobuf.DoubleValue" ->
                    schemaOf("type", "number");
            case "google.protobuf.Any" -> schemaOf("type", "object");
            default -> {
                enqueue.accept(type);
                yield schemaOf("$ref", "#/$defs/" + type.getFullName());
            }
        };
    }

    static void celInto(Map<String, Object> schema, List<CelConstraint> cel) {
        if (cel.isEmpty()) {
            return;
        }
        @SuppressWarnings("unchecked")
        List<Object> rules = (List<Object>) schema
                .computeIfAbsent(CEL_KEYWORD, k -> new ArrayList<>());
        for (CelConstraint rule : cel) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", rule.id());
            entry.put("expression", rule.expression());
            if (!rule.message().isBlank()) {
                entry.put("message", rule.message());
            }
            rules.add(entry);
        }
    }

    /**
     * Merges constraint keywords into a schema. A keyword the schema already uses
     * moves into {@code allOf} instead of overwriting — conjunction is always the
     * correct combination for constraints from multiple sources.
     */
    static void merge(Map<String, Object> schema, Map<String, Object> overlay) {
        for (Map.Entry<String, Object> entry : overlay.entrySet()) {
            if (schema.containsKey(entry.getKey())) {
                addAllOf(schema, schemaOf(entry.getKey(), entry.getValue()));
            } else {
                schema.put(entry.getKey(), entry.getValue());
            }
        }
    }

    static void addAllOf(Map<String, Object> schema, Object subschema) {
        @SuppressWarnings("unchecked")
        List<Object> allOf = (List<Object>) schema
                .computeIfAbsent("allOf", k -> new ArrayList<>());
        allOf.add(subschema);
    }

    static Map<String, Object> schemaOf(Object... pairs) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], pairs[i + 1]);
        }
        return map;
    }
}
