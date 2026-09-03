package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.Map;
import java.util.Set;

/**
 * Where the JSON parser is more permissive than proto3 JSON, and than the contract.
 *
 * <p>Protobuf's parser converts between JSON kinds rather than refusing them: {@code 42}
 * reaches a string field as {@code "42"}, and {@code "50"} reaches an int32 field as
 * {@code 50}. Proto3 JSON says otherwise — a string field takes a JSON string, a 32-bit
 * integer takes a JSON number — and a caller who wrote the wrong kind has written a request
 * they did not mean. This walks the envelope against the message and refuses those, naming
 * the member by pointer.
 *
 * <p>The exceptions are the ones the format itself makes: a 64-bit integer is canonically a
 * string because JSON numbers cannot carry it exactly, an enum is a string or a number, and
 * the non-finite floats are spelled as words.
 */
final class EnvelopeTypes {

    /**
     * Messages that carry arbitrary JSON by design. Their contents are not described by
     * fields, so nothing below them can be checked against one.
     */
    private static final Set<String> FREE_FORM = Set.of(
            "google.protobuf.Struct",
            "google.protobuf.Value",
            "google.protobuf.ListValue",
            "google.protobuf.Any");

    /** The three floating-point values proto3 JSON spells as words. */
    private static final Set<String> NON_FINITE = Set.of("NaN", "Infinity", "-Infinity");

    private EnvelopeTypes() {
    }

    /** Refuses a value whose JSON kind the message's field cannot take. */
    static void check(JsonNode node, Descriptor descriptor, String pointer)
            throws ActionException {
        if (!node.isObject() || FREE_FORM.contains(descriptor.getFullName())) {
            return;
        }
        for (Map.Entry<String, JsonNode> member : node.properties()) {
            FieldDescriptor field = field(descriptor, member.getKey());
            if (field == null) {
                // Unknown members are the parser's to refuse; it names them well already.
                continue;
            }
            checkField(member.getValue(), field, pointer + "/" + member.getKey());
        }
    }

    private static void checkField(JsonNode value, FieldDescriptor field, String pointer)
            throws ActionException {
        if (value.isNull()) {
            return;
        }
        if (field.isMapField()) {
            FieldDescriptor entryValue = field.getMessageType().findFieldByName("value");
            for (Map.Entry<String, JsonNode> entry : value.properties()) {
                checkField(entry.getValue(), entryValue, pointer + "/" + entry.getKey());
            }
            return;
        }
        if (field.isRepeated() && value.isArray()) {
            for (int i = 0; i < value.size(); i++) {
                checkSingular(value.get(i), field, pointer + "/" + i);
            }
            return;
        }
        checkSingular(value, field, pointer);
    }

    private static void checkSingular(JsonNode value, FieldDescriptor field, String pointer)
            throws ActionException {
        if (value.isNull()) {
            return;
        }
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            check(value, field.getMessageType(), pointer);
            return;
        }
        String required = switch (field.getJavaType()) {
            case STRING -> value.isTextual() ? null : "a string";
            case BOOLEAN -> value.isBoolean() ? null : "a boolean";
            case INT -> value.isIntegralNumber() ? null : "an integer";
            // A 64-bit integer is canonically a JSON string; a number is accepted too.
            case LONG -> value.isIntegralNumber() || value.isTextual() ? null : "an integer";
            case FLOAT, DOUBLE -> value.isNumber() || NON_FINITE.contains(value.asText())
                    ? null : "a number";
            // An enum is a value name or its number; bytes are base64 in a string, which the
            // parser already refuses by kind.
            default -> null;
        };
        if (required != null) {
            throw Inputs.invalidInput(
                    "Field '" + field.getJsonName() + "' must be " + required, pointer);
        }
    }

    /**
     * The field a member names. Proto3 JSON accepts either spelling, so a member is matched
     * against the JSON name first and the declared name second, exactly as the parser does.
     */
    private static FieldDescriptor field(Descriptor descriptor, String member) {
        for (FieldDescriptor field : descriptor.getFields()) {
            if (field.getJsonName().equals(member)) {
                return field;
            }
        }
        return descriptor.findFieldByName(member);
    }
}
