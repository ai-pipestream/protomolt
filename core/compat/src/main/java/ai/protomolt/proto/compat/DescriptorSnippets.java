package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;

import java.util.Locale;

/**
 * Renders the {@code before} and {@code after} declaration text a {@link SchemaChange} carries,
 * plus the name derivations the rules compare. The output is read by humans in refusal messages,
 * so it reads as the {@code .proto} the author wrote rather than as a descriptor dump.
 */
final class DescriptorSnippets {

    private DescriptorSnippets() {
    }

    /** {@code repeated string tags = 3}-style declaration snippet, map-aware. */
    static String snippet(FieldDescriptorProto field, DescriptorIndex index) {
        MessageInfo entry = DescriptorIndex.mapEntryOf(field, index);
        if (entry != null) {
            return "map<" + typeName(DescriptorIndex.keyField(entry)) + ", "
                    + typeName(DescriptorIndex.valueField(entry)) + "> "
                    + field.getName() + " = " + field.getNumber();
        }
        String label = switch (field.getLabel()) {
            case LABEL_REPEATED -> "repeated ";
            case LABEL_REQUIRED -> "required ";
            case LABEL_OPTIONAL -> field.getProto3Optional() ? "optional " : "";
        };
        return label + typeName(field) + " " + field.getName() + " = " + field.getNumber();
    }

    static String methodSnippet(MethodDescriptorProto method) {
        return "rpc " + method.getName() + "("
                + (method.getClientStreaming() ? "stream " : "") + stripDot(method.getInputType())
                + ") returns ("
                + (method.getServerStreaming() ? "stream " : "") + stripDot(method.getOutputType())
                + ")";
    }

    static String typeName(FieldDescriptorProto field) {
        if (!field.getTypeName().isEmpty()) {
            return typeFqn(field);
        }
        // TYPE_INT32 -> int32
        return field.getType().name().substring("TYPE_".length()).toLowerCase(Locale.ROOT);
    }

    static String typeFqn(FieldDescriptorProto field) {
        return stripDot(field.getTypeName());
    }

    static String stripDot(String typeName) {
        return typeName.startsWith(".") ? typeName.substring(1) : typeName;
    }

    /** The JSON payload key: explicit {@code json_name} if declared, else the derived camelCase. */
    static String effectiveJsonName(FieldDescriptorProto field) {
        if (field.hasJsonName()) {
            return field.getJsonName();
        }
        StringBuilder json = new StringBuilder(field.getName().length());
        boolean upperNext = false;
        for (char c : field.getName().toCharArray()) {
            if (c == '_') {
                upperNext = true;
            } else if (upperNext) {
                json.append(Character.toUpperCase(c));
                upperNext = false;
            } else {
                json.append(c);
            }
        }
        return json.toString();
    }
}
