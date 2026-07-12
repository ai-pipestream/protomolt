package ai.pipestream.proto.jsonschema;

import java.util.List;
import java.util.Map;

/**
 * Minimal JSON pretty-printer for the schema maps produced by
 * {@link ProtoJsonSchemaGenerator} — maps, lists, strings, numbers, booleans, null.
 * Kept internal so the module needs no JSON library dependency.
 */
final class JsonWriter {

    private JsonWriter() {
    }

    static String toJson(Object value) {
        StringBuilder sb = new StringBuilder(256);
        write(value, sb, 0);
        return sb.toString();
    }

    private static void write(Object value, StringBuilder sb, int indent) {
        switch (value) {
            case null -> sb.append("null");
            case String s -> writeString(s, sb);
            case Boolean b -> sb.append(b);
            case Double d -> writeDouble(d, sb);
            case Float f -> writeDouble(f.doubleValue(), sb);
            case Number n -> sb.append(n);
            case Map<?, ?> map -> writeObject(map, sb, indent);
            case List<?> list -> writeArray(list, sb, indent);
            default -> throw new IllegalArgumentException(
                    "Unsupported JSON value type: " + value.getClass().getName());
        }
    }

    private static void writeObject(Map<?, ?> map, StringBuilder sb, int indent) {
        if (map.isEmpty()) {
            sb.append("{}");
            return;
        }
        sb.append("{\n");
        int i = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            pad(sb, indent + 1);
            writeString(String.valueOf(entry.getKey()), sb);
            sb.append(": ");
            write(entry.getValue(), sb, indent + 1);
            if (++i < map.size()) {
                sb.append(',');
            }
            sb.append('\n');
        }
        pad(sb, indent);
        sb.append('}');
    }

    private static void writeArray(List<?> list, StringBuilder sb, int indent) {
        if (list.isEmpty()) {
            sb.append("[]");
            return;
        }
        sb.append("[\n");
        for (int i = 0; i < list.size(); i++) {
            pad(sb, indent + 1);
            write(list.get(i), sb, indent + 1);
            if (i < list.size() - 1) {
                sb.append(',');
            }
            sb.append('\n');
        }
        pad(sb, indent);
        sb.append(']');
    }

    private static void writeDouble(double d, StringBuilder sb) {
        if (!Double.isFinite(d)) {
            throw new IllegalArgumentException("JSON numbers must be finite, got " + d);
        }
        if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            sb.append((long) d);
        } else {
            sb.append(d);
        }
    }

    private static void writeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static void pad(StringBuilder sb, int indent) {
        sb.append("  ".repeat(indent));
    }
}
