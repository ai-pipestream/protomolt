package ai.pipestream.proto.validate.conformance;

import build.buf.validate.FieldPath;
import build.buf.validate.FieldPathElement;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;

/**
 * Parses a dotted, subscripted path string into a structured {@link FieldPath}, mirroring the
 * {@code internal/fieldpath} algorithm from protovalidate v1.2.2.
 *
 * <p>The conformance runner compares violation {@code field} and {@code rule} paths with
 * {@code proto.Equal}. On the expected side it stores paths as human-readable dotted strings and
 * expands them with the very same {@code Unmarshal} routine. Producing our structured paths through
 * a faithful port of that routine means our output matches the expected output element-for-element
 * whenever the underlying logical path agrees — so any mismatch reflects a genuine semantic gap,
 * not a formatting difference.
 *
 * <p>String indexing is by {@code char} rather than by byte; this is exact for the ASCII paths and
 * map keys the suite uses, and only the handling of non-ASCII map keys would differ from the Go
 * byte-oriented scanner.
 */
final class FieldPaths {

    private FieldPaths() {
    }

    /**
     * Expands {@code path} against {@code message} into a structured {@link FieldPath}.
     *
     * @throws IllegalArgumentException if a path element cannot be resolved against the descriptor
     */
    static FieldPath unmarshal(Descriptor message, String path) {
        return unmarshal(message, path, name -> null);
    }

    /**
     * As {@link #unmarshal(Descriptor, String)}, resolving {@code [full.extension.name]} elements
     * (predefined-rule extensions in rule paths) through {@code extensions}. An extension element
     * mirrors the Go runner's shape: the extension's field number and type, with the bracketed
     * full name as the element name.
     */
    static FieldPath unmarshal(
            Descriptor message, String path,
            java.util.function.Function<String, FieldDescriptor> extensions) {
        FieldPath.Builder result = FieldPath.newBuilder();
        String rest = path;
        boolean atEnd = false;
        int guard = 0;
        while (!atEnd) {
            if (++guard > 10_000) {
                throw new IllegalArgumentException("field path too deep: " + path);
            }
            Parsed p = parsePathElement(rest);
            if (p.name.isEmpty()) {
                throw new IllegalArgumentException("empty field name in path: " + path);
            }
            if (p.isExt) {
                FieldDescriptor ext = extensions.apply(p.name);
                if (ext == null) {
                    throw new IllegalArgumentException("unresolved extension in path: " + p.name);
                }
                result.addElements(FieldPathElement.newBuilder()
                        .setFieldNumber(ext.getNumber())
                        .setFieldName("[" + ext.getFullName() + "]")
                        .setFieldType(ext.getType().toProto())
                        .build());
                Descriptor extType = messageOf(ext);
                if (extType != null) {
                    message = extType;
                }
                rest = p.rest;
                atEnd = p.atEnd;
                continue;
            }

            FieldDescriptor fd = message.findFieldByName(p.name);
            if (fd == null) {
                OneofDescriptor oneof = findOneof(message, p.name);
                if (oneof != null) {
                    result.addElements(FieldPathElement.newBuilder().setFieldName(oneof.getName()).build());
                    rest = p.rest;
                    atEnd = p.atEnd;
                    continue;
                }
                throw new IllegalArgumentException(
                        "field " + p.name + " not found in " + message.getFullName());
            }

            FieldPathElement.Builder element = FieldPathElement.newBuilder()
                    .setFieldNumber(fd.getNumber())
                    .setFieldName(fd.getName())
                    .setFieldType(fd.getType().toProto());

            FieldDescriptor descend = fd;
            if (!p.subscript.isEmpty()) {
                descend = parseSubscript(fd, p.subscript, p.name, element);
                result.addElements(element.build());
            } else if (isList(fd) || fd.isMapField()) {
                if (!p.atEnd) {
                    throw new IllegalArgumentException("missing subscript on field " + p.name);
                }
                result.addElements(element.build());
                break;
            } else {
                result.addElements(element.build());
            }

            Descriptor next = messageOf(descend);
            if (next != null) {
                message = next;
            }
            rest = p.rest;
            atEnd = p.atEnd;
        }
        return result.build();
    }

    private static FieldDescriptor parseSubscript(
            FieldDescriptor fd, String subscript, String name, FieldPathElement.Builder element) {
        if (isList(fd)) {
            element.setIndex(Integer.parseInt(subscript));
            return fd;
        }
        if (fd.isMapField()) {
            FieldDescriptor keyField = fd.getMessageType().findFieldByNumber(1);
            FieldDescriptor valueField = fd.getMessageType().findFieldByNumber(2);
            parseMapKey(keyField, subscript, element);
            element.setKeyType(keyField.getType().toProto());
            element.setValueType(valueField.getType().toProto());
            return valueField;
        }
        throw new IllegalArgumentException("unexpected subscript on field " + name);
    }

    private static void parseMapKey(
            FieldDescriptor keyField, String subscript, FieldPathElement.Builder element) {
        switch (keyField.getType()) {
            case BOOL -> element.setBoolKey(parseGoBool(subscript));
            case INT32, INT64, SINT32, SINT64, SFIXED32, SFIXED64 ->
                    element.setIntKey(Long.parseLong(subscript));
            case UINT32, UINT64, FIXED32, FIXED64 ->
                    element.setUintKey(Long.parseUnsignedLong(subscript));
            case STRING -> element.setStringKey(unquote(subscript));
            default -> throw new IllegalArgumentException(
                    "unsupported map key type: " + keyField.getType());
        }
    }

    private static boolean isList(FieldDescriptor fd) {
        return fd.isRepeated() && !fd.isMapField();
    }

    private static Descriptor messageOf(FieldDescriptor fd) {
        return fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? fd.getMessageType() : null;
    }

    private static OneofDescriptor findOneof(Descriptor message, String name) {
        for (OneofDescriptor oneof : message.getOneofs()) {
            if (oneof.getName().equals(name)) {
                return oneof;
            }
        }
        return null;
    }

    /** Accepts the boolean spellings Go's {@code strconv.ParseBool} accepts. */
    private static boolean parseGoBool(String s) {
        return switch (s) {
            case "1", "t", "T", "TRUE", "true", "True" -> true;
            case "0", "f", "F", "FALSE", "false", "False" -> false;
            default -> throw new IllegalArgumentException("invalid bool map key: " + s);
        };
    }

    // ---- string scanner (port of parsePathElement / quotedPrefix / unquote) ----

    private record Parsed(String name, String subscript, String rest, boolean atEnd, boolean isExt) {
    }

    private static Parsed parsePathElement(String path) {
        String name = "";
        String subscript = "";
        boolean isExt = false;

        if (!path.isEmpty() && path.charAt(0) == '[') {
            int i = path.indexOf(']');
            if (i >= 0) {
                isExt = true;
                name = path.substring(1, i);
                path = path.substring(i + 1);
            }
        }
        if (!isExt) {
            int i = indexOfAny(path, ".[");
            if (i >= 0) {
                name = path.substring(0, i);
                path = path.substring(i);
            } else {
                name = path;
                path = "";
            }
        }
        if (path.isEmpty()) {
            return new Parsed(name, "", path, true, isExt);
        }
        if (path.charAt(0) == '.') {
            return new Parsed(name, "", path.substring(1), false, isExt);
        }
        if (path.length() == 1 || path.charAt(1) == '.') {
            name = name + path.substring(0, 1);
            return new Parsed(name, "", path.substring(1), true, isExt);
        }
        switch (path.charAt(1)) {
            case ']' -> {
                name = name + path.substring(0, 2);
                path = path.substring(2);
            }
            case '`', '"', '\'' -> {
                String quoted = quotedPrefix(path.substring(1));
                if (quoted != null) {
                    subscript = quoted;
                    path = path.substring(subscript.length() + 2);
                }
            }
            default -> {
                int i = path.indexOf(']');
                if (i >= 0) {
                    subscript = path.substring(1, i);
                    path = path.substring(i + 1);
                } else {
                    return new Parsed(name + path, "", "", true, isExt);
                }
            }
        }
        if (path.isEmpty()) {
            return new Parsed(name, subscript, path, true, isExt);
        }
        if (path.charAt(0) == '.') {
            return new Parsed(name, subscript, path.substring(1), false, isExt);
        }
        return new Parsed(name, subscript, path, false, isExt);
    }

    private static int indexOfAny(String s, String chars) {
        for (int i = 0; i < s.length(); i++) {
            if (chars.indexOf(s.charAt(i)) >= 0) {
                return i;
            }
        }
        return -1;
    }

    /** Longest prefix of {@code s} that is a valid quoted string literal (quotes included). */
    private static String quotedPrefix(String s) {
        char quote = s.charAt(0);
        if (quote == '`') {
            int end = s.indexOf('`', 1);
            return end < 0 ? null : s.substring(0, end + 1);
        }
        int i = 1;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\') {
                i += 2;
            } else if (c == quote) {
                return s.substring(0, i + 1);
            } else {
                i++;
            }
        }
        return null;
    }

    /** Decodes a quoted string literal (the value part of a string map-key subscript). */
    private static String unquote(String s) {
        if (s.length() < 2) {
            return s;
        }
        char quote = s.charAt(0);
        String body = s.substring(1, s.length() - 1);
        if (quote == '`') {
            return body;
        }
        StringBuilder out = new StringBuilder(body.length());
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c != '\\' || i + 1 >= body.length()) {
                out.append(c);
                continue;
            }
            char n = body.charAt(++i);
            switch (n) {
                case 'n' -> out.append('\n');
                case 't' -> out.append('\t');
                case 'r' -> out.append('\r');
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'a' -> out.append('\007');
                case 'v' -> out.append('\013');
                case '0' -> out.append('\0');
                case 'u' -> {
                    if (i + 5 <= body.length()) {
                        out.append((char) Integer.parseInt(body.substring(i + 1, i + 5), 16));
                        i += 4;
                    } else {
                        out.append(n);
                    }
                }
                default -> out.append(n);
            }
        }
        return out.toString();
    }
}
