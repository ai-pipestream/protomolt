package ai.pipestream.proto.lake.iceberg;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Protobuf descriptors and Iceberg schemas, both directions.
 *
 * <p><b>To Iceberg</b>: every field optional (proto3 semantics), repeated fields as lists,
 * maps as maps, nested messages as structs, {@code google.protobuf.Timestamp} as
 * {@code timestamptz}, the JSON well-known types ({@code Struct}/{@code Value}/
 * {@code ListValue}) as string columns carrying JSON — exactly the shapes the Parquet
 * emitter writes, so a table created here reads the files appended there. Field ids are
 * fresh and unique; catalogs reassign them on table creation anyway.</p>
 *
 * <p><b>From Iceberg</b>: a table schema becomes registrable {@code .proto} source — the
 * lake's shape as a typed contract the rest of the toolkit (and the registry) can hold.</p>
 */
public final class IcebergSchemas {

    private static final String TIMESTAMP = "google.protobuf.Timestamp";
    private static final Set<String> JSON_TYPES = Set.of(
            "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue");

    private IcebergSchemas() {
    }

    // ---------------------------------------------------------------- proto -> iceberg

    public static Schema fromDescriptor(Descriptor descriptor) {
        AtomicInteger nextId = new AtomicInteger(1);
        Deque<String> ancestry = new ArrayDeque<>();
        return new Schema(structFields(descriptor, nextId, ancestry));
    }

    private static List<Types.NestedField> structFields(Descriptor descriptor,
                                                        AtomicInteger nextId,
                                                        Deque<String> ancestry) {
        if (ancestry.contains(descriptor.getFullName())) {
            throw new IllegalArgumentException("Recursive message type cannot become a "
                    + "table schema: " + String.join(" -> ", ancestry.reversed())
                    + " -> " + descriptor.getFullName());
        }
        ancestry.push(descriptor.getFullName());
        List<Types.NestedField> fields = new ArrayList<>(descriptor.getFields().size());
        for (FieldDescriptor field : descriptor.getFields()) {
            fields.add(Types.NestedField.optional(nextId.getAndIncrement(), field.getName(),
                    convert(field, nextId, ancestry)));
        }
        ancestry.pop();
        return fields;
    }

    private static Type convert(FieldDescriptor field, AtomicInteger nextId,
                                Deque<String> ancestry) {
        if (field.isMapField()) {
            FieldDescriptor key = field.getMessageType().findFieldByName("key");
            FieldDescriptor value = field.getMessageType().findFieldByName("value");
            return Types.MapType.ofOptional(nextId.getAndIncrement(), nextId.getAndIncrement(),
                    scalar(key), valueType(value, nextId, ancestry));
        }
        if (field.isRepeated()) {
            return Types.ListType.ofOptional(nextId.getAndIncrement(),
                    valueType(field, nextId, ancestry));
        }
        return valueType(field, nextId, ancestry);
    }

    private static Type valueType(FieldDescriptor field, AtomicInteger nextId,
                                  Deque<String> ancestry) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            String fullName = field.getMessageType().getFullName();
            if (TIMESTAMP.equals(fullName)) {
                return Types.TimestampType.withZone();
            }
            if (JSON_TYPES.contains(fullName)) {
                return Types.StringType.get();
            }
            return Types.StructType.of(structFields(field.getMessageType(), nextId, ancestry));
        }
        return scalar(field);
    }

    private static Type.PrimitiveType scalar(FieldDescriptor field) {
        return switch (field.getType()) {
            case INT32, SINT32, SFIXED32 -> Types.IntegerType.get();
            case UINT32, FIXED32, INT64, SINT64, SFIXED64, UINT64, FIXED64 ->
                    Types.LongType.get();
            case FLOAT -> Types.FloatType.get();
            case DOUBLE -> Types.DoubleType.get();
            case BOOL -> Types.BooleanType.get();
            case STRING, ENUM -> Types.StringType.get();
            case BYTES -> Types.BinaryType.get();
            case MESSAGE, GROUP -> throw new IllegalArgumentException(
                    "Not a scalar field: " + field.getFullName());
        };
    }

    // ---------------------------------------------------------------- iceberg -> proto

    /** The table schema as registrable proto source, one message named {@code messageName}. */
    public static String toProtoSource(Schema schema, String packageName, String messageName) {
        StringBuilder source = new StringBuilder();
        source.append("syntax = \"proto3\";\n\npackage ").append(packageName).append(";\n");
        StringBuilder body = new StringBuilder();
        boolean[] needsTimestamp = {false};
        Nested nested = new Nested(messageName);
        body.append("\nmessage ").append(messageName).append(" {\n");
        int number = 1;
        for (Types.NestedField field : schema.columns()) {
            body.append("  ").append(fieldLine(field, number++, messageName, nested,
                    needsTimestamp)).append('\n');
        }
        body.append("}\n");
        for (String message : nested.bodies()) {
            body.append(message);
        }
        if (needsTimestamp[0]) {
            source.append("\nimport \"google/protobuf/timestamp.proto\";\n");
        }
        return source.append(body).toString();
    }

    private static String fieldLine(Types.NestedField field, int number, String parentName,
                                    Nested nested, boolean[] needsTimestamp) {
        String name = sanitize(field.name());
        Type type = field.type();
        if (type.isListType()) {
            Type element = type.asListType().elementType();
            return "repeated " + typeName(element, name, parentName, nested, needsTimestamp)
                    + " " + name + " = " + number + ";";
        }
        if (type.isMapType()) {
            Types.MapType map = type.asMapType();
            String key = protoMapKey(map.keyType());
            if (key != null) {
                return "map<" + key + ", " + typeName(map.valueType(), name, parentName,
                        nested, needsTimestamp) + "> " + name + " = " + number + ";";
            }
            // Proto map keys must be integral or string; anything else becomes entries.
            String entryName = nested.reserve(upper(name) + "Entry");
            StringBuilder entry = new StringBuilder("\nmessage " + entryName + " {\n");
            entry.append("  ").append(typeName(map.keyType(), name + "_key", parentName,
                    nested, needsTimestamp)).append(" key = 1;\n");
            entry.append("  ").append(typeName(map.valueType(), name + "_value", parentName,
                    nested, needsTimestamp)).append(" value = 2;\n");
            entry.append("}\n");
            nested.define(entryName, entry.toString());
            return "repeated " + entryName + " " + name + " = " + number + ";";
        }
        return typeName(type, name, parentName, nested, needsTimestamp)
                + " " + name + " = " + number + ";";
    }

    private static String typeName(Type type, String fieldName, String parentName,
                                   Nested nested, boolean[] needsTimestamp) {
        if (type.isStructType()) {
            String base = upper(sanitize(fieldName));
            if (base.equals(parentName)) {
                base = base + "Struct";
            }
            // Reserved before the body is generated so a struct nested inside this one cannot
            // claim the same name.
            String messageName = nested.reserve(base);
            StringBuilder message = new StringBuilder("\nmessage " + messageName + " {\n");
            int number = 1;
            for (Types.NestedField field : type.asStructType().fields()) {
                message.append("  ").append(fieldLine(field, number++, messageName, nested,
                        needsTimestamp)).append('\n');
            }
            message.append("}\n");
            nested.define(messageName, message.toString());
            return messageName;
        }
        if (type.isListType() || type.isMapType()) {
            // Only reachable as a list element or map value: proto has no repeated-of-repeated
            // and no collection-valued map, so there is no shape to generate.
            throw new IllegalArgumentException("Column '" + fieldName + "' nests " + type
                    + " inside a list or map, which proto cannot express");
        }
        return switch (type.typeId()) {
            case BOOLEAN -> "bool";
            case INTEGER -> "int32";
            case LONG -> "int64";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case STRING, UUID, DECIMAL, DATE, TIME -> "string";
            case TIMESTAMP, TIMESTAMP_NANO -> {
                needsTimestamp[0] = true;
                yield "google.protobuf.Timestamp";
            }
            case BINARY, FIXED -> "bytes";
            default -> "string"; // variant, geometry, unknown: carried as text
        };
    }

    /**
     * The generated nested messages of one file. Proto has no scoping that would let two of
     * them share a name, but the names come from column names, which repeat freely across
     * different structs — so every name is claimed once and suffixed on collision.
     */
    private static final class Nested {

        private final Map<String, String> byName = new LinkedHashMap<>();
        private final Set<String> taken = new LinkedHashSet<>();

        private Nested(String rootMessage) {
            taken.add(rootMessage);
        }

        /** Claims {@code base}, or the first free {@code base2}, {@code base3}, ... */
        private String reserve(String base) {
            String name = base;
            for (int i = 2; !taken.add(name); i++) {
                name = base + i;
            }
            return name;
        }

        private void define(String name, String body) {
            byName.put(name, body);
        }

        private Collection<String> bodies() {
            return byName.values();
        }
    }

    private static String protoMapKey(Type keyType) {
        return switch (keyType.typeId()) {
            case STRING -> "string";
            case INTEGER -> "int32";
            case LONG -> "int64";
            default -> null;
        };
    }

    private static String sanitize(String name) {
        StringBuilder out = new StringBuilder(name.length());
        for (char c : name.toCharArray()) {
            out.append(Character.isLetterOrDigit(c) || c == '_' ? c : '_');
        }
        String sanitized = out.isEmpty() ? "field" : out.toString();
        return Character.isDigit(sanitized.charAt(0)) ? "f" + sanitized : sanitized;
    }

    private static String upper(String name) {
        StringBuilder out = new StringBuilder(name.length());
        boolean up = true;
        for (char c : name.toCharArray()) {
            if (c == '_') {
                up = true;
                continue;
            }
            out.append(up ? Character.toUpperCase(c) : c);
            up = false;
        }
        return out.isEmpty() ? "Field" : out.toString();
    }

    static String lower(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
