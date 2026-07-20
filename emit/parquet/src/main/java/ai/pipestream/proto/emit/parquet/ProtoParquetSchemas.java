package ai.pipestream.proto.emit.parquet;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;

/**
 * Protobuf descriptor to Parquet schema, straight off the descriptor — no generated classes,
 * and spec-compliant shapes so every external reader (Iceberg, Spark, Trino, DuckDB) sees
 * exactly what a native writer would produce.
 *
 * <p>Mapping: plain singular scalars are {@code required} (proto3 defaults are values, not
 * absence); scalars that track presence ({@code optional}-keyword, oneof members, proto2
 * declarations) and singular messages are {@code optional};
 * repeated fields are three-level {@code LIST} groups; maps are annotated {@code MAP}
 * groups; enums are strings carrying the enum annotation; unsigned 32-bit values widen to
 * {@code int64} so no value ever changes sign. Two well-known types get lake-native
 * treatment: {@code google.protobuf.Timestamp} becomes a microsecond UTC timestamp column,
 * and the JSON family ({@code Struct}, {@code Value}, {@code ListValue}) becomes a JSON
 * string column — which also keeps their recursive definitions out of the columnar schema.
 * Any other recursive message type cannot exist in a columnar schema and is rejected with
 * the cycle named.</p>
 */
public final class ProtoParquetSchemas {

    static final String TIMESTAMP = "google.protobuf.Timestamp";
    static final Set<String> JSON_TYPES = Set.of(
            "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue");

    /**
     * Supplies a column id for a dotted logical path ({@code origin.host},
     * {@code tags.element}, {@code attrs.key}); {@code null} leaves the node id-less.
     * This is how table formats (Iceberg) get their field ids stamped into the file, so
     * readers resolve columns natively instead of through name-mapping fallbacks.
     */
    public interface FieldIdResolver {
        Integer idFor(String path);

        FieldIdResolver NONE = path -> null;
    }

    private ProtoParquetSchemas() {
    }

    static MessageType schema(Descriptor descriptor) {
        return schema(descriptor, FieldIdResolver.NONE);
    }

    static MessageType schema(Descriptor descriptor, FieldIdResolver ids) {
        Deque<String> ancestry = new ArrayDeque<>();
        return new MessageType(descriptor.getFullName(), fields(descriptor, "", ids, ancestry));
    }

    /**
     * The schema keeping only the named top-level columns (empty {@code projection} keeps all).
     * Nested structure under a kept column is unchanged; only the top level is filtered.
     */
    static MessageType schema(Descriptor descriptor, FieldIdResolver ids, Set<String> projection) {
        MessageType full = schema(descriptor, ids);
        if (projection == null || projection.isEmpty()) {
            return full;
        }
        List<Type> kept = full.getFields().stream()
                .filter(type -> projection.contains(type.getName()))
                .toList();
        if (kept.isEmpty()) {
            throw new IllegalArgumentException("Projection selected no columns of "
                    + descriptor.getFullName() + "; requested " + projection);
        }
        return new MessageType(descriptor.getFullName(), kept);
    }

    private static List<Type> fields(Descriptor descriptor, String prefix, FieldIdResolver ids,
                                     Deque<String> ancestry) {
        if (ancestry.contains(descriptor.getFullName())) {
            throw new IllegalArgumentException("Recursive message type cannot become a "
                    + "columnar schema: " + String.join(" -> ", ancestry.reversed())
                    + " -> " + descriptor.getFullName());
        }
        ancestry.push(descriptor.getFullName());
        List<Type> fields = new ArrayList<>(descriptor.getFields().size());
        for (FieldDescriptor field : descriptor.getFields()) {
            fields.add(convert(field, prefix, ids, ancestry));
        }
        ancestry.pop();
        return fields;
    }

    private static Type convert(FieldDescriptor field, String prefix, FieldIdResolver ids,
                                Deque<String> ancestry) {
        String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
        if (field.isMapField()) {
            FieldDescriptor key = field.getMessageType().findFieldByName("key");
            FieldDescriptor value = field.getMessageType().findFieldByName("value");
            return withId(Types.buildGroup(Type.Repetition.OPTIONAL)
                    .as(LogicalTypeAnnotation.mapType())
                    .addField(new GroupType(Type.Repetition.REPEATED, "key_value",
                            element(key, "key", path + ".key", Type.Repetition.REQUIRED,
                                    ids, ancestry),
                            element(value, "value", path + ".value", valueRepetition(value),
                                    ids, ancestry)))
                    .named(field.getName()), ids.idFor(path));
        }
        if (field.isRepeated()) {
            return withId(Types.buildGroup(Type.Repetition.OPTIONAL)
                    .as(LogicalTypeAnnotation.listType())
                    .addField(new GroupType(Type.Repetition.REPEATED, "list",
                            element(field, "element", path + ".element",
                                    Type.Repetition.REQUIRED, ids, ancestry)))
                    .named(field.getName()), ids.idFor(path));
        }
        Type.Repetition repetition = optional(field)
                ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        return element(field, field.getName(), path, repetition, ids, ancestry);
    }

    private static Type withId(Type type, Integer id) {
        return id == null ? type : type.withId(id);
    }

    /** One value position — a leaf column or a nested struct group. */
    private static Type element(FieldDescriptor field, String name, String path,
                                Type.Repetition repetition, FieldIdResolver ids,
                                Deque<String> ancestry) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && !specialMessage(field)) {
            return withId(new GroupType(repetition, name,
                    fields(field.getMessageType(), path, ids, ancestry)), ids.idFor(path));
        }
        return (PrimitiveType) withId(primitive(field, name, repetition), ids.idFor(path));
    }

    private static boolean specialMessage(FieldDescriptor field) {
        String fullName = field.getMessageType().getFullName();
        return TIMESTAMP.equals(fullName) || JSON_TYPES.contains(fullName);
    }

    /** Map values that are messages track presence; scalar values are always present. */
    private static Type.Repetition valueRepetition(FieldDescriptor value) {
        return value.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && !specialMessage(value)
                ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
    }

    /**
     * Presence as the descriptor defines it, which covers more than the proto3 {@code optional}
     * keyword: oneof members and proto2 {@code optional} fields track presence too, and encoding
     * them as {@code required} would write their default in place of "not set".
     */
    private static boolean optional(FieldDescriptor field) {
        return field.hasPresence();
    }

    private static PrimitiveType primitive(FieldDescriptor field, String name,
                                           Type.Repetition repetition) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            String fullName = field.getMessageType().getFullName();
            if (TIMESTAMP.equals(fullName)) {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                        .as(LogicalTypeAnnotation.timestampType(true,
                                LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named(name);
            }
            // Struct / Value / ListValue: dynamic JSON content as a JSON string column.
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                    .as(LogicalTypeAnnotation.jsonType()).named(name);
        }
        return switch (field.getType()) {
            case INT32, SINT32, SFIXED32 -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.INT32, repetition).named(name);
            case UINT32, FIXED32 -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.INT64, repetition).named(name);
            case INT64, SINT64, SFIXED64 -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.INT64, repetition).named(name);
            case UINT64, FIXED64 -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.INT64, repetition)
                    .as(LogicalTypeAnnotation.intType(64, false)).named(name);
            case FLOAT -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(name);
            case DOUBLE -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(name);
            case BOOL -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(name);
            case STRING -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                    .as(LogicalTypeAnnotation.stringType()).named(name);
            case BYTES -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(name);
            case ENUM -> Types.primitive(
                    PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                    .as(LogicalTypeAnnotation.enumType()).named(name);
            case MESSAGE, GROUP -> throw new IllegalArgumentException(
                    "Not a primitive field: " + field.getFullName());
        };
    }
}
