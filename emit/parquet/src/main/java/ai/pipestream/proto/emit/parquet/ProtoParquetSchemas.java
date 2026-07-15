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

/**
 * Protobuf descriptor to Parquet schema, straight off the descriptor — no generated classes.
 *
 * <p>Mapping: plain singular scalars are {@code required} (proto3 defaults are values, not
 * absence); {@code optional}-keyword scalars and singular messages are {@code optional};
 * repeated fields are {@code repeated}; maps are repeated {@code (key, value)} groups;
 * enums are strings carrying the enum annotation; unsigned 32-bit values widen to
 * {@code int64} so no value ever changes sign. Recursive message types cannot exist in a
 * columnar schema and are rejected with the cycle named.</p>
 */
final class ProtoParquetSchemas {

    private ProtoParquetSchemas() {
    }

    static MessageType schema(Descriptor descriptor) {
        Deque<String> ancestry = new ArrayDeque<>();
        return new MessageType(descriptor.getFullName(), fields(descriptor, ancestry));
    }

    private static List<Type> fields(Descriptor descriptor, Deque<String> ancestry) {
        if (ancestry.contains(descriptor.getFullName())) {
            throw new IllegalArgumentException("Recursive message type cannot become a "
                    + "columnar schema: " + String.join(" -> ", ancestry.reversed())
                    + " -> " + descriptor.getFullName());
        }
        ancestry.push(descriptor.getFullName());
        List<Type> fields = new ArrayList<>(descriptor.getFields().size());
        for (FieldDescriptor field : descriptor.getFields()) {
            fields.add(convert(field, ancestry));
        }
        ancestry.pop();
        return fields;
    }

    private static Type convert(FieldDescriptor field, Deque<String> ancestry) {
        if (field.isMapField()) {
            FieldDescriptor key = field.getMessageType().findFieldByName("key");
            FieldDescriptor value = field.getMessageType().findFieldByName("value");
            return new GroupType(Type.Repetition.REPEATED, field.getName(),
                    primitive(key, Type.Repetition.REQUIRED),
                    value.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                            ? new GroupType(Type.Repetition.OPTIONAL, "value",
                                    fields(value.getMessageType(), ancestry))
                            : primitive(value, Type.Repetition.REQUIRED));
        }
        Type.Repetition repetition = field.isRepeated()
                ? Type.Repetition.REPEATED
                : optional(field) ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return new GroupType(repetition, field.getName(),
                    fields(field.getMessageType(), ancestry));
        }
        return primitive(field, repetition);
    }

    /** Message fields track presence; so do scalars declared with the optional keyword. */
    private static boolean optional(FieldDescriptor field) {
        return field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                || field.toProto().getProto3Optional();
    }

    private static PrimitiveType primitive(FieldDescriptor field, Type.Repetition repetition) {
        String name = field.getName();
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
