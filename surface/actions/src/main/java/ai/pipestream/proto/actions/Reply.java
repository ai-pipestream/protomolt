package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;

import java.util.List;
import java.util.Map;

/**
 * Builds a verb's reply against the message it declares.
 *
 * <p>A verb whose contract is compiled from source rather than bound to generated stubs has
 * no {@code Response.newBuilder()} to reach for, so it writes its answer by field name. Every
 * name is resolved against the response descriptor as it is written: a field the message does
 * not declare, or a value of a kind it cannot hold, fails where the verb wrote it rather than
 * being carried to a caller and dropped there.
 *
 * <p>Names may be given either as declared or in their JSON spelling, because a verb reads
 * its own contract in whichever form the surrounding code already uses.
 */
public final class Reply {

    private final Descriptor type;
    private final DynamicMessage.Builder builder;
    /** Set on a nested reply: where {@link #build} attaches the message it produces. */
    private final Reply parent;
    private final FieldDescriptor attachAs;

    private Reply(Descriptor type, Reply parent, FieldDescriptor attachAs) {
        this.type = type;
        this.builder = DynamicMessage.newBuilder(type);
        this.parent = parent;
        this.attachAs = attachAs;
    }

    /** A reply under {@code type}, which is the verb's {@link ProtoAction#responseType()}. */
    public static Reply of(Descriptor type) {
        return new Reply(type, null, null);
    }

    /** Sets a singular field. */
    public Reply set(String field, Object value) {
        FieldDescriptor descriptor = field(field);
        if (descriptor.isRepeated()) {
            throw new IllegalArgumentException(
                    type.getFullName() + "." + field + " is repeated; use add");
        }
        builder.setField(descriptor, coerce(descriptor, value));
        return this;
    }

    /** Appends one element to a repeated field. */
    public Reply add(String field, Object value) {
        FieldDescriptor descriptor = field(field);
        if (!descriptor.isRepeated()) {
            throw new IllegalArgumentException(
                    type.getFullName() + "." + field + " is not repeated; use set");
        }
        builder.addRepeatedField(descriptor, coerce(descriptor, value));
        return this;
    }

    /** Appends every element of {@code values} to a repeated field. */
    public Reply addAll(String field, Iterable<?> values) {
        for (Object value : values) {
            add(field, value);
        }
        return this;
    }

    /** A reply for a singular message field, set on this one when it is built. */
    public Reply nest(String field) {
        FieldDescriptor descriptor = messageField(field, false);
        return new Reply(descriptor.getMessageType(), this, descriptor);
    }

    /** A reply for one new element of a repeated message field, appended when it is built. */
    public Reply append(String field) {
        FieldDescriptor descriptor = messageField(field, true);
        return new Reply(descriptor.getMessageType(), this, descriptor);
    }

    /**
     * Copies every field set on {@code source} onto this reply, by name.
     *
     * <p>For a verb that answers with another verb's reply plus its own additions. A field
     * the target does not declare is an error rather than a silent omission, so a field
     * added to the source forces the wrapper to say what it does with it.
     */
    public Reply copyFrom(Message source) {
        for (Map.Entry<FieldDescriptor, Object> set : source.getAllFields().entrySet()) {
            FieldDescriptor from = set.getKey();
            FieldDescriptor into = field(from.getName());
            if (from.isRepeated()) {
                for (Object element : (List<?>) set.getValue()) {
                    builder.addRepeatedField(into, element);
                }
            } else {
                builder.setField(into, set.getValue());
            }
        }
        return this;
    }

    /** The finished message. A nested reply attaches itself to its parent here. */
    public Message build() {
        Message built = builder.build();
        if (parent != null) {
            if (attachAs.isRepeated()) {
                parent.builder.addRepeatedField(attachAs, built);
            } else {
                parent.builder.setField(attachAs, built);
            }
        }
        return built;
    }

    private FieldDescriptor messageField(String field, boolean repeated) {
        FieldDescriptor descriptor = field(field);
        if (descriptor.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            throw new IllegalArgumentException(
                    type.getFullName() + "." + field + " is not a message");
        }
        if (descriptor.isRepeated() != repeated) {
            throw new IllegalArgumentException(type.getFullName() + "." + field
                    + (repeated ? " is not repeated; use nest" : " is repeated; use append"));
        }
        return descriptor;
    }

    private FieldDescriptor field(String name) {
        for (FieldDescriptor field : type.getFields()) {
            if (field.getName().equals(name) || field.getJsonName().equals(name)) {
                return field;
            }
        }
        throw new IllegalArgumentException(
                type.getFullName() + " declares no field named '" + name + "'");
    }

    /** The value as the field holds it, refusing a kind the field cannot take. */
    private Object coerce(FieldDescriptor field, Object value) {
        if (value == null) {
            throw new IllegalArgumentException(
                    type.getFullName() + "." + field.getName() + " was given no value");
        }
        return switch (field.getJavaType()) {
            case STRING -> value.toString();
            case BOOLEAN -> (Boolean) value;
            case INT -> ((Number) value).intValue();
            case LONG -> ((Number) value).longValue();
            case FLOAT -> ((Number) value).floatValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case BYTE_STRING -> value instanceof ByteString bytes
                    ? bytes
                    : ByteString.copyFrom((byte[]) value);
            case ENUM -> enumValue(field, value);
            case MESSAGE -> message(field, value);
        };
    }

    private static Object enumValue(FieldDescriptor field, Object value) {
        if (value instanceof EnumValueDescriptor already) {
            return already;
        }
        if (value instanceof Number number) {
            EnumValueDescriptor found = field.getEnumType().findValueByNumber(number.intValue());
            if (found == null) {
                throw new IllegalArgumentException(field.getEnumType().getFullName()
                        + " declares no value numbered " + number);
            }
            return found;
        }
        String name = value.toString();
        EnumValueDescriptor found = field.getEnumType().findValueByName(name);
        if (found == null) {
            throw new IllegalArgumentException(
                    field.getEnumType().getFullName() + " declares no value named '" + name + "'");
        }
        return found;
    }

    /**
     * A message-typed value. The three JSON-carrying well-known types take JSON as well,
     * because that is what a field carrying a document with no protobuf contract is for.
     */
    private static Object message(FieldDescriptor field, Object value) {
        if (value instanceof Message message) {
            return message;
        }
        Message.Builder builder = switch (field.getMessageType().getFullName()) {
            case "google.protobuf.Struct" -> Struct.newBuilder();
            case "google.protobuf.Value" -> Value.newBuilder();
            case "google.protobuf.ListValue" -> ListValue.newBuilder();
            default -> throw new IllegalArgumentException(field.getFullName() + " takes a "
                    + field.getMessageType().getFullName() + ", not a "
                    + value.getClass().getName());
        };
        String json = value instanceof JsonNode node ? node.toString() : value.toString();
        try {
            JsonFormat.parser().merge(json, builder);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    field.getFullName() + " was given JSON it cannot hold: " + e.getMessage(), e);
        }
        return builder.build();
    }
}
