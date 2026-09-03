package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a request built on a contract compiled from source.
 *
 * <p>A verb whose messages have generated stubs reads them through accessors. One whose
 * contract is compiled at load has no accessors to call, so it reads by field name, and every
 * name is resolved against the request descriptor: a name the message does not declare fails
 * where the verb asked for it rather than quietly reading as absent.
 *
 * <p>Names may be given either as declared or in their JSON spelling.
 */
public final class Fields {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Fields() {
    }

    /** A string field, empty when unset. */
    public static String string(Message message, String field) {
        return (String) message.getField(field(message, field));
    }

    /** An int32 field, zero when unset. */
    public static int integer(Message message, String field) {
        return ((Number) message.getField(field(message, field))).intValue();
    }

    /** An int64 field, zero when unset. */
    public static long number(Message message, String field) {
        return ((Number) message.getField(field(message, field))).longValue();
    }

    /** A double field, zero when unset. */
    public static double decimal(Message message, String field) {
        return ((Number) message.getField(field(message, field))).doubleValue();
    }

    /** A bool field, false when unset. */
    public static boolean flag(Message message, String field) {
        return (Boolean) message.getField(field(message, field));
    }

    /** An enum field as its declared value name. */
    public static String enumName(Message message, String field) {
        return message.getField(field(message, field)).toString();
    }

    /**
     * Whether a field carrying presence is set.
     *
     * <p>Only meaningful for a message field or one declared {@code optional}: a plain scalar
     * has no presence, and asking is the mistake that reads an explicit zero as absent.
     */
    public static boolean has(Message message, String field) {
        return message.hasField(field(message, field));
    }

    /** A message field, its default instance when unset. */
    public static Message message(Message message, String field) {
        return (Message) message.getField(field(message, field));
    }

    /** A repeated field's elements. */
    @SuppressWarnings("unchecked")
    public static <T> List<T> list(Message message, String field) {
        return (List<T>) message.getField(field(message, field));
    }

    /** A repeated string field. */
    public static List<String> strings(Message message, String field) {
        return list(message, field);
    }

    /** A {@code map<string, string>} field, in declaration order. */
    public static Map<String, String> map(Message message, String field) {
        Map<String, String> entries = new LinkedHashMap<>();
        FieldDescriptor descriptor = field(message, field);
        Descriptor entry = descriptor.getMessageType();
        FieldDescriptor key = entry.findFieldByName("key");
        FieldDescriptor value = entry.findFieldByName("value");
        for (Object element : (List<?>) message.getField(descriptor)) {
            Message pair = (Message) element;
            entries.put((String) pair.getField(key), String.valueOf(pair.getField(value)));
        }
        return entries;
    }

    /**
     * A structure field as JSON, which is how a verb reads a document whose shape its own
     * contract does not describe.
     */
    public static ObjectNode json(Message message, String field) {
        Message struct = message(message, field);
        try {
            return (ObjectNode) MAPPER.readTree(JsonFormat.printer().print(struct));
        } catch (Exception e) {
            throw new IllegalStateException(
                    field + " does not render as JSON: " + e.getMessage(), e);
        }
    }

    private static FieldDescriptor field(Message message, String name) {
        Descriptor type = message.getDescriptorForType();
        for (FieldDescriptor field : type.getFields()) {
            if (field.getName().equals(name) || field.getJsonName().equals(name)) {
                return field;
            }
        }
        throw new IllegalArgumentException(
                type.getFullName() + " declares no field named '" + name + "'");
    }
}
