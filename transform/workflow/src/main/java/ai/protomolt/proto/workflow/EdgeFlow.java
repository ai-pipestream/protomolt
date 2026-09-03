package ai.protomolt.proto.workflow;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared mechanics of a typed edge: resolving the fan-out items path against the
 * produced type, reading the items off a produced message, and building the collected
 * message from ordered branch outputs. Verifier, runner, and replay all walk the path
 * here so a malformed path fails identically on all three.
 */
final class EdgeFlow {

    private EdgeFlow() {
    }

    /**
     * Resolves {@code path} against {@code produceType}: every intermediate segment
     * must be a singular message field and the final segment a repeated message field.
     *
     * @return the repeated items field
     * @throws IllegalArgumentException naming the segment that breaks the walk
     */
    static FieldDescriptor itemsField(Descriptor produceType, String path) {
        String[] segments = path.split("\\.");
        Descriptor current = produceType;
        for (int i = 0; i < segments.length; i++) {
            FieldDescriptor field = current.findFieldByName(segments[i]);
            if (field == null) {
                throw new IllegalArgumentException("items path '" + path + "' does not "
                        + "resolve: " + current.getFullName() + " has no field '"
                        + segments[i] + "'");
            }
            boolean last = i == segments.length - 1;
            if (last) {
                if (!field.isRepeated()) {
                    throw new IllegalArgumentException("items path '" + path
                            + "' ends at a non-repeated field");
                }
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    throw new IllegalArgumentException("items path '" + path
                            + "' ends at a non-message field; branches execute on "
                            + "messages");
                }
                return field;
            }
            if (field.isRepeated()
                    || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new IllegalArgumentException("items path '" + path
                        + "' walks through '" + segments[i]
                        + "', which is not a singular message field");
            }
            current = field.getMessageType();
        }
        throw new IllegalArgumentException("items path must not be empty");
    }

    /** The item element type of the fan-out's repeated field. */
    static Descriptor itemType(Descriptor produceType, String path) {
        return itemsField(produceType, path).getMessageType();
    }

    /** Reads the items of a produced message, in field order. */
    static List<Message> items(Message produced, String path) {
        FieldDescriptor field = itemsField(produced.getDescriptorForType(), path);
        Message holder = produced;
        String[] segments = path.split("\\.");
        for (int i = 0; i < segments.length - 1; i++) {
            holder = (Message) holder.getField(
                    holder.getDescriptorForType().findFieldByName(segments[i]));
        }
        List<?> raw = (List<?>) holder.getField(field);
        List<Message> items = new ArrayList<>(raw.size());
        for (Object element : raw) {
            items.add((Message) element);
        }
        return items;
    }

    /** Builds the collected message: branch outputs in index order into one field. */
    static DynamicMessage collect(Descriptor collectType, String collectInto,
                                  List<Message> outputs) {
        FieldDescriptor field = collectType.findFieldByName(collectInto);
        if (field == null || !field.isRepeated()) {
            throw new IllegalArgumentException("collect target '" + collectInto
                    + "' is not a repeated field of " + collectType.getFullName());
        }
        DynamicMessage.Builder collected = DynamicMessage.newBuilder(collectType);
        for (Message output : outputs) {
            collected.addRepeatedField(field, output);
        }
        return collected.build();
    }
}
