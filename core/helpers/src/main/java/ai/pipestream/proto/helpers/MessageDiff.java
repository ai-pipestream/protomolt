package ai.pipestream.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Field-level diff between two messages of the same type. The two sides are matched by descriptor
 * full name, so a generated message and a runtime-compiled message of the same type can be
 * compared.
 */
public final class MessageDiff {

    private MessageDiff() {}

    public record FieldChange(String path, Object left, Object right) {}

    public static List<FieldChange> diff(MessageOrBuilder left, MessageOrBuilder right) {
        Descriptor leftType = left.getDescriptorForType();
        Descriptor rightType = right.getDescriptorForType();
        if (!leftType.getFullName().equals(rightType.getFullName())) {
            throw new IllegalArgumentException("Messages must share the same descriptor, but got "
                    + leftType.getFullName() + " and " + rightType.getFullName());
        }
        List<FieldChange> changes = new ArrayList<>();
        diffRecursive(left, right, "", changes);
        return changes;
    }

    private static void diffRecursive(MessageOrBuilder left, MessageOrBuilder right, String prefix, List<FieldChange> out) {
        Descriptor rightType = right.getDescriptorForType();
        // The two sides are matched by full name, so their descriptors may be distinct instances
        // (generated versus runtime-compiled). Reflection rejects a field from the other instance,
        // so each side is read through its own FieldDescriptor.
        boolean sameDescriptorInstance = left.getDescriptorForType() == rightType;
        for (FieldDescriptor field : left.getDescriptorForType().getFields()) {
            String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            FieldDescriptor rightField = sameDescriptorInstance ? field : rightType.findFieldByNumber(field.getNumber());
            if (rightField == null) {
                throw new IllegalArgumentException("Messages must share the same descriptor, but field '"
                        + path + "' is not defined in " + rightType.getFullName());
            }
            boolean leftSet = field.isRepeated() ? left.getRepeatedFieldCount(field) > 0 : left.hasField(field);
            boolean rightSet = rightField.isRepeated()
                    ? right.getRepeatedFieldCount(rightField) > 0 : right.hasField(rightField);
            if (!leftSet && !rightSet) {
                continue;
            }
            Object lv = leftSet ? left.getField(field) : null;
            Object rv = rightSet ? right.getField(rightField) : null;
            if (field.isMapField()) {
                // Map entry lists are insertion-ordered; compare as maps so key order is irrelevant.
                if (!Objects.equals(mapEntries(lv, sameDescriptorInstance), mapEntries(rv, sameDescriptorInstance))) {
                    out.add(new FieldChange(path, lv, rv));
                }
            } else if (field.isRepeated() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                if (!equalValues(lv, rv, sameDescriptorInstance)) {
                    out.add(new FieldChange(path, lv, rv));
                }
            } else if (lv instanceof MessageOrBuilder lm && rv instanceof MessageOrBuilder rm) {
                diffRecursive(lm, rm, path, out);
            } else if (!equalValues(lv, rv, sameDescriptorInstance)) {
                out.add(new FieldChange(path, lv, rv));
            }
        }
        if (!left.getUnknownFields().equals(right.getUnknownFields())) {
            String path = prefix.isEmpty() ? "(unknown fields)" : prefix + ".(unknown fields)";
            out.add(new FieldChange(path, left.getUnknownFields(), right.getUnknownFields()));
        }
    }

    private static Map<Object, Object> mapEntries(Object entryList, boolean sameDescriptorInstance) {
        if (entryList == null) {
            return Map.of();
        }
        Map<Object, Object> entries = new HashMap<>();
        for (Object entry : (List<?>) entryList) {
            MessageOrBuilder message = (MessageOrBuilder) entry;
            FieldDescriptor keyField = message.getDescriptorForType().findFieldByNumber(1);
            FieldDescriptor valueField = message.getDescriptorForType().findFieldByNumber(2);
            entries.put(comparable(message.getField(keyField), sameDescriptorInstance),
                    comparable(message.getField(valueField), sameDescriptorInstance));
        }
        return entries;
    }

    private static boolean equalValues(Object left, Object right, boolean sameDescriptorInstance) {
        return Objects.equals(comparable(left, sameDescriptorInstance), comparable(right, sameDescriptorInstance));
    }

    /**
     * Reduces a reflection value to a form that compares by content across descriptor instances.
     * Message and enum equality is instance-sensitive in protobuf, so the same value read through
     * two builds of the same type would otherwise never compare equal.
     */
    private static Object comparable(Object value, boolean sameDescriptorInstance) {
        if (sameDescriptorInstance) {
            return value;
        }
        if (value instanceof EnumValueDescriptor enumValue) {
            return enumValue.getNumber();
        }
        if (value instanceof MessageOrBuilder message) {
            Message built = message instanceof Message.Builder builder ? builder.build() : (Message) message;
            return built.toByteString();
        }
        if (value instanceof List<?> list) {
            List<Object> reduced = new ArrayList<>(list.size());
            for (Object item : list) {
                reduced.add(comparable(item, false));
            }
            return reduced;
        }
        return value;
    }
}
