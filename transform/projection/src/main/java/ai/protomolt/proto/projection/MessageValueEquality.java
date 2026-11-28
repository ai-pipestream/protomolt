package ai.protomolt.proto.projection;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Iterative, stack-safe protobuf value equality used by {@link MessageProjection}'s cycle guard.
 *
 * <p>{@link Message#equals} and {@link Message#hashCode} recurse once per message nesting level,
 * which would overflow the JVM call stack on adversarial inputs before {@code MAX_NESTING_DEPTH}
 * is reached. Map fields compare by key lookup rather than {@code MapEntry} list order so Java and
 * {@code protomolt-rs} agree on cycle detection regardless of entry iteration order.</p>
 */
final class MessageValueEquality {

    private static final int MAP_KEY_FIELD_NUMBER = 1;
    private static final int MAP_VALUE_FIELD_NUMBER = 2;

    private MessageValueEquality() {
    }

    static boolean sameValue(Message left, Message right) {
        ArrayDeque<Message> pending = new ArrayDeque<>();
        pending.push(left);
        pending.push(right);
        while (!pending.isEmpty()) {
            Message b = pending.pop();
            Message a = pending.pop();
            if (a == b) {
                continue;
            }
            if (a.getDescriptorForType() != b.getDescriptorForType()
                    || !a.getUnknownFields().equals(b.getUnknownFields())) {
                return false;
            }
            Map<FieldDescriptor, Object> aFields = a.getAllFields();
            Map<FieldDescriptor, Object> bFields = b.getAllFields();
            if (!aFields.keySet().equals(bFields.keySet())) {
                return false;
            }
            for (Map.Entry<FieldDescriptor, Object> entry : aFields.entrySet()) {
                FieldDescriptor field = entry.getKey();
                Object aValue = entry.getValue();
                Object bValue = bFields.get(field);
                if (field.isMapField()) {
                    if (!sameMapEntries(field, (List<?>) aValue, (List<?>) bValue, pending)) {
                        return false;
                    }
                } else if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    if (!aValue.equals(bValue)) {
                        return false;
                    }
                } else if (field.isRepeated()) {
                    List<?> aList = (List<?>) aValue;
                    List<?> bList = (List<?>) bValue;
                    if (aList.size() != bList.size()) {
                        return false;
                    }
                    for (int i = 0; i < aList.size(); i++) {
                        pending.push((Message) aList.get(i));
                        pending.push((Message) bList.get(i));
                    }
                } else {
                    pending.push((Message) aValue);
                    pending.push((Message) bValue);
                }
            }
        }
        return true;
    }

    private static boolean sameMapEntries(
            FieldDescriptor mapField,
            List<?> leftEntries,
            List<?> rightEntries,
            ArrayDeque<Message> pending) {
        FieldDescriptor keyField = mapField.getMessageType().findFieldByNumber(MAP_KEY_FIELD_NUMBER);
        FieldDescriptor valueField = mapField.getMessageType().findFieldByNumber(MAP_VALUE_FIELD_NUMBER);
        Map<Object, Object> leftByKey = indexEntries(leftEntries, keyField, valueField);
        Map<Object, Object> rightByKey = indexEntries(rightEntries, keyField, valueField);
        if (!leftByKey.keySet().equals(rightByKey.keySet())) {
            return false;
        }
        boolean messageValues = valueField.getJavaType() == FieldDescriptor.JavaType.MESSAGE;
        for (Map.Entry<Object, Object> entry : leftByKey.entrySet()) {
            Object lv = entry.getValue();
            Object rv = rightByKey.get(entry.getKey());
            if (messageValues) {
                pending.push((Message) lv);
                pending.push((Message) rv);
            } else if (!Objects.equals(lv, rv)) {
                return false;
            }
        }
        return true;
    }

    private static Map<Object, Object> indexEntries(
            List<?> entries, FieldDescriptor keyField, FieldDescriptor valueField) {
        Map<Object, Object> byKey = HashMap.newHashMap(entries.size());
        for (Object item : entries) {
            Message entry = (Message) item;
            byKey.put(entry.getField(keyField), entry.getField(valueField));
        }
        return byKey;
    }
}
