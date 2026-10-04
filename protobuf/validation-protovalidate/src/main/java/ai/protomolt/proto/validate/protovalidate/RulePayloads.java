package ai.protomolt.proto.validate.protovalidate;

import ai.protomolt.proto.validate.RuleCompilationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.ArrayDeque;
import java.util.List;

/** Rejects validation instructions that the installed dialect cannot interpret. */
final class RulePayloads {
    private RulePayloads() {}

    static void requireSupported(Message root, PredefinedIndex predefined) {
        ArrayDeque<Message> pending = new ArrayDeque<>();
        pending.add(root);
        while (!pending.isEmpty()) {
            Message rules = pending.removeFirst();
            var type = rules.getDescriptorForType();
            var extensions = predefined.forType(type.getFullName());
            for (int number : rules.getUnknownFields().asMap().keySet()) {
                var known = type.findFieldByNumber(number);
                if (known != null || !extensions.containsKey(number)) {
                    String field = known == null ? Integer.toString(number) : known.getName();
                    throw new RuleCompilationException("unsupported or malformed rule " + type.getFullName() + "." + field);
                }
            }
            for (var entry : rules.getAllFields().entrySet()) {
                FieldDescriptor field = entry.getKey();
                if (field.isExtension()) {
                    if (!extensions.containsKey(field.getNumber())) {
                        throw new RuleCompilationException("unsupported rule extension " + field.getFullName());
                    }
                    // A custom rule's value is data, not another built-in rule payload.
                    continue;
                }
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
                if (field.isRepeated()) {
                    for (Object value : (List<?>) entry.getValue()) pending.addLast((Message) value);
                } else {
                    pending.addLast((Message) entry.getValue());
                }
            }
        }
    }
}
