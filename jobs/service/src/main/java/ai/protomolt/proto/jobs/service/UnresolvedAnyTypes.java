package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Removes every {@code google.protobuf.Any} whose type a registry does not define from a copy
 * of a message. proto3 JSON cannot print such an Any, and it is the one printing failure the
 * run view reports instead of raising: the view prints the copy, and only if the copy prints
 * was the undefined type the whole reason the original did not.
 */
final class UnresolvedAnyTypes {

    /** Any-in-Any nesting deeper than this is left as it is; the caller's failure stands. */
    private static final int MAX_ANY_NESTING = 32;

    private static final String ANY = Any.getDescriptor().getFullName();

    private UnresolvedAnyTypes() {
    }

    /**
     * A copy of {@code message} without its Any values of undefined type.
     *
     * @return the copy, or empty when {@code message} holds no such Any
     */
    static Optional<Message> without(Message message, DescriptorRegistry registry) {
        Walk walk = new Walk(registry);
        Message stripped = walk.strip(message, 0);
        if (!walk.removedAny) {
            return Optional.empty();
        }
        // An undefined Any at the root leaves nothing of it to print.
        return Optional.of(stripped == null
                ? DynamicMessage.getDefaultInstance(message.getDescriptorForType())
                : stripped);
    }

    private static final class Walk {
        private final DescriptorRegistry registry;
        private boolean removedAny;

        private Walk(DescriptorRegistry registry) {
            this.registry = registry;
        }

        /** The message without undefined Any values, or null when it is one itself. */
        private Message strip(Message message, int anyNesting) {
            if (message.getDescriptorForType().getFullName().equals(ANY)) {
                return stripAny(message, anyNesting);
            }
            Message.Builder copy = null;
            for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
                FieldDescriptor descriptor = field.getKey();
                if (descriptor.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    continue;
                }
                List<?> values = descriptor.isRepeated()
                        ? (List<?>) field.getValue() : List.of(field.getValue());
                List<Message> kept = new ArrayList<>(values.size());
                boolean changed = false;
                for (Object value : values) {
                    Message stripped = strip((Message) value, anyNesting);
                    changed |= stripped != value;
                    if (stripped != null) {
                        kept.add(stripped);
                    }
                }
                if (!changed) {
                    continue;
                }
                if (copy == null) {
                    copy = message.toBuilder();
                }
                copy.clearField(descriptor);
                if (descriptor.isRepeated()) {
                    for (Message value : kept) {
                        copy.addRepeatedField(descriptor, value);
                    }
                } else if (!kept.isEmpty()) {
                    copy.setField(descriptor, kept.getFirst());
                }
            }
            return copy == null ? message : copy.build();
        }

        private Message stripAny(Message any, int anyNesting) {
            Descriptor anyType = any.getDescriptorForType();
            FieldDescriptor valueField = anyType.findFieldByNumber(Any.VALUE_FIELD_NUMBER);
            String typeUrl = (String) any.getField(
                    anyType.findFieldByNumber(Any.TYPE_URL_FIELD_NUMBER));
            String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
            if (!registry.isRegistered(typeName)) {
                removedAny = true;
                return null;
            }
            if (anyNesting >= MAX_ANY_NESTING) {
                return any;
            }
            DynamicMessage packed;
            try {
                packed = DynamicMessage.parseFrom(registry.findDescriptorByFullName(typeName),
                        (ByteString) any.getField(valueField));
            } catch (InvalidProtocolBufferException corrupt) {
                // A defined type whose bytes are not that type: left for the printer to refuse.
                return any;
            }
            Message stripped = strip(packed, anyNesting + 1);
            if (stripped == packed) {
                return any;
            }
            return any.toBuilder()
                    .setField(valueField, stripped == null
                            ? ByteString.EMPTY : stripped.toByteString())
                    .build();
        }
    }
}
