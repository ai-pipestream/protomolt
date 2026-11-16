package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;

/**
 * Finds a {@code google.protobuf.Any} whose type a registry does not define, anywhere inside a
 * message. proto3 JSON cannot print such a message, and this is the one printing failure the
 * run view reports instead of raising.
 */
final class UnresolvedAnyTypes {

    /** Any-in-Any nesting deeper than this is not searched; the caller's failure stands. */
    private static final int MAX_ANY_NESTING = 32;

    private UnresolvedAnyTypes() {
    }

    static boolean presentIn(Message message, DescriptorRegistry registry) {
        return presentIn(message, registry, 0);
    }

    private static boolean presentIn(Message message, DescriptorRegistry registry,
                                     int anyNesting) {
        if (message.getDescriptorForType().getFullName()
                .equals(Any.getDescriptor().getFullName())) {
            return anyIsUnresolved(message, registry, anyNesting);
        }
        for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            List<?> values = field.getKey().isRepeated()
                    ? (List<?>) field.getValue() : List.of(field.getValue());
            for (Object value : values) {
                if (presentIn((Message) value, registry, anyNesting)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean anyIsUnresolved(Message any, DescriptorRegistry registry,
                                           int anyNesting) {
        Descriptor anyType = any.getDescriptorForType();
        String typeUrl = (String) any.getField(anyType.findFieldByNumber(Any.TYPE_URL_FIELD_NUMBER));
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        if (!registry.isRegistered(typeName)) {
            return true;
        }
        if (anyNesting >= MAX_ANY_NESTING) {
            return false;
        }
        ByteString packed = (ByteString) any.getField(
                anyType.findFieldByNumber(Any.VALUE_FIELD_NUMBER));
        DynamicMessage unpacked;
        try {
            unpacked = DynamicMessage.parseFrom(
                    registry.findDescriptorByFullName(typeName), packed);
        } catch (InvalidProtocolBufferException corrupt) {
            // The type is known and its bytes are not that type: a different failure.
            return false;
        }
        return presentIn(unpacked, registry, anyNesting + 1);
    }
}
