package ai.protomolt.proto.samples;

import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.List;

/** Validation at the demonstration service and persisted-record boundaries. */
final class FixtureValidation {
    static final int MAX_RECORD_BYTES = 64 * 1024;

    private FixtureValidation() {}

    static void validate(Message message) {
        if (message.getSerializedSize() > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("fixture message exceeds storage limit");
        }
        rejectUnknown(message);
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) {
            throw new IllegalArgumentException("invalid fixture message: " + result.violations());
        }
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("unknown fixture fields");
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else {
                rejectUnknown((Message) field.getValue());
            }
        }
    }
}
