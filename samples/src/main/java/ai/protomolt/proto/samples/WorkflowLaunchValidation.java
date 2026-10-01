package ai.protomolt.proto.samples;

import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.List;

/** Bounds and serialization shared by the sample launch binding and its store. */
final class WorkflowLaunchValidation {
    static final int MAX_BYTES = 4 * 1024 * 1024;

    private WorkflowLaunchValidation() {}

    static void validate(Message message) {
        if (message.getSerializedSize() > MAX_BYTES) {
            throw new IllegalArgumentException("launch message exceeds 4 MiB");
        }
        rejectUnknown(message);
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) throw new IllegalArgumentException("invalid launch contract: " + result.violations());
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("unknown fields in launch contract");
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else rejectUnknown((Message) field.getValue());
        }
    }

    static byte[] deterministicBytes(Message message) {
        if (message.getSerializedSize() > MAX_BYTES) {
            throw new IllegalArgumentException("launch message exceeds 4 MiB");
        }
        byte[] bytes = new byte[message.getSerializedSize()];
        var output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try {
            message.writeTo(output);
            output.checkNoSpaceLeft();
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize launch message", e);
        }
    }

    static String sha256(Message message) {
        return WorkRecords.sha256Hex(deterministicBytes(message));
    }
}
