package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.PipelineExecutionException;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/** Binary-faithful conversion, canonical encoding and declared-rule validation of step values. */
final class StepValues {

    private StepValues() {
    }

    static DynamicMessage dynamic(Message message) {
        if (message instanceof DynamicMessage dynamic) {
            return dynamic;
        }
        try {
            return DynamicMessage.parseFrom(message.getDescriptorForType(),
                    message.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("message cannot be converted to its descriptor", e);
        }
    }

    static byte[] canonicalBytes(Message message) {
        return MessageFingerprints.deterministicBytes(message);
    }

    static void validate(DynamicMessage value, String step, String label)
            throws PipelineExecutionException {
        ValidationResult result = ProtoValidator.forMessageType(
                value.getDescriptorForType()).validate(value);
        if (!result.valid()) {
            throw PipelineExecutor.failure(step, FailureKind.VALIDATION,
                    label + " failed validation: " + validationSummary(result), null);
        }
    }

    private static String validationSummary(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + ": " + violation.message())
                .reduce((left, right) -> left + "; " + right).orElse("");
    }
}
