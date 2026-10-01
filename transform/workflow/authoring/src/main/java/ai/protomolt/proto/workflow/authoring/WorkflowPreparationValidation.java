package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationRecord;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.List;

/** Storage validation; current lease and transcript membership remain handler checks. */
final class WorkflowPreparationValidation {
    static final int INTENT_MAX = 8 * 1024 * 1024;
    static final int RESPONSE_MAX = 4 * 1024 * 1024;
    static final int RECORD_MAX = 16 * 1024 * 1024;

    private WorkflowPreparationValidation() {}

    static void validateIntent(WorkflowPreparationIntent intent) {
        validate(intent, INTENT_MAX);
        var binding = intent.getBinding();
        if (!binding.getSourceSha256().equals(
                WorkRecords.sha256Hex(intent.getRequest().getExecutableSourceJson().toByteArray()))) {
            throw new IllegalArgumentException("preparation source digest differs");
        }
        if (!binding.getOfferEntrySha256().equals(sha256(intent.getSelectedOffer()))) {
            throw new IllegalArgumentException("preparation offer digest differs");
        }
        if (!intent.getSelectedOffer().getCoordinatorFrame().getOffer().getSpec()
                .getContextList().contains(intent.getPolicy())) {
            throw new IllegalArgumentException("preparation policy absent from selected offer context");
        }
        byte[] policyBytes = intent.getPolicyProto().toByteArray();
        if (!intent.getPolicy().getSha256().equals(WorkRecords.sha256Hex(policyBytes))) {
            throw new IllegalArgumentException("preparation policy digest differs");
        }
        try {
            WorkflowAuthoringPolicy policy = WorkflowAuthoringPolicy.parseFrom(policyBytes);
            validate(policy, 4 * 1024 * 1024);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid preparation policy", e);
        }
    }

    static void validateResponse(PrepareWorkflowCandidateResponse response) {
        validate(response, RESPONSE_MAX);
    }

    static void validateRecord(WorkflowPreparationRecord record) {
        validate(record, RECORD_MAX);
        validateIntent(record.getIntent());
        if (record.hasCompleted()) validateResponse(record.getCompleted());
    }

    static void validate(Message message, int limit) {
        if (message == null) throw new IllegalArgumentException("preparation message must not be null");
        if (message.getSerializedSize() > limit) {
            throw new IllegalArgumentException("preparation message exceeds storage bound");
        }
        rejectUnknown(message);
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) throw new IllegalArgumentException("invalid preparation contract: " + result.violations());
    }

    static byte[] deterministicBytes(Message message, int limit) {
        if (message.getSerializedSize() > limit) {
            throw new IllegalArgumentException("preparation message exceeds storage bound");
        }
        byte[] bytes = new byte[message.getSerializedSize()];
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try {
            message.writeTo(output);
            output.checkNoSpaceLeft();
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize preparation", e);
        }
    }

    static String sha256(Message message) {
        return WorkRecords.sha256Hex(deterministicBytes(message, RECORD_MAX));
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("unknown fields in preparation contract");
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else rejectUnknown((Message) field.getValue());
        }
    }
}
