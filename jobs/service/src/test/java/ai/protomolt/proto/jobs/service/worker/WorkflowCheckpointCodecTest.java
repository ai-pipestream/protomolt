package ai.protomolt.proto.jobs.service.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;

/**
 * Verifies binary-faithful checkpoint round-tripping for step responses carrying unknown fields,
 * raw bytes, unsigned 64-bit values, and unregistered {@code google.protobuf.Any} payloads (#818).
 */
class WorkflowCheckpointCodecTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ActionContext CONTEXT = ActionContext.create();

    @Test void roundTripsUnknownFieldsBytesUint64AndUnregisteredAnyWithoutLoss() throws Exception {
        Descriptor stepOutput = buildStepOutputDescriptor();
        Descriptor anyDescriptor = AnyProto.getDescriptor().findMessageTypeByName("Any");

        DynamicMessage unregisteredAny = DynamicMessage.newBuilder(anyDescriptor)
                .setField(
                        anyDescriptor.findFieldByName("type_url"),
                        "type.googleapis.com/unregistered.vendor.ExtensionV9")
                .setField(
                        anyDescriptor.findFieldByName("value"),
                        ByteString.copyFrom(new byte[] {0x08, 0x2A, 0x12, 0x03, 0x61, 0x62, 0x63}))
                .build();

        UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder()
                .addField(
                        77,
                        UnknownFieldSet.Field.newBuilder()
                                .addLengthDelimited(
                                        ByteString.copyFrom("preserved-extension", StandardCharsets.UTF_8))
                                .addVarint(999_999L)
                                .build())
                .build();

        byte[] rawBinary = new byte[] {0x00, (byte) 0xFF, 0x7F, (byte) 0x80, 0x13, 0x37};
        long maxUnsigned = -1L; // 0xFFFFFFFFFFFFFFFF unsigned uint64

        DynamicMessage original = DynamicMessage.newBuilder(stepOutput)
                .setField(stepOutput.findFieldByName("raw_bytes"), ByteString.copyFrom(rawBinary))
                .setField(stepOutput.findFieldByName("unsigned_seq"), maxUnsigned)
                .setField(stepOutput.findFieldByName("detail"), unregisteredAny)
                .setUnknownFields(unknownFields)
                .build();

        ObjectNode entry = MAPPER.createObjectNode();
        entry.put("name", "enrich");
        entry.put("skipped", false);
        WorkflowCheckpointCodec.writeResponse(
                entry, new WorkflowRunner.Checkpoint("enrich", false, original), CONTEXT);

        assertThat(entry.has(WorkflowCheckpointCodec.RESPONSE_BASE64_FIELD)).isTrue();
        // Unregistered Any cannot be rendered by proto3 JSON, so response preview falls back to null
        // without failing the checkpoint write, while responseBase64 retains exact wire bytes.
        assertThat(entry.get(WorkflowCheckpointCodec.RESPONSE_FIELD).isNull()).isTrue();

        WorkflowRunner.Checkpoint restored = WorkflowCheckpointCodec.readCheckpoint(
                "enrich", false, entry, stepOutput, CONTEXT);
        assertThat(restored.skipped()).isFalse();
        assertThat(restored.response()).isNotNull();
        assertThat(restored.response().toByteArray()).isEqualTo(original.toByteArray());
        assertThat(restored.response().getUnknownFields()).isEqualTo(unknownFields);
        assertThat(restored.response().getField(stepOutput.findFieldByName("unsigned_seq")))
                .isEqualTo(maxUnsigned);
        assertThat(restored.response().getField(stepOutput.findFieldByName("raw_bytes")))
                .isEqualTo(ByteString.copyFrom(rawBinary));
    }

    @Test void fallsBackToLegacyJsonCheckpointsAndHandlesSkippedSteps() throws Exception {
        Descriptor stepOutput = buildStepOutputDescriptor();
        ObjectNode legacyEntry = MAPPER.createObjectNode();
        legacyEntry.put("name", "legacy-step");
        legacyEntry.put("skipped", false);
        ObjectNode jsonPayload = legacyEntry.putObject("response");
        jsonPayload.put("rawBytes", Base64.getEncoder().encodeToString(new byte[] {1, 2, 3}));
        jsonPayload.put("unsignedSeq", "42");

        WorkflowRunner.Checkpoint restored = WorkflowCheckpointCodec.readCheckpoint(
                "legacy-step", false, legacyEntry, stepOutput, CONTEXT);
        assertThat(restored.response()).isNotNull();
        assertThat(restored.response().getField(stepOutput.findFieldByName("unsigned_seq")))
                .isEqualTo(42L);
        assertThat(restored.response().getField(stepOutput.findFieldByName("raw_bytes")))
                .isEqualTo(ByteString.copyFrom(new byte[] {1, 2, 3}));

        WorkflowRunner.Checkpoint skipped = WorkflowCheckpointCodec.readCheckpoint(
                "skipped-step", true, legacyEntry, stepOutput, CONTEXT);
        assertThat(skipped.skipped()).isTrue();
        assertThat(skipped.response()).isNull();
    }

    @Test void rejectsCorruptBase64OrTruncatedProtobufWire() throws Exception {
        Descriptor stepOutput = buildStepOutputDescriptor();

        ObjectNode badBase64 = MAPPER.createObjectNode();
        badBase64.put(WorkflowCheckpointCodec.RESPONSE_BASE64_FIELD, "not-valid-base64!!!");
        assertThatThrownBy(() -> WorkflowCheckpointCodec.readCheckpoint(
                        "step-1", false, badBase64, stepOutput, CONTEXT))
                .isInstanceOf(WorkflowCheckpointCodec.CorruptCheckpointException.class)
                .hasMessageContaining("not valid base64");

        ObjectNode nonStringBase64 = MAPPER.createObjectNode();
        nonStringBase64.put(WorkflowCheckpointCodec.RESPONSE_BASE64_FIELD, 123);
        assertThatThrownBy(() -> WorkflowCheckpointCodec.readCheckpoint(
                        "step-1", false, nonStringBase64, stepOutput, CONTEXT))
                .isInstanceOf(WorkflowCheckpointCodec.CorruptCheckpointException.class)
                .hasMessageContaining("not a base64 string");

        // Tag 1 length-delimited claiming 50 bytes with 0 bytes remaining -> truncated wire
        ObjectNode truncatedWire = MAPPER.createObjectNode();
        truncatedWire.put(
                WorkflowCheckpointCodec.RESPONSE_BASE64_FIELD,
                Base64.getEncoder().encodeToString(new byte[] {0x0A, 0x32}));
        assertThatThrownBy(() -> WorkflowCheckpointCodec.readCheckpoint(
                        "step-1", false, truncatedWire, stepOutput, CONTEXT))
                .isInstanceOf(WorkflowCheckpointCodec.CorruptCheckpointException.class)
                .hasMessageContaining("not valid protobuf wire");
    }

    private static Descriptor buildStepOutputDescriptor() throws Exception {
        FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
                .setName("test/checkpoint_fidelity.proto")
                .setPackage("test.checkpoint")
                .setSyntax("proto3")
                .addDependency(AnyProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("StepOutput")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("raw_bytes")
                                .setNumber(1)
                                .setLabel(Label.LABEL_OPTIONAL)
                                .setType(Type.TYPE_BYTES))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("unsigned_seq")
                                .setNumber(2)
                                .setLabel(Label.LABEL_OPTIONAL)
                                .setType(Type.TYPE_UINT64))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("detail")
                                .setNumber(3)
                                .setLabel(Label.LABEL_OPTIONAL)
                                .setType(Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Any")))
                .build();
        FileDescriptor fd = FileDescriptor.buildFrom(
                fileProto, new FileDescriptor[] {AnyProto.getDescriptor()});
        return fd.findMessageTypeByName("StepOutput");
    }
}
