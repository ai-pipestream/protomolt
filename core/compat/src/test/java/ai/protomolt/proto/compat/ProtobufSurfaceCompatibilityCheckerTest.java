package ai.protomolt.proto.compat;

import ai.protomolt.proto.meta.StabilityProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufSurfaceCompatibilityCheckerTest {

    @Test
    void rejectsMalformedEmbeddedFileStabilityOptionInsteadOfDefaultingToStable() {
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
                .addField(
                        StabilityProto.fileStability.getNumber(),
                        UnknownFieldSet.Field.newBuilder()
                                .addLengthDelimited(ByteString.copyFrom(new byte[]{(byte) 0x80}))
                                .build())
                .build();

        DescriptorProtos.FileOptions malformedOptions = DescriptorProtos.FileOptions.newBuilder()
                .setUnknownFields(unknown)
                .build();

        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setSyntax("proto3")
                        .setName("example/v1/payload.proto")
                        .setPackage("example")
                        .setOptions(malformedOptions)
                        .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                                .setName("Payload")
                                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                        .setName("id")
                                        .setNumber(1)
                                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                                .build())
                        .build())
                .build();

        assertThatThrownBy(() -> ProtobufSurfaceSnapshot.of(set))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Malformed stability or metadata option");
    }

    @Test
    void rejectsUnrecognizedEnumValueOrUnknownFieldsInsideStabilityOption() {
        // Encode tag 1 (level) with unrecognized enum value 99 inside StabilityOption
        UnknownFieldSet innerUnknown = UnknownFieldSet.newBuilder()
                .addField(
                        1,
                        UnknownFieldSet.Field.newBuilder()
                                .addVarint(99L)
                                .build())
                .build();

        UnknownFieldSet outerUnknown = UnknownFieldSet.newBuilder()
                .addField(
                        StabilityProto.messageStability.getNumber(),
                        UnknownFieldSet.Field.newBuilder()
                                .addLengthDelimited(innerUnknown.toByteString())
                                .build())
                .build();

        DescriptorProtos.MessageOptions unrecognizedOptions = DescriptorProtos.MessageOptions.newBuilder()
                .setUnknownFields(outerUnknown)
                .build();

        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setSyntax("proto3")
                        .setName("example/v1/payload.proto")
                        .setPackage("example")
                        .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                                .setName("Payload")
                                .setOptions(unrecognizedOptions)
                                .build())
                        .build())
                .build();

        assertThatThrownBy(() -> ProtobufSurfaceSnapshot.of(set))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Malformed or unrecognized StabilityOption");
    }
}
