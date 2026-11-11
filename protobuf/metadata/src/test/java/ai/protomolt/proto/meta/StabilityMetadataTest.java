package ai.protomolt.proto.meta;

import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StabilityMetadataTest {

    @Test
    void resolvesStabilityFromExtensionsLabelsDeprecationAndInheritance() throws Exception {
        DescriptorProtos.FileOptions fileOptions = DescriptorProtos.FileOptions.newBuilder()
                .setExtension(
                        StabilityProto.fileStability,
                        StabilityOption.newBuilder()
                                .setLevel(StabilityLevel.STABILITY_LEVEL_STABLE)
                                .setSince("1.0.0")
                                .build())
                .build();

        DescriptorProtos.FieldOptions expFieldOptions = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(
                        StabilityProto.fieldStability,
                        StabilityOption.newBuilder()
                                .setLevel(StabilityLevel.STABILITY_LEVEL_EXPERIMENTAL)
                                .build())
                .build();

        DescriptorProtos.FieldOptions labelFieldOptions = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(
                        MetadataProto.field,
                        FieldMeta.newBuilder()
                                .putLabels(DescriptorMetadata.STABILITY_LABEL_KEY, "internal")
                                .build())
                .build();

        DescriptorProtos.FieldOptions deprecatedFieldOptions = DescriptorProtos.FieldOptions.newBuilder()
                .setDeprecated(true)
                .build();

        DescriptorProtos.DescriptorProto recordMessage = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Record")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("id")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("draft_hint")
                        .setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setOptions(expFieldOptions))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("debug_trace")
                        .setNumber(3)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setOptions(labelFieldOptions))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("legacy_code")
                        .setNumber(4)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setOptions(deprecatedFieldOptions))
                .build();

        DescriptorProtos.EnumDescriptorProto statusEnum = DescriptorProtos.EnumDescriptorProto.newBuilder()
                .setName("Status")
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                        .setName("STATUS_UNSPECIFIED")
                        .setNumber(0))
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                        .setName("STATUS_PREVIEW")
                        .setNumber(1)
                        .setOptions(DescriptorProtos.EnumValueOptions.newBuilder()
                                .setExtension(
                                        StabilityProto.enumValueStability,
                                        StabilityOption.newBuilder()
                                                .setLevel(StabilityLevel.STABILITY_LEVEL_EXPERIMENTAL)
                                                .build())
                                .build()))
                .build();

        DescriptorProtos.ServiceDescriptorProto serviceProto = DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("RecordService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                        .setName("GetRecord")
                        .setInputType(".example.v1.Record")
                        .setOutputType(".example.v1.Record"))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                        .setName("PreviewRecord")
                        .setInputType(".example.v1.Record")
                        .setOutputType(".example.v1.Record")
                        .setOptions(DescriptorProtos.MethodOptions.newBuilder()
                                .setExtension(
                                        StabilityProto.methodStability,
                                        StabilityOption.newBuilder()
                                                .setLevel(StabilityLevel.STABILITY_LEVEL_EXPERIMENTAL)
                                                .build())
                                .build()))
                .build();

        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax("proto3")
                .setName("example/v1/record.proto")
                .setPackage("example.v1")
                .setOptions(fileOptions)
                .addMessageType(recordMessage)
                .addEnumType(statusEnum)
                .addService(serviceProto)
                .build();

        FileDescriptor file = FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);
        assertThat(DescriptorMetadata.stability(file)).isEqualTo(StabilityTier.STABLE);

        Descriptor message = file.findMessageTypeByName("Record");
        assertThat(DescriptorMetadata.stability(message)).isEqualTo(StabilityTier.STABLE);

        FieldDescriptor idField = message.findFieldByName("id");
        FieldDescriptor expField = message.findFieldByName("draft_hint");
        FieldDescriptor internalField = message.findFieldByName("debug_trace");
        FieldDescriptor deprecatedField = message.findFieldByName("legacy_code");

        assertThat(DescriptorMetadata.stability(idField)).isEqualTo(StabilityTier.STABLE);
        assertThat(DescriptorMetadata.stability(expField)).isEqualTo(StabilityTier.EXPERIMENTAL);
        assertThat(DescriptorMetadata.stability(internalField)).isEqualTo(StabilityTier.INTERNAL);
        assertThat(DescriptorMetadata.stability(deprecatedField)).isEqualTo(StabilityTier.DEPRECATED);

        EnumDescriptor enumDesc = file.findEnumTypeByName("Status");
        assertThat(DescriptorMetadata.stability(enumDesc)).isEqualTo(StabilityTier.STABLE);
        assertThat(DescriptorMetadata.stability(enumDesc.findValueByName("STATUS_UNSPECIFIED")))
                .isEqualTo(StabilityTier.STABLE);
        assertThat(DescriptorMetadata.stability(enumDesc.findValueByName("STATUS_PREVIEW")))
                .isEqualTo(StabilityTier.EXPERIMENTAL);

        ServiceDescriptor service = file.findServiceByName("RecordService");
        MethodDescriptor stableMethod = service.findMethodByName("GetRecord");
        MethodDescriptor expMethod = service.findMethodByName("PreviewRecord");
        assertThat(DescriptorMetadata.stability(service)).isEqualTo(StabilityTier.STABLE);
        assertThat(DescriptorMetadata.stability(stableMethod)).isEqualTo(StabilityTier.STABLE);
        assertThat(DescriptorMetadata.stability(expMethod)).isEqualTo(StabilityTier.EXPERIMENTAL);
    }

    @Test
    void failsClosedOnUnrecognizedEnumLevelInvalidStabilityLabelAndCorruptOptionBytes() throws Exception {
        DescriptorProtos.FileDescriptorProto unrecognizedFileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax("proto3")
                .setName("example/v1/unrecognized.proto")
                .setPackage("example.v1")
                .setOptions(DescriptorProtos.FileOptions.newBuilder()
                        .setExtension(
                                StabilityProto.fileStability,
                                StabilityOption.newBuilder()
                                        .setLevelValue(99)
                                        .build())
                        .build())
                .build();
        FileDescriptor unrecognizedFile = FileDescriptor.buildFrom(unrecognizedFileProto, new FileDescriptor[0]);
        assertThatThrownBy(() -> DescriptorMetadata.stability(unrecognizedFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("99");

        DescriptorProtos.FileDescriptorProto bogusLabelProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax("proto3")
                .setName("example/v1/bogus_label.proto")
                .setPackage("example.v1")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Record")
                        .setOptions(DescriptorProtos.MessageOptions.newBuilder()
                                .setExtension(
                                        MetadataProto.message,
                                        MessageMeta.newBuilder()
                                                .putLabels(DescriptorMetadata.STABILITY_LABEL_KEY, "bogus")
                                                .build())
                                .build())
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("id")
                                .setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setOptions(DescriptorProtos.FieldOptions.newBuilder()
                                        .setExtension(
                                                MetadataProto.field,
                                                FieldMeta.newBuilder()
                                                        .putLabels(DescriptorMetadata.STABILITY_LABEL_KEY, "not-a-tier")
                                                        .build())
                                        .build())))
                .build();
        FileDescriptor bogusLabelFile = FileDescriptor.buildFrom(bogusLabelProto, new FileDescriptor[0]);
        Descriptor bogusMessage = bogusLabelFile.findMessageTypeByName("Record");
        FieldDescriptor bogusField = bogusMessage.findFieldByName("id");
        assertThatThrownBy(() -> DescriptorMetadata.stability(bogusMessage))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bogus");
        assertThatThrownBy(() -> DescriptorMetadata.stability(bogusField))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not-a-tier");

        DescriptorProtos.FileOptions corruptFileOptions = DescriptorProtos.FileOptions.newBuilder()
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(
                                StabilityProto.FILE_STABILITY_FIELD_NUMBER,
                                UnknownFieldSet.Field.newBuilder()
                                        .addLengthDelimited(ByteString.copyFrom(new byte[]{(byte) 0xFF, (byte) 0xFF}))
                                        .build())
                        .build())
                .build();
        FileDescriptor corruptFile = FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setSyntax("proto3")
                        .setName("example/v1/corrupt.proto")
                        .setPackage("example.v1")
                        .setOptions(corruptFileOptions)
                        .build(),
                new FileDescriptor[0]);
        assertThatThrownBy(() -> DescriptorMetadata.stability(corruptFile))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void preservesForwardCompatibleUnknownFieldsInStabilityOptions() throws Exception {
        StabilityOption forwardOption = StabilityOption.newBuilder()
                .setLevel(StabilityLevel.STABILITY_LEVEL_EVOLVING)
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(
                                55,
                                UnknownFieldSet.Field.newBuilder()
                                        .addLengthDelimited(ByteString.copyFromUtf8("future-hint"))
                                        .build())
                        .build())
                .build();
        DescriptorProtos.FileOptions unparsedOptions = DescriptorProtos.FileOptions.parseFrom(
                DescriptorProtos.FileOptions.newBuilder()
                        .setExtension(StabilityProto.fileStability, forwardOption)
                        .build()
                        .toByteString());
        FileDescriptor file = FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setSyntax("proto3")
                        .setName("example/v1/forward.proto")
                        .setPackage("example.v1")
                        .setOptions(unparsedOptions)
                        .build(),
                new FileDescriptor[0]);
        assertThat(DescriptorMetadata.stability(file)).isEqualTo(StabilityTier.EVOLVING);
    }

    @Test
    void validatesStabilityOptionSchemaConstraintsAndRuntimeBounds() {
        ProtoValidator validator = ProtoValidator.create();

        StabilityOption valid = StabilityOption.newBuilder()
                .setLevel(StabilityLevel.STABILITY_LEVEL_STABLE)
                .setSince("1.2.0")
                .setNotes("Public contract since 1.2.0")
                .build();
        assertThat(validator.validate(valid).valid()).isTrue();

        StabilityOption unrecognizedLevel = StabilityOption.newBuilder()
                .setLevelValue(99)
                .build();
        ValidationResult badLevelResult = validator.validate(unrecognizedLevel);
        assertThat(badLevelResult.valid()).isFalse();
        assertThat(badLevelResult.violations())
                .hasSizeGreaterThanOrEqualTo(2)
                .extracting(ValidationResult.Violation::path)
                .containsOnly("level");

        StabilityOption oversizedSince = StabilityOption.newBuilder()
                .setLevel(StabilityLevel.STABILITY_LEVEL_STABLE)
                .setSince("x".repeat(DescriptorMetadata.MAX_STABILITY_SINCE_LENGTH + 1))
                .build();
        ValidationResult badSinceResult = validator.validate(oversizedSince);
        assertThat(badSinceResult.valid()).isFalse();
        assertThat(badSinceResult.violations())
                .hasSizeGreaterThanOrEqualTo(2)
                .extracting(ValidationResult.Violation::path)
                .containsOnly("since");
        assertThatThrownBy(() -> DescriptorMetadata.toStabilityTier(oversizedSince))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("since");

        StabilityOption oversizedNotes = StabilityOption.newBuilder()
                .setLevel(StabilityLevel.STABILITY_LEVEL_STABLE)
                .setNotes("n".repeat(DescriptorMetadata.MAX_STABILITY_NOTES_LENGTH + 1))
                .build();
        ValidationResult badNotesResult = validator.validate(oversizedNotes);
        assertThat(badNotesResult.valid()).isFalse();
        assertThat(badNotesResult.violations())
                .hasSizeGreaterThanOrEqualTo(2)
                .extracting(ValidationResult.Violation::path)
                .containsOnly("notes");
        assertThatThrownBy(() -> DescriptorMetadata.toStabilityTier(oversizedNotes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("notes");
    }
}
