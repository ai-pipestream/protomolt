package ai.protomolt.proto.meta;

import ai.protomolt.proto.descriptors.StabilityTier;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
