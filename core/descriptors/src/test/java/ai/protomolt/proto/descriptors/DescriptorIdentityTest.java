package ai.protomolt.proto.descriptors;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DescriptorIdentityTest {

    @Test
    void identityIsStableAcrossIndependentDescriptorPools() throws Exception {
        Descriptor first = descriptor("string");
        Descriptor second = descriptor("string");

        assertThat(first).isNotSameAs(second);
        assertThat(DescriptorIdentity.of(first)).isEqualTo(DescriptorIdentity.of(second));
        assertThat(DescriptorIdentity.of(first).matches(second)).isTrue();
    }

    @Test
    void sameNamedSchemaDriftProducesAnotherIdentity() throws Exception {
        Descriptor first = descriptor("string");
        Descriptor drifted = descriptor("int64");

        assertThat(first.getFullName()).isEqualTo(drifted.getFullName());
        assertThat(DescriptorIdentity.of(first)).isNotEqualTo(DescriptorIdentity.of(drifted));
        assertThat(DescriptorIdentity.of(first).matches(drifted)).isFalse();
    }

    @Test
    void setFingerprintIsIndependentOfFileOrder() throws Exception {
        FileDescriptor dependency = file("identity/dependency.proto", "identity", "Dependency");
        FileDescriptor owner = FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                .setName("identity/owner.proto")
                .setPackage("identity")
                .setSyntax("proto3")
                .addDependency(dependency.getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Owner")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("dependency")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".identity.Dependency")))
                .build(), new FileDescriptor[]{dependency});

        String forward = DescriptorIdentity.fingerprintFiles(List.of(dependency, owner));
        String reverse = DescriptorIdentity.fingerprintFiles(List.of(owner, dependency));
        assertThat(forward).isEqualTo(reverse);
        assertThat(DescriptorIdentity.closure(owner.findMessageTypeByName("Owner")).getFileList())
                .extracting(FileDescriptorProto::getName)
                .containsExactly("identity/dependency.proto", "identity/owner.proto");
    }

    @Test
    void unknownDescriptorFieldsParticipateInIdentity() {
        FileDescriptorProto base = FileDescriptorProto.newBuilder()
                .setName("identity/unknown.proto")
                .setPackage("identity")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Unknown"))
                .build();
        FileDescriptorProto extended = base.toBuilder()
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(19000, UnknownFieldSet.Field.newBuilder()
                                .addVarint(7).build())
                        .build())
                .build();

        assertThat(DescriptorIdentity.fingerprint(FileDescriptorSet.newBuilder()
                .addFile(base).build()))
                .isNotEqualTo(DescriptorIdentity.fingerprint(FileDescriptorSet.newBuilder()
                        .addFile(extended).build()));
    }

    @Test
    void duplicateFileNamesWithDifferentDefinitionsAreRefused() throws Exception {
        assertThatThrownBy(() -> DescriptorIdentity.fingerprintFiles(List.of(
                descriptor("string").getFile(), descriptor("int64").getFile())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("two definitions named identity/thing.proto");
    }

    @Test
    void packageLessMessageHasAValidDescriptorIdentity() throws Exception {
        Descriptor descriptor = file("identity/root.proto", "", "Root")
                .findMessageTypeByName("Root");

        assertThat(DescriptorIdentity.of(descriptor).typeName()).isEqualTo("Root");
    }

    @Test
    void descriptorSetRefusesSameNamedDrift() {
        FileDescriptorProto first = FileDescriptorProto.newBuilder()
                .setName("identity/duplicate.proto")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("First"))
                .build();
        FileDescriptorProto second = first.toBuilder()
                .clearMessageType()
                .addMessageType(DescriptorProto.newBuilder().setName("Second"))
                .build();

        assertThatThrownBy(() -> DescriptorIdentity.fingerprint(FileDescriptorSet.newBuilder()
                .addFile(first).addFile(second).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("two definitions named identity/duplicate.proto");
    }

    private static Descriptor descriptor(String type) throws Exception {
        FieldDescriptorProto.Type fieldType = switch (type) {
            case "string" -> FieldDescriptorProto.Type.TYPE_STRING;
            case "int64" -> FieldDescriptorProto.Type.TYPE_INT64;
            default -> throw new IllegalArgumentException(type);
        };
        return FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                .setName("identity/thing.proto")
                .setPackage("identity")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Thing")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("value")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(fieldType)))
                .build(), new FileDescriptor[0])
                .findMessageTypeByName("Thing");
    }

    private static FileDescriptor file(String name, String packageName, String message)
            throws Exception {
        return FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                .setName(name)
                .setPackage(packageName)
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName(message))
                .build(), new FileDescriptor[0]);
    }
}
