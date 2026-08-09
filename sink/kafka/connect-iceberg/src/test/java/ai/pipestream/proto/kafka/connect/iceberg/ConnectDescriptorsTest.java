package ai.pipestream.proto.kafka.connect.iceberg;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ConnectDescriptors}' config diagnostics — a descriptor set that is not base64, one
 * whose imports are absent, one that parses but does not link — plus the positive message-type
 * lookups (top-level and nested) the connector's eager validation relies on. Every failure has
 * to name the offending config key or type so an operator can act on the worker log alone.
 */
class ConnectDescriptorsTest {

    private static final String PROTO = """
            syntax = "proto3";
            package desc.test;
            message Order { string id = 1; }
            message Envelope { message Body { string id = 1; } Body body = 1; }
            """;

    private static String base64(FileDescriptorSet set) {
        return Base64.getEncoder().encodeToString(set.toByteArray());
    }

    @Test
    void descriptorSetThatIsNotBase64FileDescriptorSetNamesTheConfigKey() {
        assertThatThrownBy(() -> ConnectDescriptors.linkedFiles("!!! not base64 !!!"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'schema.descriptor.set.base64' is not a base64 "
                        + "serialized FileDescriptorSet");

        String base64Garbage = Base64.getEncoder()
                .encodeToString("this is not a descriptor set".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> ConnectDescriptors.linkedFiles(base64Garbage))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'schema.descriptor.set.base64' is not a base64 "
                        + "serialized FileDescriptorSet");
    }

    @Test
    void descriptorSetMissingAnImportNamesTheMissingFile() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("desc/test/a.proto")
                        .setSyntax("proto3")
                        .setPackage("desc.test")
                        .addDependency("desc/test/missing.proto"))
                .build();

        assertThatThrownBy(() -> ConnectDescriptors.linkedFiles(base64(set)))
                .isInstanceOf(ConnectException.class)
                .hasMessage("Descriptor set is missing the import 'desc/test/missing.proto'");
    }

    @Test
    void descriptorSetThatDoesNotLinkReportsTheValidationFailure() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("desc/test/b.proto")
                        .setSyntax("proto3")
                        .setPackage("desc.test")
                        .addMessageType(DescriptorProto.newBuilder()
                                .setName("Holder")
                                .addField(FieldDescriptorProto.newBuilder()
                                        .setName("body")
                                        .setNumber(1)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                        .setTypeName(".desc.test.NoSuchType"))))
                .build();

        assertThatThrownBy(() -> ConnectDescriptors.linkedFiles(base64(set)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Descriptor set does not link")
                .hasMessageContaining(".desc.test.NoSuchType");
    }

    @Test
    void aValidDescriptorSetLinksAndResolvesTypes() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("desc/test/types.proto", PROTO, "test").build());
        List<FileDescriptor> files = ConnectDescriptors.linkedFiles(
                base64(compiled.descriptorSet()));
        assertThat(files).hasSize(1);

        assertThat(ConnectDescriptors.messageType(files, "desc.test.Order").getName())
                .isEqualTo("Order");
        assertThat(ConnectDescriptors.messageType(files, "desc.test.Envelope.Body")
                .getFullName()).isEqualTo("desc.test.Envelope.Body");
    }

    @Test
    void anUnknownMessageTypeIsNamed() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("desc/test/types.proto", PROTO, "test").build());
        List<FileDescriptor> files = ConnectDescriptors.linkedFiles(
                base64(compiled.descriptorSet()));

        assertThatThrownBy(() -> ConnectDescriptors.messageType(files, "desc.test.NoSuch"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("desc.test.NoSuch");
        // A type under a package no linked file declares can never resolve either.
        assertThatThrownBy(() -> ConnectDescriptors.messageType(files, "other.place.Order"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("other.place.Order");
    }
}
