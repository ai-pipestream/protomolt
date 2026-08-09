package ai.pipestream.proto.llm;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The options travel with the descriptor set: a consumer that parses the set with the
 * extensions registered reads them back, a consumer that does not cannot see them.
 */
class DescriptorLlmRegistryTest {

    private static final FileDescriptor FORM_FILE =
            ai.pipestream.proto.llm.testdata.AnnotatedForm.getDescriptor().getFile();

    private static byte[] descriptorSetBytes() {
        return FileDescriptorSet.newBuilder().addFile(FORM_FILE.toProto()).build().toByteArray();
    }

    private static Descriptor rebuildForm(FileDescriptorProto proto) throws Exception {
        FileDescriptor file = FileDescriptor.buildFrom(
                proto, new FileDescriptor[]{DescriptorProtos.getDescriptor()});
        return file.findMessageTypeByName("AnnotatedForm");
    }

    @Test
    void annotationsSurviveADescriptorSetRoundTripWhenExtensionsAreRegistered() throws Exception {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        DescriptorLlm.registerExtensions(registry);

        FileDescriptorProto proto = FileDescriptorSet.parseFrom(descriptorSetBytes(), registry)
                .getFile(0);
        Descriptor form = rebuildForm(proto);

        MessageLlm message = DescriptorLlm.message(form).orElseThrow();
        assertThat(message.getDirective()).isEqualTo("Fill this form from the source text alone.");

        FieldLlm court = DescriptorLlm.field(form.findFieldByName("court")).orElseThrow();
        assertThat(court.getDirective())
                .isEqualTo("Name the court exactly as it appears in the caption.");
        assertThat(DescriptorLlm.field(form.findFieldByName("note"))).isEmpty();
    }

    @Test
    void annotationsAreInvisibleWithoutExtensionRegistration() throws Exception {
        FileDescriptorProto proto = FileDescriptorSet.parseFrom(descriptorSetBytes()).getFile(0);

        // The bytes are identical, but the extensions land in the unknown field set.
        assertThat(proto.getMessageType(0).getOptions().getUnknownFields().asMap())
                .containsKey(LlmProto.message.getNumber());

        Descriptor form = rebuildForm(proto);
        assertThat(DescriptorLlm.message(form)).isEmpty();
        assertThat(DescriptorLlm.field(form.findFieldByName("court"))).isEmpty();
    }
}
