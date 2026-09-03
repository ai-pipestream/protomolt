package ai.protomolt.proto.llm;

import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DescriptorLlmTest {

    private static final Descriptor FORM =
            ai.protomolt.proto.llm.testdata.AnnotatedForm.getDescriptor();

    @Test
    void readsMessageOptions() {
        MessageLlm message = DescriptorLlm.message(FORM).orElseThrow();
        assertThat(message.getInstruction()).isEqualTo("Fill this form from the source text alone.");
        assertThat(message.getSafeguardsList()).containsExactly("Do not use outside knowledge.");
    }

    @Test
    void readsFieldOptions() {
        FieldLlm court = DescriptorLlm.field(FORM.findFieldByName("court")).orElseThrow();
        assertThat(court.getInstruction()).isEqualTo("Name the court exactly as it appears in the caption.");
        assertThat(court.getSafeguardsList()).containsExactly("Do not abbreviate.");
        assertThat(court.getVolatile()).isFalse();

        FieldLlm authority =
                DescriptorLlm.field(FORM.findFieldByName("leading_authority")).orElseThrow();
        assertThat(authority.getVolatile()).isTrue();
    }

    @Test
    void absentAnnotationReadsAsEmpty() {
        assertThat(DescriptorLlm.field(FORM.findFieldByName("note"))).isEmpty();
    }

    @Test
    void absentMessageAnnotationReadsAsEmpty() {
        Descriptor plain = ai.protomolt.proto.llm.testdata.PlainForm.getDescriptor();
        assertThat(DescriptorLlm.message(plain)).isEmpty();
        assertThat(DescriptorLlm.field(plain.findFieldByName("title"))).isEmpty();
    }

    @Test
    void safeguardsKeepTheirDeclaredOrder() {
        FieldLlm citations = DescriptorLlm.field(FORM.findFieldByName("citations")).orElseThrow();
        assertThat(citations.getSafeguardsList()).containsExactly(
                "Do not invent citations.",
                "Do not cite authority the text does not mention.");
        assertThat(citations.getInstruction()).isEqualTo("List every authority the text cites.");
        assertThat(citations.getVolatile()).isFalse();
    }

    @Test
    void rejectsNullArguments() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DescriptorLlm.field(null))
                .isInstanceOf(NullPointerException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DescriptorLlm.message(null))
                .isInstanceOf(NullPointerException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DescriptorLlm.registerExtensions(null))
                .isInstanceOf(NullPointerException.class);
    }
}
