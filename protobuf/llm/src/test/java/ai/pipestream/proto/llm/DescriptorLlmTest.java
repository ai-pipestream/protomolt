package ai.pipestream.proto.llm;

import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DescriptorLlmTest {

    private static final Descriptor FORM =
            ai.pipestream.proto.llm.testdata.AnnotatedForm.getDescriptor();

    @Test
    void readsMessageOptions() {
        MessageLlm message = DescriptorLlm.message(FORM).orElseThrow();
        assertThat(message.getDirective()).isEqualTo("Fill this form from the source text alone.");
        assertThat(message.getSafeguardsList()).containsExactly("Do not use outside knowledge.");
    }

    @Test
    void readsFieldOptions() {
        FieldLlm court = DescriptorLlm.field(FORM.findFieldByName("court")).orElseThrow();
        assertThat(court.getDirective()).isEqualTo("Name the court exactly as it appears in the caption.");
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
}
