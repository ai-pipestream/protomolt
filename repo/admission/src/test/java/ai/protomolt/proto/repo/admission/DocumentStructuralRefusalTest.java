package ai.protomolt.proto.repo.admission;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import org.junit.jupiter.api.Test;

class DocumentStructuralRefusalTest {
    @Test void structuralReasonsRemainDistinct() {
        assertThatThrownBy(() -> DocumentSchemaAdmission.requireKnownDocument(Empty.getDefaultInstance(),
                "deep.field", 65, new int[1], () -> {}))
                .hasMessage("Document nesting exceeds 64 levels at deep.field");
        assertThatThrownBy(() -> DocumentSchemaAdmission.requireKnownDocument(Empty.getDefaultInstance(),
                "", 0, new int[]{1_000_000}, () -> {}))
                .hasMessage("Document exceeds 1,000,000 nodes");
        assertThatThrownBy(() -> DocumentSchemaAdmission.requireKnownDocument(Any.getDefaultInstance(),
                "unexpected.shape", 0, new int[1], () -> {}))
                .hasMessage("An Any at unexpected.shape is not allowed; Any is accepted only at structured_data or parser_results.value.document.shape");
    }
}
