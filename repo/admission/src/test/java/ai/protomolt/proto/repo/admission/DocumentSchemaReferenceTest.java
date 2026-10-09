package ai.protomolt.proto.repo.admission;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import static ai.protomolt.proto.repo.admission.DocumentSchemaPreparationTest.*;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaReferenceTest {
    @Test void retainedFixtureAssociationsRoundTripWithoutCopyingDescriptors() throws Exception {
        for (boolean source : new boolean[]{false, true}) {
            var f = fixture(source);
            for (var asset : java.util.List.of(f.container(), f.string(), f.timestamp())) {
                var original = asset.reference();
                var encoded = original.toProto();
                assertThat(encoded.hasSourceSha256()).isEqualTo(original.sourceSha256().isPresent());
                assertThat(DocumentSchemaAdmission.Reference.fromProto(encoded, () -> {})).isEqualTo(original);
                assertThat(encoded.getSerializedSize()).isLessThan(1024);
            }
        }
    }
    @Test void unrecognizedIdentityFieldsAndExplicitEmptySourceAreRefused() throws Exception {
        var value = fixture(true).string().reference().toProto();
        var unknown = value.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        assertThatThrownBy(() -> DocumentSchemaAdmission.Reference.fromProto(unknown, () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DocumentSchemaAdmission.Reference.fromProto(value.toBuilder().setSourceSha256("").build(), () -> {}))
                .isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class);
    }
}
