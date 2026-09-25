package ai.protomolt.proto.inference.v1;

import ai.protomolt.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StructuredGenerationContractTest {

    @Test
    void omittedModeIsTheBackwardsCompatibleUnspecifiedValue() {
        GenerateStructuredRequest request = GenerateStructuredRequest.newBuilder()
                .setTargetType("example.v1.Form")
                .setModel("model")
                .build();

        assertThat(request.getMode())
                .isEqualTo(StructuredGenerationMode.STRUCTURED_GENERATION_MODE_UNSPECIFIED);
        assertThat(ProtoValidator.create().validate(request).valid()).isTrue();
    }

    @Test
    void onlyDeclaredGenerationModesAreValid() {
        GenerateStructuredRequest request = GenerateStructuredRequest.newBuilder()
                .setTargetType("example.v1.Form")
                .setModel("model")
                .setModeValue(73)
                .build();

        assertThat(ProtoValidator.create().validate(request).valid()).isFalse();
    }
}
