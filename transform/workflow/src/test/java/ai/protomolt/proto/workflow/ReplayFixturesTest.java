package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.workflow.v1.StructuredGenerationEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StructuredGenerationSpec;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.inference.v1.GenerateStructuredRequest;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The replay re-derivations that need no scope bookkeeping. */
class ReplayFixturesTest {

    private static final StructuredGenerationSpec SPEC = StructuredGenerationSpec.newBuilder()
            .setTargetType("acme.Invoice")
            .setModel("catalog/model-a")
            .setMaxAttempts(2)
            .build();

    @Test
    void aMatchingStructuredIdentityHasNoMismatch() {
        assertThat(ReplayFixtures.structuredIdentityMismatch(evidence("acme.Invoice",
                "catalog/model-a"), SPEC)).isNull();
    }

    @Test
    void theTargetTypeIsCheckedBeforeTheModel() {
        assertThat(ReplayFixtures.structuredIdentityMismatch(evidence("acme.Receipt",
                "catalog/model-b"), SPEC))
                .isEqualTo("recorded target type acme.Receipt differs from the "
                        + "workflow's acme.Invoice");
        assertThat(ReplayFixtures.structuredIdentityMismatch(evidence("acme.Invoice",
                "catalog/model-b"), SPEC))
                .isEqualTo("recorded model catalog/model-b differs from the workflow's "
                        + "catalog/model-a");
    }

    @Test
    void theSpecRequestCarriesTheSpecAndNoGrounding() {
        Message request = ReplayFixtures.structuredRequest(SPEC);

        assertThat(request).isInstanceOf(GenerateStructuredRequest.class);
        GenerateStructuredRequest typed = (GenerateStructuredRequest) request;
        assertThat(typed.getTargetType()).isEqualTo("acme.Invoice");
        assertThat(typed.getModel()).isEqualTo("catalog/model-a");
        assertThat(typed.getMaxAttempts()).isEqualTo(2);
        assertThat(typed.hasGrounding()).isFalse();
    }

    @Test
    void aSkipWithoutAGateIsNamed() {
        assertThat(ReplayFixtures.verifySkip(WorkflowStep.getDefaultInstance(),
                Map.of("input", Struct.getDefaultInstance()), null))
                .isEqualTo("step is recorded skipped but declares no gate");
    }

    @Test
    void aGateThatCannotBeRebuiltIsReportedNotThrown() {
        WorkflowStep gated = WorkflowStep.newBuilder().setWhen("input.ok").build();

        assertThat(ReplayFixtures.verifySkip(gated,
                Map.of("input", Struct.getDefaultInstance()), null))
                .startsWith("gate could not be re-evaluated: ");
    }

    private static StructuredGenerationEvidence evidence(String targetType, String model) {
        return StructuredGenerationEvidence.newBuilder()
                .setTargetType(targetType)
                .setModel(model)
                .build();
    }
}
