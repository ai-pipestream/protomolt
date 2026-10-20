package ai.protomolt.proto.workflow;

import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.workflow.WorkflowRunner.FailureKind;
import ai.protomolt.proto.workflow.WorkflowRunner.WorkflowExecutionException;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The pure message checks of a live step and the failure kinds they carry. */
class StepMessagesTest {

    @Test
    void violationsJoinAsPathAndMessageInOrder() {
        ValidationResult result = ValidationResult.failed(List.of(
                new ValidationResult.Violation("a.b", "rule.one", "too short", "x"),
                new ValidationResult.Violation("c", "rule.two", "required", "y")));

        assertThat(StepMessages.summary(result)).isEqualTo("a.b: too short; c: required");
    }

    @Test
    void aContractValueOfAnotherTypeIsAValidationFailureNamingTheSide() {
        assertThatThrownBy(() -> StepMessages.contractValue(Value.getDefaultInstance(),
                Struct.getDescriptor(), "input"))
                .isInstanceOfSatisfying(WorkflowExecutionException.class, e -> {
                    assertThat(e.kind()).isEqualTo(FailureKind.VALIDATION);
                    assertThat(e.step()).isEqualTo("input");
                    assertThat(e.grpcCode()).isNull();
                    assertThat(e.getMessage())
                            .isEqualTo("input does not name the workflow's declared type");
                });
    }

    @Test
    void aContractValueIsRepinnedToTheDeclaredDescriptor() throws Exception {
        Struct value = Struct.newBuilder()
                .putFields("k", Value.newBuilder().setStringValue("v").build())
                .build();

        DynamicMessage pinned = StepMessages.contractValue(value, Struct.getDescriptor(),
                "input");

        assertThat(pinned.getDescriptorForType()).isSameAs(Struct.getDescriptor());
        assertThat(pinned.toByteString()).isEqualTo(value.toByteString());
    }
}
