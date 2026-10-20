package ai.protomolt.proto.grpc.workflow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The fan-out branch identity grammar, parsed without a pattern. */
class StepEvidenceValidationTest {

    @Test
    void aSlugStepNameAHashAndOneToFourDigitsIsABranchId() {
        assertThat(StepEvidenceValidation.isBranchId("enrich#0")).isTrue();
        assertThat(StepEvidenceValidation.isBranchId("enrich-items#1023")).isTrue();
        assertThat(StepEvidenceValidation.isBranchId("enrich#9999")).isTrue();
    }

    @Test
    void theIndexMustBeOneToFourAsciiDigits() {
        assertThat(StepEvidenceValidation.isBranchId("enrich#")).isFalse();
        assertThat(StepEvidenceValidation.isBranchId("enrich#10000")).isFalse();
        assertThat(StepEvidenceValidation.isBranchId("enrich#1a")).isFalse();
        assertThat(StepEvidenceValidation.isBranchId("enrich#-1")).isFalse();
        assertThat(StepEvidenceValidation.isBranchId("enrich#١")).isFalse();
    }

    @Test
    void theStepNameMustBeANonEmptyBoundedSlug() {
        assertThat(StepEvidenceValidation.isBranchId("#1")).isFalse();
        assertThat(StepEvidenceValidation.isBranchId("Enrich#1")).isFalse();
        assertThat(StepEvidenceValidation.isBranchId("enrich")).isFalse();
        assertThat(StepEvidenceValidation.isBranchId("a".repeat(128) + "#1")).isTrue();
        assertThat(StepEvidenceValidation.isBranchId("a".repeat(129) + "#1")).isFalse();
    }
}
