package ai.pipestream.proto.grpc.workflow;

import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.WorkflowStep;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.RunStatus;
import ai.pipestream.proto.grpc.workflow.v1.StepCompletion;
import ai.pipestream.proto.grpc.workflow.v1.StepEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StepStatus;
import ai.pipestream.proto.grpc.workflow.v1.StructuredAttemptEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StructuredGenerationEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StructuredGenerationSpec;
import ai.pipestream.proto.inference.v1.AttemptOutcome;
import ai.pipestream.proto.inference.v1.FinishReason;
import ai.pipestream.proto.inference.v1.Usage;
import ai.pipestream.proto.meta.DescriptorMetadata;
import ai.pipestream.proto.validate.ProtoValidator;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The structured-generation additions to the workflow contract: a step with a
 * {@code structured} spec has no method, rules, gate, deadline, or validation
 * flag, and its evidence carries fingerprints, a bounded attempt history, and
 * token sums that must add up.
 */
class StructuredWorkflowValidationTest {

    private static final String FINGERPRINT = "a".repeat(64);

    private static Workflow.Builder structuredWorkflow() {
        return Workflow.newBuilder()
                .setName("fill-form")
                .setInputType("example.v1.Ticket")
                .addDependencies(TestWorkflows.dependency())
                .addSteps(structuredStep())
                .setDeadline(Duration.newBuilder().setSeconds(30).build());
    }

    private static WorkflowStep.Builder structuredStep() {
        return WorkflowStep.newBuilder()
                .setName("fill")
                .setDependency("nlp")
                .setStructured(StructuredGenerationSpec.newBuilder()
                        .setTargetType("example.v1.IntakeForm")
                        .setModel("toy-structured")
                        .setMaxAttempts(2)
                        .build())
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE);
    }

    private static StructuredAttemptEvidence attempt(int number, AttemptOutcome outcome,
                                                     long promptTokens,
                                                     long completionTokens) {
        return StructuredAttemptEvidence.newBuilder()
                .setAttempt(number)
                .setOutcome(outcome)
                .setUsage(Usage.newBuilder()
                        .setPromptTokens(promptTokens)
                        .setCompletionTokens(completionTokens)
                        .build())
                .setFinishReason(FinishReason.FINISH_REASON_STOP)
                .build();
    }

    private static StructuredGenerationEvidence.Builder evidence() {
        return StructuredGenerationEvidence.newBuilder()
                .setTargetType("example.v1.IntakeForm")
                .setModel("toy-structured")
                .setProvider("scripted")
                .setModelVersion("scripted-v1")
                .setPromptFingerprint(FINGERPRINT)
                .setSchemaFingerprint("b".repeat(64))
                .setValidationPassed(true)
                .setTotalUsage(Usage.newBuilder()
                        .setPromptTokens(20).setCompletionTokens(10).build())
                .addAttempts(attempt(1, AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED, 10, 5))
                .addAttempts(attempt(2, AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED, 10, 5));
    }

    private static RunEvidence.Builder runEvidence(Workflow workflow,
                                                   StructuredGenerationEvidence stepEvidence) {
        return RunEvidence.newBuilder()
                .setRunId("run-structured")
                .setWorkflowName(workflow.getName())
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(20).build())
                .setCompletedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(21).build())
                .addDependencies(TestWorkflows.dependency())
                .setInputArtifact(TestWorkflows.artifact("{\"title\":\"hello\"}", true))
                .addSteps(StepEvidence.newBuilder()
                        .setStepName("fill")
                        .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                        .setStartedAt(com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(20).build())
                        .setCompletedAt(com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(21).build())
                        .setRequestArtifact(TestWorkflows.artifact("{}", true))
                        .setResponseArtifact(TestWorkflows.artifact("{}", true))
                        .setGrpcStatusCode(0)
                        .setStructured(stepEvidence)
                        .build());
    }

    @Test
    void acceptsAStructuredStepAndItsEvidence() {
        Workflow workflow = structuredWorkflow().build();
        assertThatCode(() -> WorkflowValidation.validate(workflow)).doesNotThrowAnyException();
        assertThatCode(() -> WorkflowValidation.validate(runEvidence(workflow, evidence().build())
                .build())).doesNotThrowAnyException();
    }

    @Test
    void rejectsAMethodOnAStructuredStep() {
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep().setMethod("example.v1.Intake/Fill").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("method must be empty");
    }

    @Test
    void rejectsAnEmptyMethodWithoutStructured() {
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep().clearStructured().build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Service/Method");
    }

    @Test
    void rejectsRulesGatesDeadlinesAndValidationFlagsOnStructuredSteps() {
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep().addRules("name=input.title").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no mapping rules");
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep().setWhen("input.title != ''").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gates");
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep()
                        .setDeadline(Duration.newBuilder().setSeconds(5).build()).build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadline");
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep().setValidateResponse(true).build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("validate_response");
    }

    @Test
    void rejectsMalformedStructuredSpecs() {
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep().setStructured(StructuredGenerationSpec
                        .newBuilder().setTargetType("not a type").setModel("m").build()))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("target_type");
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep().setStructured(StructuredGenerationSpec
                        .newBuilder().setTargetType("example.v1.IntakeForm")
                        .setModel(" ").build()))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("model");
        assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                .setSteps(0, structuredStep().setStructured(StructuredGenerationSpec
                        .newBuilder().setTargetType("example.v1.IntakeForm")
                        .setModel("m".repeat(257)).build()))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("model");
        for (int bad : new int[]{-1, 4}) {
            assertThatThrownBy(() -> WorkflowValidation.validate(structuredWorkflow()
                    .setSteps(0, structuredStep().setStructured(StructuredGenerationSpec
                            .newBuilder().setTargetType("example.v1.IntakeForm")
                            .setModel("m").setMaxAttempts(bad).build()))
                    .build()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("max_attempts");
        }
    }

    @Test
    void rejectsAMethodOnStructuredEvidenceAndStructuredEvidenceWithoutOneIsRequired() {
        Workflow workflow = structuredWorkflow().build();
        RunEvidence withMethod = runEvidence(workflow, evidence().build()).build();
        assertThatThrownBy(() -> WorkflowValidation.validate(withMethod.toBuilder()
                .setSteps(0, withMethod.getSteps(0).toBuilder()
                        .setMethod("example.v1.Intake/Fill").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("method must be empty");

        RunEvidence withoutStructured = runEvidence(workflow, evidence().build()).build();
        assertThatThrownBy(() -> WorkflowValidation.validate(withoutStructured.toBuilder()
                .setSteps(0, withoutStructured.getSteps(0).toBuilder()
                        .clearStructured().build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Service/Method");
    }

    @Test
    void rejectsMalformedFingerprintsInStructuredEvidence() {
        Workflow workflow = structuredWorkflow().build();
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence().setPromptFingerprint("not-a-hash").build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prompt_fingerprint");
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence().setSchemaFingerprint("Z".repeat(64)).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema_fingerprint");
        // The declared annotations alone carry both fingerprint formats: this pins
        // the pattern-to-sha256_hex conversion independently of the hand checks.
        assertThat(ai.pipestream.proto.validate.ProtoValidator.create()
                .validate(evidence()
                        .setPromptFingerprint("not-a-hash")
                        .setSchemaFingerprint("Z".repeat(64))
                        .build())
                .violations())
                .filteredOn(v -> v.ruleId().equals("string.sha256_hex"))
                .hasSize(2);
    }

    @Test
    void rejectsInconsistentAttemptHistories() {
        Workflow workflow = structuredWorkflow().build();
        // More than three attempts; the declared annotations refuse first.
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence()
                        .addAttempts(attempt(3, AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED, 10, 5))
                        .addAttempts(attempt(4, AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED, 10, 5))
                        .build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 3 items");
        // Non-sequential numbering.
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence().setAttempts(1, attempt(3,
                        AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED, 10, 5)).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequentially numbered");
        // Undefined outcome; the annotation's required rule refuses the zero value.
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence().setAttempts(0, attempt(1,
                        AttemptOutcome.ATTEMPT_OUTCOME_UNSPECIFIED, 10, 5)).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outcome");
        // Total usage that is not the per-attempt sum.
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence().setTotalUsage(Usage.newBuilder()
                        .setPromptTokens(999).setCompletionTokens(10).build()).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("total_usage");
        // A succeeded step whose last attempt did not succeed.
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence().setAttempts(1, attempt(2,
                        AttemptOutcome.ATTEMPT_OUTCOME_VALIDATION_FAILED, 10, 5)).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("last attempt");
        // A succeeded step claiming validation failed.
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence().setValidationPassed(false).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("validation_passed");
    }

    @Test
    void aFailedStructuredStepMayRecordNoAttempts() {
        Workflow workflow = structuredWorkflow().build();
        RunEvidence failed = runEvidence(workflow, evidence()
                        .clearAttempts()
                        .setTotalUsage(Usage.getDefaultInstance())
                        .setValidationPassed(false)
                        .build())
                .setStatus(RunStatus.RUN_STATUS_FAILED)
                .build();
        RunEvidence failedStep = failed.toBuilder()
                .setSteps(0, failed.getSteps(0).toBuilder()
                        .setStatus(StepStatus.STEP_STATUS_FAILED)
                        .clearResponseArtifact()
                        .build())
                .build();
        assertThatCode(() -> WorkflowValidation.validate(failedStep)).doesNotThrowAnyException();
    }

    @Test
    void persistedStructuredFieldsCarryValidationAndSensitivityMetadata() {
        for (Descriptor descriptor : List.of(
                StructuredGenerationSpec.getDescriptor(),
                StructuredGenerationEvidence.getDescriptor(),
                StructuredAttemptEvidence.getDescriptor())) {
            assertThat(DescriptorMetadata.message(descriptor)).isPresent()
                    .get().extracting(meta -> meta.getSensitivity())
                    .isEqualTo("internal");
            assertThat(descriptor.getFields()).allSatisfy(field ->
                    assertThat(DescriptorMetadata.field(field)).isPresent()
                            .get().extracting(meta -> meta.getSensitivity())
                            .isEqualTo("internal"));
        }

        var invalid = ProtoValidator.create().validate(StructuredGenerationSpec.newBuilder()
                .setTargetType("not a type")
                .setModel("m".repeat(257))
                .setMaxAttempts(4)
                .build());
        assertThat(invalid.valid()).isFalse();
        assertThat(invalid.violations()).extracting(v -> v.path())
                .contains("target_type", "model", "max_attempts");
    }

    @Test
    void rejectsNegativeOverflowingAndImpossibleAttemptEvidence() {
        Workflow workflow = structuredWorkflow().build();

        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence().setAttempts(0, attempt(1,
                        AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED, -1, 5))
                        .setTotalUsage(Usage.newBuilder()
                                .setPromptTokens(-1).setCompletionTokens(10))
                        .build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");

        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence()
                        .setAttempts(0, attempt(1,
                                AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED, 10, 5))
                        .build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only the last");

        StructuredAttemptEvidence huge = attempt(1,
                AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED, Long.MAX_VALUE, 5);
        assertThatThrownBy(() -> WorkflowValidation.validate(runEvidence(workflow,
                evidence()
                        .setAttempts(0, huge)
                        .setAttempts(1, attempt(2,
                                AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED, 1, 5))
                        .setTotalUsage(Usage.newBuilder()
                                .setPromptTokens(Long.MAX_VALUE).setCompletionTokens(10))
                        .build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflows int64");
    }
}
