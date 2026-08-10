package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RecipeStep;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.grpc.recipe.v1.RunStatus;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.grpc.recipe.v1.StepEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StepStatus;
import ai.pipestream.proto.grpc.recipe.v1.StructuredAttemptEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StructuredGenerationEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StructuredGenerationSpec;
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
 * The structured-generation additions to the recipe contract: a step with a
 * {@code structured} spec has no method, rules, gate, deadline, or validation
 * flag, and its evidence carries fingerprints, a bounded attempt history, and
 * token sums that must add up.
 */
class StructuredRecipeValidationTest {

    private static final String FINGERPRINT = "a".repeat(64);

    private static GrpcRecipe.Builder structuredRecipe() {
        return GrpcRecipe.newBuilder()
                .setName("fill-form")
                .setInputType("example.v1.Ticket")
                .addDependencies(TestRecipes.dependency())
                .addSteps(structuredStep())
                .setDeadline(Duration.newBuilder().setSeconds(30).build());
    }

    private static RecipeStep.Builder structuredStep() {
        return RecipeStep.newBuilder()
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

    private static RunEvidence.Builder runEvidence(GrpcRecipe recipe,
                                                   StructuredGenerationEvidence stepEvidence) {
        return RunEvidence.newBuilder()
                .setRunId("run-structured")
                .setRecipeName(recipe.getName())
                .setRecipeFingerprint(RecipeValidation.fingerprint(recipe))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(20).build())
                .setCompletedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(21).build())
                .addDependencies(TestRecipes.dependency())
                .setInputArtifact(TestRecipes.artifact("{\"title\":\"hello\"}", true))
                .addSteps(StepEvidence.newBuilder()
                        .setStepName("fill")
                        .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                        .setStartedAt(com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(20).build())
                        .setCompletedAt(com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(21).build())
                        .setRequestArtifact(TestRecipes.artifact("{}", true))
                        .setResponseArtifact(TestRecipes.artifact("{}", true))
                        .setGrpcStatusCode(0)
                        .setStructured(stepEvidence)
                        .build());
    }

    @Test
    void acceptsAStructuredStepAndItsEvidence() {
        GrpcRecipe recipe = structuredRecipe().build();
        assertThatCode(() -> RecipeValidation.validate(recipe)).doesNotThrowAnyException();
        assertThatCode(() -> RecipeValidation.validate(runEvidence(recipe, evidence().build())
                .build())).doesNotThrowAnyException();
    }

    @Test
    void rejectsAMethodOnAStructuredStep() {
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep().setMethod("example.v1.Intake/Fill").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("method must be empty");
    }

    @Test
    void rejectsAnEmptyMethodWithoutStructured() {
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep().clearStructured().build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Service/Method");
    }

    @Test
    void rejectsRulesGatesDeadlinesAndValidationFlagsOnStructuredSteps() {
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep().addRules("name=input.title").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no mapping rules");
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep().setWhen("input.title != ''").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gates");
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep()
                        .setDeadline(Duration.newBuilder().setSeconds(5).build()).build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadline");
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep().setValidateResponse(true).build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("validate_response");
    }

    @Test
    void rejectsMalformedStructuredSpecs() {
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep().setStructured(StructuredGenerationSpec
                        .newBuilder().setTargetType("not a type").setModel("m").build()))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("target_type");
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep().setStructured(StructuredGenerationSpec
                        .newBuilder().setTargetType("example.v1.IntakeForm")
                        .setModel(" ").build()))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("model");
        assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
                .setSteps(0, structuredStep().setStructured(StructuredGenerationSpec
                        .newBuilder().setTargetType("example.v1.IntakeForm")
                        .setModel("m".repeat(257)).build()))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("model");
        for (int bad : new int[]{-1, 4}) {
            assertThatThrownBy(() -> RecipeValidation.validate(structuredRecipe()
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
        GrpcRecipe recipe = structuredRecipe().build();
        RunEvidence withMethod = runEvidence(recipe, evidence().build()).build();
        assertThatThrownBy(() -> RecipeValidation.validate(withMethod.toBuilder()
                .setSteps(0, withMethod.getSteps(0).toBuilder()
                        .setMethod("example.v1.Intake/Fill").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("method must be empty");

        RunEvidence withoutStructured = runEvidence(recipe, evidence().build()).build();
        assertThatThrownBy(() -> RecipeValidation.validate(withoutStructured.toBuilder()
                .setSteps(0, withoutStructured.getSteps(0).toBuilder()
                        .clearStructured().build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Service/Method");
    }

    @Test
    void rejectsMalformedFingerprintsInStructuredEvidence() {
        GrpcRecipe recipe = structuredRecipe().build();
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence().setPromptFingerprint("not-a-hash").build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prompt_fingerprint");
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence().setSchemaFingerprint("Z".repeat(64)).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema_fingerprint");
    }

    @Test
    void rejectsInconsistentAttemptHistories() {
        GrpcRecipe recipe = structuredRecipe().build();
        // More than three attempts.
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence()
                        .addAttempts(attempt(3, AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED, 10, 5))
                        .addAttempts(attempt(4, AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED, 10, 5))
                        .build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempts exceeds");
        // Non-sequential numbering.
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence().setAttempts(1, attempt(3,
                        AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED, 10, 5)).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequentially numbered");
        // Undefined outcome.
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence().setAttempts(0, attempt(1,
                        AttemptOutcome.ATTEMPT_OUTCOME_UNSPECIFIED, 10, 5)).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("defined");
        // Total usage that is not the per-attempt sum.
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence().setTotalUsage(Usage.newBuilder()
                        .setPromptTokens(999).setCompletionTokens(10).build()).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("total_usage");
        // A succeeded step whose last attempt did not succeed.
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence().setAttempts(1, attempt(2,
                        AttemptOutcome.ATTEMPT_OUTCOME_VALIDATION_FAILED, 10, 5)).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("last attempt");
        // A succeeded step claiming validation failed.
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence().setValidationPassed(false).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("validation_passed");
    }

    @Test
    void aFailedStructuredStepMayRecordNoAttempts() {
        GrpcRecipe recipe = structuredRecipe().build();
        RunEvidence failed = runEvidence(recipe, evidence()
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
        assertThatCode(() -> RecipeValidation.validate(failedStep)).doesNotThrowAnyException();
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
        GrpcRecipe recipe = structuredRecipe().build();

        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence().setAttempts(0, attempt(1,
                        AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED, -1, 5))
                        .setTotalUsage(Usage.newBuilder()
                                .setPromptTokens(-1).setCompletionTokens(10))
                        .build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");

        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
                evidence()
                        .setAttempts(0, attempt(1,
                                AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED, 10, 5))
                        .build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only the last");

        StructuredAttemptEvidence huge = attempt(1,
                AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED, Long.MAX_VALUE, 5);
        assertThatThrownBy(() -> RecipeValidation.validate(runEvidence(recipe,
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
