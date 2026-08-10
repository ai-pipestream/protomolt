package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.grpc.recipe.v1.RunStatus;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.grpc.recipe.v1.VersionedRecipe;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecipeValidationTest {

    @Test
    void acceptsRecipePromotionArtifactsAndRunEvidence() {
        RecipeValidation.validate(TestRecipes.recipe());
        RecipeValidation.validate(TestRecipes.versionedRecipe());
        RecipeValidation.validate(TestRecipes.artifact("{}", true));
        RecipeValidation.validate(TestRecipes.evidence());
    }

    @Test
    void rejectsUnsafeIdentityMissingDependencyAndUnspecifiedCompletion() {
        assertThatThrownBy(() -> RecipeValidation.validate(TestRecipes.recipe().toBuilder()
                .setName("../escape").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path-safe");

        assertThatThrownBy(() -> RecipeValidation.validate(TestRecipes.recipe().toBuilder()
                .setSteps(0, TestRecipes.recipe().getSteps(0).toBuilder()
                        .setDependency("missing").build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not declared");

        assertThatThrownBy(() -> RecipeValidation.validate(TestRecipes.recipe().toBuilder()
                .setSteps(0, TestRecipes.recipe().getSteps(0).toBuilder()
                        .setCompletion(StepCompletion.STEP_COMPLETION_UNSPECIFIED).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("completion");
    }

    @Test
    void rejectsChangedRecipeContentUnderAnExistingFingerprint() {
        VersionedRecipe promoted = TestRecipes.versionedRecipe();
        GrpcRecipe changed = promoted.getRecipe().toBuilder().setDescription("changed").build();

        assertThatThrownBy(() -> RecipeValidation.validate(promoted.toBuilder()
                .setRecipe(changed).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void rejectsInvalidArtifactIdentityMediaTypeAndBounds() {
        ArtifactReference artifact = TestRecipes.artifact("{}", true);
        RecipeValidation.validate(artifact.toBuilder().setSizeBytes(0).build());
        assertThatThrownBy(() -> RecipeValidation.validate(artifact.toBuilder()
                .setSha256("not-a-hash").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");
        assertThatThrownBy(() -> RecipeValidation.validate(artifact.toBuilder()
                .setMediaType("not a media type").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type/subtype");
        assertThatThrownBy(() -> RecipeValidation.validate(artifact.toBuilder()
                .setSizeBytes(RecipeValidation.MAX_ARTIFACT_BYTES + 1L).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum");
        assertThatThrownBy(() -> RecipeValidation.validate(artifact.toBuilder()
                .setSizeBytes(-1L).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum");
    }

    @Test
    void rejectsTerminalRunWithoutCompletionAndReversedTimestamps() {
        RunEvidence evidence = TestRecipes.evidence();
        assertThatThrownBy(() -> RecipeValidation.validate(evidence.toBuilder()
                .clearCompletedAt().build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("completed_at must be present");
        assertThatThrownBy(() -> RecipeValidation.validate(evidence.toBuilder()
                .setStartedAt(Timestamp.newBuilder().setSeconds(30).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not precede");
    }

    @Test
    void permitsRunningEvidenceWithoutCompletionButRejectsUnknownStatus() {
        RecipeValidation.validate(TestRecipes.evidence().toBuilder()
                .setStatus(RunStatus.RUN_STATUS_RUNNING)
                .clearCompletedAt()
                .build());

        assertThatThrownBy(() -> RecipeValidation.validate(TestRecipes.evidence().toBuilder()
                .setStatusValue(99).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("status must be recognized");
    }
}
