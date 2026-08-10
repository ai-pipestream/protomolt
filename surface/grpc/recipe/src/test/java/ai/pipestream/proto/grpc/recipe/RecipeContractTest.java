package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecipeContractTest {

    @Test
    void recipeAndEvidenceRoundTripWithoutEmbeddingArtifactsOrCredentials() throws Exception {
        GrpcRecipe recipe = TestRecipes.recipe();
        RunEvidence evidence = TestRecipes.evidence();

        assertThat(GrpcRecipe.parseFrom(recipe.toByteArray())).isEqualTo(recipe);
        assertThat(RunEvidence.parseFrom(evidence.toByteArray())).isEqualTo(evidence);
        assertThat(recipe.getDependencies(0).getDescriptorFingerprint()).hasSize(64);
        assertThat(GrpcRecipe.getDescriptor().getFields())
                .extracting(field -> field.getName())
                .doesNotContain("password", "secret", "credential", "private_key", "artifact_bytes");
        assertThat(RunEvidence.getDescriptor().getFields())
                .extracting(field -> field.getName())
                .doesNotContain("request_bytes", "response_bytes", "artifact_bytes");
        assertThat(ArtifactReference.getDescriptor().findFieldByName("content")).isNull();
    }

    @Test
    void recipeFingerprintChangesWithExecutableContent() {
        GrpcRecipe original = TestRecipes.recipe();
        GrpcRecipe changed = original.toBuilder()
                .setSteps(0, original.getSteps(0).toBuilder()
                        .setMethod("example.v1.Tokenizer/TokenizeStream").build())
                .build();

        assertThat(RecipeValidation.fingerprint(original))
                .isNotEqualTo(RecipeValidation.fingerprint(changed));
    }
}
