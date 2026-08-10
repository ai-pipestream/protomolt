package ai.pipestream.proto.registry;

import ai.pipestream.proto.grpc.recipe.RecipeRepository;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RecipeStep;
import ai.pipestream.proto.grpc.recipe.v1.ServiceDependency;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.grpc.recipe.v1.VersionedRecipe;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Recipe promotion against the git-backed registry: versions are immutable, idempotent to
 * re-promote, recoverable by version across store instances, and invalid or corrupt content
 * is rejected rather than served.
 */
class RecipePromotionTest {

    private static final String FINGERPRINT = "a".repeat(64);

    private static VersionedRecipe versioned(String version) {
        GrpcRecipe recipe = GrpcRecipe.newBuilder()
                .setName("analyze-document")
                .setInputType("example.v1.Document")
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("nlp")
                        .setServiceProfile("tokenizer")
                        .setEndpoint("local")
                        .setDescriptorFingerprint(FINGERPRINT)
                        .build())
                .addSteps(RecipeStep.newBuilder()
                        .setName("tokenize")
                        .setDependency("nlp")
                        .setMethod("example.v1.Tokenizer/Tokenize")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build())
                .setDeadline(Duration.newBuilder().setSeconds(30).build())
                .build();
        return VersionedRecipe.newBuilder()
                .setRecipe(recipe)
                .setVersion(version)
                .setRecipeFingerprint(RecipeValidation.fingerprint(recipe))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(10).build())
                .build();
    }

    private static GitSchemaRegistryStore store(Path dir) {
        return GitSchemaRegistryStore.builder().repositoryDir(dir).build();
    }

    @Test
    void promoteFindAndListRoundTrip(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            RecipeRepository repository = new RegistryRecipeRepository(git);
            VersionedRecipe v1 = versioned("v1");
            VersionedRecipe v2 = versioned("v2");

            repository.save(v1);
            repository.save(v2);

            assertThat(repository.find("analyze-document", "v1")).contains(v1);
            assertThat(repository.versions("analyze-document"))
                    .containsExactly(v1, v2);
            assertThat(git.recipeNames()).containsExactly("analyze-document");
            assertThat(repository.find("analyze-document", "v9")).isEmpty();
            assertThat(repository.versions("nobody")).isEmpty();
        }
    }

    @Test
    void rePromotionIsIdempotentButDifferentContentIsRefused(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            RecipeRepository repository = new RegistryRecipeRepository(git);
            repository.save(versioned("v1"));
            repository.save(versioned("v1")); // identical bytes: a no-op

            assertThat(repository.versions("analyze-document")).hasSize(1);

            VersionedRecipe altered = VersionedRecipe.newBuilder(versioned("v1"))
                    .setCreatedAt(Timestamp.newBuilder().setSeconds(11).build())
                    .build();
            assertThatThrownBy(() -> repository.save(altered))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("immutable");
            assertThat(repository.find("analyze-document", "v1")).contains(versioned("v1"));
        }
    }

    @Test
    void invalidContentIsRejectedBeforeItTouchesTheRepository(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            RecipeRepository repository = new RegistryRecipeRepository(git);
            VersionedRecipe broken = VersionedRecipe.newBuilder(versioned("v1"))
                    .setRecipeFingerprint("b".repeat(64)) // no longer matches the content
                    .build();

            assertThatThrownBy(() -> repository.save(broken))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fingerprint");
            assertThat(repository.find("analyze-document", "v1")).isEmpty();
            assertThat(git.recipeNames()).isEmpty();
        }
    }

    @Test
    void promotionSurvivesStoreRestart(@TempDir Path dir) throws Exception {
        VersionedRecipe v1 = versioned("v1");
        try (GitSchemaRegistryStore git = store(dir)) {
            new RegistryRecipeRepository(git).save(v1);
        }

        try (GitSchemaRegistryStore reopened = store(dir)) {
            RecipeRepository repository = new RegistryRecipeRepository(reopened);
            assertThat(repository.find("analyze-document", "v1")).contains(v1);
            assertThat(repository.versions("analyze-document")).containsExactly(v1);
        }
    }

    @Test
    void corruptStoredBytesFailLoudly(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            RecipeRepository repository = new RegistryRecipeRepository(git);
            repository.save(versioned("v1"));

            Path stored = dir.resolve("recipes/analyze-document/v1.pb");
            Files.write(stored, new byte[]{1, 2, 3});

            assertThatThrownBy(() -> repository.find("analyze-document", "v1"))
                    .isInstanceOf(RegistryStoreException.class)
                    .hasMessageContaining("analyze-document");
        }
    }

    @Test
    void identitiesStayPathSafe(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            assertThatThrownBy(() -> git.recipe("../escape", "v1"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> git.recipeVersions("not safe!"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
