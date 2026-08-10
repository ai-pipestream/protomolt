package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.profile.ServiceProfileValidation;
import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RecipeStep;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.grpc.recipe.v1.RunStatus;
import ai.pipestream.proto.grpc.recipe.v1.ServiceDependency;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.grpc.recipe.v1.StepEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StepStatus;
import ai.pipestream.proto.grpc.recipe.v1.VersionedRecipe;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

final class TestRecipes {

    static final String DESCRIPTOR_FINGERPRINT = ServiceProfileValidation.sha256(
            "descriptor".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    private TestRecipes() {
    }

    static GrpcRecipe recipe() {
        return GrpcRecipe.newBuilder()
                .setName("analyze-document")
                .setDescription("Tokenize a document through a registered service")
                .setInputType("example.v1.Document")
                .addDependencies(dependency())
                .addSteps(RecipeStep.newBuilder()
                        .setName("tokenize")
                        .setDependency("nlp")
                        .setMethod("example.v1.Tokenizer/Tokenize")
                        .addRules("text=input.body")
                        .setValidateResponse(true)
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build())
                .setDeadline(Duration.newBuilder().setSeconds(30).build())
                .build();
    }

    static ServiceDependency dependency() {
        return ServiceDependency.newBuilder()
                .setAlias("nlp")
                .setServiceProfile("tokenizer")
                .setEndpoint("local")
                .setDescriptorFingerprint(DESCRIPTOR_FINGERPRINT)
                .build();
    }

    static ArtifactReference artifact(String content, boolean redacted) {
        byte[] bytes = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return ArtifactReference.newBuilder()
                .setSha256(ServiceProfileValidation.sha256(bytes))
                .setMediaType("application/json")
                .setSizeBytes(bytes.length)
                .setRedacted(redacted)
                .build();
    }

    static VersionedRecipe versionedRecipe() {
        GrpcRecipe recipe = recipe();
        return VersionedRecipe.newBuilder()
                .setRecipe(recipe)
                .setVersion("v1")
                .setRecipeFingerprint(RecipeValidation.fingerprint(recipe))
                .setCreatedAt(timestamp(10))
                .build();
    }

    static RunEvidence evidence() {
        return RunEvidence.newBuilder()
                .setRunId("run-001")
                .setRecipeName(recipe().getName())
                .setRecipeVersion("v1")
                .setRecipeFingerprint(RecipeValidation.fingerprint(recipe()))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(timestamp(20))
                .setCompletedAt(timestamp(21))
                .addDependencies(dependency())
                .setInputArtifact(artifact("{\"body\":\"hello\"}", true))
                .setOutputArtifact(artifact("{\"tokens\":[\"hello\"]}", true))
                .addSteps(StepEvidence.newBuilder()
                        .setStepName("tokenize")
                        .setMethod("example.v1.Tokenizer/Tokenize")
                        .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                        .setStartedAt(timestamp(20))
                        .setCompletedAt(timestamp(21))
                        .setRequestArtifact(artifact("{\"text\":\"hello\"}", true))
                        .setResponseArtifact(artifact("{\"tokens\":[\"hello\"]}", true))
                        .setGrpcStatusCode(0)
                        .build())
                .build();
    }

    private static Timestamp timestamp(long seconds) {
        return Timestamp.newBuilder().setSeconds(seconds).build();
    }
}
