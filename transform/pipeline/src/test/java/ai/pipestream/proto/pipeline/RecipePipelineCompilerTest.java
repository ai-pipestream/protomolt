package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.chain.ChainDefinition;
import ai.pipestream.proto.chain.ChainRecipeCompiler;
import ai.pipestream.proto.chain.ChainVerifier;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RecipeStep;
import ai.pipestream.proto.grpc.recipe.v1.ServiceDependency;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End to end, fully offline: existing chains compile into recipes with the landed
 * {@link ChainRecipeCompiler}, recipes compile into pipelines, and the pipelines verify
 * against the same descriptors. The typed edge and fan-out cross the boundary byte-for-byte,
 * compilation is deterministic, and every broken reference fails fast with its name.
 */
class RecipePipelineCompilerTest {

    private final PipelineChecker checker = new PipelineChecker();

    private static MethodDescriptor method(String service, String name) {
        return PipelineFixtures.file().findServiceByName(service).findMethodByName(name);
    }

    @Test
    void chainCompilesToCheckingPipeline() {
        ChainDefinition chain = new ChainDefinition("fetch-flow", PipelineFixtures.files(),
                PipelineFixtures.type(PipelineFixtures.TICKET), 30_000,
                List.of(ChainDefinition.Step.grpc("fetch", "inprocess:lookup", false,
                        method("Lookup", "Fetch"), null, List.of("title = input.title"),
                        List.of(), true, 0, "")),
                null);
        assertThat(new ChainVerifier().verify(chain)).isEmpty();

        GrpcRecipe recipe = ChainRecipeCompiler.compile(chain);
        Pipeline pipeline = RecipePipelineCompiler.compile(recipe, PipelineFixtures.files());

        assertThat(pipeline.getSourceRecipeName()).isEqualTo("fetch-flow");
        assertThat(pipeline.getSourceRecipeFingerprint())
                .isEqualTo(RecipeValidation.fingerprint(recipe));
        assertThat(pipeline.getDescriptorFingerprint())
                .isEqualTo(PipelineFixtures.fingerprint());
        assertThat(pipeline.getStepsList()).singleElement().satisfies(step -> {
            // The top-level rule lane is normalized into an explicit edge over the scope.
            assertThat(step.getGrpcCall().getMethod())
                    .isEqualTo(PipelineFixtures.FETCH);
            assertThat(step.getGrpcCall().getMethodShape())
                    .isEqualTo(MethodShape.METHOD_SHAPE_UNARY);
            assertThat(step.getGrpcCall().getEdge().getSourcesList())
                    .containsExactly("input");
            assertThat(step.getGrpcCall().getEdge().getProduceType())
                    .isEqualTo(PipelineFixtures.TICKET);
            assertThat(step.getGrpcCall().getEdge().getRulesList())
                    .containsExactly("title = input.title");
            assertThat(step.getGrpcCall().getEdgeCardinality())
                    .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
            assertThat(step.getGrpcCall().getOutputCardinality())
                    .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
            assertThat(step.getGrpcCall().getValidateResponse()).isTrue();
        });
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void compilationIsDeterministic() {
        GrpcRecipe recipe = recipeWithSteps(RecipeStep.newBuilder()
                .setName("fetch")
                .setDependency("pipeline.test.Lookup")
                .setMethod(PipelineFixtures.FETCH)
                .addRules("title = input.title")
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        Pipeline first = RecipePipelineCompiler.compile(recipe, PipelineFixtures.files());
        Pipeline second = RecipePipelineCompiler.compile(recipe, PipelineFixtures.files());
        assertThat(first).isEqualTo(second);
        assertThat(first.toByteArray()).isEqualTo(second.toByteArray());
    }

    @Test
    void typedEdgeAndFanOutCrossByteForByte() {
        var edgeSpec = new ChainDefinition.EdgeSpec(List.of("input"),
                PipelineFixtures.type(PipelineFixtures.BATCH), List.of(), List.of(), null,
                false);
        var fanOutSpec = new ChainDefinition.FanOutSpec("items", 10, 4,
                ChainDefinition.BranchFailurePolicy.CONTINUE,
                PipelineFixtures.type(PipelineFixtures.TICKET_BOX), "tickets");
        ChainDefinition.Step step = new ChainDefinition.Step("process",
                "inprocess:worker", false, method("Worker", "Process"), null, List.of(),
                List.of(), false, 0, "", null, edgeSpec, fanOutSpec);
        ChainDefinition chain = new ChainDefinition("fanout-flow", PipelineFixtures.files(),
                PipelineFixtures.type(PipelineFixtures.BATCH), 30_000, List.of(step), null);
        assertThat(new ChainVerifier().verify(chain)).isEmpty();

        GrpcRecipe recipe = ChainRecipeCompiler.compile(chain);
        Pipeline pipeline = RecipePipelineCompiler.compile(recipe, PipelineFixtures.files());

        PipelineStep compiled = pipeline.getSteps(0);
        RecipeStep recipeStep = recipe.getSteps(0);
        assertThat(compiled.getGrpcCall().getEdge()).isEqualTo(recipeStep.getEdge());
        assertThat(compiled.getGrpcCall().getFanOut()).isEqualTo(recipeStep.getFanOut());
        assertThat(compiled.getGrpcCall().getOutputCardinality())
                .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void structuredChainCompilesToCheckingPipeline() {
        ChainDefinition chain = new ChainDefinition("structured-flow",
                PipelineFixtures.files(), PipelineFixtures.type(PipelineFixtures.TICKET),
                30_000,
                List.of(ChainDefinition.Step.grpc("fetch", "inprocess:lookup", false,
                                method("Lookup", "Fetch"), null,
                                List.of("title = input.title"), List.of(), false, 0, ""),
                        ChainDefinition.Step.structured("summarize",
                                PipelineFixtures.type(PipelineFixtures.SUMMARY),
                                "test-model", 1)),
                null);
        assertThat(new ChainVerifier().verify(chain)).isEmpty();

        GrpcRecipe recipe = ChainRecipeCompiler.compile(chain);
        Pipeline pipeline = RecipePipelineCompiler.compile(recipe, PipelineFixtures.files());

        PipelineStep summarize = pipeline.getSteps(1);
        assertThat(summarize.getStructured().getSpec().getTargetType())
                .isEqualTo(PipelineFixtures.SUMMARY);
        assertThat(summarize.getDependency()).isEqualTo("structured-generation");
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void streamingRecipeCompilesAndVerifies() {
        // A hand-authored recipe may name a server-streaming method; the compiler
        // declares the shape and the MANY output binding from the descriptor.
        GrpcRecipe recipe = recipeWithSteps(RecipeStep.newBuilder()
                .setName("search")
                .setDependency("pipeline.test.Search")
                .setMethod(PipelineFixtures.STREAM)
                .addRules("title = input.title")
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        Pipeline pipeline = RecipePipelineCompiler.compile(recipe, PipelineFixtures.files());
        assertThat(pipeline.getSteps(0).getGrpcCall().getMethodShape())
                .isEqualTo(MethodShape.METHOD_SHAPE_SERVER_STREAMING);
        assertThat(pipeline.getSteps(0).getGrpcCall().getOutputCardinality())
                .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_MANY);
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void streamFedUnaryRecipeCompilesButFailsChecking() {
        // The compiler translates faithfully and declares the stream edge; the checker
        // is the gate that rejects the missing collect.
        GrpcRecipe recipe = recipeWithSteps(
                RecipeStep.newBuilder()
                        .setName("search")
                        .setDependency("pipeline.test.Search")
                        .setMethod(PipelineFixtures.STREAM)
                        .addRules("title = input.title")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build(),
                RecipeStep.newBuilder()
                        .setName("fetch")
                        .setDependency("pipeline.test.Lookup")
                        .setMethod(PipelineFixtures.FETCH)
                        .addRules("title = search.title")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build());
        Pipeline pipeline = RecipePipelineCompiler.compile(recipe, PipelineFixtures.files());
        assertThat(pipeline.getSteps(1).getGrpcCall().getEdgeCardinality())
                .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_MANY);
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("collect");
                });
    }

    @Test
    void consumedRecipeStreamDoesNotLeakIntoLaterSynthesizedEdges() {
        GrpcRecipe recipe = recipeWithSteps(
                RecipeStep.newBuilder()
                        .setName("search")
                        .setDependency("pipeline.test.Search")
                        .setMethod(PipelineFixtures.STREAM)
                        .addRules("title = input.title")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build(),
                RecipeStep.newBuilder()
                        .setName("upload")
                        .setDependency("pipeline.test.Ingest")
                        .setMethod(PipelineFixtures.UPLOAD)
                        .setEdge(PipelineFixtures.edge(PipelineFixtures.TICKET,
                                List.of("search"), "title = search.title"))
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build(),
                RecipeStep.newBuilder()
                        .setName("fetch")
                        .setDependency("pipeline.test.Lookup")
                        .setMethod(PipelineFixtures.FETCH)
                        .addRules("title = input.title")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build());

        Pipeline pipeline = RecipePipelineCompiler.compile(recipe,
                PipelineFixtures.files());
        assertThat(pipeline.getSteps(2).getGrpcCall().getEdge().getSourcesList())
                .containsExactly("input", "upload");
        assertThat(pipeline.getSteps(2).getGrpcCall().getEdgeCardinality())
                .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void dependencyFingerprintMismatchFailsFast() {
        GrpcRecipe recipe = recipeWithSteps(RecipeStep.newBuilder()
                .setName("fetch")
                .setDependency("pipeline.test.Lookup")
                .setMethod(PipelineFixtures.FETCH)
                .addRules("title = input.title")
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        GrpcRecipe tampered = recipe.toBuilder()
                .setDependencies(0, recipe.getDependencies(0).toBuilder()
                        .setDescriptorFingerprint(
                                "1111111111111111111111111111111111111111111111111111111111111111")
                        .build())
                .build();
        assertThatThrownBy(() -> RecipePipelineCompiler.compile(tampered,
                PipelineFixtures.files()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pipeline.test.Lookup")
                .hasMessageContaining(PipelineFixtures.fingerprint());
    }

    @Test
    void unresolvedMethodFailsFastWithFileList() {
        GrpcRecipe recipe = recipeWithSteps(RecipeStep.newBuilder()
                .setName("fetch")
                .setDependency("pipeline.test.Lookup")
                .setMethod("pipeline.test.Lookup/Nope")
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        assertThatThrownBy(() -> RecipePipelineCompiler.compile(recipe,
                PipelineFixtures.files()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pipeline.test.Lookup/Nope")
                .hasMessageContaining("pipeline/test/pipeline.proto");
    }

    @Test
    void nonIdentifierStepNameFailsFast() {
        GrpcRecipe recipe = recipeWithSteps(RecipeStep.newBuilder()
                .setName("my-step")
                .setDependency("pipeline.test.Lookup")
                .setMethod(PipelineFixtures.FETCH)
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        assertThatThrownBy(() -> RecipePipelineCompiler.compile(recipe,
                PipelineFixtures.files()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-step")
                .hasMessageContaining("scope identifier");
    }

    @Test
    void invalidRecipeIsRejectedBeforeCompilation() {
        assertThatThrownBy(() -> RecipePipelineCompiler.compile(GrpcRecipe.getDefaultInstance(),
                PipelineFixtures.files()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recipe.name");
    }

    private static GrpcRecipe recipeWithSteps(RecipeStep... steps) {
        GrpcRecipe.Builder recipe = GrpcRecipe.newBuilder()
                .setName("authored-recipe")
                .setInputType(PipelineFixtures.TICKET)
                .setDeadline(Duration.newBuilder().setSeconds(30).build());
        java.util.Set<String> services = new java.util.LinkedHashSet<>();
        for (RecipeStep step : steps) {
            String service = step.getMethod().isEmpty()
                    ? step.getDependency()
                    : step.getMethod().substring(0, step.getMethod().indexOf('/'));
            if (services.add(service)) {
                recipe.addDependencies(ServiceDependency.newBuilder()
                        .setAlias(service)
                        .setServiceProfile(service)
                        .setEndpoint("local")
                        .setDescriptorFingerprint(PipelineFixtures.fingerprint())
                        .build());
            }
            recipe.addSteps(step);
        }
        GrpcRecipe built = recipe.build();
        RecipeValidation.validate(built);
        return built;
    }
}
