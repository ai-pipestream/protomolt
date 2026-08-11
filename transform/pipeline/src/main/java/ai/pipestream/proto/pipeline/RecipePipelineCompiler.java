package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RecipeStep;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.GrpcCallStep;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineOutput;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import ai.pipestream.proto.pipeline.v1.StructuredStep;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Compiles a validated {@link GrpcRecipe} into the streaming-aware {@link Pipeline} contract
 * against the exact descriptor set the recipe was checked against. Compilation is the
 * typed-edge normalization of a recipe: a step that already carries an edge keeps it
 * unchanged, and a step whose request is mapped by top-level rules gains a synthesized edge
 * reading the full scope at that point ({@code input} plus every prior step name), which is
 * exactly the scope the chain verifier and runner use. Every gRPC step's streaming shape and
 * both cardinalities are declared from the method descriptor's streaming flags.
 *
 * <p>Compilation is deterministic and fail-fast: the recipe is validated first, every method
 * and type reference must resolve against the supplied files, every dependency fingerprint
 * must match the supplied descriptor set, and the result passes {@link PipelineValidation}
 * before it is returned. Compilation never executes or contacts anything; whether the
 * resulting pipeline is shape-correct is the {@link PipelineChecker}'s question.</p>
 */
public final class RecipePipelineCompiler {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final int DEFAULT_MAX_STREAM_MESSAGES = 10_000;

    private RecipePipelineCompiler() {
    }

    /**
     * Compiles {@code recipe} into a validated pipeline.
     *
     * @param files the exact descriptor set the recipe was checked against; its canonical
     *        fingerprint must equal every dependency's declared fingerprint
     * @throws IllegalArgumentException when the recipe is invalid, a reference does not
     *        resolve, a dependency fingerprints a different descriptor set, or a step name
     *        is not a scope identifier
     */
    public static Pipeline compile(GrpcRecipe recipe, List<FileDescriptor> files) {
        Objects.requireNonNull(recipe, "recipe");
        Objects.requireNonNull(files, "files");
        RecipeValidation.validate(recipe);
        if (files.isEmpty()) {
            throw new IllegalArgumentException(
                    "a pipeline compiles against a non-empty descriptor set");
        }
        String fingerprint = DescriptorSets.fingerprint(files);
        for (var dependency : recipe.getDependenciesList()) {
            if (!dependency.getDescriptorFingerprint().equals(fingerprint)) {
                throw new IllegalArgumentException("dependency '" + dependency.getAlias()
                        + "' fingerprints descriptor set "
                        + dependency.getDescriptorFingerprint()
                        + " but the supplied files fingerprint " + fingerprint
                        + "; supply the exact descriptors the recipe was checked against");
            }
        }
        DescriptorRegistry registry = DescriptorRegistry.create(false);
        for (FileDescriptor file : files) {
            registry.registerFile(file);
        }
        resolveType(registry, recipe.getInputType(), "recipe.input_type");

        List<PipelineStep> steps = new ArrayList<>(recipe.getStepsCount());
        // The scope as the recipe builds it: input plus each step's binding with the
        // cardinality the step produces. Recipes are unary in practice, but a
        // hand-authored recipe may name a streaming method; synthesized edges then
        // declare the cardinality the scope actually carries.
        Map<String, EdgeCardinality> scope = new LinkedHashMap<>();
        scope.put("input", EdgeCardinality.EDGE_CARDINALITY_ONE);
        for (RecipeStep step : recipe.getStepsList()) {
            if (!IDENTIFIER.matcher(step.getName()).matches()
                    || step.getName().equals("input") || step.getName().equals("target")) {
                throw new IllegalArgumentException("step name '" + step.getName()
                        + "' is not a scope identifier; pipeline step names become mapping"
                        + " scope variables other than 'input'/'target'");
            }
            PipelineStep compiled = step.hasStructured()
                    ? compileStructured(step, registry)
                    : compileGrpc(step, files, registry, scope);
            steps.add(compiled);
            if (compiled.hasGrpcCall()) {
                GrpcCallStep call = compiled.getGrpcCall();
                boolean consumesStream = (call.getMethodShape()
                        == MethodShape.METHOD_SHAPE_CLIENT_STREAMING
                        || call.getMethodShape()
                        == MethodShape.METHOD_SHAPE_BIDI_STREAMING)
                        && call.getEdgeCardinality()
                        == EdgeCardinality.EDGE_CARDINALITY_MANY
                        && !call.hasFanOut();
                if (consumesStream) {
                    List<String> streamSources = call.getEdge().getSourcesList().stream()
                            .filter(source -> scope.get(source)
                                    == EdgeCardinality.EDGE_CARDINALITY_MANY)
                            .toList();
                    // More than one stream has no declared join policy and the checker
                    // rejects it. Preserve those bindings so later findings describe the
                    // same invalid dataflow instead of pretending it was consumed.
                    if (streamSources.size() == 1) {
                        scope.remove(streamSources.get(0));
                    }
                }
            }
            scope.put(step.getName(), compiled.hasGrpcCall()
                    ? compiled.getGrpcCall().getOutputCardinality()
                    : EdgeCardinality.EDGE_CARDINALITY_ONE);
        }

        Pipeline.Builder pipeline = Pipeline.newBuilder()
                .setName(recipe.getName())
                .setDescription(recipe.getDescription())
                .setInputType(recipe.getInputType())
                .setDescriptorFingerprint(fingerprint)
                .addAllDependencies(recipe.getDependenciesList())
                .addAllSteps(steps)
                .setDeadline(recipe.getDeadline())
                .setMaxStreamMessages(DEFAULT_MAX_STREAM_MESSAGES)
                .setSourceRecipeName(recipe.getName())
                .setSourceRecipeFingerprint(RecipeValidation.fingerprint(recipe));
        if (recipe.hasOutput()) {
            resolveType(registry, recipe.getOutput().getType(), "recipe.output.type");
            pipeline.setOutput(PipelineOutput.newBuilder()
                    .setType(recipe.getOutput().getType())
                    .addAllRules(recipe.getOutput().getRulesList())
                    .addAllCelRules(recipe.getOutput().getCelRulesList())
                    .build());
        }
        Pipeline built = pipeline.build();
        // The contract speaks last, exactly as in the recipe compiler.
        PipelineValidation.validate(built);
        return built;
    }

    /** The streaming shape a method descriptor declares through its streaming flags. */
    public static MethodShape shapeOf(MethodDescriptor method) {
        Objects.requireNonNull(method, "method");
        if (method.isClientStreaming()) {
            return method.isServerStreaming()
                    ? MethodShape.METHOD_SHAPE_BIDI_STREAMING
                    : MethodShape.METHOD_SHAPE_CLIENT_STREAMING;
        }
        return method.isServerStreaming()
                ? MethodShape.METHOD_SHAPE_SERVER_STREAMING
                : MethodShape.METHOD_SHAPE_UNARY;
    }

    private static PipelineStep compileGrpc(RecipeStep step, List<FileDescriptor> files,
                                            DescriptorRegistry registry,
                                            Map<String, EdgeCardinality> scope) {
        MethodDescriptor method = DescriptorSets.resolveMethod(files, step.getMethod());
        if (method == null) {
            throw new IllegalArgumentException("method '" + step.getMethod()
                    + "' not found in the descriptor set (files: "
                    + DescriptorSets.fileNames(files) + ")");
        }
        MethodShape shape = shapeOf(method);
        boolean external = step.getCompletion() == StepCompletion.STEP_COMPLETION_EXTERNAL;
        boolean streamInScope = scope.containsValue(EdgeCardinality.EDGE_CARDINALITY_MANY);

        GrpcCallStep.Builder call = GrpcCallStep.newBuilder()
                .setMethod(step.getMethod())
                .setMethodShape(shape)
                .setValidateResponse(step.getValidateResponse())
                .setCompletion(step.getCompletion())
                .setOutputCardinality(
                        !step.hasFanOut() && method.isServerStreaming()
                                ? EdgeCardinality.EDGE_CARDINALITY_MANY
                                : EdgeCardinality.EDGE_CARDINALITY_ONE);
        if (step.hasEdge()) {
            // The edge is embedded unchanged: one meaning across recipes and pipelines.
            resolveEdgeTypes(registry, step.getEdge(), "step '" + step.getName() + "'");
            call.setEdge(step.getEdge());
            call.setEdgeCardinality(step.getEdge().getSourcesList().stream()
                    .anyMatch(source -> scope.get(source)
                            == EdgeCardinality.EDGE_CARDINALITY_MANY)
                    ? EdgeCardinality.EDGE_CARDINALITY_MANY
                    : EdgeCardinality.EDGE_CARDINALITY_ONE);
        } else if (!external) {
            // Normalize the top-level rule lane into an explicit edge over the full
            // scope: identical semantics, one dataflow model.
            call.setEdge(ai.pipestream.proto.grpc.recipe.v1.TypedEdge.newBuilder()
                    .addAllSources(scope.keySet())
                    .setProduceType(method.getInputType().getFullName())
                    .addAllRules(step.getRulesList())
                    .addAllCelRules(step.getCelRulesList())
                    .setValidate(false)
                    .build());
            call.setEdgeCardinality(streamInScope
                    ? EdgeCardinality.EDGE_CARDINALITY_MANY
                    : EdgeCardinality.EDGE_CARDINALITY_ONE);
        }
        if (step.hasFanOut()) {
            resolveType(registry, step.getFanOut().getCollectType(),
                    "step.fan_out.collect_type");
            call.setFanOut(step.getFanOut());
        }

        PipelineStep.Builder builder = PipelineStep.newBuilder()
                .setName(step.getName())
                .setDependency(step.getDependency())
                .setDeadline(step.getDeadline())
                .setGrpcCall(call.build());
        if (!step.getWhen().isBlank()) {
            builder.setWhen(step.getWhen());
        }
        return builder.build();
    }

    private static PipelineStep compileStructured(RecipeStep step,
                                                  DescriptorRegistry registry) {
        resolveType(registry, step.getStructured().getTargetType(),
                "step.structured.target_type");
        StructuredStep.Builder structured = StructuredStep.newBuilder()
                .setSpec(step.getStructured());
        if (step.hasEdge()) {
            resolveEdgeTypes(registry, step.getEdge(), "step '" + step.getName() + "'");
            structured.setEdge(step.getEdge());
        }
        if (step.hasFanOut()) {
            resolveType(registry, step.getFanOut().getCollectType(),
                    "step.fan_out.collect_type");
            structured.setFanOut(step.getFanOut());
        }
        return PipelineStep.newBuilder()
                .setName(step.getName())
                .setDependency(step.getDependency())
                .setStructured(structured.build())
                .build();
    }

    /** Fails fast when an edge's type references dangle against the descriptor set. */
    private static void resolveEdgeTypes(DescriptorRegistry registry,
                                         ai.pipestream.proto.grpc.recipe.v1.TypedEdge edge,
                                         String owner) {
        resolveType(registry, edge.getProduceType(), owner + " edge.produce_type");
        if (!edge.getProjectTo().isEmpty()) {
            resolveType(registry, edge.getProjectTo(), owner + " edge.project_to");
        }
    }

    private static Descriptor resolveType(DescriptorRegistry registry, String fullName,
                                          String field) {
        Descriptor descriptor = registry.findDescriptorByFullName(fullName);
        if (descriptor == null) {
            throw new IllegalArgumentException(field + " type '" + fullName
                    + "' does not resolve against the descriptor set");
        }
        return descriptor;
    }
}
