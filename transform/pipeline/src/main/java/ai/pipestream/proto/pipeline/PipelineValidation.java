package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.pipeline.v1.CollectStep;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.GrpcCallStep;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineOutput;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import ai.pipestream.proto.pipeline.v1.StructuredStep;
import ai.pipestream.proto.pipeline.v1.UnnestStep;

import java.util.HashSet;
import java.util.Set;

/**
 * Structural and safety validation for the pipeline contract, mirroring the validate.v1
 * annotations on {@code pipeline.proto} the way {@link RecipeValidation} mirrors the recipe
 * contract. Descriptor-grounded shape, cardinality, and mapping checks are the
 * {@link PipelineChecker}'s job; this class only guarantees a pipeline is well-formed enough
 * to store and to check.
 */
public final class PipelineValidation {

    /** Maximum serialized pipeline size. */
    public static final int MAX_PIPELINE_BYTES = 1024 * 1024;

    private static final int MAX_DEPENDENCIES = 64;
    private static final int MAX_STEPS = 256;
    private static final int MAX_RULES = 1_024;

    private PipelineValidation() {
    }

    /** Validates a compiled or hand-authored pipeline. */
    public static void validate(Pipeline pipeline) {
        require(pipeline != null, "pipeline must not be null");
        require(pipeline.getSerializedSize() <= MAX_PIPELINE_BYTES,
                "pipeline exceeds the maximum serialized size of "
                        + MAX_PIPELINE_BYTES + " bytes");
        RecipeValidation.validateName(pipeline.getName(), "pipeline.name");
        RecipeValidation.validateText(pipeline.getDescription(), "pipeline.description");
        RecipeValidation.validateType(pipeline.getInputType(), "pipeline.input_type");
        RecipeValidation.validateFingerprint(pipeline.getDescriptorFingerprint(),
                "pipeline.descriptor_fingerprint");
        require(pipeline.getDependenciesCount() > 0,
                "pipeline.dependencies must not be empty");
        require(pipeline.getDependenciesCount() <= MAX_DEPENDENCIES,
                "pipeline.dependencies exceeds the maximum of " + MAX_DEPENDENCIES);
        Set<String> aliases = new HashSet<>();
        for (var dependency : pipeline.getDependenciesList()) {
            RecipeValidation.validate(dependency);
            require(aliases.add(dependency.getAlias()),
                    "duplicate dependency alias: " + dependency.getAlias());
            require(dependency.getDescriptorFingerprint()
                            .equals(pipeline.getDescriptorFingerprint()),
                    "dependency '" + dependency.getAlias()
                            + "' fingerprints a different descriptor set than the pipeline");
        }
        require(pipeline.getStepsCount() > 0, "pipeline.steps must not be empty");
        require(pipeline.getStepsCount() <= MAX_STEPS,
                "pipeline.steps exceeds the maximum of " + MAX_STEPS);
        Set<String> stepNames = new HashSet<>();
        for (PipelineStep step : pipeline.getStepsList()) {
            validate(step, aliases);
            require(stepNames.add(step.getName()),
                    "duplicate step name: " + step.getName());
        }
        if (pipeline.hasOutput()) {
            validate(pipeline.getOutput());
        }
        RecipeValidation.validatePositiveDuration(pipeline.getDeadline(),
                "pipeline.deadline");
        require(pipeline.getSourceRecipeName().isEmpty()
                        == pipeline.getSourceRecipeFingerprint().isEmpty(),
                "pipeline.source_recipe_name and pipeline.source_recipe_fingerprint must "
                        + "be set together");
        if (!pipeline.getSourceRecipeName().isEmpty()) {
            RecipeValidation.validateName(pipeline.getSourceRecipeName(),
                    "pipeline.source_recipe_name");
            RecipeValidation.validateFingerprint(pipeline.getSourceRecipeFingerprint(),
                    "pipeline.source_recipe_fingerprint");
        }
    }

    /** Validates one pipeline step, dispatching on its kind. */
    public static void validate(PipelineStep step) {
        validate(step, null);
    }

    private static void validate(PipelineStep step, Set<String> aliases) {
        require(step != null, "step must not be null");
        RecipeValidation.validateName(step.getName(), "step.name");
        if (!step.getDependency().isEmpty()) {
            RecipeValidation.validateName(step.getDependency(), "step.dependency");
            require(aliases == null || aliases.contains(step.getDependency()),
                    "step dependency is not declared: " + step.getDependency());
        }
        RecipeValidation.validateText(step.getWhen(), "step.when");
        RecipeValidation.validateDuration(step.getDeadline(), "step.deadline");
        switch (step.getKindCase()) {
            case GRPC_CALL -> validate(step.getGrpcCall());
            case STRUCTURED -> validate(step.getStructured());
            case UNNEST -> validate(step.getUnnest());
            case COLLECT -> validate(step.getCollect());
            default -> require(false, "step.kind must be set");
        }
        require(step.getKindCase() == PipelineStep.KindCase.GRPC_CALL
                        || step.getWhen().isBlank(),
                "step.when is allowed on gRPC steps only");
        require(step.getKindCase() == PipelineStep.KindCase.GRPC_CALL
                        || step.getKindCase() == PipelineStep.KindCase.STRUCTURED
                        || step.getDependency().isEmpty(),
                "unnest and collect steps declare no dependency; they call no service");
    }

    /** Validates a gRPC call step. */
    public static void validate(GrpcCallStep call) {
        require(call != null, "grpc_call must not be null");
        RecipeValidation.validateMethod(call.getMethod(), "step.grpc_call.method");
        require(defined(call.getMethodShape()) && call.getMethodShape()
                        != MethodShape.METHOD_SHAPE_UNSPECIFIED,
                "step.grpc_call.method_shape must be a declared streaming shape");
        require(defined(call.getEdgeCardinality()),
                "step.grpc_call.edge_cardinality must be a defined value");
        require(defined(call.getOutputCardinality())
                        && call.getOutputCardinality()
                                != EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED,
                "step.grpc_call.output_cardinality must be ONE or MANY");
        require(call.getCompletion()
                        == ai.pipestream.proto.grpc.recipe.v1.StepCompletion
                                .STEP_COMPLETION_LIVE
                        || call.getCompletion()
                                == ai.pipestream.proto.grpc.recipe.v1.StepCompletion
                                        .STEP_COMPLETION_EXTERNAL,
                "step.grpc_call.completion must be live or external");
        boolean external = call.getCompletion()
                == ai.pipestream.proto.grpc.recipe.v1.StepCompletion
                        .STEP_COMPLETION_EXTERNAL;
        if (external) {
            require(!call.hasEdge() && !call.hasFanOut(),
                    "external-completion steps carry no edge and no fan-out; the "
                            + "completion lane owns their request");
            require(call.getEdgeCardinality()
                            == EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED,
                    "step.grpc_call.edge_cardinality must be UNSPECIFIED when the step "
                            + "carries no edge");
        } else {
            require(call.hasEdge(),
                    "a live gRPC step requires an edge; the compiler synthesizes one "
                            + "from a recipe step's top-level rules");
            require(call.getEdgeCardinality()
                            != EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED,
                    "step.grpc_call.edge_cardinality must be ONE or MANY when the step "
                            + "carries an edge");
        }
        if (call.hasEdge()) {
            RecipeValidation.validate(call.getEdge());
        }
        if (call.hasFanOut()) {
            require(call.hasEdge(),
                    "step.grpc_call.fan_out requires step.grpc_call.edge; the items "
                            + "resolve against the edge's produced message");
            RecipeValidation.validate(call.getFanOut());
        }
    }

    /** Validates a structured-generation step. */
    public static void validate(StructuredStep structured) {
        require(structured != null, "structured must not be null");
        require(structured.hasSpec(), "step.structured.spec must be present");
        RecipeValidation.validate(structured.getSpec());
        if (structured.hasEdge()) {
            RecipeValidation.validate(structured.getEdge());
        }
        if (structured.hasFanOut()) {
            require(structured.hasEdge(),
                    "step.structured.fan_out requires step.structured.edge; the items "
                            + "resolve against the edge's produced message");
            RecipeValidation.validate(structured.getFanOut());
        }
    }

    /** Validates an unnest step. */
    public static void validate(UnnestStep unnest) {
        require(unnest != null, "unnest must not be null");
        require(unnest.getSource().matches("[A-Za-z_][A-Za-z0-9_]*"),
                "step.unnest.source must be an identifier");
        require(!unnest.getPath().isBlank()
                        && unnest.getPath().length() <= 512
                        && unnest.getPath().codePoints().noneMatch(Character::isWhitespace),
                "step.unnest.path must be a non-blank dotted field path of at most "
                        + "512 characters");
    }

    /** Validates a collect step. */
    public static void validate(CollectStep collect) {
        require(collect != null, "collect must not be null");
        require(collect.getSource().matches("[A-Za-z_][A-Za-z0-9_]*"),
                "step.collect.source must be an identifier");
        RecipeValidation.validateType(collect.getCollectType(), "step.collect.collect_type");
        require(!collect.getCollectInto().isBlank()
                        && collect.getCollectInto().length() <= 256
                        && collect.getCollectInto().codePoints()
                                .noneMatch(Character::isWhitespace),
                "step.collect.collect_into must be a non-blank field name of at most "
                        + "256 characters");
    }

    /** Validates a pipeline output projection. */
    public static void validate(PipelineOutput output) {
        require(output != null, "output must not be null");
        RecipeValidation.validateType(output.getType(), "output.type");
        require(output.getRulesCount() <= MAX_RULES,
                "output.rules exceeds the maximum of " + MAX_RULES);
        require(output.getCelRulesCount() <= MAX_RULES,
                "output.cel_rules exceeds the maximum of " + MAX_RULES);
    }

    private static boolean defined(EdgeCardinality cardinality) {
        return cardinality != EdgeCardinality.UNRECOGNIZED;
    }

    private static boolean defined(MethodShape shape) {
        return shape != MethodShape.UNRECOGNIZED;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
