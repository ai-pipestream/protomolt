package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import ai.pipestream.proto.pipeline.v1.CollectStep;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.GrpcCallStep;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineOutput;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import ai.pipestream.proto.pipeline.v1.StructuredStep;
import ai.pipestream.proto.pipeline.v1.UnnestStep;

import ai.pipestream.format.Formats;

import java.util.HashSet;
import java.util.Set;

/**
 * Structural and safety validation for the pipeline contract, mirroring the validate.v1
 * annotations on {@code pipeline.proto} the way {@link WorkflowValidation} mirrors the workflow
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
    private static final int MAX_IDENTIFIER_LENGTH = 128;
    private static final int MAX_STREAM_MESSAGES = 100_000;

    private PipelineValidation() {
    }

    /** Validates a compiled or hand-authored pipeline. */
    public static void validate(Pipeline pipeline) {
        require(pipeline != null, "pipeline must not be null");
        require(pipeline.getSerializedSize() <= MAX_PIPELINE_BYTES,
                "pipeline exceeds the maximum serialized size of "
                        + MAX_PIPELINE_BYTES + " bytes");
        WorkflowValidation.validateName(pipeline.getName(), "pipeline.name");
        WorkflowValidation.validateText(pipeline.getDescription(), "pipeline.description");
        WorkflowValidation.validateType(pipeline.getInputType(), "pipeline.input_type");
        WorkflowValidation.validateFingerprint(pipeline.getDescriptorFingerprint(),
                "pipeline.descriptor_fingerprint");
        require(pipeline.getDependenciesCount() > 0,
                "pipeline.dependencies must not be empty");
        require(pipeline.getDependenciesCount() <= MAX_DEPENDENCIES,
                "pipeline.dependencies exceeds the maximum of " + MAX_DEPENDENCIES);
        Set<String> aliases = new HashSet<>();
        for (var dependency : pipeline.getDependenciesList()) {
            WorkflowValidation.validate(dependency);
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
        WorkflowValidation.validatePositiveDuration(pipeline.getDeadline(),
                "pipeline.deadline");
        require(pipeline.getMaxStreamMessages() >= 1
                        && pipeline.getMaxStreamMessages() <= MAX_STREAM_MESSAGES,
                "pipeline.max_stream_messages must be between 1 and "
                        + MAX_STREAM_MESSAGES);
        require(pipeline.getSourceWorkflowName().isEmpty()
                        == pipeline.getSourceWorkflowFingerprint().isEmpty(),
                "pipeline.source_workflow_name and pipeline.source_workflow_fingerprint must "
                        + "be set together");
        if (!pipeline.getSourceWorkflowName().isEmpty()) {
            WorkflowValidation.validateName(pipeline.getSourceWorkflowName(),
                    "pipeline.source_workflow_name");
            WorkflowValidation.validateFingerprint(pipeline.getSourceWorkflowFingerprint(),
                    "pipeline.source_workflow_fingerprint");
        }
    }

    /** Validates one pipeline step, dispatching on its kind. */
    public static void validate(PipelineStep step) {
        validate(step, null);
    }

    private static void validate(PipelineStep step, Set<String> aliases) {
        require(step != null, "step must not be null");
        require(identifier(step.getName())
                        && !step.getName().equals("input")
                        && !step.getName().equals("target"),
                "step.name must be an identifier other than 'input'/'target'");
        if (!step.getDependency().isEmpty()) {
            WorkflowValidation.validateReference(step.getDependency(), "step.dependency");
            require(aliases == null || aliases.contains(step.getDependency()),
                    "step dependency is not declared: " + step.getDependency());
        }
        WorkflowValidation.validateText(step.getWhen(), "step.when");
        WorkflowValidation.validateDuration(step.getDeadline(), "step.deadline");
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
        WorkflowValidation.validateMethod(call.getMethod(), "step.grpc_call.method");
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
                        == ai.pipestream.proto.grpc.workflow.v1.StepCompletion
                                .STEP_COMPLETION_LIVE
                        || call.getCompletion()
                                == ai.pipestream.proto.grpc.workflow.v1.StepCompletion
                                        .STEP_COMPLETION_EXTERNAL,
                "step.grpc_call.completion must be live or external");
        boolean external = call.getCompletion()
                == ai.pipestream.proto.grpc.workflow.v1.StepCompletion
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
                            + "from a workflow step's top-level rules");
            require(call.getEdgeCardinality()
                            != EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED,
                    "step.grpc_call.edge_cardinality must be ONE or MANY when the step "
                            + "carries an edge");
        }
        if (call.hasEdge()) {
            WorkflowValidation.validate(call.getEdge());
        }
        if (call.hasFanOut()) {
            require(call.hasEdge(),
                    "step.grpc_call.fan_out requires step.grpc_call.edge; the items "
                            + "resolve against the edge's produced message");
            WorkflowValidation.validate(call.getFanOut());
        }
    }

    /** Validates a structured-generation step. */
    public static void validate(StructuredStep structured) {
        require(structured != null, "structured must not be null");
        require(structured.hasSpec(), "step.structured.spec must be present");
        WorkflowValidation.validate(structured.getSpec());
        if (structured.hasEdge()) {
            WorkflowValidation.validate(structured.getEdge());
        }
        if (structured.hasFanOut()) {
            require(structured.hasEdge(),
                    "step.structured.fan_out requires step.structured.edge; the items "
                            + "resolve against the edge's produced message");
            WorkflowValidation.validate(structured.getFanOut());
        }
    }

    /** Validates an unnest step. */
    public static void validate(UnnestStep unnest) {
        require(unnest != null, "unnest must not be null");
        require(identifier(unnest.getSource()),
                "step.unnest.source must be an identifier");
        require(unnest.getPath().length() <= 512
                        && Formats.isProtobufFqn(unnest.getPath()),
                "step.unnest.path must be a non-blank dotted field path of at most "
                        + "512 characters");
    }

    /** Validates a collect step. */
    public static void validate(CollectStep collect) {
        require(collect != null, "collect must not be null");
        require(identifier(collect.getSource()),
                "step.collect.source must be an identifier");
        WorkflowValidation.validateType(collect.getCollectType(), "step.collect.collect_type");
        require(collect.getCollectInto().length() <= 256
                        && bareIdentifier(collect.getCollectInto()),
                "step.collect.collect_into must be a non-blank field name of at most "
                        + "256 characters");
    }

    /** Validates a pipeline output projection. */
    public static void validate(PipelineOutput output) {
        require(output != null, "output must not be null");
        WorkflowValidation.validateType(output.getType(), "output.type");
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

    private static boolean identifier(String value) {
        return value.length() <= MAX_IDENTIFIER_LENGTH && bareIdentifier(value);
    }

    /** A single dotless protobuf identifier: the FQN parser confined to one segment. */
    private static boolean bareIdentifier(String value) {
        return value.indexOf('.') < 0 && Formats.isProtobufFqn(value);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
