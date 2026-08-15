package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.grpc.workflow.v1.StepCompletion;
import ai.pipestream.proto.pipeline.v1.CollectStep;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Structural validation of the pipeline contract: the same fail-fast, field-naming style as
 * the workflow contract's validation, covering the invariants the descriptor checker assumes.
 */
class PipelineValidationTest {

    private static Pipeline validPipeline() {
        return PipelineFixtures.base("valid", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .build();
    }

    @Test
    void validPipelinePasses() {
        assertThatCode(() -> PipelineValidation.validate(validPipeline()))
                .doesNotThrowAnyException();
    }

    @Test
    void stepNameMustMatchTheContractIdentifierPattern() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setSteps(0, validPipeline().getSteps(0).toBuilder()
                        .setName("not-a-scope-variable")
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step.name")
                .hasMessageContaining("identifier");
    }

    @Test
    void stepNameMustRespectTheContractLengthBound() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setSteps(0, validPipeline().getSteps(0).toBuilder()
                        .setName("a".repeat(129))
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step.name")
                .hasMessageContaining("identifier");
    }

    @Test
    void unnestPathMustMatchTheContractDottedPathPattern() {
        assertThatThrownBy(() -> PipelineValidation.validate(
                ai.pipestream.proto.pipeline.v1.UnnestStep.newBuilder()
                        .setSource("input")
                        .setPath("items..value")
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step.unnest.path");
    }

    @Test
    void collectFieldMustMatchTheContractIdentifierPattern() {
        assertThatThrownBy(() -> PipelineValidation.validate(
                CollectStep.newBuilder()
                        .setSource("stream")
                        .setCollectType(PipelineFixtures.RESULTS)
                        .setCollectInto("results.value")
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step.collect.collect_into");
    }

    @Test
    void liveGrpcStepWithoutEdgeIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setSteps(0, validPipeline().getSteps(0).toBuilder()
                        .setGrpcCall(validPipeline().getSteps(0).getGrpcCall().toBuilder()
                                .clearEdge()
                                .setEdgeCardinality(
                                        EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED)
                                .build())
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires an edge");
    }

    @Test
    void externalStepWithEdgeIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setSteps(0, validPipeline().getSteps(0).toBuilder()
                        .setGrpcCall(validPipeline().getSteps(0).getGrpcCall().toBuilder()
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                                .build())
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("external");
    }

    @Test
    void unspecifiedOutputCardinalityIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setSteps(0, validPipeline().getSteps(0).toBuilder()
                        .setGrpcCall(validPipeline().getSteps(0).getGrpcCall().toBuilder()
                                .setOutputCardinality(
                                        EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED)
                                .build())
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("output_cardinality");
    }

    @Test
    void sourceWorkflowProvenanceMustPair() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setSourceWorkflowName("some-workflow")
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source_workflow_name")
                .hasMessageContaining("set together");
    }

    @Test
    void dependencyFingerprintMustMatchPipeline() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setDescriptorFingerprint(
                        "2222222222222222222222222222222222222222222222222222222222222222")
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pipeline.test.Lookup")
                .hasMessageContaining("different descriptor set");
    }

    @Test
    void undeclaredDependencyAliasIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setSteps(0, validPipeline().getSteps(0).toBuilder()
                        .setDependency("pipeline.test.Missing")
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not declared: pipeline.test.Missing");
    }

    @Test
    void whenGateOnNonGrpcStepIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .addSteps(PipelineStep.newBuilder()
                        .setName("gather")
                        .setWhen("true")
                        .setCollect(CollectStep.newBuilder()
                                .setSource("fetch")
                                .setCollectType(PipelineFixtures.RESULTS)
                                .setCollectInto("results")
                                .build())
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("when")
                .hasMessageContaining("gRPC steps only");
    }

    @Test
    void dependencyOnDataflowStepIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .addSteps(PipelineStep.newBuilder()
                        .setName("gather")
                        .setDependency("pipeline.test.Lookup")
                        .setCollect(CollectStep.newBuilder()
                                .setSource("fetch")
                                .setCollectType(PipelineFixtures.RESULTS)
                                .setCollectInto("results")
                                .build())
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("call no service");
    }

    @Test
    void missingKindIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .addSteps(PipelineStep.newBuilder().setName("empty").build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step.kind must be set");
    }

    @Test
    void invalidMethodFormIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .setSteps(0, validPipeline().getSteps(0).toBuilder()
                        .setGrpcCall(validPipeline().getSteps(0).getGrpcCall().toBuilder()
                                .setMethod("no-slash")
                                .build())
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Service/Method");
    }

    @Test
    void structuredStepWithoutSpecIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .addSteps(PipelineStep.newBuilder()
                        .setName("gen")
                        .setDependency("pipeline.test.Lookup")
                        .setStructured(ai.pipestream.proto.pipeline.v1.StructuredStep
                                .getDefaultInstance())
                        .build())
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("spec must be present");
    }

    @Test
    void zeroDeadlineIsRejected() {
        Pipeline pipeline = validPipeline().toBuilder()
                .clearDeadline()
                .build();
        assertThatThrownBy(() -> PipelineValidation.validate(pipeline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadline");
    }

    @Test
    void streamMaterializationLimitIsRequiredAndBounded() {
        assertThatThrownBy(() -> PipelineValidation.validate(validPipeline().toBuilder()
                .setMaxStreamMessages(0).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max_stream_messages")
                .hasMessageContaining("between 1 and 100000");
        assertThatThrownBy(() -> PipelineValidation.validate(validPipeline().toBuilder()
                .setMaxStreamMessages(100_001).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max_stream_messages")
                .hasMessageContaining("between 1 and 100000");
    }
}
