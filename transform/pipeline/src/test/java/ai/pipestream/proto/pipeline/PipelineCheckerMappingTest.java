package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.grpc.workflow.v1.CelMappingRule;
import ai.pipestream.proto.grpc.workflow.v1.StepCompletion;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.GrpcCallStep;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mapping, projection, and fan-out rejections: the checker runs every rule through the same
 * scoped rule checker the workflow verifier uses, and every failure names the step, the kind
 * of problem, and the rule or field path involved.
 */
class PipelineCheckerMappingTest {

    private final PipelineChecker checker = new PipelineChecker();

    private static Pipeline.Builder unaryBase() {
        return PipelineFixtures.base("map", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"));
    }

    private static Pipeline pipelineWithEdge(
            ai.pipestream.proto.grpc.workflow.v1.TypedEdge edge) {
        return unaryBase()
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE, edge))
                .build();
    }

    @Test
    void unknownEdgeSourceIsRejected() {
        Pipeline pipeline = pipelineWithEdge(PipelineFixtures.edge(PipelineFixtures.TICKET,
                List.of("nosuch"), "title = nosuch.title"));
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("edge");
                    assertThat(finding.error()).contains("'nosuch'")
                            .contains("not 'input' or a prior step");
                });
    }

    @Test
    void unresolvableTargetPathIsRejected() {
        Pipeline pipeline = pipelineWithEdge(PipelineFixtures.edge(PipelineFixtures.TICKET,
                List.of("input"), "bogus = input.title"));
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("edge");
                    assertThat(finding.error()).contains("bogus = input.title");
                });
    }

    @Test
    void invalidCelSelectorIsRejected() {
        var edge = PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"))
                .toBuilder()
                .addCelRules(CelMappingRule.newBuilder()
                        .setSelector("input.nosuch_field")
                        .setTarget("title")
                        .build())
                .build();
        Pipeline pipeline = pipelineWithEdge(edge);
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("edge");
                    assertThat(finding.error()).contains("title");
                });
    }

    @Test
    void produceTypeMismatchIsRejectedWithRequestTypeAndFile() {
        Pipeline pipeline = pipelineWithEdge(
                PipelineFixtures.edge(PipelineFixtures.LOOKUP_RESULT, List.of("input"),
                        "doc_id = input.title"));
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("edge");
                    assertThat(finding.error()).contains("pipeline.test.Ticket")
                            .contains("pipeline/test/pipeline.proto")
                            .contains("pipeline.test.LookupResult");
                });
    }

    @Test
    void projectionOnPlainGrpcStepIsRejected() {
        var edge = PipelineFixtures.edge(PipelineFixtures.LOOKUP_RESULT, List.of("input"),
                        "doc_id = input.title")
                .toBuilder()
                .setProjectTo(PipelineFixtures.GROUNDING)
                .build();
        Pipeline pipeline = pipelineWithEdge(edge);
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("edge");
                    assertThat(finding.error()).contains("no projection");
                });
    }

    @Test
    void unsupportedProjectionSourceIsRejected() {
        var fanOut = ai.pipestream.proto.grpc.workflow.v1.FanOutSpec.newBuilder()
                .setItems("items")
                .setMaxItems(10)
                .setMaxConcurrency(4)
                .setFailurePolicy(ai.pipestream.proto.grpc.workflow.v1.BranchFailurePolicy
                        .BRANCH_FAILURE_POLICY_FAIL_FAST)
                .setCollectType(PipelineFixtures.TICKET_BOX)
                .setCollectInto("tickets")
                .build();
        // The grounding projection reads LookupResult, never a Ticket item.
        var edge = PipelineFixtures.edge(PipelineFixtures.BATCH, List.of("input"))
                .toBuilder()
                .setProjectTo(PipelineFixtures.GROUNDING)
                .build();
        Pipeline pipeline = unaryBase()
                .addSteps(PipelineStep.newBuilder()
                        .setName("process")
                        .setDependency("pipeline.test.Worker")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.PROCESS)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(edge)
                                .setFanOut(fanOut)
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("process");
                    assertThat(finding.kind()).isEqualTo("edge");
                    assertThat(finding.error()).contains(PipelineFixtures.GROUNDING)
                            .contains("does not support")
                            .contains("pipeline.test.Ticket");
                });
    }

    @Test
    void fanOutItemsPathThroughMissingFieldIsRejected() {
        Pipeline pipeline = fanOutPipeline("nosuch");
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("process");
                    assertThat(finding.kind()).isEqualTo("fanOut");
                    assertThat(finding.error()).contains("nosuch")
                            .contains("pipeline.test.Batch");
                });
    }

    @Test
    void fanOutItemsPathEndingAtSingularFieldIsRejected() {
        // Ticket.title is singular: a fan-out items path must end at a repeated field.
        var fanOut = ai.pipestream.proto.grpc.workflow.v1.FanOutSpec.newBuilder()
                .setItems("title")
                .setMaxItems(10)
                .setMaxConcurrency(4)
                .setFailurePolicy(ai.pipestream.proto.grpc.workflow.v1.BranchFailurePolicy
                        .BRANCH_FAILURE_POLICY_FAIL_FAST)
                .setCollectType(PipelineFixtures.TICKET_BOX)
                .setCollectInto("tickets")
                .build();
        Pipeline pipeline = PipelineFixtures.base("map", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(PipelineStep.newBuilder()
                        .setName("process")
                        .setDependency("pipeline.test.Worker")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.PROCESS)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(PipelineFixtures.edge(PipelineFixtures.TICKET,
                                        List.of("input")))
                                .setFanOut(fanOut)
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("process");
                    assertThat(finding.kind()).isEqualTo("fanOut");
                    assertThat(finding.error()).contains("non-repeated");
                });
    }

    @Test
    void fanOutCollectElementMismatchIsRejected() {
        var fanOut = ai.pipestream.proto.grpc.workflow.v1.FanOutSpec.newBuilder()
                .setItems("items")
                .setMaxItems(10)
                .setMaxConcurrency(4)
                .setFailurePolicy(ai.pipestream.proto.grpc.workflow.v1.BranchFailurePolicy
                        .BRANCH_FAILURE_POLICY_CONTINUE)
                .setCollectType(PipelineFixtures.RESULTS)
                .setCollectInto("results")
                .build();
        Pipeline pipeline = fanOutPipeline(fanOut);
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("process");
                    assertThat(finding.kind()).isEqualTo("fanOut");
                    assertThat(finding.error()).contains("pipeline.test.LookupResult")
                            .contains("pipeline.test.Ticket");
                });
    }

    @Test
    void fanOutOnStreamingMethodIsRejected() {
        var fanOut = ai.pipestream.proto.grpc.workflow.v1.FanOutSpec.newBuilder()
                .setItems("items")
                .setMaxItems(10)
                .setMaxConcurrency(4)
                .setFailurePolicy(ai.pipestream.proto.grpc.workflow.v1.BranchFailurePolicy
                        .BRANCH_FAILURE_POLICY_FAIL_FAST)
                .setCollectType(PipelineFixtures.RESULTS)
                .setCollectInto("results")
                .build();
        Pipeline pipeline = unaryBase()
                .addSteps(PipelineStep.newBuilder()
                        .setName("search")
                        .setDependency("pipeline.test.Search")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.STREAM)
                                .setMethodShape(MethodShape.METHOD_SHAPE_SERVER_STREAMING)
                                .setEdge(PipelineFixtures.edge(PipelineFixtures.BATCH,
                                        List.of("input")))
                                .setFanOut(fanOut)
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("search");
                    assertThat(finding.kind()).isEqualTo("fanOut");
                    assertThat(finding.error()).contains("must be unary")
                            .contains("serverStreaming=true");
                });
    }

    @Test
    void unknownMethodIsRejectedWithFileList() {
        Pipeline pipeline = unaryBase()
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        "pipeline.test.Lookup/Nope", MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("method");
                    assertThat(finding.error()).contains("pipeline.test.Lookup/Nope")
                            .contains("pipeline/test/pipeline.proto");
                });
    }

    @Test
    void duplicateStepNameIsRejected() {
        var step = PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                EdgeCardinality.EDGE_CARDINALITY_ONE,
                EdgeCardinality.EDGE_CARDINALITY_ONE,
                PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                        "title = input.title"))
                .build();
        // Structural validation rejects duplicates before scope checking begins.
        Pipeline pipeline = unaryBase().addSteps(step).addSteps(step).build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .singleElement()
                .satisfies(finding -> {
                    assertThat(finding.kind()).isEqualTo("pipeline");
                    assertThat(finding.error()).contains("duplicate step name: fetch");
                });
    }

    @Test
    void stepNamedInputIsRejected() {
        Pipeline pipeline = unaryBase()
                .addSteps(PipelineFixtures.grpcStep("input", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> assertThat(finding.error())
                        .contains("identifier other than 'input'/'target'"));
    }

    private static Pipeline fanOutPipeline(String itemsPath) {
        var fanOut = ai.pipestream.proto.grpc.workflow.v1.FanOutSpec.newBuilder()
                .setItems(itemsPath)
                .setMaxItems(10)
                .setMaxConcurrency(4)
                .setFailurePolicy(ai.pipestream.proto.grpc.workflow.v1.BranchFailurePolicy
                        .BRANCH_FAILURE_POLICY_FAIL_FAST)
                .setCollectType(PipelineFixtures.TICKET_BOX)
                .setCollectInto("tickets")
                .build();
        return fanOutPipeline(fanOut);
    }

    private static Pipeline fanOutPipeline(
            ai.pipestream.proto.grpc.workflow.v1.FanOutSpec fanOut) {
        return PipelineFixtures.base("map", PipelineFixtures.BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(PipelineStep.newBuilder()
                        .setName("process")
                        .setDependency("pipeline.test.Worker")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.PROCESS)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(PipelineFixtures.edge(PipelineFixtures.BATCH,
                                        List.of("input")))
                                .setFanOut(fanOut)
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
    }
}
