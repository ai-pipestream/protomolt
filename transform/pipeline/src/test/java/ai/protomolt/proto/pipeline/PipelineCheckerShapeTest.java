package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.pipeline.v1.CollectStep;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.pipeline.v1.UnnestStep;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shape-correct pipelines of every gRPC streaming shape verify clean against the fixture
 * descriptors: unary, server-streaming into collect, unnest into client-streaming, unnest
 * into bidi into collect, bounded fan-out, structured generation over a projected
 * grounding, and external completion.
 */
class PipelineCheckerShapeTest {

    private final PipelineChecker checker = new PipelineChecker();

    @Test
    void unaryPipelineVerifies() {
        Pipeline pipeline = PipelineFixtures.base("unary-ok", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void serverStreamingIntoCollectVerifies() {
        Pipeline pipeline = PipelineFixtures.base("stream-ok", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addSteps(PipelineFixtures.grpcStep("search", "pipeline.test.Search",
                        PipelineFixtures.STREAM, MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .addSteps(PipelineStep.newBuilder()
                        .setName("gather")
                        .setCollect(CollectStep.newBuilder()
                                .setSource("search")
                                .setCollectType(PipelineFixtures.RESULTS)
                                .setCollectInto("results")
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void unnestIntoClientStreamingVerifies() {
        Pipeline pipeline = PipelineFixtures.base("upload-ok", PipelineFixtures.BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Ingest"))
                .addSteps(PipelineStep.newBuilder()
                        .setName("spread")
                        .setUnnest(UnnestStep.newBuilder()
                                .setSource("input")
                                .setPath("items")
                                .build())
                        .build())
                .addSteps(PipelineFixtures.grpcStep("upload", "pipeline.test.Ingest",
                        PipelineFixtures.UPLOAD, MethodShape.METHOD_SHAPE_CLIENT_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("spread"),
                                "title = spread.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void unnestIntoBidiIntoCollectVerifies() {
        Pipeline pipeline = PipelineFixtures.base("bidi-ok", PipelineFixtures.BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Relay"))
                .addSteps(PipelineStep.newBuilder()
                        .setName("spread")
                        .setUnnest(UnnestStep.newBuilder()
                                .setSource("input")
                                .setPath("items")
                                .build())
                        .build())
                .addSteps(PipelineFixtures.grpcStep("relay", "pipeline.test.Relay",
                        PipelineFixtures.CONVERSE,
                        MethodShape.METHOD_SHAPE_BIDI_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("spread"),
                                "title = spread.title")))
                .addSteps(PipelineStep.newBuilder()
                        .setName("box")
                        .setCollect(CollectStep.newBuilder()
                                .setSource("relay")
                                .setCollectType(PipelineFixtures.TICKET_BOX)
                                .setCollectInto("tickets")
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void boundedFanOutVerifies() {
        Pipeline pipeline = PipelineFixtures.base("fanout-ok", PipelineFixtures.BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(PipelineStep.newBuilder()
                        .setName("process")
                        .setDependency("pipeline.test.Worker")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.PROCESS)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(PipelineFixtures.edge(PipelineFixtures.BATCH,
                                        List.of("input")))
                                .setFanOut(fanOut())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void structuredStepOverProjectedGroundingVerifies() {
        var groundingEdge = PipelineFixtures.edge(PipelineFixtures.LOOKUP_RESULT,
                        List.of("fetch"), "doc_id = fetch.doc_id", "title = fetch.title",
                        "internal_notes = fetch.internal_notes")
                .toBuilder()
                .setProjectTo(PipelineFixtures.GROUNDING)
                .setValidate(true)
                .build();
        Pipeline pipeline = PipelineFixtures.base("structured-ok", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addDependencies(PipelineFixtures.dependency("structured-generation"))
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .addSteps(PipelineFixtures.structuredStep("summarize",
                        "structured-generation", "test-model", groundingEdge))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void externalCompletionStepVerifies() {
        Pipeline pipeline = PipelineFixtures.base("external-ok", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addSteps(PipelineStep.newBuilder()
                        .setName("human")
                        .setDependency("pipeline.test.Lookup")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.FETCH)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdgeCardinality(
                                        EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED)
                                .setOutputCardinality(
                                        EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void gatedStreamingStepVerifies() {
        Pipeline pipeline = PipelineFixtures.base("gated-ok", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addSteps(PipelineFixtures.grpcStep("search", "pipeline.test.Search",
                                PipelineFixtures.STREAM,
                                MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                                EdgeCardinality.EDGE_CARDINALITY_ONE,
                                EdgeCardinality.EDGE_CARDINALITY_MANY,
                                PipelineFixtures.edge(PipelineFixtures.TICKET,
                                        List.of("input"), "title = input.title"))
                        .setWhen("input.title != ''")
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    private static ai.protomolt.proto.grpc.workflow.v1.FanOutSpec fanOut() {
        return ai.protomolt.proto.grpc.workflow.v1.FanOutSpec.newBuilder()
                .setItems("items")
                .setMaxItems(10)
                .setMaxConcurrency(4)
                .setFailurePolicy(ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy
                        .BRANCH_FAILURE_POLICY_FAIL_FAST)
                .setCollectType(PipelineFixtures.TICKET_BOX)
                .setCollectInto("tickets")
                .build();
    }
}
