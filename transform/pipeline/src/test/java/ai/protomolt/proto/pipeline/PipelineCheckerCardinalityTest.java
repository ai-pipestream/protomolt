package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.pipeline.v1.CollectStep;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineOutput;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.pipeline.v1.UnnestStep;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cardinality and streaming-shape discipline: every rejection names the method, its
 * descriptor streaming flags, or the exact binding whose cardinality breaks the dataflow,
 * and points at the explicit collect or unnest that would fix it.
 */
class PipelineCheckerCardinalityTest {

    private final PipelineChecker checker = new PipelineChecker();

    /** A server-streaming first step, the starting point for most mismatches here. */
    private static Pipeline.Builder streamingFirstStep() {
        return PipelineFixtures.base("card", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Ingest"))
                .addSteps(PipelineFixtures.grpcStep("search", "pipeline.test.Search",
                        PipelineFixtures.STREAM, MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")));
    }

    @Test
    void streamEdgeIntoUnarySlotIsRejected() {
        Pipeline pipeline = streamingFirstStep()
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("search"),
                                "title = search.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("pipeline.test.Lookup.Fetch")
                            .contains("clientStreaming=false")
                            .contains("collect");
                });
    }

    @Test
    void oneEdgeIntoClientStreamingSlotIsRejected() {
        Pipeline pipeline = PipelineFixtures.base("card", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Ingest"))
                .addSteps(PipelineFixtures.grpcStep("upload", "pipeline.test.Ingest",
                        PipelineFixtures.UPLOAD, MethodShape.METHOD_SHAPE_CLIENT_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("upload");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("pipeline.test.Ingest.Upload")
                            .contains("clientStreaming=true")
                            .contains("unnest");
                });
    }

    @Test
    void declaredShapeDriftIsRejectedWithDescriptorFlags() {
        Pipeline pipeline = PipelineFixtures.base("card", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addSteps(PipelineFixtures.grpcStep("search", "pipeline.test.Search",
                        PipelineFixtures.STREAM, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("search");
                    assertThat(finding.kind()).isEqualTo("shape");
                    assertThat(finding.error()).contains("pipeline/test/pipeline.proto")
                            .contains("clientStreaming=false")
                            .contains("serverStreaming=true")
                            .contains("METHOD_SHAPE_SERVER_STREAMING")
                            .contains("METHOD_SHAPE_UNARY");
                });
    }

    @Test
    void declaredOutputCardinalityDriftIsRejected() {
        Pipeline pipeline = PipelineFixtures.base("card", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addSteps(PipelineFixtures.grpcStep("search", "pipeline.test.Search",
                        PipelineFixtures.STREAM, MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("search");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("output_cardinality")
                            .contains("EDGE_CARDINALITY_MANY");
                });
    }

    @Test
    void declaredEdgeCardinalityDriftIsRejected() {
        Pipeline pipeline = streamingFirstStep()
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("search"),
                                "title = search.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("EDGE_CARDINALITY_ONE")
                            .contains("EDGE_CARDINALITY_MANY");
                });
    }

    @Test
    void collectFromOneBindingIsRejected() {
        Pipeline pipeline = PipelineFixtures.base("card", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addSteps(PipelineStep.newBuilder()
                        .setName("gather")
                        .setCollect(CollectStep.newBuilder()
                                .setSource("input")
                                .setCollectType(PipelineFixtures.TICKET_BOX)
                                .setCollectInto("tickets")
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("gather");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("'input'")
                            .contains("nothing to collect");
                });
    }

    @Test
    void unnestFromStreamBindingIsRejected() {
        Pipeline pipeline = streamingFirstStep()
                .addSteps(PipelineStep.newBuilder()
                        .setName("spread")
                        .setUnnest(UnnestStep.newBuilder()
                                .setSource("search")
                                .setPath("title")
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("spread");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("'search'")
                            .contains("stream (MANY)");
                });
    }

    @Test
    void collectElementTypeMismatchIsRejected() {
        Pipeline pipeline = streamingFirstStep()
                .addSteps(PipelineStep.newBuilder()
                        .setName("gather")
                        .setCollect(CollectStep.newBuilder()
                                .setSource("search")
                                .setCollectType(PipelineFixtures.TICKET_BOX)
                                .setCollectInto("tickets")
                                .build())
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("gather");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("pipeline.test.Ticket")
                            .contains("pipeline.test.LookupResult");
                });
    }

    @Test
    void outputMappingOverStreamBindingIsRejected() {
        Pipeline pipeline = streamingFirstStep()
                .setOutput(PipelineOutput.newBuilder()
                        .setType(PipelineFixtures.TICKET)
                        .addRules("title = input.title")
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.kind()).isEqualTo("output");
                    assertThat(finding.error()).contains("'search'")
                            .contains("stream (MANY)")
                            .contains("collect");
                });
    }

    @Test
    void collectedStreamCanFeedTheFinalOutput() {
        Pipeline pipeline = streamingFirstStep()
                .addSteps(PipelineStep.newBuilder()
                        .setName("gather")
                        .setCollect(CollectStep.newBuilder()
                                .setSource("search")
                                .setCollectType(PipelineFixtures.RESULTS)
                                .setCollectInto("results")
                                .build())
                        .build())
                .setOutput(PipelineOutput.newBuilder()
                        .setType(PipelineFixtures.RESULTS)
                        .addRules("results = gather.results")
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void clientStreamingCallConsumesItsInputStreamBeforeOutput() {
        Pipeline pipeline = streamingFirstStep()
                .addSteps(PipelineFixtures.grpcStep("upload", "pipeline.test.Ingest",
                        PipelineFixtures.UPLOAD,
                        MethodShape.METHOD_SHAPE_CLIENT_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET,
                                List.of("search"), "title = search.title")))
                .setOutput(PipelineOutput.newBuilder()
                        .setType(PipelineFixtures.TICKET)
                        .addRules("title = input.title")
                        .build())
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void multipleLiveStreamSourcesRequireAnExplicitJoinPolicy() {
        Pipeline pipeline = streamingFirstStep()
                .addSteps(PipelineFixtures.grpcStep("search_again",
                        "pipeline.test.Search", PipelineFixtures.STREAM,
                        MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        PipelineFixtures.edge(PipelineFixtures.TICKET,
                                List.of("input"), "title = input.title")))
                .addSteps(PipelineFixtures.grpcStep("upload", "pipeline.test.Ingest",
                        PipelineFixtures.UPLOAD,
                        MethodShape.METHOD_SHAPE_CLIENT_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET,
                                List.of("search", "search_again"),
                                "title = search.title")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("upload");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("multiple live stream bindings")
                            .contains("search")
                            .contains("search_again")
                            .contains("explicit join");
                });
    }

    @Test
    void structuredGroundingFromStreamIsRejected() {
        Pipeline pipeline = streamingFirstStep()
                .addDependencies(PipelineFixtures.dependency("structured-generation"))
                .addSteps(PipelineFixtures.structuredStep("summarize",
                        "structured-generation", "test-model",
                        PipelineFixtures.edge(PipelineFixtures.LOOKUP_RESULT,
                                List.of("search"), "doc_id = search.doc_id")))
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("summarize");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("'search'")
                            .contains("collect");
                });
    }

    @Test
    void serialGateWhileAStreamIsLiveIsRejected() {
        Pipeline pipeline = streamingFirstStep()
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                                PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                                EdgeCardinality.EDGE_CARDINALITY_ONE,
                                EdgeCardinality.EDGE_CARDINALITY_ONE,
                                PipelineFixtures.edge(PipelineFixtures.TICKET,
                                        List.of("input"), "title = input.title"))
                        .setWhen("input.title != ''").build())
                .build();

        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("when");
                    assertThat(finding.error()).contains("live stream bindings")
                            .contains("search")
                            .contains("collect");
                });
    }

    @Test
    void descriptorFingerprintMismatchIsRejectedPrecisely() {
        Pipeline pipeline = PipelineFixtures.base("card", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .setDescriptorFingerprint(
                        "0000000000000000000000000000000000000000000000000000000000000000")
                .build();
        // Structural validation rejects the tampered fingerprint because the dependency
        // still carries the real one: the pipeline must be internally consistent first.
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .singleElement()
                .satisfies(finding -> {
                    assertThat(finding.kind()).isEqualTo("pipeline");
                    assertThat(finding.error()).contains("pipeline.test.Lookup");
                });
    }

    @Test
    void descriptorSetDriftIsRejectedWithBothFingerprints() {
        String wrong = "0000000000000000000000000000000000000000000000000000000000000000";
        Pipeline pipeline = PipelineFixtures.base("card", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup")
                        .toBuilder().setDescriptorFingerprint(wrong).build())
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .setDescriptorFingerprint(wrong)
                .build();
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.kind()).isEqualTo("pipeline");
                    assertThat(finding.error()).contains(wrong)
                            .contains(PipelineFixtures.fingerprint())
                            .contains("pipeline/test/pipeline.proto");
                });
    }
}
