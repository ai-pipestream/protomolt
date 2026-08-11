package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.recipe.v1.BranchFailurePolicy;
import ai.pipestream.proto.grpc.recipe.v1.FanOutSpec;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.inference.spi.ChunkObserver;
import ai.pipestream.proto.inference.spi.InferenceCatalog;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.InferenceProvider;
import ai.pipestream.proto.inference.structured.StructuredGenerator;
import ai.pipestream.proto.inference.v1.FinishReason;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStreamRequest;
import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import ai.pipestream.proto.pipeline.PipelineExecutor.FailureKind;
import ai.pipestream.proto.pipeline.v1.CollectStep;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.GrpcCallStep;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineOutput;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import ai.pipestream.proto.pipeline.v1.UnnestStep;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runtime acceptance for every cardinality transition and gRPC streaming shape. */
class PipelineExecutorTest {

    @Test
    void unaryEdgeMapsAndInvokes() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        Pipeline pipeline = PipelineFixtures.base("unary-run", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .build();

        PipelineExecutor.Result result = executor(transport).run(pipeline,
                PipelineFixtures.files(), ticket("alpha"));

        assertThat(string(result.message(), "title")).isEqualTo("found:alpha");
        assertThat(result.steps()).containsExactly(
                new PipelineExecutor.StepOutcome("fetch", false, 1, 1));
        assertThat(transport.calls()).isEqualTo(1);
    }

    @Test
    void serverStreamCollectsInOrderAndMapsOutput() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        Pipeline pipeline = PipelineFixtures.base("server-run", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addSteps(PipelineFixtures.grpcStep("search", "pipeline.test.Search",
                        PipelineFixtures.STREAM, MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .addSteps(collect("gather", "search", PipelineFixtures.RESULTS, "results"))
                .setOutput(PipelineOutput.newBuilder()
                        .setType(PipelineFixtures.RESULTS)
                        .addRules("results = gather.results")
                        .build())
                .build();

        PipelineExecutor.Result result = executor(transport).run(pipeline,
                PipelineFixtures.files(), ticket("query"));

        assertThat(repeated(result.message(), "results"))
                .extracting(message -> string(message, "title"))
                .containsExactly("query#0", "query#1");
        assertThat(result.steps()).extracting(PipelineExecutor.StepOutcome::responseCount)
                .containsExactly(2, 1);
    }

    @Test
    void unnestFeedsClientStreamAndConsumesIt() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        Pipeline pipeline = PipelineFixtures.base("client-run", PipelineFixtures.BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Ingest"))
                .addSteps(unnest("spread", "input", "items"))
                .addSteps(PipelineFixtures.grpcStep("upload", "pipeline.test.Ingest",
                        PipelineFixtures.UPLOAD, MethodShape.METHOD_SHAPE_CLIENT_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("spread"),
                                "title = spread.title")))
                .setOutput(PipelineOutput.newBuilder().setType(PipelineFixtures.BATCH)
                        .addRules("items = upload.items").build())
                .build();

        PipelineExecutor.Result result = executor(transport).run(pipeline,
                PipelineFixtures.files(), batch("a", "b", "c"));

        assertThat(repeated(result.message(), "items"))
                .extracting(message -> string(message, "title"))
                .containsExactly("a", "b", "c");
        assertThat(result.steps().get(1).requestCount()).isEqualTo(3);
    }

    @Test
    void bidiStreamPreservesOrderBeforeCollect() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        Pipeline pipeline = PipelineFixtures.base("bidi-run", PipelineFixtures.BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Relay"))
                .addSteps(unnest("spread", "input", "items"))
                .addSteps(PipelineFixtures.grpcStep("relay", "pipeline.test.Relay",
                        PipelineFixtures.CONVERSE, MethodShape.METHOD_SHAPE_BIDI_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("spread"),
                                "title = spread.title")))
                .addSteps(collect("box", "relay", PipelineFixtures.TICKET_BOX, "tickets"))
                .build();

        PipelineExecutor.Result result = executor(transport).run(pipeline,
                PipelineFixtures.files(), batch("a", "b"));

        assertThat(repeated(result.message(), "tickets"))
                .extracting(message -> string(message, "title"))
                .containsExactly("relay:a", "relay:b");
    }

    @Test
    void fanOutUsesVirtualThreadsButHonorsTheConcurrencyCap() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        FanOutSpec fanOut = FanOutSpec.newBuilder()
                .setItems("items").setMaxItems(10).setMaxConcurrency(2)
                .setFailurePolicy(BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST)
                .setCollectType(PipelineFixtures.TICKET_BOX).setCollectInto("tickets")
                .build();
        Pipeline pipeline = PipelineFixtures.base("fan-run", PipelineFixtures.BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(PipelineStep.newBuilder().setName("process")
                        .setDependency("pipeline.test.Worker")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.PROCESS)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(PipelineFixtures.edge(PipelineFixtures.BATCH,
                                        List.of("input"), "items = input.items"))
                                .setFanOut(fanOut)
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build()).build())
                .build();

        PipelineExecutor.Result result = executor(transport).run(pipeline,
                PipelineFixtures.files(), batch("0", "1", "2", "3", "4"));

        assertThat(repeated(result.message(), "tickets"))
                .extracting(message -> string(message, "title"))
                .containsExactly("done:0", "done:1", "done:2", "done:3", "done:4");
        assertThat(transport.maxConcurrent()).isEqualTo(2);
    }

    @Test
    void continueFanOutCollectsSurvivorsAndRecordsTheFailedBranch() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        FanOutSpec fanOut = FanOutSpec.newBuilder()
                .setItems("items").setMaxItems(10).setMaxConcurrency(3)
                .setFailurePolicy(BranchFailurePolicy.BRANCH_FAILURE_POLICY_CONTINUE)
                .setCollectType(PipelineFixtures.TICKET_BOX).setCollectInto("tickets")
                .build();
        Pipeline pipeline = PipelineFixtures.base("fan-continue", PipelineFixtures.BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(PipelineStep.newBuilder().setName("process")
                        .setDependency("pipeline.test.Worker")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.PROCESS)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(PipelineFixtures.edge(PipelineFixtures.BATCH,
                                        List.of("input"), "items = input.items"))
                                .setFanOut(fanOut)
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build()).build())
                .build();

        PipelineExecutor.Result result = executor(transport).run(pipeline,
                PipelineFixtures.files(), batch("good", "bad", "also-good"));

        assertThat(repeated(result.message(), "tickets"))
                .extracting(message -> string(message, "title"))
                .containsExactly("done:good", "done:also-good");
        assertThat(result.branches()).hasSize(3);
        assertThat(result.branches().get(1).succeeded()).isFalse();
        assertThat(result.branches().get(1).kind()).isEqualTo(FailureKind.GRPC);
        assertThat(result.branches().get(1).error()).contains("INVALID_ARGUMENT")
                .contains("bad branch");
    }

    @Test
    void falseGateSkipsAStreamingCallWithoutOpeningTransport() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        Pipeline pipeline = PipelineFixtures.base("gate-run", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addSteps(PipelineFixtures.grpcStep("search", "pipeline.test.Search",
                                PipelineFixtures.STREAM,
                                MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                                EdgeCardinality.EDGE_CARDINALITY_ONE,
                                EdgeCardinality.EDGE_CARDINALITY_MANY,
                                PipelineFixtures.edge(PipelineFixtures.TICKET,
                                        List.of("input"), "title = input.title"))
                        .setWhen("input.title != ''").build())
                .build();

        PipelineExecutor.Result result = executor(transport).run(pipeline,
                PipelineFixtures.files(), ticket(""));

        assertThat(result.cardinality()).isEqualTo(EdgeCardinality.EDGE_CARDINALITY_MANY);
        assertThat(result.messages()).isEmpty();
        assertThat(result.steps().get(0).skipped()).isTrue();
        assertThat(transport.calls()).isZero();
    }

    @Test
    void structuredStepReceivesOnlyProjectedGrounding() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        CapturingProvider provider = new CapturingProvider();
        InferenceCatalog catalog = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(catalog, List.of(provider));
        engines.register(ModelEntry.newBuilder().setId("structured-model")
                .setProvider(provider.id()).setEndpoint("in-process://structured")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true))
                .build());
        DescriptorRegistry descriptors = DescriptorRegistry.create(false);
        descriptors.registerFile(PipelineFixtures.file());
        StructuredGenerator generator = new StructuredGenerator(engines, descriptors);

        var grounding = PipelineFixtures.edge(PipelineFixtures.LOOKUP_RESULT,
                        List.of("fetch"), "doc_id = fetch.doc_id", "title = fetch.title",
                        "internal_notes = fetch.internal_notes")
                .toBuilder().setProjectTo(PipelineFixtures.GROUNDING).setValidate(true)
                .build();
        Pipeline pipeline = PipelineFixtures.base("structured-run", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addDependencies(PipelineFixtures.dependency("structured-generation"))
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"),
                                "title = input.title")))
                .addSteps(PipelineFixtures.structuredStep("summarize",
                        "structured-generation", "structured-model", grounding))
                .build();

        PipelineExecutor.Result result = new PipelineExecutor(transport, generator)
                .run(pipeline, PipelineFixtures.files(), ticket("private"));

        assertThat(string(result.message(), "headline")).isEqualTo("safe summary");
        assertThat(provider.lastRequest.getMessagesList())
                .extracting(turn -> turn.getContent())
                .anySatisfy(prompt -> {
                    assertThat(prompt).contains("found:private");
                    assertThat(prompt).doesNotContain("secret-notes");
                });
    }

    @Test
    void responseStreamOverThePipelineCapFailsLoudly() {
        RecordingTransport transport = new RecordingTransport();
        Pipeline pipeline = PipelineFixtures.base("cap-run", PipelineFixtures.TICKET)
                .setMaxStreamMessages(1)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addSteps(PipelineFixtures.grpcStep("search", "pipeline.test.Search",
                        PipelineFixtures.STREAM, MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_MANY,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"))))
                .build();

        assertThatThrownBy(() -> executor(transport).run(pipeline,
                PipelineFixtures.files(), ticket("q")))
                .isInstanceOfSatisfying(PipelineExecutor.PipelineExecutionException.class,
                        failure -> {
                            assertThat(failure.step()).isEqualTo("search");
                            assertThat(failure.kind()).isEqualTo(FailureKind.PIPELINE);
                            assertThat(failure).hasMessageContaining("max_stream_messages 1");
                        });
    }

    @Test
    void externalStepRequiresTheDurableCoordinatorWithoutCallingTransport() {
        RecordingTransport transport = new RecordingTransport();
        Pipeline pipeline = PipelineFixtures.base("external-run", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addSteps(PipelineStep.newBuilder().setName("human")
                        .setDependency("pipeline.test.Lookup")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PipelineFixtures.FETCH)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                                .build()).build())
                .build();

        assertThatThrownBy(() -> executor(transport).run(pipeline,
                PipelineFixtures.files(), ticket("q")))
                .isInstanceOfSatisfying(PipelineExecutor.PipelineExecutionException.class,
                        failure -> assertThat(failure.kind()).isEqualTo(FailureKind.EXTERNAL));
        assertThat(transport.calls()).isZero();
    }

    @Test
    void wrongInputTypeFailsBeforeTransport() {
        RecordingTransport transport = new RecordingTransport();
        Pipeline pipeline = PipelineFixtures.base("input-run", PipelineFixtures.TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addSteps(PipelineFixtures.grpcStep("fetch", "pipeline.test.Lookup",
                        PipelineFixtures.FETCH, MethodShape.METHOD_SHAPE_UNARY,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(PipelineFixtures.TICKET, List.of("input"))))
                .build();

        assertThatThrownBy(() -> executor(transport).run(pipeline,
                PipelineFixtures.files(), batch("wrong")))
                .isInstanceOfSatisfying(PipelineExecutor.PipelineExecutionException.class,
                        failure -> assertThat(failure.kind()).isEqualTo(FailureKind.PREFLIGHT));
        assertThat(transport.calls()).isZero();
    }

    private static PipelineExecutor executor(RecordingTransport transport) {
        return new PipelineExecutor(transport);
    }

    private static PipelineStep unnest(String name, String source, String path) {
        return PipelineStep.newBuilder().setName(name)
                .setUnnest(UnnestStep.newBuilder().setSource(source).setPath(path).build())
                .build();
    }

    private static PipelineStep collect(String name, String source, String type,
                                        String field) {
        return PipelineStep.newBuilder().setName(name)
                .setCollect(CollectStep.newBuilder().setSource(source)
                        .setCollectType(type).setCollectInto(field).build())
                .build();
    }

    private static DynamicMessage ticket(String title) {
        return message(PipelineFixtures.type(PipelineFixtures.TICKET), "title", title);
    }

    private static DynamicMessage batch(String... titles) {
        Descriptor type = PipelineFixtures.type(PipelineFixtures.BATCH);
        FieldDescriptor items = type.findFieldByName("items");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        for (String title : titles) {
            builder.addRepeatedField(items, ticket(title));
        }
        return builder.build();
    }

    private static DynamicMessage message(Descriptor type, String field, Object value) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        builder.setField(type.findFieldByName(field), value);
        return builder.build();
    }

    private static String string(DynamicMessage message, String field) {
        return (String) message.getField(message.getDescriptorForType().findFieldByName(field));
    }

    private static List<DynamicMessage> repeated(DynamicMessage message, String field) {
        List<?> raw = (List<?>) message.getField(
                message.getDescriptorForType().findFieldByName(field));
        List<DynamicMessage> values = new ArrayList<>();
        raw.forEach(value -> values.add((DynamicMessage) value));
        return values;
    }

    private static final class RecordingTransport implements PipelineTransport {
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();

        @Override
        public List<DynamicMessage> invoke(
                ai.pipestream.proto.grpc.recipe.v1.ServiceDependency dependency,
                com.google.protobuf.Descriptors.MethodDescriptor method,
                List<DynamicMessage> requests, long deadlineMillis, int maxResponses) {
            calls.incrementAndGet();
            return switch (method.getName()) {
                case "Fetch" -> List.of(message(PipelineFixtures.type(
                                PipelineFixtures.LOOKUP_RESULT), "title",
                        "found:" + string(requests.get(0), "title")).toBuilder()
                        .setField(PipelineFixtures.type(PipelineFixtures.LOOKUP_RESULT)
                                .findFieldByName("internal_notes"), "secret-notes")
                        .build());
                case "Stream" -> List.of(
                        message(PipelineFixtures.type(PipelineFixtures.LOOKUP_RESULT),
                                "title", string(requests.get(0), "title") + "#0"),
                        message(PipelineFixtures.type(PipelineFixtures.LOOKUP_RESULT),
                                "title", string(requests.get(0), "title") + "#1"));
                case "Upload" -> List.of(batch(requests.stream()
                        .map(request -> string(request, "title")).toArray(String[]::new)));
                case "Converse" -> requests.stream().map(request ->
                        ticket("relay:" + string(request, "title"))).toList();
                case "Process" -> List.of(process(requests.get(0)));
                default -> throw new AssertionError("unexpected method " + method.getName());
            };
        }

        private DynamicMessage process(DynamicMessage request) {
            if (string(request, "title").equals("bad")) {
                throw Status.INVALID_ARGUMENT.withDescription("bad branch")
                        .asRuntimeException();
            }
            int active = concurrent.incrementAndGet();
            maxConcurrent.accumulateAndGet(active, Math::max);
            try {
                Thread.sleep(20);
                return ticket("done:" + string(request, "title"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } finally {
                concurrent.decrementAndGet();
            }
        }

        int calls() {
            return calls.get();
        }

        int maxConcurrent() {
            return maxConcurrent.get();
        }
    }

    private static final class CapturingProvider implements InferenceProvider {
        private GenerateRequest lastRequest;

        @Override
        public String id() {
            return "pipeline-scripted";
        }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            lastRequest = request;
            return GenerateResponse.newBuilder().setText("{\"headline\":\"safe summary\"}")
                    .setModel(model.getId()).setProvider(id()).setModelVersion("test-v1")
                    .setFinishReason(FinishReason.FINISH_REASON_STOP).build();
        }

        @Override
        public void generateStream(ModelEntry model, GenerateStreamRequest request,
                                   ChunkObserver observer) {
            throw new UnsupportedOperationException();
        }
    }
}
