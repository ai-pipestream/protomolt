package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.pipeline.PipelineExecutor.BranchOutcome;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.PipelineExecutor.ExecutionIdentity;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.PipelineExecutionException;
import ai.protomolt.proto.pipeline.PipelineExecutor.Segment;
import ai.protomolt.proto.pipeline.PipelineExecutor.StepOutcome;
import ai.protomolt.proto.pipeline.v1.CollectStep;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineOutput;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.StringValue;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.protomolt.proto.pipeline.PipelineFixtures.BATCH;
import static ai.protomolt.proto.pipeline.PipelineFixtures.FETCH;
import static ai.protomolt.proto.pipeline.PipelineFixtures.LOOKUP_RESULT;
import static ai.protomolt.proto.pipeline.PipelineFixtures.PROCESS;
import static ai.protomolt.proto.pipeline.PipelineFixtures.RESULTS;
import static ai.protomolt.proto.pipeline.PipelineFixtures.STREAM;
import static ai.protomolt.proto.pipeline.PipelineFixtures.SUMMARY;
import static ai.protomolt.proto.pipeline.PipelineFixtures.TICKET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit coverage for {@link PipelineExecutor#runSegment}, checkpoint replay validation,
 * binary protobuf fidelity, and operation identity propagation on {@link PipelineTransport}.
 */
class PipelineSegmentsTest {

    @Test
    void synchronousRunStillRefusesExternalStepWhileSkippingFalseGatedExternalStep()
            throws Exception {
        PipelineStep gatedExternal = externalStep("approval", "pipeline.test.Lookup", FETCH,
                MethodShape.METHOD_SHAPE_UNARY, EdgeCardinality.EDGE_CARDINALITY_ONE, false)
                .setWhen("input.title == 'require-approval'")
                .build();
        PipelineStep finish = PipelineFixtures.grpcStep("finish", "pipeline.test.Worker", PROCESS,
                MethodShape.METHOD_SHAPE_UNARY, EdgeCardinality.EDGE_CARDINALITY_ONE,
                EdgeCardinality.EDGE_CARDINALITY_ONE,
                PipelineFixtures.edge(TICKET, List.of("input"), "title = input.title")).build();
        Pipeline pipeline = PipelineFixtures.base("gated-external", TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(gatedExternal)
                .addSteps(finish)
                .build();

        PipelineExecutor executor = new PipelineExecutor((dep, method, reqs, deadline, max) ->
                List.of(ticket("done:" + reqs.getFirst().getField(
                        reqs.getFirst().getDescriptorForType().findFieldByName("title")))));

        // When gate is false, synchronous run skips the external step without parking.
        PipelineExecutor.Result skippedResult = executor.run(
                pipeline, PipelineFixtures.files(), ticket("fast-path"));
        assertThat(skippedResult.steps()).containsExactly(
                new StepOutcome("approval", true, 0, 0),
                new StepOutcome("finish", false, 1, 1));
        assertThat(titleOf(skippedResult.message())).isEqualTo("done:fast-path");

        // When gate is true, synchronous run refuses STEP_COMPLETION_EXTERNAL with EXTERNAL.
        assertThatThrownBy(() -> executor.run(
                pipeline, PipelineFixtures.files(), ticket("require-approval")))
                .isInstanceOfSatisfying(PipelineExecutionException.class, e -> {
                    assertThat(e.kind()).isEqualTo(FailureKind.EXTERNAL);
                    assertThat(e.step()).isEqualTo("approval");
                    assertThat(e.getMessage()).contains("durable job coordinator");
                });
    }

    @Test
    void segmentParksAndReplaysAcrossNewExecutorWithoutReinvokingCompletedSteps() throws Exception {
        Pipeline pipeline = PipelineFixtures.base("three-step-approval", TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Lookup"))
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(PipelineFixtures.grpcStep("lookup", "pipeline.test.Lookup", FETCH,
                        MethodShape.METHOD_SHAPE_UNARY, EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(TICKET, List.of("input"),
                                "title = input.title")).build())
                .addSteps(externalStep("review", "pipeline.test.Lookup", FETCH,
                        MethodShape.METHOD_SHAPE_UNARY, EdgeCardinality.EDGE_CARDINALITY_ONE,
                        false).build())
                .addSteps(PipelineFixtures.grpcStep("apply", "pipeline.test.Worker", PROCESS,
                        MethodShape.METHOD_SHAPE_UNARY, EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(TICKET, List.of("lookup", "review"),
                                "title = review.title")).build())
                .build();

        AtomicInteger lookupCalls = new AtomicInteger();
        AtomicInteger applyCalls = new AtomicInteger();
        List<Checkpoint> observedFirstRun = new ArrayList<>();

        PipelineExecutor firstExecutor = new PipelineExecutor((dep, method, reqs, deadline, max) -> {
            if (method.getFullName().equals("pipeline.test.Lookup.Fetch")) {
                lookupCalls.incrementAndGet();
                return List.of(lookupResult("doc-1", "fetched", "secret"));
            }
            throw new AssertionError("unexpected call in first segment: " + method.getFullName());
        });

        UnknownFieldSet unknownOnInput = UnknownFieldSet.newBuilder()
                .addField(77, UnknownFieldSet.Field.newBuilder()
                        .addLengthDelimited(ByteString.copyFromUtf8("input-tag"))
                        .build())
                .build();
        DynamicMessage input = DynamicMessage.newBuilder(PipelineFixtures.type(TICKET))
                .setField(PipelineFixtures.type(TICKET).findFieldByName("title"), "req-1")
                .setUnknownFields(unknownOnInput)
                .build();

        Segment first = firstExecutor.runSegment(
                pipeline, PipelineFixtures.files(), input, List.of(), observedFirstRun::add);
        assertThat(first).isInstanceOf(Segment.Parked.class);
        Segment.Parked parked = (Segment.Parked) first;
        assertThat(parked.step()).isEqualTo("review");
        assertThat(parked.cardinality()).isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
        assertThat(parked.checkpoints()).hasSize(1);
        assertThat(observedFirstRun).isEqualTo(parked.checkpoints());
        assertThat(lookupCalls.get()).isEqualTo(1);

        UnknownFieldSet unknownOnApproval = UnknownFieldSet.newBuilder()
                .addField(88, UnknownFieldSet.Field.newBuilder()
                        .addVarint(424242L)
                        .build())
                .build();
        DynamicMessage approvalMessage = DynamicMessage.newBuilder(
                        PipelineFixtures.type(LOOKUP_RESULT))
                .setField(PipelineFixtures.type(LOOKUP_RESULT).findFieldByName("doc_id"), "doc-1")
                .setField(PipelineFixtures.type(LOOKUP_RESULT).findFieldByName("title"),
                        "approved-doc-1")
                .setUnknownFields(unknownOnApproval)
                .build();

        List<Checkpoint> resumePrefix = new ArrayList<>(parked.checkpoints());
        resumePrefix.add(parked.completion(List.of(approvalMessage)));

        List<Checkpoint> observedSecondRun = new ArrayList<>();
        PipelineExecutor secondExecutor = new PipelineExecutor((dep, method, reqs, deadline, max) -> {
            if (method.getFullName().equals("pipeline.test.Worker.Process")) {
                applyCalls.incrementAndGet();
                return List.of(ticket("committed:" + titleOf(reqs.getFirst())));
            }
            throw new AssertionError("lookup must not re-run during replay: " + method.getFullName());
        });

        Segment second = secondExecutor.runSegment(
                pipeline, PipelineFixtures.files(), input, resumePrefix, observedSecondRun::add);
        assertThat(second).isInstanceOf(Segment.Completed.class);
        Segment.Completed completed = (Segment.Completed) second;
        assertThat(lookupCalls.get()).isEqualTo(1);
        assertThat(applyCalls.get()).isEqualTo(1);
        assertThat(observedSecondRun).hasSize(1);
        assertThat(completed.checkpoints()).hasSize(3);
        assertThat(completed.checkpoints().get(1).messages().getFirst().getUnknownFields())
                .isEqualTo(unknownOnApproval);
        assertThat(titleOf(completed.result().message())).isEqualTo("committed:approved-doc-1");
        assertThat(completed.result().steps()).containsExactly(
                new StepOutcome("lookup", false, 1, 1),
                new StepOutcome("review", false, 0, 1),
                new StepOutcome("apply", false, 1, 1));
    }

    @Test
    void segmentSupportsStreamingExternalCompletionLinearConsumptionAndFanOutEvidence()
            throws Exception {
        FanOutSpec fanOut = FanOutSpec.newBuilder()
                .setItems("items")
                .setCollectType(BATCH)
                .setCollectInto("items")
                .setMaxItems(10)
                .setMaxConcurrency(2)
                .setFailurePolicy(BranchFailurePolicy.BRANCH_FAILURE_POLICY_CONTINUE)
                .build();
        PipelineStep fanStep = PipelineStep.newBuilder()
                .setName("fan")
                .setDependency("pipeline.test.Worker")
                .setGrpcCall(GrpcCallStep.newBuilder()
                        .setMethod(PROCESS)
                        .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                        .setEdge(PipelineFixtures.edge(BATCH, List.of("input"),
                                "items = input.items"))
                        .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                        .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .setFanOut(fanOut)
                        .build())
                .build();
        PipelineStep externalStream = externalStep("external_stream", "pipeline.test.Search",
                STREAM, MethodShape.METHOD_SHAPE_SERVER_STREAMING,
                EdgeCardinality.EDGE_CARDINALITY_MANY, false).build();
        PipelineStep collectStream = PipelineStep.newBuilder()
                .setName("collected")
                .setCollect(CollectStep.newBuilder()
                        .setSource("external_stream")
                        .setCollectType(RESULTS)
                        .setCollectInto("results")
                        .build())
                .build();
        Pipeline pipeline = PipelineFixtures.base("fan-and-stream", BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Search"))
                .addSteps(fanStep)
                .addSteps(externalStream)
                .addSteps(collectStream)
                .setOutput(PipelineOutput.newBuilder()
                        .setType(RESULTS)
                        .addRules("results = collected.results")
                        .build())
                .build();

        PipelineExecutor firstExecutor = new PipelineExecutor((dep, method, reqs, deadline, max) -> {
            String t = titleOf(reqs.getFirst());
            if ("bad".equals(t)) {
                throw new StatusRuntimeException(Status.INTERNAL.withDescription("boom"));
            }
            return List.of(ticket("ok:" + t));
        });

        Descriptor batchType = PipelineFixtures.type(BATCH);
        DynamicMessage batchInput = DynamicMessage.newBuilder(batchType)
                .addRepeatedField(batchType.findFieldByName("items"), ticket("a"))
                .addRepeatedField(batchType.findFieldByName("items"), ticket("bad"))
                .addRepeatedField(batchType.findFieldByName("items"), ticket("c"))
                .build();

        Segment.Parked parked = (Segment.Parked) firstExecutor.runSegment(
                pipeline, PipelineFixtures.files(), batchInput, List.of(), cp -> { });
        assertThat(parked.step()).isEqualTo("external_stream");
        assertThat(parked.cardinality()).isEqualTo(EdgeCardinality.EDGE_CARDINALITY_MANY);
        assertThat(parked.checkpoints()).hasSize(1);
        assertThat(parked.checkpoints().getFirst().branches()).hasSize(3);

        List<Checkpoint> resume = new ArrayList<>(parked.checkpoints());
        resume.add(parked.completion(List.of(
                lookupResult("d1", "first", ""),
                lookupResult("d2", "second", ""))));

        PipelineExecutor replayExecutor = new PipelineExecutor((dep, method, reqs, deadline, max) -> {
            throw new AssertionError("no live calls remain after external stream completion");
        });
        Segment.Completed done = (Segment.Completed) replayExecutor.runSegment(
                pipeline, PipelineFixtures.files(), batchInput, resume, cp -> { });
        assertThat(done.result().branches()).hasSize(3);
        assertThat(done.result().branches().get(1).succeeded()).isFalse();
        Descriptor resultsType = PipelineFixtures.type(RESULTS);
        assertThat(done.result().message().getRepeatedFieldCount(
                resultsType.findFieldByName("results"))).isEqualTo(2);
    }

    @Test
    void binaryFidelityPreservesBytesUint64AnyAndUnknownFieldsAcrossReplay() throws Exception {
        String binaryProto = """
                syntax = "proto3";
                package pipeline.binary;
                import "google/protobuf/any.proto";
                import "google/protobuf/wrappers.proto";
                message BinaryEnvelope {
                  bytes raw_digest = 1;
                  uint64 unsigned_seq = 2;
                  google.protobuf.Any payload = 3;
                }
                service Gate { rpc Await(BinaryEnvelope) returns (BinaryEnvelope); }
                service Sink { rpc Commit(BinaryEnvelope) returns (BinaryEnvelope); }
                """;
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("google/protobuf/any.proto",
                        PipelineFixtures.resource("google/protobuf/any.proto"), "test")
                .add("google/protobuf/wrappers.proto",
                        PipelineFixtures.resource("google/protobuf/wrappers.proto"), "test")
                .add("pipeline/binary/binary.proto", binaryProto, "test")
                .build());
        FileDescriptor file = compiled.descriptorFor("pipeline/binary/binary.proto").orElseThrow();
        List<FileDescriptor> files = List.of(file);
        String fp = ai.protomolt.proto.grpc.profile.ServiceProfileValidation.sha256(
                FileDescriptorSet.newBuilder().addFile(file.toProto()).build().toByteArray());
        Descriptor envType = file.findMessageTypeByName("BinaryEnvelope");

        Pipeline pipeline = Pipeline.newBuilder()
                .setName("binary-fidelity")
                .setInputType("pipeline.binary.BinaryEnvelope")
                .setDescriptorFingerprint(fp)
                .setMaxStreamMessages(16)
                .setDeadline(Duration.newBuilder().setSeconds(10).build())
                .addDependencies(ai.protomolt.proto.grpc.workflow.v1.ServiceDependency.newBuilder()
                        .setAlias("gate")
                        .setServiceProfile("gate")
                        .setEndpoint("local")
                        .setDescriptorFingerprint(fp)
                        .build())
                .addDependencies(ai.protomolt.proto.grpc.workflow.v1.ServiceDependency.newBuilder()
                        .setAlias("sink")
                        .setServiceProfile("sink")
                        .setEndpoint("local")
                        .setDescriptorFingerprint(fp)
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("await_external")
                        .setDependency("gate")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("pipeline.binary.Gate/Await")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                                .build())
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("commit")
                        .setDependency("sink")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("pipeline.binary.Sink/Commit")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(PipelineFixtures.edge(
                                        "pipeline.binary.BinaryEnvelope",
                                        List.of("await_external"),
                                        "raw_digest = await_external.raw_digest",
                                        "unsigned_seq = await_external.unsigned_seq",
                                        "payload = await_external.payload"))
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();

        long maxUnsigned = -42L; // > Long.MAX_VALUE when interpreted as uint64
        ByteString exactBytes = ByteString.copyFrom(new byte[] {0x00, (byte) 0xff, 0x13, 0x7f});
        Any packed = Any.pack(StringValue.of("typed-any-payload"));
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
                .addField(555, UnknownFieldSet.Field.newBuilder()
                        .addLengthDelimited(ByteString.copyFrom("opaque-wire-bytes",
                                StandardCharsets.UTF_8))
                        .build())
                .build();
        DynamicMessage binaryMsg = DynamicMessage.newBuilder(envType)
                .setField(envType.findFieldByName("raw_digest"), exactBytes)
                .setField(envType.findFieldByName("unsigned_seq"), maxUnsigned)
                .setField(envType.findFieldByName("payload"),
                        DynamicMessage.parseFrom(Any.getDescriptor(), packed.toByteString()))
                .setUnknownFields(unknown)
                .build();

        PipelineExecutor executor = new PipelineExecutor((dep, method, reqs, deadline, max) ->
                List.of(reqs.getFirst()));
        Segment.Parked parked = (Segment.Parked) executor.runSegment(
                pipeline, files, binaryMsg, List.of(), cp -> { });
        Checkpoint externalCp = parked.completion(List.of(binaryMsg));
        Segment.Completed completed = (Segment.Completed) executor.runSegment(
                pipeline, files, binaryMsg, List.of(externalCp), cp -> { });

        DynamicMessage replayedExternal = completed.checkpoints().getFirst().messages().getFirst();
        assertThat(replayedExternal.toByteString()).isEqualTo(binaryMsg.toByteString());
        assertThat(replayedExternal.getUnknownFields()).isEqualTo(unknown);

        DynamicMessage finalResult = completed.result().message();
        assertThat(finalResult.getField(envType.findFieldByName("raw_digest")))
                .isEqualTo(exactBytes);
        assertThat(finalResult.getField(envType.findFieldByName("unsigned_seq")))
                .isEqualTo(maxUnsigned);
        DynamicMessage anyField = (DynamicMessage) finalResult.getField(
                envType.findFieldByName("payload"));
        assertThat(Any.parseFrom(anyField.toByteString()).unpack(StringValue.class).getValue())
                .isEqualTo("typed-any-payload");
    }

    @Test
    void dynamicPipelineTransportStampsDurableOperationHeadersOnlyWhenJobIdPresent()
            throws Exception {
        List<String> recordedHeaders = new CopyOnWriteArrayList<>();
        Channel echoChannel = new Channel() {
            @Override
            public String authority() {
                return "local";
            }

            @Override
            @SuppressWarnings("unchecked")
            public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                    io.grpc.MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                    CallOptions callOptions) {
                return new ClientCall<>() {
                    private Listener<ResponseT> listener;

                    @Override
                    public void start(Listener<ResponseT> responseListener, Metadata headers) {
                        this.listener = responseListener;
                        String job = headers.get(DynamicPipelineTransport.JOB_ID_HEADER);
                        String step = headers.get(DynamicPipelineTransport.STEP_NAME_HEADER);
                        String branch = headers.get(DynamicPipelineTransport.BRANCH_ID_HEADER);
                        String op = headers.get(DynamicPipelineTransport.OPERATION_ID_HEADER);
                        recordedHeaders.add((job == null ? "-" : job) + "|"
                                + (step == null ? "-" : step) + "|"
                                + (branch == null ? "-" : branch) + "|"
                                + (op == null ? "-" : op));
                    }

                    @Override
                    public void request(int numMessages) {
                    }

                    @Override
                    public void cancel(String message, Throwable cause) {
                    }

                    @Override
                    public void halfClose() {
                        listener.onClose(Status.OK, new Metadata());
                    }

                    @Override
                    public void sendMessage(RequestT message) {
                        listener.onHeaders(new Metadata());
                        listener.onMessage((ResponseT) message);
                    }
                };
            }
        };

        FanOutSpec fanOut = FanOutSpec.newBuilder()
                .setItems("items")
                .setCollectType(BATCH)
                .setCollectInto("items")
                .setMaxItems(10)
                .setMaxConcurrency(1)
                .setFailurePolicy(BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST)
                .build();
        Pipeline pipeline = PipelineFixtures.base("op-headers", BATCH)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(PipelineStep.newBuilder()
                        .setName("fan_apply")
                        .setDependency("pipeline.test.Worker")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod(PROCESS)
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(PipelineFixtures.edge(BATCH, List.of("input"),
                                        "items = input.items"))
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .setFanOut(fanOut)
                                .build())
                        .build())
                .build();

        Descriptor batchType = PipelineFixtures.type(BATCH);
        DynamicMessage input = DynamicMessage.newBuilder(batchType)
                .addRepeatedField(batchType.findFieldByName("items"), ticket("first"))
                .addRepeatedField(batchType.findFieldByName("items"), ticket("second"))
                .build();

        PipelineExecutor executor = new PipelineExecutor(
                new DynamicPipelineTransport(dep -> echoChannel));

        // In-process run stamps no job operation headers.
        executor.run(pipeline, PipelineFixtures.files(), input);
        assertThat(recordedHeaders).containsExactly("-|-|-|-", "-|-|-|-");

        // Coordinated segment run stamps jobId, stepName, branchId, and operationId.
        recordedHeaders.clear();
        executor.runSegment(pipeline, PipelineFixtures.files(), input, List.of(),
                new ExecutionIdentity("job-123", 2), cp -> { });
        assertThat(recordedHeaders).containsExactlyInAnyOrder(
                "job-123|fan_apply|fan_apply#0|job-123/fan_apply#0",
                "job-123|fan_apply|fan_apply#1|job-123/fan_apply#1");
    }

    @Test
    void observerFailureStopsImmediatelyAndPreservesCause() {
        Pipeline pipeline = PipelineFixtures.base("observer-failure", TICKET)
                .addDependencies(PipelineFixtures.dependency("pipeline.test.Worker"))
                .addSteps(PipelineFixtures.grpcStep("first", "pipeline.test.Worker", PROCESS,
                        MethodShape.METHOD_SHAPE_UNARY, EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(TICKET, List.of("input"),
                                "title = input.title")).build())
                .addSteps(PipelineFixtures.grpcStep("second", "pipeline.test.Worker", PROCESS,
                        MethodShape.METHOD_SHAPE_UNARY, EdgeCardinality.EDGE_CARDINALITY_ONE,
                        EdgeCardinality.EDGE_CARDINALITY_ONE,
                        PipelineFixtures.edge(TICKET, List.of("first"),
                                "title = first.title")).build())
                .build();

        AtomicInteger calls = new AtomicInteger();
        PipelineExecutor executor = new PipelineExecutor((dep, method, reqs, deadline, max) -> {
            calls.incrementAndGet();
            return List.of(reqs.getFirst());
        });
        IllegalStateException rootCause = new IllegalStateException("simulated store fencing loss");

        assertThatThrownBy(() -> executor.runSegment(
                pipeline, PipelineFixtures.files(), ticket("x"), List.of(),
                cp -> {
                    throw rootCause;
                }))
                .isInstanceOfSatisfying(PipelineExecutionException.class, e -> {
                    assertThat(e.kind()).isEqualTo(FailureKind.PIPELINE);
                    assertThat(e.step()).isEqualTo("first");
                    assertThat(e.getCause()).isSameAs(rootCause);
                });
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void replayRejectsTamperedIdentityCardinalityForgedCountsAndValidationViolations()
            throws Exception {
        String validatedProto = """
                syntax = "proto3";
                package pipeline.validated;
                import "ai/protomolt/proto/validate/v1/validate.proto";
                message Note {
                  string text = 1 [(ai.protomolt.proto.validate.v1.field) = {
                    required: true
                    string: {min_len: 4}
                  }];
                }
                service Desk { rpc Approve(Note) returns (Note); }
                """;
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(PipelineFixtures.VALIDATE,
                        PipelineFixtures.resource(PipelineFixtures.VALIDATE), "test")
                .add("pipeline/validated/validated.proto", validatedProto, "test")
                .build());
        FileDescriptor file = compiled.descriptorFor("pipeline/validated/validated.proto")
                .orElseThrow();
        List<FileDescriptor> files = List.of(file);
        String fp = ai.protomolt.proto.grpc.profile.ServiceProfileValidation.sha256(
                FileDescriptorSet.newBuilder().addFile(file.toProto()).build().toByteArray());
        Descriptor noteType = file.findMessageTypeByName("Note");

        Pipeline pipeline = Pipeline.newBuilder()
                .setName("validated-external")
                .setInputType("pipeline.validated.Note")
                .setDescriptorFingerprint(fp)
                .setMaxStreamMessages(10)
                .setDeadline(Duration.newBuilder().setSeconds(10).build())
                .addDependencies(ai.protomolt.proto.grpc.workflow.v1.ServiceDependency.newBuilder()
                        .setAlias("desk")
                        .setServiceProfile("desk")
                        .setEndpoint("local")
                        .setDescriptorFingerprint(fp)
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("approve")
                        .setDependency("desk")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("pipeline.validated.Desk/Approve")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                                .setValidateResponse(true)
                                .build())
                        .build())
                .build();

        DynamicMessage validInput = DynamicMessage.newBuilder(noteType)
                .setField(noteType.findFieldByName("text"), "valid-input")
                .build();
        DynamicMessage invalidShort = DynamicMessage.newBuilder(noteType)
                .setField(noteType.findFieldByName("text"), "no")
                .build();

        PipelineExecutor executor = new PipelineExecutor((dep, method, reqs, deadline, max) ->
                List.of());
        Segment.Parked parked = (Segment.Parked) executor.runSegment(
                pipeline, files, validInput, List.of(), cp -> { });

        // 1. Altered input fails identity check with PIPELINE.
        DynamicMessage otherInput = DynamicMessage.newBuilder(noteType)
                .setField(noteType.findFieldByName("text"), "different-input")
                .build();
        Checkpoint validCompletion = parked.completion(List.of(validInput));
        assertThatThrownBy(() -> executor.runSegment(
                pipeline, files, otherInput, List.of(validCompletion), cp -> { }))
                .isInstanceOfSatisfying(PipelineExecutionException.class,
                        e -> assertThat(e.kind()).isEqualTo(FailureKind.PIPELINE));

        // 2. Forged non-zero requestCount on an external step fails with PIPELINE.
        Checkpoint forgedRequestCount = new Checkpoint(
                parked.pipelineFingerprint(),
                parked.inputFingerprint(),
                new StepOutcome("approve", false, 1, 1),
                EdgeCardinality.EDGE_CARDINALITY_ONE,
                List.of(validInput),
                List.of());
        assertThatThrownBy(() -> executor.runSegment(
                pipeline, files, validInput, List.of(forgedRequestCount), cp -> { }))
                .isInstanceOfSatisfying(PipelineExecutionException.class,
                        e -> assertThat(e.kind()).isEqualTo(FailureKind.PIPELINE));

        // 3. Invalid response under validate_response fails with VALIDATION.
        Checkpoint invalidCompletion = parked.completion(List.of(invalidShort));
        assertThatThrownBy(() -> executor.runSegment(
                pipeline, files, validInput, List.of(invalidCompletion), cp -> { }))
                .isInstanceOfSatisfying(PipelineExecutionException.class,
                        e -> assertThat(e.kind()).isEqualTo(FailureKind.VALIDATION));

        // 4. Valid completion succeeds.
        Segment resumed = executor.runSegment(
                pipeline, files, validInput, List.of(validCompletion), cp -> { });
        assertThat(resumed).isInstanceOf(Segment.Completed.class);
    }

    private static PipelineStep.Builder externalStep(String name, String service, String method,
                                                     MethodShape shape,
                                                     EdgeCardinality outputCardinality,
                                                     boolean validateResponse) {
        return PipelineStep.newBuilder()
                .setName(name)
                .setDependency(service)
                .setGrpcCall(GrpcCallStep.newBuilder()
                        .setMethod(method)
                        .setMethodShape(shape)
                        .setOutputCardinality(outputCardinality)
                        .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                        .setValidateResponse(validateResponse)
                        .build());
    }

    private static DynamicMessage ticket(String title) {
        Descriptor type = PipelineFixtures.type(TICKET);
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("title"), title)
                .build();
    }

    private static DynamicMessage lookupResult(String docId, String title, String notes) {
        Descriptor type = PipelineFixtures.type(LOOKUP_RESULT);
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("doc_id"), docId)
                .setField(type.findFieldByName("title"), title)
                .setField(type.findFieldByName("internal_notes"), notes)
                .build();
    }

    private static String titleOf(DynamicMessage msg) {
        return (String) msg.getField(msg.getDescriptorForType().findFieldByName("title"));
    }
}
