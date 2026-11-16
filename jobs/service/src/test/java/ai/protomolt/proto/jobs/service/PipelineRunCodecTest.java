package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.http.json.ProtobufJsonException;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.actions.CompleteStepAction;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.pipeline.DynamicPipelineTransport;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoint;
import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoints;
import ai.protomolt.proto.pipeline.v1.PipelineRunDefinition;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ListValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PipelineRunCodec}, {@link PipelineCheckpointCodec} and
 * {@link PipelineRunView}: protobuf run state round-trips, corrupt state is refused, and the
 * JSON view is output only.
 */
class PipelineRunCodecTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String SCHEMA = """
            syntax = "proto3";
            package jobs.pipeline.codec;
            import "google/protobuf/any.proto";

            message EnvelopeInput {
              string request_id = 1;
              google.protobuf.Any detail = 2;
              repeated google.protobuf.Any extras = 3;
            }
            message EnvelopeOutput {
              string status = 1;
            }
            service EchoService {
              rpc Echo(EnvelopeInput) returns (EnvelopeOutput);
            }
            """;

    private static FileDescriptor file;
    private static List<FileDescriptor> files;
    private static Pipeline pipeline;
    private static DynamicMessage validInput;

    @BeforeAll
    static void compileSchema() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("google/protobuf/any.proto", resource("google/protobuf/any.proto"), "test")
                .add("jobs/pipeline/codec/codec.proto", SCHEMA, "test")
                .build());
        file = compiled.descriptorFor("jobs/pipeline/codec/codec.proto").orElseThrow();
        files = List.of(file);
        String descriptorFp = ServiceProfileValidation.sha256(
                FileDescriptorSet.newBuilder().addFile(file.toProto()).build().toByteArray());
        pipeline = Pipeline.newBuilder()
                .setName("codec-pipeline")
                .setInputType("jobs.pipeline.codec.EnvelopeInput")
                .setDescriptorFingerprint(descriptorFp)
                .setMaxStreamMessages(16)
                .setDeadline(Duration.newBuilder().setSeconds(10).build())
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("echo")
                        .setServiceProfile("echo")
                        .setEndpoint("in-process.unused")
                        .setDescriptorFingerprint(descriptorFp)
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("echo")
                        .setDependency("echo")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.codec.EchoService/Echo")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("input")
                                        .setProduceType("jobs.pipeline.codec.EnvelopeInput")
                                        .addRules("request_id = input.request_id")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
        Descriptor inputType = file.findMessageTypeByName("EnvelopeInput");
        validInput = DynamicMessage.newBuilder(inputType)
                .setField(inputType.findFieldByName("request_id"), "req-1")
                .build();
    }

    @Test
    void definitionRoundTripsPipelineDescriptorsInputAndBothFingerprints() {
        PipelineRunDefinition stored = PipelineRunCodec.encodeDefinition(pipeline, files, validInput);
        assertThat(stored.getRootFilesList()).containsExactly("jobs/pipeline/codec/codec.proto");
        assertThat(stored.getDescriptorSet().getFileList())
                .extracting(f -> f.getName())
                .containsExactly("google/protobuf/any.proto", "jobs/pipeline/codec/codec.proto");

        PipelineJobCoordinator.Definition restored =
                PipelineRunCodec.readDefinition(pipelineRow(stored.toByteArray()));
        assertThat(restored.pipeline()).isEqualTo(pipeline);
        assertThat(restored.input().toByteString()).isEqualTo(validInput.toByteString());
        assertThat(restored.pipelineFingerprint())
                .isEqualTo(PipelineExecutor.pipelineFingerprint(pipeline, files));
        assertThat(restored.inputFingerprint())
                .isEqualTo(PipelineExecutor.inputFingerprint(validInput));
        // The same submission always serializes to the same bytes.
        assertThat(PipelineRunCodec.encodeDefinition(pipeline, files, validInput).toByteArray())
                .isEqualTo(stored.toByteArray());
    }

    @Test
    void readDefinitionRefusesUnreadableIncompleteAndTamperedDefinitions() {
        PipelineRunDefinition valid = PipelineRunCodec.encodeDefinition(pipeline, files, validInput);

        assertThatThrownBy(() -> PipelineRunCodec.readDefinition(pipelineRow(null)))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessage("job has no stored pipeline definition");
        assertThatThrownBy(() -> PipelineRunCodec.readDefinition(
                        pipelineRow(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff})))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageStartingWith(
                        "stored pipeline definition is not a readable PipelineRunDefinition");
        assertThatThrownBy(() -> PipelineRunCodec.readDefinition(
                        valid.toBuilder().clearPipeline().build()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageStartingWith("pipeline run definition is invalid at pipeline");
        assertThatThrownBy(() -> PipelineRunCodec.readDefinition(
                        valid.toBuilder().clearRootFiles().build()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageStartingWith("pipeline run definition is invalid at root_files");
        assertThatThrownBy(() -> PipelineRunCodec.readDefinition(
                        valid.toBuilder().setRootFiles(0, "absent.proto").build()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessage("root file 'absent.proto' is absent from the stored descriptor set");
        assertThatThrownBy(() -> PipelineRunCodec.readDefinition(valid.toBuilder()
                        .setDescriptorSet(FileDescriptorSet.newBuilder()
                                .addFile(file.toProto()))
                        .build()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageStartingWith(
                        "unresolved descriptor dependency 'google/protobuf/any.proto'");
        assertThatThrownBy(() -> PipelineRunCodec.readDefinition(
                        valid.toBuilder().setPipelineFingerprint("tampered").build()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessage("stored pipeline fingerprint does not match reconstructed "
                        + "pipeline and descriptors");
        Descriptor inputType = file.findMessageTypeByName("EnvelopeInput");
        DynamicMessage otherInput = DynamicMessage.newBuilder(inputType)
                .setField(inputType.findFieldByName("request_id"), "req-2")
                .build();
        assertThatThrownBy(() -> PipelineRunCodec.readDefinition(
                        valid.toBuilder().setInput(otherInput.toByteString()).build()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessage("stored input fingerprint does not match decoded input bytes");
    }

    @Test
    void checkpointsRoundTripMessagesCountsAndBranchEvidenceAndRefuseCorruption() {
        PipelineJobCoordinator.Definition definition = PipelineRunCodec.readDefinition(
                PipelineRunCodec.encodeDefinition(pipeline, files, validInput));
        // The run's own descriptors, as rebuilt from the stored set, decode its messages.
        Descriptor outputType = definition.files().getFirst()
                .findMessageTypeByName("EnvelopeOutput");
        DynamicMessage output = DynamicMessage.newBuilder(outputType)
                .setField(outputType.findFieldByName("status"), "ok")
                .build();
        PipelineExecutor.Checkpoint checkpoint = new PipelineExecutor.Checkpoint(
                definition.pipelineFingerprint(),
                definition.inputFingerprint(),
                new PipelineExecutor.StepOutcome("echo", false, 1, 1),
                EdgeCardinality.EDGE_CARDINALITY_ONE,
                List.of(output),
                List.of(
                        new PipelineExecutor.BranchOutcome("echo#0", 0, true, null, ""),
                        new PipelineExecutor.BranchOutcome(
                                "echo#1", 1, false, PipelineExecutor.FailureKind.GRPC, "boom")));

        byte[] stored = PipelineJobCoordinator.writeCheckpoints(List.of(checkpoint));
        WorkflowRunRecord row = pipelineRow(
                PipelineRunCodec.encodeDefinition(pipeline, files, validInput).toByteArray());
        row.pipelineCheckpoints = stored;
        assertThat(PipelineJobCoordinator.readCheckpoints(row, definition))
                .containsExactly(checkpoint);

        row.pipelineCheckpoints = null;
        assertThat(PipelineJobCoordinator.readCheckpoints(row, definition)).isEmpty();

        row.pipelineCheckpoints = new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff};
        assertThatThrownBy(() -> PipelineJobCoordinator.readCheckpoints(row, definition))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageStartingWith(
                        "stored checkpoints are not a readable PipelineRunCheckpoints");

        PipelineRunCheckpoint wrongStep = PipelineCheckpointCodec.toProto(checkpoint).toBuilder()
                .setOutcome(PipelineCheckpointCodec.toProto(checkpoint).getOutcome().toBuilder()
                        .setStep("other"))
                .build();
        assertThatThrownBy(() -> PipelineCheckpointCodec.readCheckpoints(
                        PipelineRunCheckpoints.newBuilder().addCheckpoints(wrongStep).build(),
                        definition))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("belongs to step 'other'");

        PipelineRunCheckpoint twice = PipelineCheckpointCodec.toProto(checkpoint);
        assertThatThrownBy(() -> PipelineCheckpointCodec.readCheckpoints(
                        PipelineRunCheckpoints.newBuilder()
                                .addCheckpoints(twice).addCheckpoints(twice).build(),
                        definition))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("outnumber pipeline steps");

        PipelineRunCheckpoint badMessage = PipelineCheckpointCodec.toProto(checkpoint).toBuilder()
                .setMessages(0, ByteString.copyFrom(new byte[] {(byte) 0xff, (byte) 0xff}))
                .build();
        assertThatThrownBy(() -> PipelineCheckpointCodec.readCheckpoints(
                        PipelineRunCheckpoints.newBuilder().addCheckpoints(badMessage).build(),
                        definition))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageStartingWith(
                        "stored message cannot be decoded as jobs.pipeline.codec.EnvelopeOutput");
    }

    @Test
    void workerAndCompleteStepRefuseCorruptPipelineDefinitions() {
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunWorker worker = new WorkflowRunWorker(
                store,
                ActionContext.create(),
                null,
                new WorkflowRunner(step -> InProcessChannelBuilder.forName("unused").build()),
                new PipelineExecutor(new DynamicPipelineTransport(
                        dep -> InProcessChannelBuilder.forName("unused").build())),
                new WorkflowRunsConfig("codec-worker", 1, java.time.Duration.ofSeconds(30),
                        java.time.Duration.ofMillis(20), 1, 3, 4, null,
                        "workflow-run-events", null, null));
        try {
            UUID unreadableId = insert(store, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff},
                    WorkflowRunRecord.STATUS_QUEUED);
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord unreadable = store.get(unreadableId).orElseThrow();
            assertThat(unreadable.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(unreadable.error).startsWith(
                    "PIPELINE: stored pipeline definition is not a readable PipelineRunDefinition");

            byte[] tampered = PipelineRunCodec.encodeDefinition(pipeline, files, validInput)
                    .toBuilder().setPipelineFingerprint("tampered").build().toByteArray();
            UUID tamperedId = insert(store, tampered, WorkflowRunRecord.STATUS_QUEUED);
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord tamperedRow = store.get(tamperedId).orElseThrow();
            assertThat(tamperedRow.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(tamperedRow.error).isEqualTo(
                    "PIPELINE: stored pipeline fingerprint does not match reconstructed "
                            + "pipeline and descriptors");
            assertThat(store.events().stream()
                    .filter(e -> WorkflowRunEventRecord.TYPE_FAILED.equals(e.eventType))
                    .count()).isEqualTo(2);

            UUID parkedCorruptId = insert(store, new byte[] {(byte) 0xff, (byte) 0xff},
                    WorkflowRunRecord.STATUS_WAITING);
            ObjectNode completeReq = JSON.createObjectNode()
                    .put("jobId", parkedCorruptId.toString())
                    .put("stepName", "echo");
            completeReq.putObject("response");
            CompleteStepAction completeStep = new CompleteStepAction(store);
            assertThatThrownBy(() -> ActionCatalog.defaults(ActionContext.create())
                    .replace(completeStep).execute(completeStep.name(), completeReq))
                    .isInstanceOfSatisfying(ActionException.class,
                            e -> assertThat(e.code()).isEqualTo("job-corrupt"));
        } finally {
            worker.close();
        }
    }

    @Test
    void viewRendersARunAsJsonAndMarksAMessageItCannotPrint() throws Exception {
        WorkflowRunRecord row = pipelineRow(
                PipelineRunCodec.encodeDefinition(pipeline, files, validInput).toByteArray());
        PipelineJobCoordinator.RunView view = PipelineJobCoordinator.view(row);
        assertThat(view.input().get("requestId").asText()).isEqualTo("req-1");
        assertThat(view.checkpoints()).isEmpty();
        assertThat(view.result()).isNull();

        Descriptor inputType = file.findMessageTypeByName("EnvelopeInput");
        Any unregisteredAny = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/unregistered.OpaqueSecret")
                .setValue(ByteString.copyFrom(new byte[] {0x0a, 0x03, 0x78, 0x79, 0x7a}))
                .build();
        DynamicMessage withUnregisteredAny = DynamicMessage.newBuilder(inputType)
                .setField(inputType.findFieldByName("request_id"), "req-opaque")
                .setField(inputType.findFieldByName("detail"),
                        DynamicMessage.parseFrom(Any.getDescriptor(), unregisteredAny.toByteString()))
                .build();
        WorkflowRunRecord opaqueRow = pipelineRow(PipelineRunCodec.encodeDefinition(
                pipeline, files, withUnregisteredAny).toByteArray());
        PipelineJobCoordinator.RunView opaque = PipelineJobCoordinator.view(opaqueRow);
        assertThat(opaque.input().size()).isEqualTo(1);
        assertThat(opaque.input().get(PipelineRunView.UNRENDERABLE_TYPE_KEY).asText())
                .isEqualTo("jobs.pipeline.codec.EnvelopeInput");
        // The stored input is untouched by the view.
        assertThat(PipelineRunCodec.readDefinition(opaqueRow).input().toByteString())
                .isEqualTo(withUnregisteredAny.toByteString());
    }

    @Test
    void submitRefusesADefinitionThatWouldNotReadBackAndStoresNothing() throws Exception {
        // 4097 root files: one past the stored definition's root_files bound.
        List<FileDescriptor> tooMany = new ArrayList<>();
        tooMany.add(file);
        for (int i = 0; i < 4096; i++) {
            tooMany.add(FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                    .setName(String.format("extra/f%04d.proto", i))
                    .setSyntax("proto3")
                    .build(), new FileDescriptor[0]));
        }
        String descriptorFp = ServiceProfileValidation.sha256(FileDescriptorSet.newBuilder()
                .addAllFile(tooMany.stream()
                        .map(FileDescriptor::toProto)
                        .sorted(Comparator.comparing(FileDescriptorProto::getName))
                        .toList())
                .build().toByteArray());
        Pipeline wide = pipeline.toBuilder()
                .setDescriptorFingerprint(descriptorFp)
                .setDependencies(0, pipeline.getDependencies(0).toBuilder()
                        .setDescriptorFingerprint(descriptorFp))
                .build();

        assertThatThrownBy(() -> PipelineRunCodec.encodeDefinition(wide, tooMany, validInput))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("root_files");

        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter.Outcome refused = new WorkflowRunSubmitter(store, null, 3)
                .submitPipeline(wide, tooMany, validInput, UUID.randomUUID().toString());
        assertThat(refused.ok()).isFalse();
        assertThat(refused.error()).contains("root_files");
        assertThat(store.list(null, null, 10, 0)).isEmpty();

        // At the bound the same submission is stored and reads back.
        List<FileDescriptor> atBound = tooMany.subList(0, 4096);
        String boundFp = ServiceProfileValidation.sha256(FileDescriptorSet.newBuilder()
                .addAllFile(atBound.stream()
                        .map(FileDescriptor::toProto)
                        .sorted(Comparator.comparing(FileDescriptorProto::getName))
                        .toList())
                .build().toByteArray());
        Pipeline bounded = pipeline.toBuilder()
                .setDescriptorFingerprint(boundFp)
                .setDependencies(0, pipeline.getDependencies(0).toBuilder()
                        .setDescriptorFingerprint(boundFp))
                .build();
        WorkflowRunSubmitter.Outcome accepted = new WorkflowRunSubmitter(store, null, 3)
                .submitPipeline(bounded, atBound, validInput, UUID.randomUUID().toString());
        assertThat(accepted.error()).isNull();
        assertThat(accepted.ok()).isTrue();
        WorkflowRunRecord stored = store.get(UUID.fromString(accepted.jobId())).orElseThrow();
        assertThat(PipelineRunCodec.readDefinition(stored).files()).hasSize(4096);
    }

    @Test
    void viewFailsOnAMessageThatCannotPrintForAReasonOtherThanAnUnknownAnyType()
            throws Exception {
        Descriptor inputType = file.findMessageTypeByName("EnvelopeInput");
        // The Any names a type the run does define, but its bytes are not that type.
        Any corruptKnownAny = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/jobs.pipeline.codec.EnvelopeOutput")
                .setValue(ByteString.copyFrom(new byte[] {(byte) 0xFF}))
                .build();
        DynamicMessage withCorruptAny = DynamicMessage.newBuilder(inputType)
                .setField(inputType.findFieldByName("request_id"), "req-corrupt")
                .setField(inputType.findFieldByName("detail"),
                        DynamicMessage.parseFrom(Any.getDescriptor(),
                                corruptKnownAny.toByteString()))
                .build();
        WorkflowRunRecord row = pipelineRow(PipelineRunCodec.encodeDefinition(
                pipeline, files, withCorruptAny).toByteArray());

        assertThatThrownBy(() -> PipelineJobCoordinator.view(row))
                .isInstanceOf(ProtobufJsonException.class);
    }

    @Test
    void viewCarriesAMessageThatDoesNotPrintAsAnObjectUnderValue() throws Exception {
        PipelineRunView.Printer printer =
                PipelineRunView.Printer.over(PipelineRunCodec.buildRegistry(files, null));
        DynamicMessage text = DynamicMessage.parseFrom(
                StringValue.getDescriptor(), StringValue.of("plain").toByteString());
        assertThat(PipelineRunView.message(text, printer).asText()).isEqualTo("plain");
        assertThat(PipelineRunView.asObject(text, printer).get("value").asText())
                .isEqualTo("plain");

        DynamicMessage instant = DynamicMessage.parseFrom(Timestamp.getDescriptor(),
                Timestamp.newBuilder().setSeconds(1).build().toByteString());
        assertThat(PipelineRunView.asObject(instant, printer).get("value").asText())
                .isEqualTo("1970-01-01T00:00:01Z");

        DynamicMessage list = DynamicMessage.parseFrom(ListValue.getDescriptor(),
                ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("a"))
                        .addValues(Value.newBuilder().setNumberValue(2))
                        .build().toByteString());
        assertThat(PipelineRunView.asObject(list, printer).get("value")).hasSize(2);

        // A Value that happens to hold an object is still carried under "value": the rule
        // goes by type, so a reader never has to guess.
        DynamicMessage structValue = DynamicMessage.parseFrom(Value.getDescriptor(),
                Value.newBuilder()
                        .setStructValue(com.google.protobuf.Struct.newBuilder()
                                .putFields("k", Value.newBuilder().setBoolValue(true).build()))
                        .build().toByteString());
        assertThat(PipelineRunView.asObject(structValue, printer).get("value").get("k")
                .asBoolean()).isTrue();

        // An ordinary message is its own object.
        assertThat(PipelineRunView.asObject(validInput, printer).get("requestId").asText())
                .isEqualTo("req-1");
    }

    @Test
    void externalCompletionAcceptsTypesThatDoNotPrintAsObjects() throws Exception {
        ObjectNode unary = JSON.createObjectNode().put("value", "approved");
        assertThat(PipelineExternalCompletionParser.parseResponses(
                        unary, EdgeCardinality.EDGE_CARDINALITY_ONE, StringValue.getDescriptor(),
                        8, "approve", files, null))
                .singleElement()
                .satisfies(m -> assertThat(m.toByteString())
                        .isEqualTo(StringValue.of("approved").toByteString()));

        ObjectNode stream = JSON.createObjectNode();
        stream.putArray("items").add("a").add("b");
        assertThat(PipelineExternalCompletionParser.parseResponses(
                        stream, EdgeCardinality.EDGE_CARDINALITY_MANY, StringValue.getDescriptor(),
                        8, "approve", files, null))
                .extracting(m -> m.toByteString())
                .containsExactly(StringValue.of("a").toByteString(),
                        StringValue.of("b").toByteString());

        // Without the "value" key there is no way to tell what was meant.
        assertThatThrownBy(() -> PipelineExternalCompletionParser.parseResponses(
                        JSON.createObjectNode().put("text", "approved"),
                        EdgeCardinality.EDGE_CARDINALITY_ONE, StringValue.getDescriptor(),
                        8, "approve", files, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("google.protobuf.StringValue")
                .hasMessageContaining("{\"value\": ...}");

        // A stream of an ordinary message type still refuses an item that is not an object.
        ObjectNode badStream = JSON.createObjectNode();
        badStream.putArray("items").add("not-an-object");
        assertThatThrownBy(() -> PipelineExternalCompletionParser.parseResponses(
                        badStream, EdgeCardinality.EDGE_CARDINALITY_MANY,
                        file.findMessageTypeByName("EnvelopeOutput"),
                        8, "approve", files, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("item 0 must be a JSON object");
    }

    @Test
    void anAnyIsAlwaysAnObjectEvenWhenItPacksATypeThatIsNot() throws Exception {
        // proto3 JSON prints Any as {"@type": ..., "value": <custom JSON>} for a packed
        // well-known type, so Any needs no "value" carrier of its own.
        DescriptorRegistry wellKnown = DescriptorRegistry.create(false);
        wellKnown.registerFile(Timestamp.getDescriptor().getFile());
        wellKnown.registerFile(Any.getDescriptor().getFile());
        PipelineRunView.Printer printer = PipelineRunView.Printer.over(wellKnown);
        Any packedInstant = Any.pack(Timestamp.newBuilder().setSeconds(1).build());
        DynamicMessage any = DynamicMessage.parseFrom(
                Any.getDescriptor(), packedInstant.toByteString());

        ObjectNode printed = PipelineRunView.asObject(any, printer);
        assertThat(printed.get("@type").asText())
                .isEqualTo("type.googleapis.com/google.protobuf.Timestamp");
        assertThat(printed.get("value").asText()).isEqualTo("1970-01-01T00:00:01Z");

        // What the view shows is what complete-step accepts, unary and as a stream item.
        assertThat(PipelineExternalCompletionParser.parseResponses(
                        printed, EdgeCardinality.EDGE_CARDINALITY_ONE, Any.getDescriptor(),
                        8, "approve", files, wellKnown))
                .singleElement()
                .satisfies(m -> assertThat(m.toByteString())
                        .isEqualTo(packedInstant.toByteString()));
        ObjectNode stream = JSON.createObjectNode();
        stream.putArray("items").add(printed).add(printed);
        assertThat(PipelineExternalCompletionParser.parseResponses(
                        stream, EdgeCardinality.EDGE_CARDINALITY_MANY, Any.getDescriptor(),
                        8, "approve", files, wellKnown))
                .extracting(m -> m.toByteString())
                .containsExactly(packedInstant.toByteString(), packedInstant.toByteString());
    }

    @Test
    void viewRaisesCorruptionEvenWhenAnotherAnyHasAnUnknownType() throws Exception {
        Descriptor inputType = file.findMessageTypeByName("EnvelopeInput");
        Any corruptKnown = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/jobs.pipeline.codec.EnvelopeOutput")
                .setValue(ByteString.copyFrom(new byte[] {(byte) 0xFF}))
                .build();
        Any unknown = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/unregistered.OpaqueSecret")
                .setValue(ByteString.copyFrom(new byte[] {0x0a, 0x01, 0x78}))
                .build();
        PipelineRunView.Printer printer =
                PipelineRunView.Printer.over(PipelineRunCodec.buildRegistry(files, null));
        for (List<Any> order : List.of(
                List.of(corruptKnown, unknown), List.of(unknown, corruptKnown))) {
            DynamicMessage.Builder mixed = DynamicMessage.newBuilder(inputType)
                    .setField(inputType.findFieldByName("request_id"), "req-mixed");
            for (Any extra : order) {
                mixed.addRepeatedField(inputType.findFieldByName("extras"),
                        DynamicMessage.parseFrom(Any.getDescriptor(), extra.toByteString()));
            }
            assertThatThrownBy(() -> PipelineRunView.message(mixed.build(), printer))
                    .isInstanceOf(ProtobufJsonException.class);
        }

        // An unknown type nested inside a known Any is still only an unknown type.
        DynamicMessage inner = DynamicMessage.newBuilder(inputType)
                .setField(inputType.findFieldByName("detail"),
                        DynamicMessage.parseFrom(Any.getDescriptor(), unknown.toByteString()))
                .build();
        Any knownAroundUnknown = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/jobs.pipeline.codec.EnvelopeInput")
                .setValue(inner.toByteString())
                .build();
        DynamicMessage nested = DynamicMessage.newBuilder(inputType)
                .addRepeatedField(inputType.findFieldByName("extras"),
                        DynamicMessage.parseFrom(Any.getDescriptor(),
                                knownAroundUnknown.toByteString()))
                .build();
        assertThat(PipelineRunView.message(nested, printer)
                .get(PipelineRunView.UNRENDERABLE_TYPE_KEY).asText())
                .isEqualTo("jobs.pipeline.codec.EnvelopeInput");
    }

    @Test
    void theRunsOwnTypesWinOverASupplementalRegistryThatRedefinesThem() throws Exception {
        // The host has since moved EnvelopeOutput to another file and changed its field.
        FileDescriptor moved = FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                .setName("jobs/pipeline/codec/moved.proto")
                .setPackage("jobs.pipeline.codec")
                .setSyntax("proto3")
                .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("EnvelopeOutput")
                        .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto
                                .newBuilder()
                                .setName("status_code").setNumber(1)
                                .setType(com.google.protobuf.DescriptorProtos
                                        .FieldDescriptorProto.Type.TYPE_INT32)))
                .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("HostOnly"))
                .build(), new FileDescriptor[0]);
        DescriptorRegistry host = DescriptorRegistry.create(false);
        host.registerFile(moved);
        Descriptor retained = file.findMessageTypeByName("EnvelopeOutput");

        DescriptorRegistry combined = PipelineRunCodec.buildRegistry(files, host);
        assertThat(combined.findDescriptorByFullName("jobs.pipeline.codec.EnvelopeOutput"))
                .as("the type the run was submitted with")
                .isSameAs(retained);
        assertThat(combined.isRegistered("jobs.pipeline.codec.HostOnly"))
                .as("a type only the host defines is still added")
                .isTrue();

        // complete-step therefore encodes a response against the run's schema.
        assertThat(PipelineExternalCompletionParser.parseResponses(
                        JSON.createObjectNode().put("status", "approved"),
                        EdgeCardinality.EDGE_CARDINALITY_ONE, retained, 8, "echo", files, host))
                .singleElement()
                .satisfies(m -> assertThat(m.getField(retained.findFieldByName("status")))
                        .isEqualTo("approved"));
    }

    private static WorkflowRunRecord pipelineRow(byte[] definition) {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = UUID.randomUUID();
        record.workflowName = "codec-pipeline";
        record.pipelineDefinition = definition;
        record.checkpoints = null;
        record.maxAttempts = 3;
        record.runAfter = Instant.now();
        return record;
    }

    private static UUID insert(InMemoryWorkflowRunStore store, byte[] definition, String status) {
        WorkflowRunRecord record = pipelineRow(definition);
        record.status = status;
        if (WorkflowRunRecord.STATUS_WAITING.equals(status)) {
            record.outstandingStep = "echo";
        }
        store.insert(record, WorkflowRunEventFactory.accepted(record));
        return record.jobId;
    }

    private static String resource(String path) throws Exception {
        try (InputStream in = PipelineRunCodecTest.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
