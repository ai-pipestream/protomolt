package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.http.json.ProtobufJsonTranscoder;
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
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PipelineRunCodec} and {@link PipelineCheckpointCodec} fail-hard
 * validation of pipeline run envelopes and auxiliary JSON previews.
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
    void isPipelineDefinitionDistinguishesValidPipelineFromOrdinaryWorkflow() {
        ObjectNode validEnvelope = PipelineRunCodec.encodeDefinition(pipeline, files, validInput);
        assertThat(PipelineRunCodec.isPipelineDefinition(validEnvelope.toString())).isTrue();

        WorkflowRunRecord pipelineRow = new WorkflowRunRecord();
        pipelineRow.workflowDefinition = validEnvelope.toString();
        assertThat(PipelineJobCoordinator.isPipelineJob(pipelineRow)).isTrue();
        assertThat(PipelineJobCoordinator.isPipelineJob(null)).isFalse();

        TestWorkflows workflows = new TestWorkflows();
        try {
            String ordinaryJson = workflows.twoStepWorkflow("in-process", null).toString();
            assertThat(PipelineRunCodec.isPipelineDefinition(ordinaryJson)).isFalse();
            WorkflowRunRecord workflowRow = new WorkflowRunRecord();
            workflowRow.workflowDefinition = ordinaryJson;
            assertThat(PipelineJobCoordinator.isPipelineJob(workflowRow)).isFalse();
        } finally {
            workflows.stop();
        }
    }

    @Test
    void isPipelineDefinitionRefusesMalformedAndNonObjectJson() {
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not readable JSON");
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition("{not json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not readable JSON");
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a JSON object");
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition("[1, 2, 3]"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a JSON object");
    }

    @Test
    void isPipelineDefinitionRefusesCorruptPipelineEnvelopes() {
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(
                "{\"format\":\"protomolt.pipeline.v1\"}"))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("missing pipelineBase64");

        ObjectNode badPipelineBytes = PipelineRunCodec.encodeDefinition(pipeline, files, validInput);
        badPipelineBytes.put(PipelineRunCodec.PIPELINE_BASE64_KEY, "!!!not-base64!!!");
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(badPipelineBytes.toString()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("invalid pipelineBase64");

        ObjectNode missingDescriptors = PipelineRunCodec.encodeDefinition(pipeline, files, validInput);
        missingDescriptors.remove(PipelineRunCodec.DESCRIPTOR_SET_BASE64_KEY);
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(missingDescriptors.toString()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("missing descriptorSetBase64");

        ObjectNode missingInput = PipelineRunCodec.encodeDefinition(pipeline, files, validInput);
        missingInput.remove(PipelineRunCodec.INPUT_BASE64_KEY);
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(missingInput.toString()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("missing inputBase64");

        ObjectNode tamperedFp = PipelineRunCodec.encodeDefinition(pipeline, files, validInput);
        tamperedFp.put(PipelineRunCodec.PIPELINE_FINGERPRINT_KEY, "tampered");
        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(tamperedFp.toString()))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("pipeline fingerprint");

        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(
                "{\"format\":\"protomolt.pipeline.v2\"}"))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("unsupported pipeline definition format");

        assertThatThrownBy(() -> PipelineRunCodec.isPipelineDefinition(
                "{\"pipelineBase64\":\"YWJj\"}"))
                .isInstanceOf(PipelineDefinitionException.class)
                .hasMessageContaining("missing format");
    }

    @Test
    void workerAndCompleteStepRefuseCorruptPipelineDefinitionsWithoutWorkflowFallback() {
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
            UUID missingPayloadId = insertQueued(store, "{\"format\":\"protomolt.pipeline.v1\"}");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord missingPayload = store.get(missingPayloadId).orElseThrow();
            assertThat(missingPayload.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(missingPayload.error).isEqualTo("PIPELINE: missing pipelineBase64");

            ObjectNode corruptEnvelope = PipelineRunCodec.encodeDefinition(pipeline, files, validInput);
            corruptEnvelope.put(PipelineRunCodec.PIPELINE_BASE64_KEY, "***corrupt***");
            UUID corruptPayloadId = insertQueued(store, corruptEnvelope.toString());
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord corruptPayload = store.get(corruptPayloadId).orElseThrow();
            assertThat(corruptPayload.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(corruptPayload.error).startsWith("PIPELINE: invalid pipelineBase64:");

            UUID unformattedMarkerId = insertQueued(store, "{\"pipelineBase64\":\"YWJj\"}");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord unformatted = store.get(unformattedMarkerId).orElseThrow();
            assertThat(unformatted.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(unformatted.error)
                    .isEqualTo("PIPELINE: stored pipeline definition is missing format 'protomolt.pipeline.v1'");
            assertThat(store.events().stream()
                    .filter(e -> WorkflowRunEventRecord.TYPE_FAILED.equals(e.eventType))
                    .count()).isEqualTo(3);

            UUID parkedCorruptId = insertWaiting(store, "{\"format\":\"protomolt.pipeline.v1\"}");
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
    void messageJsonObjectPreservesBinaryFallbackOnUnregisteredAnyAndPropagatesMapperFaults()
            throws Exception {
        Descriptor inputType = file.findMessageTypeByName("EnvelopeInput");
        DescriptorRegistry registry = PipelineRunCodec.buildRegistry(files, null);
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(registry);

        Any unregisteredAny = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/unregistered.OpaqueSecret")
                .setValue(ByteString.copyFrom(new byte[] {0x0a, 0x03, 0x78, 0x79, 0x7a}))
                .build();
        DynamicMessage withUnregisteredAny = DynamicMessage.newBuilder(inputType)
                .setField(inputType.findFieldByName("request_id"), "req-opaque")
                .setField(inputType.findFieldByName("detail"),
                        DynamicMessage.parseFrom(Any.getDescriptor(), unregisteredAny.toByteString()))
                .build();

        ObjectNode fallback = PipelineCheckpointCodec.messageJsonObject(
                withUnregisteredAny, transcoder, JSON);
        assertThat(fallback.size()).isEqualTo(1);
        byte[] fallbackBytes = Base64.getDecoder().decode(
                fallback.get(PipelineCheckpointCodec.BINARY_BASE64_FALLBACK_KEY).asText());
        assertThat(fallbackBytes)
                .isEqualTo(MessageFingerprints.deterministicBytes(withUnregisteredAny));

        ObjectMapper brokenRuntimeMapper = new ObjectMapper() {
            @Override
            public JsonNode readTree(String content) {
                throw new IllegalStateException("mapper fault");
            }
        };
        assertThatThrownBy(() -> PipelineCheckpointCodec.messageJsonObject(
                validInput, transcoder, brokenRuntimeMapper))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("mapper fault");

        ObjectMapper brokenParseMapper = new ObjectMapper() {
            @Override
            public JsonNode readTree(String content) throws JsonProcessingException {
                throw new JsonParseException(null, "synthetic parse failure");
            }
        };
        assertThatThrownBy(() -> PipelineCheckpointCodec.messageJsonObject(
                validInput, transcoder, brokenParseMapper))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("proto3 JSON printer produced text that is not JSON")
                .hasCauseInstanceOf(JsonParseException.class);
    }

    private static UUID insertQueued(InMemoryWorkflowRunStore store, String workflowDefinition) {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = UUID.randomUUID();
        record.workflowName = "corrupt-pipeline";
        record.workflowDefinition = workflowDefinition;
        record.input = "{}";
        record.checkpoints = "[]";
        record.maxAttempts = 3;
        record.runAfter = Instant.now();
        store.insert(record, WorkflowRunEventFactory.accepted(record));
        return record.jobId;
    }

    private static UUID insertWaiting(InMemoryWorkflowRunStore store, String workflowDefinition) {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = UUID.randomUUID();
        record.workflowName = "corrupt-pipeline";
        record.workflowDefinition = workflowDefinition;
        record.input = "{}";
        record.checkpoints = "[]";
        record.status = WorkflowRunRecord.STATUS_WAITING;
        record.outstandingStep = "echo";
        record.maxAttempts = 3;
        record.runAfter = Instant.now();
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
