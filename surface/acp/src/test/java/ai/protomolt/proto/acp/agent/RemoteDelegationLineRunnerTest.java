package ai.protomolt.proto.acp.agent;

import ai.protomolt.proto.acp.PromptContext;
import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.delegation.DelegationActions;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Remote ACP delegation RPCs preserve custom Any contracts across attempts and reconnects. */
class RemoteDelegationLineRunnerTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOKEN = "acp-delegation-test-token";
    private static final String WORKER = "acp-fixture-worker";
    private static final String CHECK = "report-check";

    @Test
    void serverRefusalIsDistinctFromClientJsonFailureAndDoesNotAppend() throws Exception {
        try (Fixture fixture = new Fixture(); RemoteCatalogLineRunner runner = fixture.runner(TOKEN)) {
            RecordingContext context = new RecordingContext();
            String taskId = UUID.randomUUID().toString();
            runner.run("delegation/RegisterWorker {\"workerId\":\"" + WORKER
                    + "\",\"provider\":\"fixture\"}", context);
            DeliverableContract contract = contract("count", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
            offer(runner, context, taskId, contract, 1);
            runner.run("delegation/AcceptTask {\"workerId\":\"" + WORKER
                    + "\",\"taskId\":\"" + taskId + "\",\"attempt\":1}", context);
            int before = fixture.coordinator.transcript().getEntriesCount();
            String candidate = candidateJson(contract, 1, "count", -1);
            runner.run("delegation/SubmitCandidate {\"workerId\":\"" + WORKER
                    + "\",\"taskId\":\"" + taskId + "\",\"candidate\":" + candidate + "}", context);
            assertThat(last(context)).startsWith("worker-stream-failed:");
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
            runner.run("delegation/RegisterWorker {\"workerId\":\"" + WORKER
                    + "\",\"provider\":\"fixture\"}", context);
            assertThat(last(context)).contains("\"admitted\": true");
            submit(runner, context, taskId, contract, 1, "count", 1);
        }
    }

    @Test
    void customAnyUsesEachHistoricalAttemptContractAfterRunnerRestart() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RecordingContext first = new RecordingContext();
            String taskId = UUID.randomUUID().toString();
            try (RemoteCatalogLineRunner runner = fixture.runner(TOKEN)) {
                runner.run("delegation/RegisterWorker {\"workerId\":\"" + WORKER
                        + "\",\"provider\":\"fixture\"}", first);
                assertThat(last(first)).contains("\"admitted\": true");

                DeliverableContract firstContract = contract("label", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
                offer(runner, first, taskId, firstContract, 1);
                runner.run("delegation/AcceptTask {\"workerId\":\"" + WORKER
                        + "\",\"taskId\":\"" + taskId + "\",\"attempt\":1}", first);
                submit(runner, first, taskId, firstContract, 1, "label", "attempt one");
                runner.run("delegation/CancelTask {\"taskId\":\"" + taskId
                        + "\",\"reason\":\"Close attempt one for descriptor rollover\"}", first);
                assertThat(last(first)).contains("\"ok\": true");

                DeliverableContract secondContract = contract("count", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
                offer(runner, first, taskId, secondContract, 2);
                runner.run("delegation/AcceptTask {\"workerId\":\"" + WORKER
                        + "\",\"taskId\":\"" + taskId + "\",\"attempt\":2}", first);

                int beforeInvalidCandidate = fixture.coordinator.transcript().getEntriesCount();
                runner.run("delegation/SubmitCandidate {\"workerId\":\"" + WORKER
                        + "\",\"taskId\":\"" + taskId
                        + "\",\"candidate\":{\"attempt\":2,\"revision\":1,\"summary\":\"wrong payload type\",\"evidence\":[{\"checkName\":\""
                        + CHECK + "\",\"verdict\":\"CHECK_VERDICT_PASSED\",\"ranAt\":\"2026-09-25T12:00:00Z\"}],\"artifacts\":[{\"sha256\":\""
                        + "a".repeat(64) + "\",\"mediaType\":\"application/json\",\"sizeBytes\":1}],\"result\":{\"@type\":\"type.googleapis.com/native.v1.Report\",\"label\":\"wrong attempt schema\"}}}", first);
                assertThat(last(first)).contains("invalid-input");
                assertThat(fixture.coordinator.transcript().getEntriesCount())
                        .isEqualTo(beforeInvalidCandidate);

                submit(runner, first, taskId, secondContract, 2, "count", 42);
            }

            // The second process has no in-memory offer cache. It must recover the descriptor
            // attached to each recorded attempt before printing the Any payloads.
            RecordingContext afterRestart = new RecordingContext();
            try (RemoteCatalogLineRunner restarted = fixture.runner(TOKEN)) {
                restarted.run("delegation/ReadTranscript {\"taskId\":\"" + taskId
                        + "\",\"maxEntries\":100}", afterRestart);
            }
            var rendered = JSON.readTree(last(afterRestart));
            assertThat(rendered.findValues("attempt").stream().mapToInt(node -> node.asInt()).toArray())
                    .contains(1, 2);
            assertThat(rendered.findValues("label")).extracting(node -> node.asText())
                    .contains("attempt one");
            assertThat(rendered.findValues("count")).extracting(node -> node.asInt()).contains(42);
            assertThat(rendered.findValues("value")).isEmpty();

            RecordingContext watch = new RecordingContext();
            try (RemoteCatalogLineRunner restarted = fixture.runner(TOKEN)) {
                restarted.run("delegation/WatchEvents {\"taskId\":\"" + taskId
                        + "\",\"afterCursor\":0,\"timeoutMs\":0,\"maxEvents\":100}", watch);
            }
            var watched = JSON.readTree(last(watch));
            assertThat(watched.path("cursor").asLong()).isPositive();
            assertThat(watched.path("truncated").asBoolean()).isFalse();
            assertThat(watched.findValues("label")).extracting(node -> node.asText())
                    .contains("attempt one");
            assertThat(watched.findValues("count")).extracting(node -> node.asInt()).contains(42);
        }
    }

    @Test
    void wrongCredentialIsRedactedForDelegationRpc() {
        try (Fixture fixture = new Fixture();
             RemoteCatalogLineRunner runner = fixture.runner("wrong-delegation-token")) {
            RecordingContext result = new RecordingContext();
            runner.run("delegation/ListWorkers {}", result);
            assertThat(result.messages).containsExactly(
                    "UNAUTHENTICATED: remote coordinator refused or could not complete the command");
            assertThat(String.join("", result.messages)).doesNotContain("wrong-delegation-token");
        }
    }

    @Test
    void wrongCredentialCannotSubmitTypedCandidate() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RecordingContext authorizedOutput = new RecordingContext();
            String taskId = UUID.randomUUID().toString();
            DeliverableContract contract = contract("label",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
            try (RemoteCatalogLineRunner authorized = fixture.runner(TOKEN)) {
                authorized.run("delegation/RegisterWorker {\"workerId\":\"" + WORKER
                        + "\",\"provider\":\"fixture\"}", authorizedOutput);
                offer(authorized, authorizedOutput, taskId, contract, 1);
                authorized.run("delegation/AcceptTask {\"workerId\":\"" + WORKER
                        + "\",\"taskId\":\"" + taskId + "\",\"attempt\":1}", authorizedOutput);
            }
            int beforeUnauthorized = fixture.coordinator.transcript().getEntriesCount();
            RecordingContext denied = new RecordingContext();
            try (RemoteCatalogLineRunner unauthorized = fixture.runner("wrong-delegation-token")) {
                String candidate = candidateJson(contract, 1, "label", "unauthorized");
                unauthorized.run("delegation/SubmitCandidate {\"workerId\":\"" + WORKER
                        + "\",\"taskId\":\"" + taskId + "\",\"candidate\":" + candidate + "}", denied);
            }
            assertThat(denied.messages).containsExactly(
                    "UNAUTHENTICATED: remote coordinator refused or could not complete the command");
            assertThat(String.join("", denied.messages)).doesNotContain("wrong-delegation-token");
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(beforeUnauthorized);
        }
    }

    private static void offer(RemoteCatalogLineRunner runner, RecordingContext context,
                              String taskId, DeliverableContract contract, int attempt) throws Exception {
        TaskSpec spec = TaskSpec.newBuilder().setObjective("Produce a typed report")
                .addAllowedScope("protocol/**")
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName(CHECK)
                        .setDescription("The harness inspected the report"))
                .setContract(contract).build();
        String json = JsonFormat.printer().print(spec);
        runner.run("delegation/OfferTask {\"workerId\":\"" + WORKER + "\",\"taskId\":\""
                + taskId + "\",\"spec\":" + json + "}", context);
        var response = JSON.readTree(last(context));
        assertThat(response.path("offer").path("attempt").asInt()).isEqualTo(attempt);
    }

    private static void submit(RemoteCatalogLineRunner runner, RecordingContext context,
                               String taskId, DeliverableContract contract, int attempt,
                               String fieldName, Object fieldValue) throws Exception {
        String candidateJson = candidateJson(contract, attempt, fieldName, fieldValue);
        runner.run("delegation/SubmitCandidate {\"workerId\":\"" + WORKER
                + "\",\"taskId\":\"" + taskId + "\",\"candidate\":" + candidateJson + "}", context);
        assertThat(last(context)).contains("\"ok\": true");
    }

    private static String candidateJson(DeliverableContract contract, int attempt,
                                        String fieldName, Object fieldValue) throws Exception {
        Descriptors.Descriptor descriptor = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorSet.parseFrom(contract.getDescriptorSet())
                        .getFile(0), new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Report");
        Object typedValue = fieldValue instanceof Integer ? fieldValue : fieldValue.toString();
        DynamicMessage report = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName(fieldName), typedValue).build();
        Any any = Any.newBuilder().setTypeUrl("type.googleapis.com/native.v1.Report")
                .setValue(report.toByteString()).build();
        ArtifactReference artifact = ArtifactReference.newBuilder().setSha256("a".repeat(64))
                .setMediaType("application/json").setSizeBytes(1).build();
        CompletionCandidate candidate = CompletionCandidate.newBuilder().setAttempt(attempt)
                .setRevision(1).setSummary("Completed attempt " + attempt)
                .addEvidence(CheckEvidence.newBuilder().setCheckName(CHECK)
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(Instant.parse("2026-09-25T12:00:00Z").getEpochSecond()))
                        .addArtifacts(artifact))
                .addArtifacts(artifact).setResult(any).build();
        JsonFormat.TypeRegistry registry = ai.protomolt.proto.delegation.DeliverableContracts
                .typeRegistry(contract);
        return JsonFormat.printer().usingTypeRegistry(registry).print(candidate);
    }

    private static DeliverableContract contract(String fieldName,
            DescriptorProtos.FieldDescriptorProto.Type fieldType) {
        DescriptorProtos.FieldDescriptorProto field = DescriptorProtos.FieldDescriptorProto
                .newBuilder().setName(fieldName).setNumber(1).setType(fieldType).build();
        var report = DescriptorProtos.DescriptorProto.newBuilder().setName("Report").addField(field);
        if (fieldType == DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32) {
            report.setOptions(DescriptorProtos.MessageOptions.newBuilder().setExtension(
                    ai.protomolt.proto.validate.ValidateProto.message,
                    ai.protomolt.proto.validate.MessageRules.newBuilder().addCel(
                            ai.protomolt.proto.validate.CelRule.newBuilder().setId("positive-count")
                                    .setMessage("count must be positive").setExpression("this.count > 0"))
                            .build()));
        }
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto
                .newBuilder().setName("native.proto").setPackage("native.v1").setSyntax("proto3")
                .addMessageType(report).build();
        return DeliverableContract.newBuilder().setTypeName("native.v1.Report")
                .setDescriptorSet(DescriptorProtos.FileDescriptorSet.newBuilder().addFile(file)
                        .build().toByteString()).build();
    }

    private static String last(RecordingContext context) {
        return context.messages.get(context.messages.size() - 1);
    }

    private static final class Fixture implements AutoCloseable {
        private final InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator();
        private final DelegationBridge bridge = new DelegationBridge(coordinator);
        private final ActionCatalog catalog = DelegationActions.register(
                ActionCatalog.defaults(ActionContext.create()), bridge);
        private final ProtoMoltGrpcServer server = ProtoMoltGrpcServer.start("127.0.0.1", 0,
                catalog, TOKEN, (CallerResolver) credential -> Optional.empty(), List.of(
                        ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                                .findServiceByName("DelegationService")));

        RemoteCatalogLineRunner runner(String token) {
            return RemoteCatalogLineRunner.connect("127.0.0.1:" + server.port(), false, token);
        }

        @Override public void close() {
            server.close();
            bridge.close();
            coordinator.close();
        }
    }

    private static final class RecordingContext implements PromptContext {
        private final List<String> messages = new ArrayList<>();
        @Override public void sendMessage(String text) { messages.add(text); }
        @Override public void sendThought(String text) { }
    }
}
