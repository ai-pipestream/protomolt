package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.test.Result;
import ai.protomolt.proto.mesh.runtime.test.RuntimeTestProto;
import ai.protomolt.proto.mesh.runtime.v1.ChangeDeadLetterStatusRequest;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterReplayStatus;
import ai.protomolt.proto.mesh.runtime.v1.FlowDefinition;
import ai.protomolt.proto.mesh.runtime.v1.FlowEdge;
import ai.protomolt.proto.mesh.runtime.v1.FlowOutput;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.GetDeadLetterRequest;
import ai.protomolt.proto.mesh.runtime.v1.ListDeadLettersRequest;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorNode;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.runtime.v1.RecoveryServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.ReplayDeadLetterRequest;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Duration;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryGrpcServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final String SOURCE_RUN = "dd2d7260-25bd-4be9-bbba-693cf39b13bb";
    private static final String REPLAY_RUN = "4f2ceab0-781c-4cb5-a883-e5457e6cc9f2";
    private static final String NAMESPACE = "a2afcaf1-e96f-4519-a20d-49530ac905a5";

    @TempDir
    Path temporary;

    @Test
    void deadLetterReplayDelegatesToThePinnedDurableFlowExactlyOnce() throws Exception {
        try (Fixture fixture = new Fixture(temporary)) {
            var record = fixture.deadLetter(fixture.sourceWork(), false);
            var page = fixture.client.listDeadLetters(ListDeadLettersRequest.newBuilder()
                    .setNamespace(NAMESPACE).setLimit(1).build());
            assertThat(page.getEntriesList()).singleElement().satisfies(entry -> {
                assertThat(entry.getSequence()).isEqualTo(1);
                assertThat(entry.getRecord()).isEqualTo(record);
            });
            assertThat(page.getNextSequence()).isEqualTo(1);
            var exhausted = fixture.client.listDeadLetters(ListDeadLettersRequest.newBuilder()
                    .setNamespace(NAMESPACE).setAfterSequence(page.getNextSequence())
                    .setLimit(1).build());
            assertThat(exhausted.getEntriesList()).isEmpty();
            assertThat(exhausted.getNextSequence()).isEqualTo(1);
            assertThat(fixture.client.getDeadLetter(GetDeadLetterRequest.newBuilder()
                    .setDeadLetterId(record.getDeadLetterId()).build())).isEqualTo(record);
            assertStatus(Status.Code.NOT_FOUND, () -> fixture.client.getDeadLetter(
                    GetDeadLetterRequest.newBuilder()
                            .setDeadLetterId(UUID.randomUUID().toString()).build()),
                    "unknown dead_letter_id");

            fixture.coordinator.publish("v2", fixture.definition.toBuilder()
                    .setDeadline(Duration.newBuilder().setSeconds(601)).build());
            fixture.coordinator.deploy("recovery-flow", "v2", OptionalLong.of(1));

            var request = ReplayDeadLetterRequest.newBuilder()
                    .setDeadLetterId(record.getDeadLetterId())
                    .setReplayRunId(REPLAY_RUN)
                    .build();
            var replayed = fixture.client.replayDeadLetter(request);
            assertThat(replayed.getReplayStatus()).isEqualTo(
                    DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_COMPLETED);
            assertThat(replayed.getReplayRunId()).isEqualTo(REPLAY_RUN);
            var replayRun = fixture.coordinator.get(REPLAY_RUN);
            assertThat(replayRun.getWorkflowVersion()).isEqualTo("v1");
            assertThat(replayRun.getReplayOfRunId()).isEqualTo(SOURCE_RUN);
            assertThat(replayRun.getHistory().getEventsList())
                    .extracting(event -> event.getKind())
                    .contains(HistoryEventKind.HISTORY_EVENT_KIND_REPLAY_REQUESTED,
                            HistoryEventKind.HISTORY_EVENT_KIND_REPLAY_STARTED);

            int recordsAfterFirstReplay = fixture.channel.records().size();
            assertThat(fixture.client.replayDeadLetter(request)).isEqualTo(replayed);
            assertThat(fixture.channel.records()).hasSize(recordsAfterFirstReplay);

            assertStatus(Status.Code.ABORTED, () -> fixture.client.replayDeadLetter(
                    request.toBuilder().setReplayRunId(UUID.randomUUID().toString()).build()),
                    "dead-letter-replay-conflict");

            var retained = fixture.client.retainDeadLetter(
                    ChangeDeadLetterStatusRequest.newBuilder()
                            .setDeadLetterId(record.getDeadLetterId())
                            .setReason("investigation").setRetain(true).build());
            assertThat(retained.getRetentionHold()).isTrue();
            var acknowledged = fixture.client.acknowledgeDeadLetter(
                    ChangeDeadLetterStatusRequest.newBuilder()
                            .setDeadLetterId(record.getDeadLetterId())
                            .setReason("resolved").setRetain(false).build());
            assertThat(acknowledged.getReplayStatus()).isEqualTo(
                    DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_ACKNOWLEDGED);
        }
    }

    @Test
    void replayRefusesADeadLetterWithoutTheOriginalRunIdentity() throws Exception {
        try (Fixture fixture = new Fixture(temporary)) {
            ProcessorWork incomplete = fixture.sourceWork().toBuilder()
                    .setDeliveryId(UUID.randomUUID().toString())
                    .clearWorkflowName()
                    .clearWorkflowVersion()
                    .clearPlanFingerprint()
                    .setSourceHistorySequence(0)
                    .build();
            var record = fixture.deadLetter(incomplete, false);
            assertStatus(Status.Code.FAILED_PRECONDITION,
                    () -> fixture.client.replayDeadLetter(
                            ReplayDeadLetterRequest.newBuilder()
                                    .setDeadLetterId(record.getDeadLetterId())
                                    .setReplayRunId(REPLAY_RUN).build()),
                    "dead-letter-replay-identity-missing");
        }
    }

    @Test
    void cancelledRetryBecomesOneDurableTerminalRecord() throws Exception {
        Path channelWal = temporary.resolve("retry-channel.wal");
        var descriptors = ProcessorChannelFixtures.descriptors();
        var contract = ProcessorChannelFixtures.contract();
        var work = ProcessorChannelFixtures.work(UUID.randomUUID().toString(),
                ChannelPolicies.localDurable().getPolicy(), contract);
        String deadLetterId;
        try (var channel = new FileDurableProcessorChannel(channelWal, descriptors,
                Clock.fixed(NOW, ZoneOffset.UTC))) {
            channel.enqueue(work);
            var claim = channel.claim("worker-a", List.of(contract), 1,
                    java.time.Duration.ofSeconds(30), NOW).getFirst();
            channel.fail("worker-a", ProcessorFailure.newBuilder()
                    .setDeliveryId(work.getDeliveryId())
                    .setLeaseToken(claim.getLeaseToken())
                    .setCompletionId(UUID.randomUUID().toString())
                    .setCode("transient")
                    .setMessage("retry later")
                    .setOutcome(ProcessorOutcomes.retryable(
                            "transient", "retry later", contract.getProcessorId(), 1, 3))
                    .build(), NOW);
            assertThat(channel.delivery(work.getDeliveryId()).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.PENDING);
            var cancelled = channel.cancelRetry(
                    work.getDeliveryId(), "operator cancelled", NOW);
            deadLetterId = cancelled.getDeadLetterId();
            assertThat(channel.cancelRetry(
                    work.getDeliveryId(), "same result", NOW)).isEqualTo(cancelled);
            assertThat(channel.deadLetters(NAMESPACE, 0, 10).entries())
                    .singleElement()
                    .extracting(DurableProcessorChannel.DeadLetterPage.Entry::record)
                    .isEqualTo(cancelled);
        }
        try (var recovered = new FileDurableProcessorChannel(channelWal, descriptors,
                Clock.fixed(NOW, ZoneOffset.UTC))) {
            assertThat(recovered.delivery(work.getDeliveryId()).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.FAILED);
            assertThat(recovered.deadLetters(NAMESPACE, 0, 10).entries())
                    .singleElement()
                    .extracting(DurableProcessorChannel.DeadLetterPage.Entry::record)
                    .satisfies(record ->
                            assertThat(record.getDeadLetterId()).isEqualTo(deadLetterId));
        }
    }

    private static void assertStatus(
            Status.Code code, Runnable operation, String description) {
        assertThatThrownBy(operation::run)
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> {
                    var status = ((StatusRuntimeException) error).getStatus();
                    assertThat(status.getCode()).isEqualTo(code);
                    assertThat(status.getDescription()).contains(description);
                });
    }

    private static SchemaReference schema(
            com.google.protobuf.Descriptors.Descriptor descriptor) {
        return SchemaReference.newBuilder()
                .setTypeName(descriptor.getFullName())
                .setDescriptorFingerprint(DescriptorIdentity.fingerprint(descriptor))
                .build();
    }

    private static final class Fixture implements AutoCloseable {
        private final DescriptorRegistry descriptors = DescriptorRegistry.create(false);
        private final ProcessorRegistry processors;
        private final FileFlowLifecycleStore lifecycleStore;
        private final FileDurableProcessorChannel channel;
        private final DurableFlowCoordinator coordinator;
        private final FlowDefinition definition;
        private final ai.protomolt.proto.mesh.runtime.v1.HistoryEvent frontier;
        private final Server server;
        private final ManagedChannel grpcChannel;
        private final RecoveryServiceGrpc.RecoveryServiceBlockingStub client;

        private Fixture(Path temporary) throws Exception {
            descriptors.registerFile(RuntimeTestProto.getDescriptor());
            processors = new ProcessorRegistry(descriptors);
            ProcessorContract declared = ProcessorContract.newBuilder()
                    .setProcessorId("echo")
                    .setInputSchema(schema(RawInput.getDescriptor()))
                    .addOutputSchemas(schema(Result.getDescriptor()))
                    .setMaxOutputs(1)
                    .build();
            processors.register(new MessageProcessor() {
                @Override
                public ProcessorContract contract() {
                    return declared;
                }

                @Override
                public List<? extends com.google.protobuf.Message> process(
                        ProcessorContext context, com.google.protobuf.Message message)
                        throws Exception {
                    RawInput input = RawInput.parseFrom(message.toByteString());
                    return List.of(Result.newBuilder().setBranch("recovery")
                            .setText(input.getText()).build());
                }
            });
            ProcessorContract contract = processors.contracts().get("echo");
            definition = FlowDefinition.newBuilder()
                    .setName("recovery-flow")
                    .addChannelPolicies(ChannelPolicies.localDurable())
                    .setInputSchema(contract.getInputSchema())
                    .addNodes(ProcessorNode.newBuilder()
                            .setNodeId("echo_node")
                            .setProcessorId("echo")
                            .setInputSchema(contract.getInputSchema())
                            .addOutputSchemas(contract.getOutputSchemas(0)))
                    .addEdges(FlowEdge.newBuilder()
                            .setEdgeId("to_echo")
                            .setChannelPolicyId(ChannelPolicies.LOCAL_DURABLE_ID)
                            .setFlowInput(true)
                            .setTargetNode("echo_node")
                            .setSourceSchema(contract.getInputSchema()))
                    .addOutputs(FlowOutput.newBuilder()
                            .setNodeId("echo_node")
                            .setSchema(contract.getOutputSchemas(0)))
                    .setMaxMessages(10)
                    .setDeadline(Duration.newBuilder().setSeconds(600))
                    .build();
            lifecycleStore = new FileFlowLifecycleStore(
                    temporary.resolve("recovery-lifecycle.wal"));
            coordinator = new DurableFlowCoordinator(
                    descriptors, processors, lifecycleStore,
                    PayloadResolver.inlineOnly(descriptors),
                    Clock.fixed(NOW, ZoneOffset.UTC));
            coordinator.publish("v1", definition);
            coordinator.deploy("recovery-flow", "v1", OptionalLong.empty());
            var source = coordinator.start("recovery-flow", SOURCE_RUN,
                    EntityEnvelopes.root(UUID.randomUUID().toString(), NAMESPACE,
                            RawInput.newBuilder().setText("source").build(), NOW,
                            NOW.plusSeconds(3_600), CompletionPolicy.COMPLETION_POLICY_STRICT));
            frontier = source.getHistory().getEventsList().stream()
                    .filter(event -> event.getKind()
                            == HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED)
                    .findFirst().orElseThrow();
            channel = new FileDurableProcessorChannel(
                    temporary.resolve("recovery-channel.wal"), descriptors,
                    Clock.fixed(NOW, ZoneOffset.UTC));
            String serverName = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(serverName).directExecutor()
                    .addService(new RecoveryGrpcService(channel, coordinator,
                            RecoveryGrpcService.RecoveryReconciler.reportOnly(),
                            Clock.fixed(NOW, ZoneOffset.UTC)))
                    .build().start();
            grpcChannel = InProcessChannelBuilder.forName(serverName)
                    .directExecutor().build();
            client = RecoveryServiceGrpc.newBlockingStub(grpcChannel);
        }

        private ProcessorWork sourceWork() {
            var source = coordinator.get(SOURCE_RUN);
            var policy = ChannelPolicies.localDurable();
            return ProcessorWork.newBuilder()
                    .setDeliveryId(UUID.randomUUID().toString())
                    .setRunId(SOURCE_RUN)
                    .setNodeId(frontier.getNodeId())
                    .setInvocationId(UUID.randomUUID().toString())
                    .setInvocationOrdinal(1)
                    .setContract(processors.contracts().get("echo"))
                    .setInput(frontier.getMessage())
                    .setDeadline(RemoteValidation.timestamp(NOW.plusSeconds(600)))
                    .setMaxAttempts(1)
                    .setWorkflowName(source.getWorkflowName())
                    .setWorkflowVersion(source.getWorkflowVersion())
                    .setPlanFingerprint(source.getPlanFingerprint())
                    .setDeploymentRevision(source.getDeploymentRevision())
                    .setEdgeId(frontier.getEdgeId())
                    .setChannelPolicyId(policy.getPolicyId())
                    .setSourceHistorySequence(frontier.getSequence())
                    .setNamespace(NAMESPACE)
                    .setChannelPolicy(policy.getPolicy())
                    .build();
        }

        private ai.protomolt.proto.mesh.runtime.v1.DeadLetterRecord deadLetter(
                ProcessorWork work, boolean retryable) {
            channel.enqueue(work);
            var claim = channel.claim("worker-a", List.of(work.getContract()), 1,
                    java.time.Duration.ofSeconds(30), NOW).getFirst();
            var outcome = retryable
                    ? ProcessorOutcomes.retryable("failed", "failed",
                    work.getContract().getProcessorId(), 1, work.getMaxAttempts())
                    : ProcessorOutcomes.permanent("failed", "failed",
                    work.getContract().getProcessorId(), 1);
            channel.fail("worker-a", ProcessorFailure.newBuilder()
                    .setDeliveryId(work.getDeliveryId())
                    .setLeaseToken(claim.getLeaseToken())
                    .setCompletionId(UUID.randomUUID().toString())
                    .setCode("failed")
                    .setMessage("failed")
                    .setOutcome(outcome)
                    .build(), NOW);
            return channel.delivery(work.getDeliveryId()).orElseThrow().deadLetter();
        }

        @Override
        public void close() throws Exception {
            grpcChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            channel.close();
            lifecycleStore.close();
        }
    }
}
