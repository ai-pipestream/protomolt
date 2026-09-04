package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.test.Result;
import ai.protomolt.proto.mesh.runtime.test.RuntimeTestProto;
import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;
import ai.protomolt.proto.mesh.runtime.v1.FlowDefinition;
import ai.protomolt.proto.mesh.runtime.v1.FlowEdge;
import ai.protomolt.proto.mesh.runtime.v1.FlowOutput;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.NamedChannelPolicy;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorNode;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadLifecycleFlowTest {

    private static final Instant NOW = ProcessorChannelFixtures.NOW;
    private static final String PROFILE = "flow-payloads";
    private static final String RUN = "dd2d7260-25bd-4be9-bbba-693cf39b13bb";
    private static final String REPLAY = "4f2ceab0-781c-4cb5-a883-e5457e6cc9f2";
    private static final String NAMESPACE = "a2afcaf1-e96f-4519-a20d-49530ac905a5";

    @TempDir
    Path temporary;

    @Test
    void claimCheckedInputKeepsItsLeaseAcrossRestartAndSelectedReplay() throws Exception {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        DescriptorRegistry descriptors = DescriptorRegistry.create(false);
        descriptors.registerFile(RuntimeTestProto.getDescriptor());
        InMemoryPayloadStore payloadStore = new InMemoryPayloadStore(
                clock, 100, 10_000_000);
        AtomicInteger attempts = new AtomicInteger();
        ProcessorRegistry firstProcessors = processors(descriptors, input -> {
            if (attempts.getAndIncrement() == 0) {
                throw new FlowExecutionSuspendedException("restart after hydration");
            }
            return Result.newBuilder().setBranch("resumed")
                    .setText(input.getText()).build();
        });
        ProcessorContract contract = firstProcessors.contracts().get("payload-echo");
        NamedChannelPolicy namedPolicy = claimCheckPolicy();
        FlowDefinition definition = definition(contract, namedPolicy);
        Path lifecycleWal = temporary.resolve("payload-flow.wal");

        try (FileFlowLifecycleStore lifecycle = new FileFlowLifecycleStore(lifecycleWal)) {
            DurableFlowCoordinator coordinator = coordinator(
                    descriptors, firstProcessors, lifecycle, payloadStore, clock);
            coordinator.publish("v1", definition);
            coordinator.deploy("payload-flow", "v1", OptionalLong.empty());
            assertThatThrownBy(() -> coordinator.start("payload-flow", RUN,
                    EntityEnvelopes.root(UUID.randomUUID().toString(), NAMESPACE,
                            RawInput.newBuilder().setText("large-enough").build(), NOW,
                            NOW.plusSeconds(600), CompletionPolicy.COMPLETION_POLICY_STRICT)))
                    .isInstanceOf(FlowExecutionSuspendedException.class);

            var interrupted = coordinator.get(RUN);
            assertThat(interrupted.getCheckpoint().getPayloadLeasesList()).singleElement();
            var lease = interrupted.getCheckpoint().getPayloadLeases(0);
            assertThat(lease.getDescendantMessageIdsList())
                    .containsExactly(interrupted.getCheckpoint().getActive()
                            .getPending().getInput().getHeader().getEntityId());
            assertThat(payloadStore.head(lease.getIdentity()).getActiveLeases()).isEqualTo(1);
            assertThat(interrupted.getHistory().getEventsList())
                    .extracting(event -> event.getKind())
                    .contains(HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_EXTERNALIZED,
                            HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_HYDRATED);
        }

        ProcessorRegistry resumedProcessors = processors(descriptors, input -> {
            attempts.incrementAndGet();
            return Result.newBuilder().setBranch("resumed")
                    .setText(input.getText()).build();
        });
        try (FileFlowLifecycleStore lifecycle = new FileFlowLifecycleStore(lifecycleWal)) {
            DurableFlowCoordinator coordinator = coordinator(
                    descriptors, resumedProcessors, lifecycle, payloadStore, clock);
            var completed = coordinator.resume(RUN);
            assertThat(completed.getHistory().getOutputsList()).singleElement();
            var identity = completed.getHistory().getEventsList().stream()
                    .filter(event -> event.getKind()
                            == HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED)
                    .filter(event -> event.getMessage().hasClaimCheck())
                    .findFirst().orElseThrow().getMessage().getClaimCheck();
            var storedIdentity = completed.getHistory().getEventsList().stream()
                    .filter(event -> event.getKind()
                            == HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_EXTERNALIZED)
                    .findFirst().orElseThrow().getMessage();
            assertThat(completed.getCheckpoint().getPayloadLeasesCount()).isZero();
            var payloadIdentity = ai.protomolt.proto.mesh.runtime.v1.PayloadIdentity.newBuilder()
                    .setNamespace(NAMESPACE).setProfile(PROFILE)
                    .setArtifact(identity.getArtifact())
                    .setPayloadTypeName(identity.getPayloadTypeName())
                    .setDescriptorFingerprint(identity.getDescriptorFingerprint()).build();
            assertThat(payloadStore.head(payloadIdentity).getActiveLeases()).isZero();
            assertThat(payloadStore.head(payloadIdentity).getEligibleForDeletion()).isTrue();
            assertThat(storedIdentity.hasClaimCheck()).isTrue();

            long frontier = completed.getHistory().getEventsList().stream()
                    .filter(event -> event.getKind()
                            == HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED)
                    .filter(event -> event.getMessage().hasClaimCheck())
                    .mapToLong(event -> event.getSequence()).findFirst().orElseThrow();
            var replay = coordinator.replay(RUN, REPLAY, List.of(frontier));
            assertThat(replay.getWorkflowVersion()).isEqualTo("v1");
            assertThat(replay.getHistory().getOutputsList()).singleElement();
            assertThat(replay.getCheckpoint().getPayloadLeasesCount()).isZero();
            assertThat(payloadStore.head(payloadIdentity).getActiveLeases()).isZero();
        }
        assertThat(attempts).hasValue(3);
    }

    @Test
    void twoEdgesOwnIndependentLeasesUntilBothDescendantsSettle() throws Exception {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        DescriptorRegistry descriptors = DescriptorRegistry.create(false);
        descriptors.registerFile(RuntimeTestProto.getDescriptor());
        InMemoryPayloadStore payloadStore = new InMemoryPayloadStore(
                clock, 100, 10_000_000);
        AtomicInteger attempts = new AtomicInteger();
        ProcessorRegistry processors = processors(descriptors, input -> {
            if (attempts.getAndIncrement() == 0) {
                throw new FlowExecutionSuspendedException(
                        "inspect both edge-owned payload leases");
            }
            return Result.newBuilder().setBranch("resumed")
                    .setText(input.getText()).build();
        });
        ProcessorContract contract = processors.contracts().get("payload-echo");
        NamedChannelPolicy policy = claimCheckPolicy();
        FlowDefinition definition = twoEdgeDefinition(contract, policy);

        try (FileFlowLifecycleStore lifecycle = new FileFlowLifecycleStore(
                temporary.resolve("two-edge-payload-flow.wal"))) {
            DurableFlowCoordinator coordinator = coordinator(
                    descriptors, processors, lifecycle, payloadStore, clock);
            coordinator.publish("v1", definition);
            coordinator.deploy("payload-flow-two-edge", "v1", OptionalLong.empty());
            assertThatThrownBy(() -> coordinator.start("payload-flow-two-edge", RUN,
                    EntityEnvelopes.root(UUID.randomUUID().toString(), NAMESPACE,
                            RawInput.newBuilder().setText("large-enough").build(), NOW,
                            NOW.plusSeconds(600), CompletionPolicy.COMPLETION_POLICY_STRICT)))
                    .isInstanceOf(FlowExecutionSuspendedException.class);

            var interrupted = coordinator.get(RUN);
            assertThat(interrupted.getCheckpoint().getPayloadLeasesList()).hasSize(2);
            assertThat(interrupted.getCheckpoint().getPayloadLeasesList())
                    .extracting(lease -> lease.getOwnerId())
                    .anySatisfy(owner -> assertThat(owner).contains(":to_echo_a:"))
                    .anySatisfy(owner -> assertThat(owner).contains(":to_echo_b:"));
            assertThat(interrupted.getCheckpoint().getPayloadLeasesList())
                    .extracting(lease -> lease.getIdentity())
                    .containsOnly(interrupted.getCheckpoint().getPayloadLeases(0).getIdentity());
            var identity = interrupted.getCheckpoint().getPayloadLeases(0).getIdentity();
            assertThat(payloadStore.head(identity).getActiveLeases()).isEqualTo(2);
            assertThat(payloadStore.head(identity).getEligibleForDeletion()).isFalse();

            var completed = coordinator.resume(RUN);

            assertThat(completed.getHistory().getOutputsList()).hasSize(2);
            assertThat(completed.getCheckpoint().getPayloadLeasesCount()).isZero();
            assertThat(payloadStore.head(identity).getActiveLeases()).isZero();
            assertThat(payloadStore.head(identity).getEligibleForDeletion()).isTrue();
        }
        assertThat(attempts).hasValue(3);
    }

    private static DurableFlowCoordinator coordinator(
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            FlowLifecycleStore lifecycle,
            PayloadStore payloadStore,
            Clock clock) {
        return new DurableFlowCoordinator(descriptors, processors, lifecycle,
                new PayloadStoreResolver(descriptors, payloadStore,
                        envelope -> envelope.getHeader().getScopeId(), PROFILE),
                PayloadLifecycle.stored(payloadStore, clock),
                new ChannelResourceCatalog(Set.of(PROFILE), Set.of(),
                        Set.of("default-retry"), Set.of("default-dead-letter"),
                        Set.of(), Set.of()), clock);
    }

    private static ProcessorRegistry processors(
            DescriptorRegistry descriptors, Body body) {
        ProcessorRegistry processors = new ProcessorRegistry(descriptors);
        processors.register(new MessageProcessor() {
            private final ProcessorContract contract = ProcessorContract.newBuilder()
                    .setProcessorId("payload-echo")
                    .setInputSchema(schema(RawInput.getDescriptor()))
                    .addOutputSchemas(schema(Result.getDescriptor()))
                    .setMaxOutputs(1)
                    .build();

            @Override
            public ProcessorContract contract() {
                return contract;
            }

            @Override
            public List<? extends com.google.protobuf.Message> process(
                    ProcessorContext context, com.google.protobuf.Message message)
                    throws Exception {
                return List.of(body.process(RawInput.parseFrom(message.toByteString())));
            }
        });
        return processors;
    }

    private static NamedChannelPolicy claimCheckPolicy() {
        ChannelPolicy policy = ChannelPolicies.localDurable().getPolicy().toBuilder()
                .setInlineByteLimit(1)
                .setPayloadStoreProfile(PROFILE)
                .build();
        return NamedChannelPolicy.newBuilder()
                .setPolicyId("claim-check-wal")
                .setPolicy(policy)
                .build();
    }

    private static FlowDefinition definition(
            ProcessorContract contract, NamedChannelPolicy policy) {
        return FlowDefinition.newBuilder()
                .setName("payload-flow")
                .addChannelPolicies(policy)
                .setInputSchema(contract.getInputSchema())
                .addNodes(ProcessorNode.newBuilder()
                        .setNodeId("echo_node")
                        .setProcessorId(contract.getProcessorId())
                        .setInputSchema(contract.getInputSchema())
                        .addOutputSchemas(contract.getOutputSchemas(0)))
                .addEdges(FlowEdge.newBuilder()
                        .setEdgeId("to_echo")
                        .setChannelPolicyId(policy.getPolicyId())
                        .setFlowInput(true)
                        .setTargetNode("echo_node")
                        .setSourceSchema(contract.getInputSchema()))
                .addOutputs(FlowOutput.newBuilder()
                        .setNodeId("echo_node")
                        .setSchema(contract.getOutputSchemas(0)))
                .setMaxMessages(10)
                .setDeadline(Duration.newBuilder().setSeconds(600))
                .build();
    }

    private static FlowDefinition twoEdgeDefinition(
            ProcessorContract contract, NamedChannelPolicy policy) {
        return FlowDefinition.newBuilder()
                .setName("payload-flow-two-edge")
                .addChannelPolicies(policy)
                .setInputSchema(contract.getInputSchema())
                .addNodes(ProcessorNode.newBuilder()
                        .setNodeId("echo_node")
                        .setProcessorId(contract.getProcessorId())
                        .setInputSchema(contract.getInputSchema())
                        .addOutputSchemas(contract.getOutputSchemas(0)))
                .addEdges(FlowEdge.newBuilder()
                        .setEdgeId("to_echo_a")
                        .setChannelPolicyId(policy.getPolicyId())
                        .setFlowInput(true)
                        .setTargetNode("echo_node")
                        .setSourceSchema(contract.getInputSchema()))
                .addEdges(FlowEdge.newBuilder()
                        .setEdgeId("to_echo_b")
                        .setChannelPolicyId(policy.getPolicyId())
                        .setFlowInput(true)
                        .setTargetNode("echo_node")
                        .setSourceSchema(contract.getInputSchema()))
                .addOutputs(FlowOutput.newBuilder()
                        .setNodeId("echo_node")
                        .setSchema(contract.getOutputSchemas(0)))
                .setMaxMessages(10)
                .setDeadline(Duration.newBuilder().setSeconds(600))
                .build();
    }

    private static SchemaReference schema(
            com.google.protobuf.Descriptors.Descriptor descriptor) {
        return SchemaReference.newBuilder()
                .setTypeName(descriptor.getFullName())
                .setDescriptorFingerprint(DescriptorIdentity.fingerprint(descriptor))
                .build();
    }

    @FunctionalInterface
    private interface Body {
        Result process(RawInput input) throws Exception;
    }
}
