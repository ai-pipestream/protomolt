package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.test.Result;
import ai.protomolt.proto.mesh.runtime.test.RuntimeTestProto;
import ai.protomolt.proto.mesh.runtime.test.Token;
import ai.protomolt.proto.mesh.runtime.v1.DurableFlowRun;
import ai.protomolt.proto.mesh.runtime.v1.DurableRunState;
import ai.protomolt.proto.mesh.runtime.v1.FlowDefinition;
import ai.protomolt.proto.mesh.runtime.v1.FlowEdge;
import ai.protomolt.proto.mesh.runtime.v1.FlowOutput;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEvent;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorNode;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DurableFlowCoordinatorTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final String INPUT_ID = "5cb5ad7d-a497-4bec-bb69-e33036ed8d66";
    private static final String SCOPE_ID = "a2afcaf1-e96f-4519-a20d-49530ac905a5";
    private static final String RUN_ID = "dd2d7260-25bd-4be9-bbba-693cf39b13bb";
    private static final String REPLAY_ID = "4f2ceab0-781c-4cb5-a883-e5457e6cc9f2";

    @TempDir
    Path temporary;

    @Test
    void publicationDeploymentRunsAndHistorySurviveReopen() throws Exception {
        Path wal = temporary.resolve("lifecycle.wal");
        TestRuntime runtime = singleProcessor(input -> Result.newBuilder()
                .setBranch("v1").setText(input.getText()).build());
        DurableFlowRun completed;

        try (FileFlowLifecycleStore store = new FileFlowLifecycleStore(wal)) {
            DurableFlowCoordinator coordinator = runtime.coordinator(store);
            var published = coordinator.publish("v1", runtime.definition());
            assertThat(published.getValidation().getValid()).isTrue();
            assertThat(coordinator.publish("v1", runtime.definition()).getPublished())
                    .isEqualTo(published.getPublished());
            assertThatThrownBy(() -> coordinator.publish("v1",
                    runtime.definition().toBuilder()
                            .setDeadline(Duration.newBuilder().setSeconds(601)).build()))
                    .isInstanceOf(LifecycleConflictException.class)
                    .hasMessageContaining("immutable");

            var deployed = coordinator.deploy("durable-flow", "v1", OptionalLong.empty());
            assertThat(deployed.getRevision()).isEqualTo(1);
            assertThat(coordinator.deploy(
                    "durable-flow", "v1", OptionalLong.of(1))).isEqualTo(deployed);

            completed = coordinator.start("durable-flow", RUN_ID, input("durable"));
            assertThat(completed.getState())
                    .isEqualTo(DurableRunState.DURABLE_RUN_STATE_COMPLETED);
            assertThat(completed.getWorkflowVersion()).isEqualTo("v1");
            assertThat(completed.getDeploymentRevision()).isEqualTo(1);
            assertThat(completed.getHistory().getOutputsCount()).isEqualTo(1);

            coordinator.publish("v2", runtime.definition().toBuilder()
                    .setDeadline(Duration.newBuilder().setSeconds(601)).build());
            assertThatThrownBy(() -> coordinator.deploy(
                    "durable-flow", "v2", OptionalLong.empty()))
                    .isInstanceOf(LifecycleConflictException.class)
                    .hasMessageContaining("requires expected_revision 1");
            assertThat(coordinator.deploy("durable-flow", "v2", OptionalLong.of(1))
                    .getRevision()).isEqualTo(2);
        }

        try (FileFlowLifecycleStore recovered = new FileFlowLifecycleStore(wal)) {
            DurableFlowCoordinator coordinator = runtime.coordinator(recovered);
            assertThat(coordinator.get(RUN_ID)).isEqualTo(completed);
            assertThat(coordinator.deployment("durable-flow").getVersion()).isEqualTo("v2");
            assertThat(coordinator.start("durable-flow", RUN_ID, input("durable")))
                    .isEqualTo(completed);

            FlowLifecycleStore.HistoryPage first = coordinator.history(RUN_ID, 0, 3);
            FlowLifecycleStore.HistoryPage rest = coordinator.history(
                    RUN_ID, first.nextSequence(), 10_000);
            assertThat(first.events()).hasSize(3);
            assertThat(first.events()).extracting(HistoryEvent::getSequence)
                    .containsExactly(1L, 2L, 3L);
            assertThat(rest.events()).isNotEmpty();
            assertThat(rest.nextSequence())
                    .isEqualTo(completed.getHistory().getEventsCount());
            assertThat(rest.terminal()).isTrue();
        }
    }

    @Test
    void activeInvocationRestartsWithTheSameIdentityAndWithoutDuplicateHistory()
            throws Exception {
        Path wal = temporary.resolve("restart.wal");
        AtomicInteger attempts = new AtomicInteger();
        TestRuntime suspending = singleProcessor(input -> {
            attempts.incrementAndGet();
            throw new FlowExecutionSuspendedException("simulated process exit");
        });

        try (FileFlowLifecycleStore store = new FileFlowLifecycleStore(wal)) {
            DurableFlowCoordinator coordinator = suspending.coordinator(store);
            coordinator.publish("v1", suspending.definition());
            coordinator.deploy("durable-flow", "v1", OptionalLong.empty());
            assertThatThrownBy(() -> coordinator.start(
                    "durable-flow", RUN_ID, input("restart")))
                    .isInstanceOf(FlowExecutionSuspendedException.class);
            DurableFlowRun interrupted = coordinator.get(RUN_ID);
            assertThat(interrupted.getState())
                    .isEqualTo(DurableRunState.DURABLE_RUN_STATE_RUNNING);
            assertThat(interrupted.getCheckpoint().hasActive()).isTrue();
        }

        TestRuntime resumed = singleProcessor(input -> {
            attempts.incrementAndGet();
            return Result.newBuilder().setBranch("resumed")
                    .setText(input.getText()).build();
        });
        try (FileFlowLifecycleStore store = new FileFlowLifecycleStore(wal)) {
            DurableFlowRun completed = resumed.coordinator(store).resume(RUN_ID);
            assertThat(completed.getState())
                    .isEqualTo(DurableRunState.DURABLE_RUN_STATE_COMPLETED);
            assertThat(attempts).hasValue(2);
            assertThat(completed.getHistory().getEventsList())
                    .filteredOn(event -> event.getKind()
                            == HistoryEventKind.HISTORY_EVENT_KIND_PROCESSOR_STARTED)
                    .singleElement();
            assertThat(completed.getHistory().getEventsList())
                    .filteredOn(event -> event.getKind()
                            == HistoryEventKind.HISTORY_EVENT_KIND_PROCESSOR_COMPLETED)
                    .singleElement();
        }
    }

    @Test
    void cancellationIsPersistedBeforeTheActiveProcessorIsInterrupted() throws Exception {
        Path wal = temporary.resolve("cancel.wal");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TestRuntime blocking = singleProcessor(input -> {
            entered.countDown();
            release.await();
            return Result.newBuilder().setBranch("too-late")
                    .setText(input.getText()).build();
        });

        try (FileFlowLifecycleStore store = new FileFlowLifecycleStore(wal);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            DurableFlowCoordinator coordinator = blocking.coordinator(store);
            coordinator.publish("v1", blocking.definition());
            coordinator.deploy("durable-flow", "v1", OptionalLong.empty());
            var running = executor.submit(() -> coordinator.start(
                    "durable-flow", RUN_ID, input("cancel")));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

            DurableFlowRun requested = coordinator.cancel(RUN_ID, "operator stop");
            assertThat(requested.getState()).isEqualTo(
                    DurableRunState.DURABLE_RUN_STATE_CANCELLATION_REQUESTED);
            DurableFlowRun cancelled = running.get(5, TimeUnit.SECONDS);

            assertThat(cancelled.getState())
                    .isEqualTo(DurableRunState.DURABLE_RUN_STATE_CANCELLED);
            assertThat(cancelled.getCancellationReason()).isEqualTo("operator stop");
            assertThat(cancelled.getHistory().getOutputsCount()).isZero();
            assertThat(cancelled.getHistory().getEventsList())
                    .extracting(HistoryEvent::getKind)
                    .endsWith(HistoryEventKind.HISTORY_EVENT_KIND_CANCELLATION_REQUESTED,
                            HistoryEventKind.HISTORY_EVENT_KIND_RUN_CANCELLED);
            assertThat(cancelled.getCheckpoint()).isEqualTo(
                    ai.protomolt.proto.mesh.runtime.v1.FlowExecutionCheckpoint
                            .getDefaultInstance());
        } finally {
            release.countDown();
        }

        try (FileFlowLifecycleStore recovered = new FileFlowLifecycleStore(wal)) {
            assertThat(recovered.run(RUN_ID).orElseThrow().getState())
                    .isEqualTo(DurableRunState.DURABLE_RUN_STATE_CANCELLED);
        }
    }

    @Test
    void cancellationIsRefusedAfterDescendantSettlementBegins() throws Exception {
        Path wal = temporary.resolve("settlement-boundary.wal");
        CountDownLatch settling = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DescriptorRegistry descriptors = descriptors();
        ProcessorRegistry processors = new ProcessorRegistry(descriptors);
        ProcessorContract contract = ProcessorContract.newBuilder()
                .setProcessorId("commit-aware")
                .setInputSchema(schema(RawInput.getDescriptor()))
                .addOutputSchemas(schema(Result.getDescriptor()))
                .setMaxOutputs(1)
                .build();
        processors.register(new ProcessorInvoker() {
            @Override
            public ProcessorContract contract() {
                return contract;
            }

            @Override
            public ProcessorInvocationResult invoke(ProcessorInvocation invocation) {
                InvocationSettlement settlement = new InvocationSettlement() {
                    @Override
                    public String deliveryId() {
                        return "";
                    }

                    @Override
                    public void settle() {
                        settling.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("settlement interrupted", e);
                        }
                    }

                    @Override
                    public void release(String reason) {
                    }
                };
                return new ProcessorInvocationResult(List.of(RuntimeSchemas.pack(
                        Result.newBuilder().setBranch("committed").build())), settlement);
            }
        });
        FlowDefinition definition = FlowDefinition.newBuilder()
                .setName("commit-flow")
                .addChannelPolicies(ChannelPolicies.localDurable())
                .setInputSchema(contract.getInputSchema())
                .addNodes(node("commit_node", "commit-aware",
                        RawInput.getDescriptor(), Result.getDescriptor()))
                .addEdges(inputEdge("to_commit", "commit_node", RawInput.getDescriptor()))
                .addOutputs(output("commit_node", Result.getDescriptor()))
                .setMaxMessages(10)
                .setDeadline(Duration.newBuilder().setSeconds(600))
                .build();
        TestRuntime runtime = new TestRuntime(descriptors, processors, definition);

        try (FileFlowLifecycleStore store = new FileFlowLifecycleStore(wal);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            DurableFlowCoordinator coordinator = runtime.coordinator(store);
            coordinator.publish("v1", definition);
            coordinator.deploy("commit-flow", "v1", OptionalLong.empty());
            var running = executor.submit(() -> coordinator.start(
                    "commit-flow", RUN_ID, input("commit")));
            assertThat(settling.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(coordinator.get(RUN_ID).getCheckpoint().getSettlementStarted()).isTrue();

            assertThatThrownBy(() -> coordinator.cancel(RUN_ID, "too late"))
                    .isInstanceOf(LifecycleConflictException.class)
                    .hasMessageContaining("commit boundary");
            release.countDown();
            assertThat(running.get(5, TimeUnit.SECONDS).getState())
                    .isEqualTo(DurableRunState.DURABLE_RUN_STATE_COMPLETED);
        } finally {
            release.countDown();
        }
    }

    @Test
    void frontierReplayRunsOnlySelectedRoutedMessagesAgainstThePinnedVersion()
            throws Exception {
        Path wal = temporary.resolve("replay.wal");
        AtomicInteger splits = new AtomicInteger();
        AtomicInteger lefts = new AtomicInteger();
        AtomicInteger rights = new AtomicInteger();
        TestRuntime runtime = branchedRuntime(splits, lefts, rights);

        try (FileFlowLifecycleStore store = new FileFlowLifecycleStore(wal)) {
            DurableFlowCoordinator coordinator = runtime.coordinator(store);
            coordinator.publish("v1", runtime.definition());
            coordinator.deploy("branched-flow", "v1", OptionalLong.empty());
            DurableFlowRun source = coordinator.start(
                    "branched-flow", RUN_ID, input("branch"));
            long rightFrontier = source.getHistory().getEventsList().stream()
                    .filter(event -> event.getKind()
                            == HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED)
                    .filter(event -> event.getNodeId().equals("right_node"))
                    .mapToLong(HistoryEvent::getSequence)
                    .findFirst().orElseThrow();

            coordinator.publish("v2", runtime.definition().toBuilder()
                    .setDeadline(Duration.newBuilder().setSeconds(601)).build());
            coordinator.deploy("branched-flow", "v2", OptionalLong.of(1));
            splits.set(0);
            lefts.set(0);
            rights.set(0);

            DurableFlowRun replay = coordinator.replay(
                    RUN_ID, REPLAY_ID, List.of(rightFrontier));
            assertThat(replay.getState())
                    .isEqualTo(DurableRunState.DURABLE_RUN_STATE_COMPLETED);
            assertThat(replay.getWorkflowVersion()).isEqualTo("v1");
            assertThat(replay.getDeploymentRevision()).isEqualTo(1);
            assertThat(replay.getReplayOfRunId()).isEqualTo(RUN_ID);
            assertThat(replay.getReplayFrontierSequencesList())
                    .containsExactly(rightFrontier);
            assertThat(splits).hasValue(0);
            assertThat(lefts).hasValue(0);
            assertThat(rights).hasValue(1);
            assertThat(replay.getHistory().getOutputsList())
                    .singleElement().satisfies(output ->
                            assertThat(parseResult(output).getBranch()).isEqualTo("right"));
        }
    }

    @Test
    void walRepairsOnlyAnIncompleteTailAndRefusesConcurrentWritersOrCorruption()
            throws Exception {
        Path tailWal = temporary.resolve("tail.wal");
        TestRuntime runtime = singleProcessor(input -> Result.newBuilder()
                .setBranch("ok").setText(input.getText()).build());
        try (FileFlowLifecycleStore store = new FileFlowLifecycleStore(tailWal)) {
            runtime.coordinator(store).publish("v1", runtime.definition());
            assertThatThrownBy(() -> new FileFlowLifecycleStore(tailWal))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("already has a writer");
        }
        long completeSize = Files.size(tailWal);
        Files.write(tailWal, new byte[]{0, 0}, StandardOpenOption.APPEND);
        try (FileFlowLifecycleStore repaired = new FileFlowLifecycleStore(tailWal)) {
            assertThat(repaired.published("durable-flow", "v1")).isPresent();
        }
        assertThat(Files.size(tailWal)).isEqualTo(completeSize);

        Path corruptWal = temporary.resolve("corrupt.wal");
        try (FileFlowLifecycleStore store = new FileFlowLifecycleStore(corruptWal)) {
            runtime.coordinator(store).publish("v1", runtime.definition());
        }
        byte[] corrupt = Files.readAllBytes(corruptWal);
        corrupt[12] ^= 0x01;
        Files.write(corruptWal, corrupt);
        assertThatThrownBy(() -> new FileFlowLifecycleStore(corruptWal))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC mismatch");
    }

    private static TestRuntime singleProcessor(Body body) {
        DescriptorRegistry descriptors = descriptors();
        ProcessorRegistry processors = new ProcessorRegistry(descriptors);
        processors.register(processor("echo", RawInput.getDescriptor(),
                List.of(Result.getDescriptor()), 1,
                (context, message) -> List.of(body.process(
                        RawInput.parseFrom(message.toByteString())))));
        FlowDefinition definition = FlowDefinition.newBuilder()
                .setName("durable-flow")
                .addChannelPolicies(ChannelPolicies.localDurable())
                .setInputSchema(schema(RawInput.getDescriptor()))
                .addNodes(node("echo_node", "echo",
                        RawInput.getDescriptor(), Result.getDescriptor()))
                .addEdges(inputEdge("to_echo", "echo_node", RawInput.getDescriptor()))
                .addOutputs(output("echo_node", Result.getDescriptor()))
                .setMaxMessages(10)
                .setDeadline(Duration.newBuilder().setSeconds(600))
                .build();
        return new TestRuntime(descriptors, processors, definition);
    }

    private static TestRuntime branchedRuntime(
            AtomicInteger splits, AtomicInteger lefts, AtomicInteger rights) {
        DescriptorRegistry descriptors = descriptors();
        ProcessorRegistry processors = new ProcessorRegistry(descriptors);
        processors.register(processor("split", RawInput.getDescriptor(),
                List.of(Token.getDescriptor()), 2, (context, message) -> {
                    splits.incrementAndGet();
                    RawInput input = RawInput.parseFrom(message.toByteString());
                    return List.of(Token.newBuilder().setText(input.getText()).build(),
                            Token.newBuilder().setText(input.getText())
                                    .setRouteRight(true).build());
                }));
        processors.register(processor("left", Token.getDescriptor(),
                List.of(Result.getDescriptor()), 1, (context, message) -> {
                    lefts.incrementAndGet();
                    return List.of(Result.newBuilder().setBranch("left")
                            .setText(Token.parseFrom(message.toByteString()).getText()).build());
                }));
        processors.register(processor("right", Token.getDescriptor(),
                List.of(Result.getDescriptor()), 1, (context, message) -> {
                    rights.incrementAndGet();
                    return List.of(Result.newBuilder().setBranch("right")
                            .setText(Token.parseFrom(message.toByteString()).getText()).build());
                }));
        FlowDefinition definition = FlowDefinition.newBuilder()
                .setName("branched-flow")
                .addChannelPolicies(ChannelPolicies.localDurable())
                .setInputSchema(schema(RawInput.getDescriptor()))
                .addNodes(node("split_node", "split",
                        RawInput.getDescriptor(), Token.getDescriptor()))
                .addNodes(node("left_node", "left",
                        Token.getDescriptor(), Result.getDescriptor()))
                .addNodes(node("right_node", "right",
                        Token.getDescriptor(), Result.getDescriptor()))
                .addEdges(inputEdge("to_split", "split_node", RawInput.getDescriptor()))
                .addEdges(nodeEdge("to_left", "split_node", "left_node",
                        Token.getDescriptor(), "!message.route_right"))
                .addEdges(nodeEdge("to_right", "split_node", "right_node",
                        Token.getDescriptor(), "message.route_right"))
                .addOutputs(output("left_node", Result.getDescriptor()))
                .addOutputs(output("right_node", Result.getDescriptor()))
                .setMaxMessages(100)
                .setDeadline(Duration.newBuilder().setSeconds(600))
                .build();
        return new TestRuntime(descriptors, processors, definition);
    }

    private static DescriptorRegistry descriptors() {
        DescriptorRegistry descriptors = DescriptorRegistry.create(false);
        descriptors.registerFile(RuntimeTestProto.getDescriptor());
        return descriptors;
    }

    private static EntityEnvelope input(String text) {
        return EntityEnvelopes.root(INPUT_ID, SCOPE_ID,
                RawInput.newBuilder().setText(text).build(), NOW,
                NOW.plusSeconds(3_600), CompletionPolicy.COMPLETION_POLICY_STRICT);
    }

    private static ProcessorNode node(
            String nodeId, String processorId, Descriptor input, Descriptor output) {
        return ProcessorNode.newBuilder()
                .setNodeId(nodeId)
                .setProcessorId(processorId)
                .setInputSchema(schema(input))
                .addOutputSchemas(schema(output))
                .build();
    }

    private static FlowEdge inputEdge(
            String edgeId, String target, Descriptor source) {
        return FlowEdge.newBuilder()
                .setEdgeId(edgeId)
                .setChannelPolicyId(ChannelPolicies.LOCAL_DURABLE_ID)
                .setFlowInput(true)
                .setTargetNode(target)
                .setSourceSchema(schema(source))
                .build();
    }

    private static FlowEdge nodeEdge(
            String edgeId, String source, String target, Descriptor schema, String when) {
        return FlowEdge.newBuilder()
                .setEdgeId(edgeId)
                .setChannelPolicyId(ChannelPolicies.LOCAL_DURABLE_ID)
                .setSourceNode(source)
                .setTargetNode(target)
                .setSourceSchema(schema(schema))
                .setWhen(when)
                .build();
    }

    private static FlowOutput output(String node, Descriptor descriptor) {
        return FlowOutput.newBuilder().setNodeId(node).setSchema(schema(descriptor)).build();
    }

    private static SchemaReference schema(Descriptor descriptor) {
        return SchemaReference.newBuilder()
                .setTypeName(descriptor.getFullName())
                .setDescriptorFingerprint(DescriptorIdentity.fingerprint(descriptor))
                .build();
    }

    private static MessageProcessor processor(
            String id, Descriptor input, List<Descriptor> outputs,
            int maxOutputs, ProcessorBody body) {
        ProcessorContract contract = ProcessorContract.newBuilder()
                .setProcessorId(id)
                .setInputSchema(schema(input))
                .addAllOutputSchemas(outputs.stream()
                        .map(DurableFlowCoordinatorTest::schema).toList())
                .setMaxOutputs(maxOutputs)
                .build();
        return new MessageProcessor() {
            @Override
            public ProcessorContract contract() {
                return contract;
            }

            @Override
            public List<? extends Message> process(
                    ProcessorContext context, Message input) throws Exception {
                return body.process(context, input);
            }
        };
    }

    private static Result parseResult(EntityEnvelope output) {
        try {
            return Result.parseFrom(output.getPayload().getValue());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    private record TestRuntime(
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            FlowDefinition definition) {
        DurableFlowCoordinator coordinator(FlowLifecycleStore store) {
            return new DurableFlowCoordinator(descriptors, processors, store,
                    PayloadResolver.inlineOnly(descriptors),
                    Clock.fixed(NOW, ZoneOffset.UTC));
        }
    }

    @FunctionalInterface
    private interface Body {
        Result process(RawInput input) throws Exception;
    }

    @FunctionalInterface
    private interface ProcessorBody {
        List<? extends Message> process(ProcessorContext context, Message input)
                throws Exception;
    }
}
