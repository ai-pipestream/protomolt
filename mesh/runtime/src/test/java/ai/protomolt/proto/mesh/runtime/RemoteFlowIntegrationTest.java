package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.ProcessorContracts;
import ai.protomolt.proto.mesh.cluster.ClusterValidation;
import ai.protomolt.proto.mesh.cluster.InMemoryClusterEventRepository;
import ai.protomolt.proto.mesh.cluster.PersistentClusterDirectory;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.mesh.cluster.v1.Endpoint;
import ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.TlsMode;
import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.test.Result;
import ai.protomolt.proto.mesh.runtime.test.RuntimeTestProto;
import ai.protomolt.proto.mesh.runtime.test.Token;
import ai.protomolt.proto.mesh.runtime.v1.FlowDefinition;
import ai.protomolt.proto.mesh.runtime.v1.FlowEdge;
import ai.protomolt.proto.mesh.runtime.v1.FlowOutput;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorNode;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorOutcomeKind;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import ai.protomolt.proto.mesh.v1.ProcessorKind;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteFlowIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final String INPUT_ID = "5cb5ad7d-a497-4bec-bb69-e33036ed8d66";
    private static final String SCOPE_ID = "a2afcaf1-e96f-4519-a20d-49530ac905a5";
    private static final String RUN_ID = "dd2d7260-25bd-4be9-bbba-693cf39b13bb";

    @TempDir
    Path temporary;

    @Test
    void workWaitsForDemandThenSettlesOnlyAfterTheFlowCompletes() throws Exception {
        try (Fixture fixture = new Fixture(temporary.resolve("remote.wal"))) {
            ProcessorContract contract = fixture.remoteContract();
            fixture.startWorker(contract, uppercaseProcessor(contract));
            CompiledDirectedFlow flow = fixture.singleRemoteFlow(contract);
            var input = fixture.input();

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var pending = executor.submit(() -> fixture.runtime().execute(flow, input, RUN_ID));
                fixture.awaitOneDelivery();

                assertThat(pending.isDone()).isFalse();
                assertThat(fixture.channel.deliveries()).singleElement()
                        .extracting(DurableProcessorChannel.DeliveryView::state)
                        .isEqualTo(DurableProcessorChannel.DeliveryState.PENDING);

                fixture.worker.request(1);
                FlowExecutionResult execution = pending.get(5, TimeUnit.SECONDS);

                ProcessorRegistry localProcessors = new ProcessorRegistry(fixture.descriptors);
                localProcessors.register(uppercaseProcessor(contract));
                CompiledDirectedFlow localFlow = new FlowCompiler(
                        fixture.descriptors, localProcessors)
                        .compile(flow.plan().getDefinition());
                FlowExecutionResult local = fixture.runtime()
                        .execute(localFlow, input, RUN_ID);

                assertThat(execution.outputs()).singleElement().satisfies(output -> {
                    assertThat(parseResult(output).getText()).isEqualTo("REMOTE");
                    assertThat(output.getHeader().getParentEntityId()).isEqualTo(INPUT_ID);
                });
                assertThat(execution.outputs()).isEqualTo(local.outputs());
                assertThat(fixture.channel.deliveries()).singleElement()
                        .extracting(DurableProcessorChannel.DeliveryView::state)
                        .isEqualTo(DurableProcessorChannel.DeliveryState.SETTLED);
                assertThat(execution.history().getEventsList())
                        .filteredOn(event -> event.getKind()
                                == HistoryEventKind.HISTORY_EVENT_KIND_PROCESSOR_COMPLETED)
                        .singleElement()
                        .satisfies(event -> assertThat(event.getDeliveryId()).isNotBlank());
                assertThat(execution.history().getEventsList())
                        .filteredOn(event -> event.getKind()
                                == HistoryEventKind.HISTORY_EVENT_KIND_DOWNSTREAM_SETTLED)
                        .singleElement()
                        .satisfies(event -> assertThat(event.getDeliveryId()).isNotBlank());
            }
        }
    }

    @Test
    void downstreamFailureReleasesRatherThanSettlesRemoteCompletion() throws Exception {
        try (Fixture fixture = new Fixture(temporary.resolve("release.wal"))) {
            ProcessorContract contract = fixture.remoteContract();
            fixture.startWorker(contract, uppercaseProcessor(contract));

            ProcessorContract refusingContract = ProcessorContract.newBuilder()
                    .setProcessorId("refusing-local")
                    .setInputSchema(EntityEnvelopes.schemaOf(Result.getDefaultInstance()))
                    .addOutputSchemas(EntityEnvelopes.schemaOf(Token.getDefaultInstance()))
                    .setMaxOutputs(1)
                    .build();
            fixture.flowProcessors.register(new MessageProcessor() {
                @Override
                public ProcessorContract contract() {
                    return refusingContract;
                }

                @Override
                public List<? extends Message> process(
                        ProcessorContext context, Message input) {
                    throw new IllegalStateException("downstream validation refused result");
                }
            });
            CompiledDirectedFlow flow = fixture.remoteThenRefusingFlow(
                    contract, refusingContract);
            fixture.worker.request(1);

            assertThatThrownBy(() -> fixture.runtime().execute(flow, fixture.input(), RUN_ID))
                    .isInstanceOf(FlowExecutionException.class)
                    .hasMessageContaining("downstream validation refused result");

            assertThat(fixture.channel.records())
                    .anyMatch(record -> record.hasReleased())
                    .noneMatch(record -> record.hasSettled());
        }
    }

    @Test
    void suspectDirectoryHealthStopsNewClaimsUntilTheSameLeaseIsActiveAgain()
            throws Exception {
        try (Fixture fixture = new Fixture(temporary.resolve("suspect.wal"))) {
            ProcessorContract contract = fixture.remoteContract();
            fixture.startWorker(contract, uppercaseProcessor(contract));
            CompiledDirectedFlow flow = fixture.singleRemoteFlow(contract);

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var pending = executor.submit(() ->
                        fixture.runtime().execute(flow, fixture.input(), RUN_ID));
                try {
                    fixture.awaitOneDelivery();
                    fixture.updatePresence(PresenceState.PRESENCE_STATE_SUSPECT);

                    fixture.worker.request(1);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(25));

                    assertThat(pending.isDone()).isFalse();
                    assertThat(fixture.worker.activeInvocations()).isZero();
                    assertThat(fixture.channel.deliveries()).singleElement()
                            .extracting(DurableProcessorChannel.DeliveryView::state)
                            .isEqualTo(DurableProcessorChannel.DeliveryState.PENDING);

                    fixture.updatePresence(PresenceState.PRESENCE_STATE_ACTIVE);
                    fixture.coordinator.workAvailable();

                    assertThat(pending.get(5, TimeUnit.SECONDS).outputs())
                            .singleElement()
                            .satisfies(output -> assertThat(parseResult(output).getText())
                                    .isEqualTo("REMOTE"));
                } finally {
                    pending.cancel(true);
                }
            }
        }
    }

    @Test
    void workerHeartbeatCapacityAndDrainAreReflectedInTheDirectory() throws Exception {
        try (Fixture fixture = new Fixture(temporary.resolve("control.wal"))) {
            ProcessorContract contract = fixture.remoteContract();
            fixture.startWorker(contract, uppercaseProcessor(contract));

            assertThat(fixture.directory.nodeCapacity("node-a")).hasValueSatisfying(capacity -> {
                assertThat(capacity.getMaxInFlight()).isEqualTo(4);
                assertThat(capacity.getInFlight()).isZero();
            });
            assertThat(fixture.directory.processorCapacity(
                    "node-a", contract.getProcessorId()))
                    .hasValueSatisfying(capacity -> {
                        assertThat(capacity.getMaxInFlight()).isEqualTo(4);
                        assertThat(capacity.getInFlight()).isZero();
                    });
            long heartbeatSequence = fixture.directory.presence("node-a")
                    .orElseThrow().getHeartbeatSeq();

            fixture.worker.heartbeat();

            assertThat(fixture.directory.presence("node-a")).hasValueSatisfying(presence -> {
                assertThat(presence.getState()).isEqualTo(
                        PresenceState.PRESENCE_STATE_ACTIVE);
                assertThat(presence.getHeartbeatSeq()).isGreaterThan(heartbeatSequence);
            });

            assertThat(fixture.coordinator.drainWorker(
                    "worker-a", "maintenance", NOW.plusSeconds(30))).isTrue();

            assertThat(fixture.coordinator.connectedWorkers()).isZero();
            assertThat(fixture.directory.presence("node-a")).hasValueSatisfying(presence ->
                    assertThat(presence.getState()).isEqualTo(
                            PresenceState.PRESENCE_STATE_DRAINING));
            assertThatThrownBy(() -> fixture.worker.request(1))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void cancellationDuringExecutionIsAcknowledgedAndReleasesTheDurableClaim()
            throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        try (Fixture fixture = new Fixture(temporary.resolve("cancel.wal"))) {
            ProcessorContract contract = fixture.remoteContract();
            fixture.startWorker(contract, new MessageProcessor() {
                @Override
                public ProcessorContract contract() {
                    return contract;
                }

                @Override
                public List<? extends Message> process(
                        ProcessorContext context, Message input) {
                    entered.countDown();
                    while (true) {
                        context.cancellation().throwIfRequested();
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                }
            });
            ProcessorWork work = ProcessorChannelFixtures.work(
                    UUID.randomUUID().toString(),
                    ChannelPolicies.localDurable().getPolicy(),
                    ProcessorContracts.canonical(contract));
            fixture.channel.enqueue(work);
            fixture.worker.request(1);
            fixture.coordinator.workAvailable();
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.channel.delivery(work.getDeliveryId()).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.CLAIMED);

            fixture.updatePresence(PresenceState.PRESENCE_STATE_SUSPECT);
            assertThat(fixture.coordinator.cancelDelivery(
                    work.getDeliveryId(), "operator cancelled")).isTrue();
            fixture.awaitNoActiveInvocations();

            assertThat(fixture.worker.streamFailure()).isEmpty();
            assertThat(fixture.channel.delivery(work.getDeliveryId()).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.PENDING);
            assertThat(fixture.channel.records())
                    .filteredOn(record -> record.hasReleased())
                    .anySatisfy(record -> assertThat(record.getReleased().getOutcome().getKind())
                            .isEqualTo(ProcessorOutcomeKind
                                    .PROCESSOR_OUTCOME_KIND_CANCELLED));
        }
    }

    private static MessageProcessor uppercaseProcessor(ProcessorContract contract) {
        return new MessageProcessor() {
            @Override
            public ProcessorContract contract() {
                return contract;
            }

            @Override
            public List<? extends Message> process(ProcessorContext context, Message input)
                    throws Exception {
                RawInput request = RawInput.parseFrom(input.toByteString());
                return List.of(Result.newBuilder()
                        .setBranch("remote")
                        .setText(request.getText().toUpperCase(java.util.Locale.ROOT))
                        .build());
            }
        };
    }

    private static Result parseResult(
            ai.protomolt.proto.mesh.v1.EntityEnvelope output) {
        try {
            return Result.parseFrom(output.getPayload().getValue());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    private final class Fixture implements AutoCloseable {
        private final DescriptorRegistry descriptors = DescriptorRegistry.create(false);
        private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        private final FileDurableProcessorChannel channel;
        private final PersistentClusterDirectory directory;
        private final DemandProcessorCoordinator coordinator;
        private final Server server;
        private final ManagedChannel grpc;
        private final ProcessorRegistry flowProcessors;
        private DemandProcessorWorker worker;

        private Fixture(Path wal) throws Exception {
            descriptors.registerFile(RuntimeTestProto.getDescriptor());
            channel = new FileDurableProcessorChannel(wal, descriptors, clock);
            flowProcessors = new ProcessorRegistry(descriptors);
            ClusterDescriptor unsigned = ClusterDescriptor.newBuilder()
                    .setClusterId("test-cluster")
                    .setDisplayName("Runtime test cluster")
                    .setTrustDomain("test")
                    .addProtocolRevisions(1)
                    .setCreatedAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                    .build();
            ClusterDescriptor cluster = unsigned.toBuilder()
                    .setFingerprint(ClusterValidation.descriptorFingerprint(unsigned))
                    .build();
            directory = new PersistentClusterDirectory(
                    cluster, clock, new InMemoryClusterEventRepository());
            var coordinatorReference = new AtomicReference<DemandProcessorCoordinator>();
            var view = (java.util.function.Supplier<ProcessorDirectoryClient.View>) () ->
                    ProcessorDirectoryClient.View.from(
                            directory.snapshot(), directory.generation());
            coordinator = new DemandProcessorCoordinator(
                    descriptors, channel,
                    new DirectoryWorkerAdmission(view, clock, contract ->
                            flowProcessors.registerOrVerify(new RemoteProcessorInvoker(
                                    contract, channel,
                                    () -> coordinatorReference.get().workAvailable(),
                                    clock, 3))),
                    new DirectoryProcessorResolver(view, clock),
                    new PersistentDirectoryWorkerControl(directory, clock),
                    new WorkerSessionRegistry(), new WorkerCapacityController(16),
                    clock, java.time.Duration.ofSeconds(60));
            coordinatorReference.set(coordinator);
            String name = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(name)
                    .directExecutor()
                    .addService(coordinator)
                    .build()
                    .start();
            grpc = InProcessChannelBuilder.forName(name).directExecutor().build();
        }

        private ProcessorContract remoteContract() {
            return ProcessorContract.newBuilder()
                    .setProcessorId("uppercase-remote")
                    .setInputSchema(EntityEnvelopes.schemaOf(RawInput.getDefaultInstance()))
                    .addOutputSchemas(EntityEnvelopes.schemaOf(Result.getDefaultInstance()))
                    .setMaxOutputs(1)
                    .build();
        }

        private void startWorker(
                ProcessorContract contract, MessageProcessor processor) throws Exception {
            ProcessorRegistry workerProcessors = new ProcessorRegistry(descriptors);
            workerProcessors.register(processor);
            ProcessorContract exact = ProcessorContracts.canonical(contract);
            directory.register(NodeAdvertisement.newBuilder()
                    .setNodeId("node-a")
                    .setClusterId("test-cluster")
                    .addEndpoints(Endpoint.newBuilder()
                            .setEndpointId("grpc-main")
                            .setAddress("127.0.0.1:9090")
                            .setTlsMode(TlsMode.TLS_MODE_SYSTEM)
                            .setDirect(true))
                    .setAdvertisedAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                    .setTtl(Durations.fromSeconds(60))
                    .setEpoch(1)
                    .setSeq(1)
                    .build());
            directory.registerProcessor(ProcessorAdvertisement.newBuilder()
                    .setProcessorId(exact.getProcessorId())
                    .setNodeId("node-a")
                    .setKind(ProcessorKind.PROCESSOR_KIND_DETERMINISTIC)
                    .addAcceptedSchemas(exact.getInputSchema())
                    .setContract(exact)
                    .setNodeEpoch(1)
                    .setLeaseEpoch(1)
                    .setAdvertisedAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                    .setLeaseExpiresAt(Timestamps.fromMillis(
                            NOW.plusSeconds(120).toEpochMilli()))
                    .setSupportsSessionResume(true)
                    .setMaxDisconnectGrace(Durations.fromSeconds(10))
                    .setSeq(1)
                    .build());
            worker = new DemandProcessorWorker(
                    "worker-a", descriptors, workerProcessors,
                    PayloadResolver.inlineOnly(descriptors),
                    ai.protomolt.proto.mesh.runtime.v1.DemandProcessorServiceGrpc
                            .newStub(grpc), RemoteFailurePolicy.retryAll(),
                    "node-a", 1, "grpc-main", Map.of(exact.getProcessorId(), 1L), "", 4);
            worker.start();
            assertThat(worker.awaitAdmission(java.time.Duration.ofSeconds(2))).isTrue();
            assertThat(coordinator.connectedWorkers()).isEqualTo(1);
        }

        private CompiledDirectedFlow singleRemoteFlow(ProcessorContract contract) {
            FlowDefinition definition = FlowDefinition.newBuilder()
                    .setName("remote-only")
                    .addChannelPolicies(ChannelPolicies.localDurable())
                    .setInputSchema(contract.getInputSchema())
                    .addNodes(ProcessorNode.newBuilder()
                            .setNodeId("remote_node")
                            .setProcessorId(contract.getProcessorId())
                            .setInputSchema(contract.getInputSchema())
                            .addAllOutputSchemas(contract.getOutputSchemasList()))
                    .addEdges(FlowEdge.newBuilder()
                            .setEdgeId("to_remote")
                            .setChannelPolicyId(ChannelPolicies.LOCAL_DURABLE_ID)
                            .setFlowInput(true)
                            .setTargetNode("remote_node")
                            .setSourceSchema(contract.getInputSchema()))
                    .addOutputs(FlowOutput.newBuilder()
                            .setNodeId("remote_node")
                            .setSchema(contract.getOutputSchemas(0)))
                    .setMaxMessages(10)
                    .setDeadline(Duration.newBuilder().setSeconds(600))
                    .build();
            return new FlowCompiler(descriptors, flowProcessors).compile(definition);
        }

        private CompiledDirectedFlow remoteThenRefusingFlow(
                ProcessorContract remote, ProcessorContract refusing) {
            FlowDefinition definition = FlowDefinition.newBuilder()
                    .setName("remote-then-refuse")
                    .addChannelPolicies(ChannelPolicies.localDurable())
                    .setInputSchema(remote.getInputSchema())
                    .addNodes(ProcessorNode.newBuilder()
                            .setNodeId("remote_node")
                            .setProcessorId(remote.getProcessorId())
                            .setInputSchema(remote.getInputSchema())
                            .addAllOutputSchemas(remote.getOutputSchemasList()))
                    .addNodes(ProcessorNode.newBuilder()
                            .setNodeId("refusing_node")
                            .setProcessorId(refusing.getProcessorId())
                            .setInputSchema(refusing.getInputSchema())
                            .addAllOutputSchemas(refusing.getOutputSchemasList()))
                    .addEdges(FlowEdge.newBuilder()
                            .setEdgeId("to_remote")
                            .setChannelPolicyId(ChannelPolicies.LOCAL_DURABLE_ID)
                            .setFlowInput(true)
                            .setTargetNode("remote_node")
                            .setSourceSchema(remote.getInputSchema()))
                    .addEdges(FlowEdge.newBuilder()
                            .setEdgeId("to_refusing")
                            .setChannelPolicyId(ChannelPolicies.LOCAL_DURABLE_ID)
                            .setSourceNode("remote_node")
                            .setTargetNode("refusing_node")
                            .setSourceSchema(remote.getOutputSchemas(0)))
                    .addOutputs(FlowOutput.newBuilder()
                            .setNodeId("refusing_node")
                            .setSchema(refusing.getOutputSchemas(0)))
                    .setMaxMessages(10)
                    .setDeadline(Duration.newBuilder().setSeconds(600))
                    .build();
            return new FlowCompiler(descriptors, flowProcessors).compile(definition);
        }

        private ai.protomolt.proto.mesh.v1.EntityEnvelope input() {
            return EntityEnvelopes.root(INPUT_ID, SCOPE_ID,
                    RawInput.newBuilder().setText("remote").build(), NOW,
                    NOW.plusSeconds(3600), CompletionPolicy.COMPLETION_POLICY_STRICT);
        }

        private FlowRuntime runtime() {
            return new FlowRuntime(descriptors,
                    PayloadResolver.inlineOnly(descriptors), clock);
        }

        private void updatePresence(PresenceState state) {
            NodePresence current = directory.presence("node-a").orElseThrow();
            directory.heartbeat(current.toBuilder()
                    .setState(state)
                    .setHeartbeatSeq(current.getHeartbeatSeq() + 1)
                    .setLastHeartbeatAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                    .setExpiresAt(Timestamps.fromMillis(NOW.plusSeconds(60).toEpochMilli()))
                    .build());
        }

        private void awaitOneDelivery() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (channel.deliveries().isEmpty() && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertThat(channel.deliveries()).hasSize(1);
        }

        private void awaitNoActiveInvocations() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (worker.activeInvocations() != 0 && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertThat(worker.activeInvocations()).isZero();
        }

        @Override
        public void close() throws Exception {
            if (worker != null) {
                worker.close();
            }
            coordinator.close();
            grpc.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            channel.close();
        }
    }
}
