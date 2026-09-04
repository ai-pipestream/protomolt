package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
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
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
        private final DemandProcessorCoordinator coordinator;
        private final Server server;
        private final ManagedChannel grpc;
        private final ProcessorRegistry flowProcessors;
        private DemandProcessorWorker worker;

        private Fixture(Path wal) throws Exception {
            descriptors.registerFile(RuntimeTestProto.getDescriptor());
            channel = new FileDurableProcessorChannel(wal, descriptors, clock);
            coordinator = new DemandProcessorCoordinator(
                    descriptors, channel, RemoteWorkerAdmission.allowAll(),
                    clock, java.time.Duration.ofSeconds(60));
            String name = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(name)
                    .directExecutor()
                    .addService(coordinator)
                    .build()
                    .start();
            grpc = InProcessChannelBuilder.forName(name).directExecutor().build();
            flowProcessors = new ProcessorRegistry(descriptors);
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
            worker = new DemandProcessorWorker(
                    "worker-a", descriptors, workerProcessors,
                    ai.protomolt.proto.mesh.runtime.v1.DemandProcessorServiceGrpc
                            .newStub(grpc));
            worker.start();
            assertThat(worker.awaitAdmission(java.time.Duration.ofSeconds(2))).isTrue();
            assertThat(coordinator.connectedWorkers()).isEqualTo(1);
            RemoteProcessorInvoker remote = new RemoteProcessorInvoker(
                    contract, channel, coordinator::workAvailable, clock, 3);
            flowProcessors.register(remote);
        }

        private CompiledDirectedFlow singleRemoteFlow(ProcessorContract contract) {
            FlowDefinition definition = FlowDefinition.newBuilder()
                    .setName("remote-only")
                    .setInputSchema(contract.getInputSchema())
                    .addNodes(ProcessorNode.newBuilder()
                            .setNodeId("remote_node")
                            .setProcessorId(contract.getProcessorId())
                            .setInputSchema(contract.getInputSchema())
                            .addAllOutputSchemas(contract.getOutputSchemasList()))
                    .addEdges(FlowEdge.newBuilder()
                            .setEdgeId("to_remote")
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
                            .setFlowInput(true)
                            .setTargetNode("remote_node")
                            .setSourceSchema(remote.getInputSchema()))
                    .addEdges(FlowEdge.newBuilder()
                            .setEdgeId("to_refusing")
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

        private void awaitOneDelivery() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (channel.deliveries().isEmpty() && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertThat(channel.deliveries()).hasSize(1);
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
