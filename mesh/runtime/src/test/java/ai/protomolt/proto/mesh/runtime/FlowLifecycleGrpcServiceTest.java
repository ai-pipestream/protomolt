package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.test.Result;
import ai.protomolt.proto.mesh.runtime.test.RuntimeTestProto;
import ai.protomolt.proto.mesh.runtime.v1.FlowDefinition;
import ai.protomolt.proto.mesh.runtime.v1.FlowEdge;
import ai.protomolt.proto.mesh.runtime.v1.FlowLifecycleServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.FlowOutput;
import ai.protomolt.proto.mesh.runtime.v1.GetRunRequest;
import ai.protomolt.proto.mesh.runtime.v1.PublishFlowRequest;
import ai.protomolt.proto.mesh.runtime.v1.ReadRunHistoryRequest;
import ai.protomolt.proto.mesh.runtime.v1.SetDeploymentRequest;
import ai.protomolt.proto.mesh.runtime.v1.StartRunRequest;
import ai.protomolt.proto.mesh.runtime.v1.ValidateFlowRequest;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorNode;
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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlowLifecycleGrpcServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final String RUN_ID = "dd2d7260-25bd-4be9-bbba-693cf39b13bb";

    @TempDir
    Path temporary;

    @Test
    void protobufRpcPublishesDeploysExecutesAndPagesDurableHistory() throws Exception {
        try (Fixture fixture = new Fixture(temporary.resolve("grpc.wal"))) {
            var validation = fixture.client.validateFlow(ValidateFlowRequest.newBuilder()
                    .setVersion("v1").setDefinition(fixture.definition).build());
            assertThat(validation.getValid()).isTrue();

            var publication = fixture.client.publishFlow(PublishFlowRequest.newBuilder()
                    .setVersion("v1").setDefinition(fixture.definition).build());
            assertThat(publication.hasPublished()).isTrue();
            var deployment = fixture.client.setDeployment(SetDeploymentRequest.newBuilder()
                    .setWorkflowName("grpc-flow").setVersion("v1").build());
            assertThat(deployment.getRevision()).isEqualTo(1);

            var run = fixture.client.startRun(StartRunRequest.newBuilder()
                    .setWorkflowName("grpc-flow")
                    .setRunId(RUN_ID)
                    .setInput(fixture.input())
                    .build());
            assertThat(run.getHistory().getOutputsCount()).isEqualTo(1);
            assertThat(fixture.client.getRun(GetRunRequest.newBuilder()
                    .setRunId(RUN_ID).build())).isEqualTo(run);

            var first = fixture.client.readRunHistory(ReadRunHistoryRequest.newBuilder()
                    .setRunId(RUN_ID).setLimit(2).build());
            var rest = fixture.client.readRunHistory(ReadRunHistoryRequest.newBuilder()
                    .setRunId(RUN_ID).setAfterSequence(first.getNextSequence())
                    .setLimit(10_000).build());
            assertThat(first.getEventsCount()).isEqualTo(2);
            assertThat(rest.getEventsCount()).isPositive();
            assertThat(rest.getTerminal()).isTrue();
            assertThat(rest.getNextSequence()).isEqualTo(run.getHistory().getEventsCount());
        }
    }

    @Test
    void rpcUsesNamedStatusesForMalformedMissingAndRevisionConflicts() throws Exception {
        try (Fixture fixture = new Fixture(temporary.resolve("refusals.wal"))) {
            assertStatus(Status.Code.NOT_FOUND, () -> fixture.client.getRun(
                    GetRunRequest.newBuilder().setRunId(RUN_ID).build()));
            assertStatus(Status.Code.INVALID_ARGUMENT, () -> fixture.client.getRun(
                    GetRunRequest.newBuilder().setRunId("not-a-uuid").build()));

            var invalid = fixture.client.validateFlow(ValidateFlowRequest.newBuilder()
                    .setVersion("INVALID VERSION")
                    .setDefinition(fixture.definition).build());
            assertThat(invalid.getValid()).isFalse();
            assertThat(invalid.getFindingsList()).singleElement()
                    .satisfies(finding -> assertThat(finding.getCode())
                            .isEqualTo("invalid-version"));

            fixture.client.publishFlow(PublishFlowRequest.newBuilder()
                    .setVersion("v1").setDefinition(fixture.definition).build());
            fixture.client.setDeployment(SetDeploymentRequest.newBuilder()
                    .setWorkflowName("grpc-flow").setVersion("v1").build());
            assertStatus(Status.Code.ABORTED, () -> fixture.client.setDeployment(
                    SetDeploymentRequest.newBuilder()
                            .setWorkflowName("grpc-flow")
                            .setVersion("v1")
                            .setExpectedRevision(99)
                            .build()));
        }
    }

    private static void assertStatus(Status.Code code, Runnable operation) {
        assertThatThrownBy(operation::run)
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> assertThat(
                        ((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(code));
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
        private final FileFlowLifecycleStore store;
        private final Server server;
        private final ManagedChannel channel;
        private final FlowLifecycleServiceGrpc.FlowLifecycleServiceBlockingStub client;
        private final FlowDefinition definition;

        private Fixture(Path wal) throws Exception {
            descriptors.registerFile(RuntimeTestProto.getDescriptor());
            ProcessorRegistry processors = new ProcessorRegistry(descriptors);
            ProcessorContract contract = ProcessorContract.newBuilder()
                    .setProcessorId("echo")
                    .setInputSchema(schema(RawInput.getDescriptor()))
                    .addOutputSchemas(schema(Result.getDescriptor()))
                    .setMaxOutputs(1)
                    .build();
            processors.register(new MessageProcessor() {
                @Override
                public ProcessorContract contract() {
                    return contract;
                }

                @Override
                public List<? extends com.google.protobuf.Message> process(
                        ProcessorContext context, com.google.protobuf.Message message)
                        throws Exception {
                    RawInput input = RawInput.parseFrom(message.toByteString());
                    return List.of(Result.newBuilder().setBranch("grpc")
                            .setText(input.getText()).build());
                }
            });
            definition = FlowDefinition.newBuilder()
                    .setName("grpc-flow")
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
            store = new FileFlowLifecycleStore(wal);
            DurableFlowCoordinator coordinator = new DurableFlowCoordinator(
                    descriptors, processors, store,
                    PayloadResolver.inlineOnly(descriptors),
                    Clock.fixed(NOW, ZoneOffset.UTC));
            String name = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(new FlowLifecycleGrpcService(coordinator))
                    .build().start();
            channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            client = FlowLifecycleServiceGrpc.newBlockingStub(channel);
        }

        private ai.protomolt.proto.mesh.v1.EntityEnvelope input() {
            return EntityEnvelopes.root(
                    "5cb5ad7d-a497-4bec-bb69-e33036ed8d66",
                    "a2afcaf1-e96f-4519-a20d-49530ac905a5",
                    RawInput.newBuilder().setText("grpc").build(),
                    NOW, NOW.plusSeconds(600), CompletionPolicy.COMPLETION_POLICY_STRICT);
        }

        @Override
        public void close() throws Exception {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            store.close();
        }
    }
}
