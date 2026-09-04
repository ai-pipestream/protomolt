package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.test.NormalizedInput;
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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlowRuntimeTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final String INPUT_ID = "5cb5ad7d-a497-4bec-bb69-e33036ed8d66";
    private static final String SCOPE_ID = "a2afcaf1-e96f-4519-a20d-49530ac905a5";
    private static final String RUN_ID = "dd2d7260-25bd-4be9-bbba-693cf39b13bb";

    private DescriptorRegistry descriptors;
    private ProcessorRegistry processors;
    private FlowCompiler compiler;

    @BeforeEach
    void setUp() {
        descriptors = DescriptorRegistry.create(false);
        descriptors.registerFile(RuntimeTestProto.getDescriptor());
        processors = new ProcessorRegistry(descriptors);
        processors.register(processor("split", NormalizedInput.getDescriptor(),
                List.of(Token.getDescriptor()), 2, (context, input) -> {
                    NormalizedInput normalized = NormalizedInput.parseFrom(input.toByteString());
                    return List.of(
                            Token.newBuilder().setText(normalized.getText() + "-left").build(),
                            Token.newBuilder().setText(normalized.getText() + "-right")
                                    .setRouteRight(true).build());
                }));
        processors.register(processor("left", Token.getDescriptor(),
                List.of(Result.getDescriptor()), 1, (context, input) -> List.of(
                        Result.newBuilder().setBranch("left")
                                .setText(Token.parseFrom(input.toByteString()).getText()).build())));
        processors.register(processor("right", Token.getDescriptor(),
                List.of(Result.getDescriptor()), 1, (context, input) -> List.of(
                        Result.newBuilder().setBranch("right")
                                .setText(Token.parseFrom(input.toByteString()).getText()).build())));
        compiler = new FlowCompiler(descriptors, processors);
    }

    @Test
    void compilesAndExecutesAProjectedBranchedMultiOutputFlow() throws Exception {
        CompiledDirectedFlow flow = compiler.compile(definition());
        var input = EntityEnvelopes.root(INPUT_ID, SCOPE_ID,
                RawInput.newBuilder().setText("case").build(), NOW,
                NOW.plusSeconds(3600), CompletionPolicy.COMPLETION_POLICY_STRICT);
        FlowRuntime runtime = new FlowRuntime(descriptors,
                PayloadResolver.inlineOnly(descriptors), Clock.fixed(NOW, ZoneOffset.UTC));

        FlowExecutionResult execution = runtime.execute(flow, input, RUN_ID);

        assertThat(execution.outputs()).hasSize(2);
        assertThat(execution.outputs()).extracting(output -> output.getSchema().getTypeName())
                .containsOnly(Result.getDescriptor().getFullName());
        assertThat(execution.outputs().stream()
                .map(FlowRuntimeTest::parseResult)
                .map(Result::getBranch))
                .containsExactly("left", "right");
        assertThat(execution.outputs()).allSatisfy(output -> {
            assertThat(output.getHeader().getScopeId()).isEqualTo(SCOPE_ID);
            assertThat(output.getHeader().getScopeDepth()).isEqualTo(3);
        });

        var history = execution.history();
        assertThat(history.getRunId()).isEqualTo(RUN_ID);
        assertThat(history.getPlanFingerprint()).isEqualTo(flow.plan().getPlanFingerprint());
        assertThat(history.getOutputsList()).isEqualTo(execution.outputs());
        assertThat(history.getEventsList()).extracting(event -> event.getSequence())
                .containsExactlyElementsOf(java.util.stream.LongStream
                        .rangeClosed(1, history.getEventsCount()).boxed().toList());
        assertThat(history.getEvents(0).getKind())
                .isEqualTo(HistoryEventKind.HISTORY_EVENT_KIND_RUN_STARTED);
        assertThat(history.getEvents(history.getEventsCount() - 1).getKind())
                .isEqualTo(HistoryEventKind.HISTORY_EVENT_KIND_RUN_COMPLETED);
        assertThat(history.getEventsList()).extracting(event -> event.getKind())
                .contains(HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED,
                        HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_PRODUCED,
                        HistoryEventKind.HISTORY_EVENT_KIND_DOWNSTREAM_SETTLED);
    }

    @Test
    void stableRunIdMakesDerivedMessageIdsReplayable() {
        CompiledDirectedFlow flow = compiler.compile(definition());
        var input = EntityEnvelopes.root(INPUT_ID, SCOPE_ID,
                RawInput.newBuilder().setText("case").build(), NOW,
                NOW.plusSeconds(3600), CompletionPolicy.COMPLETION_POLICY_STRICT);
        FlowRuntime runtime = new FlowRuntime(descriptors,
                PayloadResolver.inlineOnly(descriptors), Clock.fixed(NOW, ZoneOffset.UTC));

        List<String> first = runtime.execute(flow, input, RUN_ID).outputs().stream()
                .map(output -> output.getHeader().getEntityId()).toList();
        List<String> replay = runtime.execute(flow, input, RUN_ID).outputs().stream()
                .map(output -> output.getHeader().getEntityId()).toList();

        assertThat(replay).isEqualTo(first);
    }

    @Test
    void persistedPlanRestoresOnlyAgainstTheSameContracts() {
        CompiledDirectedFlow flow = compiler.compile(definition());

        assertThat(compiler.restore(flow.plan()).plan()).isEqualTo(flow.plan());
        assertThatThrownBy(() -> compiler.restore(flow.plan().toBuilder()
                .setPlanFingerprint("0".repeat(64)).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fingerprint mismatch");
    }

    @Test
    void planFingerprintIsStableForTheSameProtobufDefinitionAndContracts() {
        assertThat(compiler.compile(definition()).plan().getPlanFingerprint())
                .isEqualTo(compiler.compile(definition()).plan().getPlanFingerprint());
    }

    @Test
    void compileRefusesAProcessorCycleBeforeExecution() {
        Descriptor token = Token.getDescriptor();
        ProcessorRegistry echoes = new ProcessorRegistry(descriptors);
        echoes.register(processor("echo-a", token, List.of(token), 1,
                (context, input) -> List.of(input)));
        echoes.register(processor("echo-b", token, List.of(token), 1,
                (context, input) -> List.of(input)));
        FlowDefinition cyclic = FlowDefinition.newBuilder()
                .setName("cyclic")
                .addChannelPolicies(ChannelPolicies.localDurable())
                .setInputSchema(EntityEnvelopes.schemaOf(Token.getDefaultInstance()))
                .addNodes(node("a", "echo-a", token, token))
                .addNodes(node("b", "echo-b", token, token))
                .addEdges(edgeFromInput("input_to_a", "a", token, null, ""))
                .addEdges(edgeFromNode("a_to_b", "a", "b", token, ""))
                .addEdges(edgeFromNode("b_to_a", "b", "a", token, ""))
                .addOutputs(output("b", token))
                .setMaxMessages(10)
                .setDeadline(Duration.newBuilder().setSeconds(30))
                .build();

        assertThatThrownBy(() -> new FlowCompiler(descriptors, echoes).compile(cyclic))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void compileRefusesSameNamedDescriptorFingerprintDrift() {
        FlowDefinition drifted = definition().toBuilder()
                .setInputSchema(EntityEnvelopes.schemaOf(RawInput.getDefaultInstance())
                        .toBuilder().setDescriptorFingerprint("0".repeat(64)))
                .build();

        assertThatThrownBy(() -> compiler.compile(drifted))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fingerprint mismatch");
    }

    @Test
    void compileRefusesANonBooleanEdgePredicate() {
        FlowDefinition definition = definition();
        FlowDefinition invalid = definition.toBuilder()
                .setEdges(0, definition.getEdges(0).toBuilder().setWhen("message.text"))
                .build();

        assertThatThrownBy(() -> compiler.compile(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("predicate")
                .hasMessageContaining("bool");
    }

    @Test
    void runtimeEnforcesTheGlobalMessageBound() {
        FlowDefinition tooSmall = definition().toBuilder().setMaxMessages(2).build();
        CompiledDirectedFlow flow = compiler.compile(tooSmall);
        var input = EntityEnvelopes.root(INPUT_ID, SCOPE_ID,
                RawInput.newBuilder().setText("case").build(), NOW,
                NOW.plusSeconds(3600), CompletionPolicy.COMPLETION_POLICY_STRICT);

        assertThatThrownBy(() -> new FlowRuntime(descriptors,
                PayloadResolver.inlineOnly(descriptors), Clock.fixed(NOW, ZoneOffset.UTC))
                .execute(flow, input, RUN_ID))
                .isInstanceOf(FlowExecutionException.class)
                .satisfies(error -> assertThat(((FlowExecutionException) error).history()
                        .getEventsList()).extracting(event -> event.getKind())
                        .contains(HistoryEventKind.HISTORY_EVENT_KIND_RUN_FAILED))
                .hasMessageContaining("max_messages 2");
    }

    private FlowDefinition definition() {
        return FlowDefinition.newBuilder()
                .setName("project-and-branch")
                .addChannelPolicies(ChannelPolicies.localDurable())
                .setInputSchema(EntityEnvelopes.schemaOf(RawInput.getDefaultInstance()))
                .addNodes(node("split_node", "split", NormalizedInput.getDescriptor(),
                        Token.getDescriptor()))
                .addNodes(node("left_node", "left", Token.getDescriptor(),
                        Result.getDescriptor()))
                .addNodes(node("right_node", "right", Token.getDescriptor(),
                        Result.getDescriptor()))
                .addEdges(edgeFromInput("normalize", "split_node", RawInput.getDescriptor(),
                        NormalizedInput.getDescriptor(), ""))
                .addEdges(edgeFromNode("to_left", "split_node", "left_node",
                        Token.getDescriptor(), "!message.route_right"))
                .addEdges(edgeFromNode("to_right", "split_node", "right_node",
                        Token.getDescriptor(), "message.route_right"))
                .addOutputs(output("left_node", Result.getDescriptor()))
                .addOutputs(output("right_node", Result.getDescriptor()))
                .setMaxMessages(100)
                .setDeadline(Duration.newBuilder().setSeconds(600))
                .build();
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

    private static FlowEdge edgeFromInput(
            String edgeId, String target, Descriptor source,
            Descriptor projection, String when) {
        FlowEdge.Builder edge = FlowEdge.newBuilder()
                .setEdgeId(edgeId)
                .setChannelPolicyId(ChannelPolicies.LOCAL_DURABLE_ID)
                .setFlowInput(true)
                .setTargetNode(target)
                .setSourceSchema(schema(source))
                .setWhen(when);
        if (projection != null) {
            edge.setProjectTo(schema(projection));
        }
        return edge.build();
    }

    private static FlowEdge edgeFromNode(
            String edgeId, String sourceNode, String target,
            Descriptor source, String when) {
        return FlowEdge.newBuilder()
                .setEdgeId(edgeId)
                .setChannelPolicyId(ChannelPolicies.LOCAL_DURABLE_ID)
                .setSourceNode(sourceNode)
                .setTargetNode(target)
                .setSourceSchema(schema(source))
                .setWhen(when)
                .build();
    }

    private static FlowOutput output(String nodeId, Descriptor schema) {
        return FlowOutput.newBuilder().setNodeId(nodeId).setSchema(schema(schema)).build();
    }

    private static ai.protomolt.proto.mesh.v1.SchemaReference schema(Descriptor descriptor) {
        return ai.protomolt.proto.mesh.v1.SchemaReference.newBuilder()
                .setTypeName(descriptor.getFullName())
                .setDescriptorFingerprint(
                        ai.protomolt.proto.descriptors.DescriptorIdentity.fingerprint(descriptor))
                .build();
    }

    private static MessageProcessor processor(
            String id,
            Descriptor input,
            List<Descriptor> outputs,
            int maxOutputs,
            ProcessorBody body) {
        ProcessorContract contract = ProcessorContract.newBuilder()
                .setProcessorId(id)
                .setInputSchema(schema(input))
                .addAllOutputSchemas(outputs.stream().map(FlowRuntimeTest::schema).toList())
                .setMaxOutputs(maxOutputs)
                .build();
        return new MessageProcessor() {
            @Override
            public ProcessorContract contract() {
                return contract;
            }

            @Override
            public List<? extends Message> process(ProcessorContext context, Message message)
                    throws Exception {
                return body.process(context, message);
            }
        };
    }

    private static Result parseResult(ai.protomolt.proto.mesh.v1.EntityEnvelope output) {
        try {
            return Result.parseFrom(output.getPayload().getValue());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    @FunctionalInterface
    private interface ProcessorBody {
        List<? extends Message> process(ProcessorContext context, Message input) throws Exception;
    }
}
