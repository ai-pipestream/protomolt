package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.ProcessorContracts;
import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.test.RuntimeTestProto;
import ai.protomolt.proto.mesh.runtime.test.Token;
import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;

import java.time.Instant;
import java.util.UUID;

final class ProcessorChannelFixtures {

    static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");

    private ProcessorChannelFixtures() {
    }

    static DescriptorRegistry descriptors() {
        DescriptorRegistry descriptors = DescriptorRegistry.create(false);
        descriptors.registerFile(RuntimeTestProto.getDescriptor());
        return descriptors;
    }

    static ProcessorContract contract() {
        return ProcessorContracts.canonical(ProcessorContract.newBuilder()
                .setProcessorId("remote-tokenizer")
                .setInputSchema(EntityEnvelopes.schemaOf(RawInput.getDefaultInstance()))
                .addOutputSchemas(EntityEnvelopes.schemaOf(Token.getDefaultInstance()))
                .setMaxOutputs(2)
                .build());
    }

    static ProcessorWork work(
            String deliveryId, ChannelPolicy policy, ProcessorContract contract) {
        var input = EntityEnvelopes.root(UUID.randomUUID().toString(),
                "a2afcaf1-e96f-4519-a20d-49530ac905a5",
                RawInput.newBuilder().setText("durable").build(), NOW,
                NOW.plusSeconds(3_600), CompletionPolicy.COMPLETION_POLICY_STRICT);
        return ProcessorWork.newBuilder()
                .setDeliveryId(deliveryId)
                .setRunId(UUID.randomUUID().toString())
                .setNodeId("remote_node")
                .setInvocationId(UUID.randomUUID().toString())
                .setInvocationOrdinal(1)
                .setContract(contract)
                .setInput(input)
                .setDeadline(RemoteValidation.timestamp(NOW.plusSeconds(600)))
                .setMaxAttempts(policy.getMaximumAttempts())
                .setChannelPolicyId("test-policy")
                .setChannelPolicy(policy)
                .build();
    }

    static ProcessorCompletion completion(
            ProcessorWork work, String leaseToken, String completionId) {
        return ProcessorCompletion.newBuilder()
                .setDeliveryId(work.getDeliveryId())
                .setLeaseToken(leaseToken)
                .setCompletionId(completionId)
                .addOutputs(RuntimeSchemas.pack(
                        Token.newBuilder().setText("durable-token").build()))
                .build();
    }
}
