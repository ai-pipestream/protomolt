package ai.protomolt.proto.mesh;

import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorExecutionGuarantees;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcessorContractsTest {

    private static final SchemaReference A = SchemaReference.newBuilder()
            .setTypeName("acme.v1.A").setDescriptorFingerprint("ab".repeat(32)).build();
    private static final SchemaReference B = SchemaReference.newBuilder()
            .setTypeName("acme.v1.B").setDescriptorFingerprint("cd".repeat(32)).build();

    @Test
    void fingerprintIsStableAcrossOutputDeclarationOrder() {
        ProcessorContract first = ProcessorContracts.canonical(ProcessorContract.newBuilder()
                .setProcessorId("processor")
                .setInputSchema(A)
                .addOutputSchemas(B)
                .addOutputSchemas(A)
                .setMaxOutputs(2)
                .setGuarantees(ProcessorExecutionGuarantees.newBuilder()
                        .setIdempotentInvocation(true))
                .build());
        ProcessorContract second = ProcessorContracts.canonical(ProcessorContract.newBuilder()
                .setProcessorId("processor")
                .setInputSchema(A)
                .addOutputSchemas(A)
                .addOutputSchemas(B)
                .setMaxOutputs(2)
                .setGuarantees(ProcessorExecutionGuarantees.newBuilder()
                        .setIdempotentInvocation(true))
                .build());

        assertThat(first).isEqualTo(second);
        assertThat(first.getContractFingerprint()).hasSize(64);
    }

    @Test
    void everyExecutionGuaranteeAndOutputBoundParticipatesInIdentity() {
        ProcessorContract base = ProcessorContracts.canonical(ProcessorContract.newBuilder()
                .setProcessorId("processor").setInputSchema(A).addOutputSchemas(B)
                .setMaxOutputs(1).build());
        ProcessorContract changed = ProcessorContracts.canonical(base.toBuilder()
                .clearContractFingerprint().setMaxOutputs(2).build());
        ProcessorContract guaranteed = ProcessorContracts.canonical(base.toBuilder()
                .clearContractFingerprint()
                .setGuarantees(ProcessorExecutionGuarantees.newBuilder()
                        .setCooperativeCancellation(true)).build());

        assertThat(changed.getContractFingerprint())
                .isNotEqualTo(base.getContractFingerprint());
        assertThat(guaranteed.getContractFingerprint())
                .isNotEqualTo(base.getContractFingerprint());
    }

    @Test
    void suppliedFingerprintMustCommitToTheCanonicalContract() {
        ProcessorContract forged = ProcessorContract.newBuilder()
                .setProcessorId("processor").setInputSchema(A).addOutputSchemas(B)
                .setMaxOutputs(1).setContractFingerprint("ef".repeat(32)).build();

        assertThatThrownBy(() -> ProcessorContracts.canonical(forged))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fingerprint");
    }
}
