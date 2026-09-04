package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.mesh.cluster.ClusterDirectory.ApplyOutcome;
import ai.protomolt.proto.mesh.cluster.ClusterFixtures.MutableClock;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.protomolt.proto.mesh.ProcessorContracts;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.v1.ProcessorKind;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The hello-to-advertisement bridge mapping: identity, kind selection, capability mapping,
 * and lease epoch/expiry derivation.
 */
class DelegationBridgeTest {

    private static final Instant NOW = ClusterFixtures.T0;
    private static final Duration LEASE = Duration.ofSeconds(45);

    private static WorkerHello.Builder hello() {
        return WorkerHello.newBuilder()
                .setWorkerId("worker-7")
                .setProtocolVersion(1)
                .setProvider("kimi")
                .setModel("kimi-k2")
                .addCapabilities(WorkerCapability.newBuilder()
                        .setName("proto-edit")
                        .setDescription("edits protobuf contracts"))
                .addCapabilities(WorkerCapability.newBuilder()
                        .setName("java-build"))
                .addCapabilities(WorkerCapability.newBuilder()
                        .setName("proto-edit"));
    }

    private static ProcessorContract contract() {
        return ProcessorContracts.canonical(ProcessorContract.newBuilder()
                .setProcessorId("worker-7")
                .setInputSchema(ClusterFixtures.schema())
                .addOutputSchemas(ClusterFixtures.schema())
                .setMaxOutputs(1)
                .build());
    }

    @Test
    void mapsHelloToLeasedAdvertisement() {
        ProcessorAdvertisement advertisement = DelegationBridge.toProcessorAdvertisement(
                hello().build(), contract(), "node-1", 7, 3, 1, LEASE, NOW);

        assertThat(advertisement.getProcessorId()).isEqualTo("worker-7");
        assertThat(advertisement.getNodeId()).isEqualTo("node-1");
        assertThat(advertisement.getKind()).isEqualTo(ProcessorKind.PROCESSOR_KIND_LLM);
        assertThat(advertisement.getNodeEpoch()).isEqualTo(7);
        assertThat(advertisement.getProvider()).isEqualTo("kimi");
        assertThat(advertisement.getModel()).isEqualTo("kimi-k2");
        // Capability names in declaration order, deduplicated: the bridge maps a set.
        assertThat(advertisement.getCapabilitiesList())
                .containsExactly("proto-edit", "java-build");
        assertThat(advertisement.getCapabilityDetailsList())
                .extracting(ai.protomolt.proto.mesh.cluster.v1.CapabilityDescription::getName)
                .containsExactly("proto-edit", "java-build");
        assertThat(advertisement.getAcceptedSchemasList())
                .containsExactly(contract().getInputSchema());
        assertThat(advertisement.getContract()).isEqualTo(contract());
        assertThat(advertisement.getLeaseEpoch()).isEqualTo(3);
        assertThat(advertisement.getSeq()).isEqualTo(1);
        assertThat(advertisement.getAdvertisedAt()).isEqualTo(ClusterFixtures.ts(NOW));
        assertThat(advertisement.getLeaseExpiresAt())
                .isEqualTo(ClusterFixtures.ts(NOW.plus(LEASE)));
        ClusterValidation.validate(advertisement);
    }

    @Test
    void providerlessWorkerMapsToDeterministic() {
        ProcessorAdvertisement advertisement = DelegationBridge.toProcessorAdvertisement(
                hello().clearProvider().clearModel().build(), contract(),
                "node-1", 1, 1, 1, LEASE, NOW);

        assertThat(advertisement.getKind()).isEqualTo(ProcessorKind.PROCESSOR_KIND_DETERMINISTIC);
    }

    @Test
    void derivedAdvertisementRegistersCleanly() {
        ClusterDirectory directory = new ClusterDirectory(
                ClusterFixtures.cluster(), new MutableClock(NOW));
        directory.register(ClusterFixtures.node("node-1"));
        ProcessorAdvertisement advertisement = DelegationBridge.toProcessorAdvertisement(
                hello().build(), contract(), "node-1", 1, 1, 1, LEASE, NOW);

        assertThat(directory.registerProcessor(advertisement))
                .isEqualTo(ApplyOutcome.REGISTERED);
        assertThat(directory.eligibleProcessors("", "", "proto-edit"))
                .extracting(ProcessorAdvertisement::getProcessorId)
                .containsExactly("worker-7");
    }

    @Test
    void invalidHelloIsRejectedBeforeMapping() {
        assertThatThrownBy(() -> DelegationBridge.toProcessorAdvertisement(
                hello().setWorkerId("not a valid id").build(), contract(),
                "node-1", 1, 1, 1,
                LEASE, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hello fails the delegation contract annotations");
    }

    @Test
    void nonPositiveLeaseIsRejected() {
        assertThatThrownBy(() -> DelegationBridge.toProcessorAdvertisement(
                hello().build(), contract(), "node-1", 1, 1, 1, Duration.ZERO, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseDuration must be positive");
        assertThatThrownBy(() -> DelegationBridge.toProcessorAdvertisement(
                hello().build(), contract(), "node-1", 1, 1, 1,
                Duration.ofSeconds(-1), NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseDuration must be positive");
    }

    @Test
    void leaseEpochAndSeqPassThroughToFencingFields() {
        ProcessorAdvertisement advertisement = DelegationBridge.toProcessorAdvertisement(
                hello().build(), contract(), "node-1", 5, 9, 4, LEASE, NOW);

        assertThat(advertisement.getNodeEpoch()).isEqualTo(5);
        assertThat(advertisement.getLeaseEpoch()).isEqualTo(9);
        assertThat(advertisement.getSeq()).isEqualTo(4);
    }
}
