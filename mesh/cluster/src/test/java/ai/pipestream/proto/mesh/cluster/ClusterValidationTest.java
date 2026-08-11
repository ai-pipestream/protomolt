package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEventType;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The fail-fast validation layer: canonical fingerprint agreement, the message-level CEL
 * rules, and the field bounds on the cluster contract.
 */
class ClusterValidationTest {

    @Test
    void validFixturesPass() {
        assertThatCode(() -> ClusterValidation.validate(ClusterFixtures.cluster()))
                .doesNotThrowAnyException();
        assertThatCode(() -> ClusterValidation.validate(ClusterFixtures.node("node-1")))
                .doesNotThrowAnyException();
        assertThatCode(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1").build()))
                .doesNotThrowAnyException();
        assertThatCode(() -> ClusterValidation.validate(
                ClusterFixtures.presenceBuilder("node-1", 1).build()))
                .doesNotThrowAnyException();
        assertThatCode(() -> ClusterValidation.validate(
                ClusterFixtures.capacityBuilder("node-1", 1).build()))
                .doesNotThrowAnyException();
    }

    @Test
    void clusterFingerprintMismatchIsRejected() {
        ClusterDescriptor forged = ClusterFixtures.cluster().toBuilder()
                .setFingerprint("ef".repeat(32))
                .build();

        assertThatThrownBy(() -> ClusterValidation.validate(forged))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.fingerprint")
                .hasMessageContaining("canonical fingerprint");
    }

    @Test
    void recomputedFingerprintIsStableAcrossSerialization() {
        ClusterDescriptor cluster = ClusterFixtures.cluster();
        ClusterDescriptor roundTripped;
        try {
            roundTripped = ClusterDescriptor.parseFrom(cluster.toByteArray());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        org.assertj.core.api.Assertions.assertThat(
                ClusterValidation.descriptorFingerprint(roundTripped))
                .isEqualTo(cluster.getFingerprint());
    }

    @Test
    void leaseExpiryMustFollowAdvertisementTime() {
        ProcessorAdvertisement inverted = ClusterFixtures.processorBuilder("proc-1", "node-1")
                .setLeaseExpiresAt(ClusterFixtures.ts(ClusterFixtures.T0.minusSeconds(1)))
                .build();

        assertThatThrownBy(() -> ClusterValidation.validate(inverted))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lease-outlives-advertisement");
    }

    @Test
    void inFlightMustFitInsideTheDeclaredLimit() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.capacityBuilder("node-1", 1).setInFlight(17).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("in-flight-within-limit");
        // A zero limit declares no limit, so any in-flight value validates.
        assertThatCode(() -> ClusterValidation.validate(
                ClusterFixtures.capacityBuilder("node-1", 1)
                        .setMaxInFlight(0)
                        .setInFlight(10_000)
                        .build()))
                .doesNotThrowAnyException();
    }

    @Test
    void presenceExpiryMustFollowTheLastHeartbeat() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.presenceBuilder("node-1", 1)
                        .setExpiresAt(ClusterFixtures.ts(ClusterFixtures.T0.minusSeconds(1)))
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("presence-expiry-after-heartbeat");
    }

    @Test
    void processorEventMustNameItsProcessor() {
        ClusterEvent unnamed = ClusterEvent.newBuilder()
                .setSeq(1)
                .setOccurredAt(ClusterFixtures.ts(ClusterFixtures.T0))
                .setType(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED)
                .setNodeId("node-1")
                .setProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build())
                .build();

        assertThatThrownBy(() -> ClusterValidation.validate(unnamed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("processor-events-name-processor");
    }

    @Test
    void fieldBoundsAreEnforced() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.nodeBuilder("node 1", 1, 1).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.nodeBuilder("node-1", 1, 1)
                        .clearEndpoints()
                        .addEndpoints(ai.pipestream.proto.mesh.cluster.v1.Endpoint.newBuilder()
                                .setEndpointId("grpc-main")
                                .setAddress("not an address")
                                .setTlsMode(ai.pipestream.proto.mesh.cluster.v1.TlsMode.TLS_MODE_SYSTEM))
                        .build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .setKind(ai.pipestream.proto.mesh.v1.ProcessorKind.PROCESSOR_KIND_UNSPECIFIED)
                        .build()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
