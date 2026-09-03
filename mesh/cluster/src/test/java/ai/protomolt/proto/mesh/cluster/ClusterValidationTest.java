package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEventType;
import ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.protomolt.proto.mesh.cluster.v1.Endpoint;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.TlsMode;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;

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
    void presenceExpiryMustExactlyMatchItsTtl() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.presenceBuilder("node-1", 1)
                        .setExpiresAt(ClusterFixtures.ts(ClusterFixtures.T0.plusSeconds(31)))
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("presence-expiry-matches-ttl");
    }

    @Test
    void invalidWellKnownDurationsAreRejectedBeforeCel() {
        Duration invalid = Duration.newBuilder().setSeconds(315_576_000_001L).build();

        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.nodeBuilder("node-1", 1, 1).setTtl(invalid).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertisement.ttl must be a valid protobuf Duration");
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.presenceBuilder("node-1", 1).setTtl(invalid).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("presence.ttl must be a valid protobuf Duration");
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .setSupportsSessionResume(true)
                        .setMaxDisconnectGrace(invalid)
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max_disconnect_grace must be a valid protobuf Duration");
    }

    @Test
    void processorMetadataMustBeInternallyConsistent() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .setModel("gpt-example")
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("model-requires-provider");
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .setSupportsSessionResume(true)
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resume-support-has-grace");
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .addCapabilityDetails(
                                ai.protomolt.proto.mesh.cluster.v1.CapabilityDescription
                                        .newBuilder().setName("llm-edit"))
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capabilities does not contain it");
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
                .hasMessageContaining("event-subject-matches-detail");
    }

    @Test
    void eventTypeAndDetailMustAgree() {
        ClusterEvent wrongType = ClusterEvent.newBuilder()
                .setSeq(1)
                .setOccurredAt(ClusterFixtures.ts(ClusterFixtures.T0))
                .setType(ClusterEventType.CLUSTER_EVENT_TYPE_CAPACITY_UPDATED)
                .setNodeId("node-1")
                .setNode(ClusterFixtures.node("node-1"))
                .build();

        assertThatThrownBy(() -> ClusterValidation.validate(wrongType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("event-type-matches-detail");
    }

    @Test
    void eventLogMustBeGapFreeAndChronological() {
        ClusterEvent first = nodeEvent(1, ClusterFixtures.T0);
        ClusterEvent gap = nodeEvent(3, ClusterFixtures.T0.plusSeconds(1));
        assertThatThrownBy(() -> ClusterValidation.validateEventLog(List.of(first, gap)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("event.seq 3 does not match expected 2");

        ClusterEvent backwards = nodeEvent(2, ClusterFixtures.T0.minusSeconds(1));
        assertThatThrownBy(() -> ClusterValidation.validateEventLog(List.of(first, backwards)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("occurred_at moves backward at seq 2");
    }

    @Test
    void snapshotRejectsUnorderedProcessorsAndCrossRecordEpochs() {
        ClusterDirectory directory = new ClusterDirectory(ClusterFixtures.cluster(),
                Clock.fixed(ClusterFixtures.T0, ZoneOffset.UTC));
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-a", "node-1").build());
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-b", "node-1").build());
        ClusterSnapshot valid = directory.snapshot();

        ClusterSnapshot.Builder unordered = valid.toBuilder()
                .clearProcessors()
                .addProcessors(valid.getProcessors(1))
                .addProcessors(valid.getProcessors(0));
        assertThatThrownBy(() -> ClusterValidation.validate(sign(unordered)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly ordered by processor_id");

        ClusterSnapshot.Builder staleEpoch = valid.toBuilder()
                .setProcessors(0, valid.getProcessors(0).toBuilder().setNodeEpoch(2));
        assertThatThrownBy(() -> ClusterValidation.validate(sign(staleEpoch)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stale node_epoch");
    }

    @Test
    void fieldBoundsAreEnforced() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.nodeBuilder("node 1", 1, 1).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.nodeBuilder("node-1", 1, 1)
                        .clearEndpoints()
                        .addEndpoints(ai.protomolt.proto.mesh.cluster.v1.Endpoint.newBuilder()
                                .setEndpointId("grpc-main")
                                .setAddress("not an address")
                                .setTlsMode(ai.protomolt.proto.mesh.cluster.v1.TlsMode.TLS_MODE_SYSTEM))
                        .build()))
                .isInstanceOf(IllegalArgumentException.class);
        // An authority-shaped address with a bad port refuses through the named
        // format and never falls back to the URI reading.
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.nodeBuilder("node-1", 1, 1)
                        .clearEndpoints()
                        .addEndpoints(Endpoint.newBuilder()
                                .setEndpointId("grpc-main")
                                .setAddress("node.example:99999")
                                .setTlsMode(TlsMode.TLS_MODE_SYSTEM))
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("string.endpoint_address");
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .setKind(ai.protomolt.proto.mesh.v1.ProcessorKind.PROCESSOR_KIND_UNSPECIFIED)
                        .build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void identifiersFollowTheAgreedSlugContract() {
        // The historical pattern allowed uppercase and doubled separators;
        // the agreed slug contract does not, and this pins the tightening.
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.nodeBuilder("Node-1", 1, 1).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slug");
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.nodeBuilder("node--1", 1, 1).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slug");
    }

    @Test
    void optionalIdentifiersAcceptEmptyAndStillRefuseNonSlugs() {
        // trust_domain carries the optional idiom: the empty string declares
        // absence and skips the format, a present value still has to be a slug.
        ClusterDescriptor.Builder untrusted = ClusterFixtures.clusterBuilder().setTrustDomain("");
        assertThatCode(() -> ClusterValidation.validate(sign(untrusted)))
                .doesNotThrowAnyException();

        ClusterDescriptor.Builder shouty =
                ClusterFixtures.clusterBuilder().setTrustDomain("Trust--Domain");
        assertThatThrownBy(() -> ClusterValidation.validate(sign(shouty)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slug");
    }

    private static ClusterEvent nodeEvent(long seq, java.time.Instant occurredAt) {
        return ClusterEvent.newBuilder()
                .setSeq(seq)
                .setOccurredAt(ClusterFixtures.ts(occurredAt))
                .setType(ClusterEventType.CLUSTER_EVENT_TYPE_NODE_REGISTERED)
                .setNodeId("node-1")
                .setNode(ClusterFixtures.node("node-1"))
                .build();
    }

    private static ClusterDescriptor sign(ClusterDescriptor.Builder builder) {
        ClusterDescriptor unsigned = builder.clearFingerprint().build();
        return unsigned.toBuilder()
                .setFingerprint(ClusterValidation.descriptorFingerprint(unsigned))
                .build();
    }

    private static ClusterSnapshot sign(ClusterSnapshot.Builder builder) {
        ClusterSnapshot unsigned = builder.clearFingerprint().build();
        return unsigned.toBuilder()
                .setFingerprint(ClusterValidation.snapshotFingerprint(unsigned))
                .build();
    }
}
