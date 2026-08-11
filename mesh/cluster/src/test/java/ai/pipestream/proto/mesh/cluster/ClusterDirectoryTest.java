package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.ClusterDirectory.ApplyOutcome;
import ai.pipestream.proto.mesh.cluster.ClusterFixtures.MutableClock;
import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEventType;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.PresenceState;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Directory semantics: registration, idempotent refresh, conflicting updates, TTL expiry with
 * a fake clock, deterministic snapshots, capacity updates, and eligibility queries.
 */
class ClusterDirectoryTest {

    private MutableClock clock;
    private ClusterDirectory directory;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(ClusterFixtures.T0);
        directory = new ClusterDirectory(ClusterFixtures.cluster(), clock);
    }

    @Test
    void registrationArmsPresenceAndEmitsEvent() {
        ApplyOutcome outcome = directory.register(ClusterFixtures.node("node-1"));

        assertThat(outcome).isEqualTo(ApplyOutcome.REGISTERED);
        assertThat(directory.node("node-1")).isPresent();
        NodePresence presence = directory.presence("node-1").orElseThrow();
        assertThat(presence.getState()).isEqualTo(PresenceState.PRESENCE_STATE_ACTIVE);
        assertThat(presence.getExpiresAt())
                .isEqualTo(ClusterFixtures.ts(ClusterFixtures.T0.plusSeconds(30)));
        assertThat(directory.events()).hasSize(1);
        ClusterEvent event = directory.events().get(0);
        assertThat(event.getType()).isEqualTo(ClusterEventType.CLUSTER_EVENT_TYPE_NODE_REGISTERED);
        assertThat(event.getSeq()).isEqualTo(1);
        assertThat(event.getNode().getNodeId()).isEqualTo("node-1");
        ClusterValidation.validate(event);
    }

    @Test
    void identicalRefreshIsANoOp() {
        directory.register(ClusterFixtures.node("node-1"));

        assertThat(directory.register(ClusterFixtures.node("node-1")))
                .isEqualTo(ApplyOutcome.UNCHANGED);
        assertThat(directory.events()).hasSize(1);
    }

    @Test
    void changedAdvertisementWithNewerPositionUpdates() {
        directory.register(ClusterFixtures.node("node-1"));
        NodeAdvertisement changed = ClusterFixtures.nodeBuilder("node-1", 1, 2)
                .addCapabilities("reflection")
                .build();

        assertThat(directory.register(changed)).isEqualTo(ApplyOutcome.UPDATED);
        assertThat(directory.node("node-1").orElseThrow().getCapabilitiesList())
                .containsExactly("relay", "reflection");
        assertThat(directory.events()).hasSize(2);
    }

    @Test
    void newerEpochWithLowerSeqStillUpdates() {
        directory.register(ClusterFixtures.node("node-1"));
        NodeAdvertisement rejoined = ClusterFixtures.nodeBuilder("node-1", 2, 1)
                .clearEndpoints()
                .build();

        assertThat(directory.register(rejoined)).isEqualTo(ApplyOutcome.UPDATED);
        assertThat(directory.node("node-1").orElseThrow().getEpoch()).isEqualTo(2);
    }

    @Test
    void changedAdvertisementWithStaleSeqConflicts() {
        directory.register(ClusterFixtures.nodeBuilder("node-1", 1, 2).build());
        NodeAdvertisement stale = ClusterFixtures.nodeBuilder("node-1", 1, 1)
                .clearEndpoints()
                .build();

        assertThatThrownBy(() -> directory.register(stale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting update for node 'node-1'")
                .hasMessageContaining("epoch 1 seq 1")
                .hasMessageContaining("epoch 1 seq 2");
    }

    @Test
    void changedAdvertisementWithOlderEpochConflicts() {
        directory.register(ClusterFixtures.nodeBuilder("node-1", 2, 1).build());
        NodeAdvertisement stale = ClusterFixtures.nodeBuilder("node-1", 1, 9)
                .clearEndpoints()
                .build();

        assertThatThrownBy(() -> directory.register(stale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting update for node 'node-1'");
    }

    @Test
    void advertisementForAnotherClusterIsRejected() {
        NodeAdvertisement wrong = ClusterFixtures.nodeBuilder("node-1", 1, 1)
                .setClusterId("cluster-b")
                .build();

        assertThatThrownBy(() -> directory.register(wrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster-b")
                .hasMessageContaining("cluster-a");
    }

    @Test
    void heartbeatExtendsLivenessAcrossTheOriginalExpiry() {
        directory.register(ClusterFixtures.node("node-1"));
        clock.advance(Duration.ofSeconds(20));
        NodePresence beat = ClusterFixtures.presenceBuilder("node-1", 2)
                .setLastHeartbeatAt(ClusterFixtures.ts(clock.instant()))
                .setExpiresAt(ClusterFixtures.ts(clock.instant().plusSeconds(30)))
                .build();

        assertThat(directory.heartbeat(beat)).isEqualTo(ApplyOutcome.UPDATED);

        clock.advance(Duration.ofSeconds(25));
        assertThat(directory.sweep()).isEmpty();
        assertThat(directory.node("node-1")).isPresent();
    }

    @Test
    void identicalHeartbeatIsANoOp() {
        directory.register(ClusterFixtures.node("node-1"));
        NodePresence beat = ClusterFixtures.presenceBuilder("node-1", 2).build();
        directory.heartbeat(beat);

        assertThat(directory.heartbeat(beat)).isEqualTo(ApplyOutcome.UNCHANGED);
        assertThat(directory.events()).hasSize(2);
    }

    @Test
    void staleHeartbeatConflicts() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 3).build());

        assertThatThrownBy(() -> directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 2)
                .setState(PresenceState.PRESENCE_STATE_SUSPECT)
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting presence for node 'node-1'");
    }

    @Test
    void heartbeatForUnregisteredNodeIsRejected() {
        assertThatThrownBy(() -> directory.heartbeat(ClusterFixtures.presenceBuilder("ghost", 1)
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unregistered node 'ghost'");
    }

    @Test
    void sweepRemovesExpiredNodeAndCascadesItsProcessors() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-2", "node-1").build());

        clock.advance(Duration.ofSeconds(31));
        List<ClusterEvent> swept = directory.sweep();

        assertThat(swept).extracting(ClusterEvent::getType)
                .containsExactly(
                        ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED,
                        ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED,
                        ClusterEventType.CLUSTER_EVENT_TYPE_NODE_EXPIRED);
        assertThat(directory.node("node-1")).isEmpty();
        assertThat(directory.presence("node-1")).isEmpty();
        assertThat(directory.processor("proc-1")).isEmpty();
        assertThat(directory.processor("proc-2")).isEmpty();
    }

    @Test
    void sweepRemovesAnExpiredProcessorWithoutTouchingItsNode() {
        directory.register(ClusterFixtures.node("node-1"));
        // A 15-second lease lapses well inside the node's 30-second presence window.
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1")
                .setLeaseExpiresAt(ClusterFixtures.ts(ClusterFixtures.T0.plusSeconds(15)))
                .build());

        clock.advance(Duration.ofSeconds(16));
        List<ClusterEvent> swept = directory.sweep();
        assertThat(swept).extracting(ClusterEvent::getType)
                .containsExactly(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED);
        assertThat(directory.node("node-1")).isPresent();
        assertThat(directory.processor("proc-1")).isEmpty();
    }

    @Test
    void goneNodeIsSweptBeforeItsExpiry() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 2)
                .setState(PresenceState.PRESENCE_STATE_GONE)
                .build());

        List<ClusterEvent> swept = directory.sweep();
        assertThat(swept).extracting(ClusterEvent::getType)
                .containsExactly(ClusterEventType.CLUSTER_EVENT_TYPE_NODE_EXPIRED);
        assertThat(directory.node("node-1")).isEmpty();
    }

    @Test
    void sweepOnALiveDirectoryIsQuiet() {
        directory.register(ClusterFixtures.node("node-1"));
        assertThat(directory.sweep()).isEmpty();
        assertThat(directory.events()).hasSize(1);
    }

    @Test
    void processorRegistrationRequiresARegisteredNode() {
        assertThatThrownBy(() -> directory.registerProcessor(
                ClusterFixtures.processorBuilder("proc-1", "ghost").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unregistered node 'ghost'");
    }

    @Test
    void processorRefreshFollowsTheSameRules() {
        directory.register(ClusterFixtures.node("node-1"));
        ProcessorAdvertisement initial = ClusterFixtures.processorBuilder("proc-1", "node-1")
                .build();
        directory.registerProcessor(initial);

        assertThat(directory.registerProcessor(initial)).isEqualTo(ApplyOutcome.UNCHANGED);

        ProcessorAdvertisement renewed = initial.toBuilder()
                .setSeq(2)
                .setLeaseExpiresAt(ClusterFixtures.ts(ClusterFixtures.T0.plusSeconds(120)))
                .build();
        assertThat(directory.registerProcessor(renewed)).isEqualTo(ApplyOutcome.UPDATED);

        ProcessorAdvertisement stale = initial.toBuilder().clearCapabilities().build();
        assertThatThrownBy(() -> directory.registerProcessor(stale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting update for processor 'proc-1'");

        ProcessorAdvertisement newEpoch = renewed.toBuilder()
                .setLeaseEpoch(2)
                .setSeq(1)
                .clearCapabilities()
                .addCapabilities("llm-edit")
                .build();
        assertThat(directory.registerProcessor(newEpoch)).isEqualTo(ApplyOutcome.UPDATED);
        assertThat(directory.processor("proc-1").orElseThrow().getCapabilitiesList())
                .containsExactly("llm-edit");
    }

    @Test
    void capacityUpdatesFollowSequencePerKey() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        CapacityAdvertisement nodeWide = ClusterFixtures.capacityBuilder("node-1", 1).build();
        CapacityAdvertisement perProcessor = ClusterFixtures.capacityBuilder("node-1", 1)
                .setProcessorId("proc-1")
                .build();

        assertThat(directory.updateCapacity(nodeWide)).isEqualTo(ApplyOutcome.REGISTERED);
        assertThat(directory.updateCapacity(perProcessor)).isEqualTo(ApplyOutcome.REGISTERED);
        assertThat(directory.updateCapacity(nodeWide)).isEqualTo(ApplyOutcome.UNCHANGED);
        assertThat(directory.nodeCapacity("node-1")).isPresent();
        assertThat(directory.processorCapacity("node-1", "proc-1")).isPresent();

        CapacityAdvertisement newer = nodeWide.toBuilder().setSeq(2).setInFlight(7).build();
        assertThat(directory.updateCapacity(newer)).isEqualTo(ApplyOutcome.UPDATED);
        assertThat(directory.nodeCapacity("node-1").orElseThrow().getInFlight()).isEqualTo(7);

        CapacityAdvertisement stale = nodeWide.toBuilder().setInFlight(9).build();
        assertThatThrownBy(() -> directory.updateCapacity(stale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting capacity for node 'node-1'");
    }

    @Test
    void capacityRequiresRegisteredTargets() {
        assertThatThrownBy(() -> directory.updateCapacity(
                ClusterFixtures.capacityBuilder("ghost", 1).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unregistered node 'ghost'");

        directory.register(ClusterFixtures.node("node-1"));
        assertThatThrownBy(() -> directory.updateCapacity(
                ClusterFixtures.capacityBuilder("node-1", 1).setProcessorId("ghost").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unregistered processor 'ghost'");
    }

    @Test
    void eligibilityMatchesSchemaAndCapability() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());

        assertThat(directory.eligibleProcessors(
                ClusterFixtures.SCHEMA_TYPE, ClusterFixtures.SCHEMA_FINGERPRINT, "llm-generate"))
                .extracting(ProcessorAdvertisement::getProcessorId)
                .containsExactly("proc-1");
        // An empty fingerprint accepts any fingerprint for the named type.
        assertThat(directory.eligibleProcessors(ClusterFixtures.SCHEMA_TYPE, "", "llm-generate"))
                .hasSize(1);
    }

    @Test
    void eligibilityRejectsCapabilityAndFingerprintMismatch() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());

        assertThat(directory.eligibleProcessors(
                ClusterFixtures.SCHEMA_TYPE, ClusterFixtures.SCHEMA_FINGERPRINT, "opennlp-pii"))
                .isEmpty();
        assertThat(directory.eligibleProcessors(
                ClusterFixtures.SCHEMA_TYPE, ClusterFixtures.OTHER_FINGERPRINT, "llm-generate"))
                .isEmpty();
        assertThat(directory.eligibleProcessors(
                "acme.docs.v1.Other", ClusterFixtures.SCHEMA_FINGERPRINT, "llm-generate"))
                .isEmpty();
    }

    @Test
    void eligibilitySkipsExpiredLeasesAndUnhealthyNodes() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        directory.register(ClusterFixtures.node("node-2"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-2", "node-2").build());

        // proc-1's node goes suspect: still registered, but not serving.
        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 2)
                .setState(PresenceState.PRESENCE_STATE_SUSPECT)
                .build());
        assertThat(directory.eligibleProcessors(
                ClusterFixtures.SCHEMA_TYPE, ClusterFixtures.SCHEMA_FINGERPRINT, "llm-generate"))
                .extracting(ProcessorAdvertisement::getProcessorId)
                .containsExactly("proc-2");

        // Past every lease expiry: nothing is eligible.
        clock.advance(Duration.ofSeconds(61));
        assertThat(directory.eligibleProcessors(
                ClusterFixtures.SCHEMA_TYPE, ClusterFixtures.SCHEMA_FINGERPRINT, "llm-generate"))
                .isEmpty();
    }

    @Test
    void eligibilityOrdersByProcessorId() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-z", "node-1").build());
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-a", "node-1").build());
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-m", "node-1").build());

        assertThat(directory.eligibleProcessors("", "", ""))
                .extracting(ProcessorAdvertisement::getProcessorId)
                .containsExactly("proc-a", "proc-m", "proc-z");
    }

    @Test
    void snapshotsAreDeterministicAcrossRuns() {
        ClusterSnapshot first = populatedSnapshot();
        ClusterSnapshot second = populatedSnapshot();

        assertThat(second.toByteArray()).isEqualTo(first.toByteArray());
        assertThat(second.getFingerprint()).isEqualTo(first.getFingerprint());
        ClusterValidation.validate(first);
    }

    @Test
    void snapshotFingerprintTracksContent() {
        ClusterSnapshot first = populatedSnapshot();
        directory.register(ClusterFixtures.node("node-1"));
        ClusterSnapshot afterJoin = directory.snapshot();

        assertThat(afterJoin.getFingerprint()).isNotEqualTo(first.getFingerprint());
        assertThat(afterJoin.getSnapshotSeq()).isEqualTo(1);
        assertThat(afterJoin.getNodesList()).extracting(n -> n.getAdvertisement().getNodeId())
                .containsExactly("node-1");
    }

    private ClusterSnapshot populatedSnapshot() {
        MutableClock fixed = new MutableClock(ClusterFixtures.T0);
        ClusterDirectory dir = new ClusterDirectory(ClusterFixtures.cluster(), fixed);
        dir.register(ClusterFixtures.node("node-b"));
        dir.register(ClusterFixtures.node("node-a"));
        dir.registerProcessor(ClusterFixtures.processorBuilder("proc-2", "node-a").build());
        dir.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-b").build());
        dir.updateCapacity(ClusterFixtures.capacityBuilder("node-a", 1).build());
        dir.updateCapacity(ClusterFixtures.capacityBuilder("node-b", 1)
                .setProcessorId("proc-1")
                .build());
        return dir.snapshot();
    }
}
