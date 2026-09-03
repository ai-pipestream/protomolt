package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.ClusterDirectory.ApplyOutcome;
import ai.protomolt.proto.mesh.cluster.ClusterFixtures.MutableClock;
import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEventType;
import ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
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
    void restartedNodeResetsPresenceAndFencesTheOldIncarnation() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 9).build());
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", 1).build());

        clock.advance(Duration.ofSeconds(5));
        NodeAdvertisement restarted = ClusterFixtures.nodeBuilder("node-1", 2, 1)
                .setAdvertisedAt(ClusterFixtures.ts(clock.instant()))
                .build();

        assertThat(directory.register(restarted)).isEqualTo(ApplyOutcome.UPDATED);
        assertThat(directory.processor("proc-1")).isEmpty();
        assertThat(directory.nodeCapacity("node-1")).isEmpty();
        assertThat(directory.presence("node-1").orElseThrow())
                .satisfies(p -> {
                    assertThat(p.getNodeEpoch()).isEqualTo(2);
                    assertThat(p.getHeartbeatSeq()).isEqualTo(1);
                    assertThat(p.getLastHeartbeatAt()).isEqualTo(restarted.getAdvertisedAt());
                });

        NodePresence stale = ClusterFixtures.presenceBuilder("node-1", 99).build();
        assertThatThrownBy(() -> directory.heartbeat(stale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("node_epoch 1")
                .hasMessageContaining("registered epoch is 2");
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
        // Registration alone. Presence is soft state, so neither the accepted heartbeat nor
        // the repeated one leaves anything in the log.
        assertThat(directory.events()).hasSize(1);
    }

    @Test
    void aLogWrittenBeforePresenceWentSoftStillReplays() {
        // Nothing emits a presence event any more, so this log cannot be produced by the
        // code under test. Every directory stored before the change is full of them, and
        // replaying one is the only thing standing between an upgrade and a coordinator that
        // refuses its own history, so the event has to be built by hand to keep the path
        // covered rather than merely present.
        directory.register(ClusterFixtures.node("node-1"));
        ClusterEvent registration = directory.events().get(0);
        NodePresence beat = ClusterFixtures.presenceBuilder("node-1", 5)
                .setState(PresenceState.PRESENCE_STATE_SUSPECT)
                .build();
        ClusterEvent legacyPresence = ClusterEvent.newBuilder()
                .setSeq(registration.getSeq() + 1)
                .setOccurredAt(registration.getOccurredAt())
                .setType(ClusterEventType.CLUSTER_EVENT_TYPE_PRESENCE_UPDATED)
                .setNodeId("node-1")
                .setPresence(beat)
                .build();

        ClusterDirectory restored = ClusterDirectory.replay(ClusterFixtures.cluster(),
                List.of(registration, legacyPresence), clock);

        assertThat(restored.presence("node-1").orElseThrow().getState())
                .isEqualTo(PresenceState.PRESENCE_STATE_SUSPECT);
        assertThat(restored.presence("node-1").orElseThrow().getHeartbeatSeq()).isEqualTo(5);
        // The stored sequence is preserved, so a fold of this log accounts for the presence
        // events it drops rather than renumbering around them.
        assertThat(restored.snapshot().getSnapshotSeq()).isEqualTo(legacyPresence.getSeq());
    }

    @Test
    void aHeartbeatEmitsNoEvent() {
        directory.register(ClusterFixtures.node("node-1"));
        int afterRegistration = directory.events().size();

        for (int beat = 2; beat <= 20; beat++) {
            assertThat(directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", beat)
                    .build())).isEqualTo(ApplyOutcome.UPDATED);
        }

        assertThat(directory.events()).hasSize(afterRegistration);
        assertThat(directory.presence("node-1").orElseThrow().getHeartbeatSeq()).isEqualTo(20);
    }

    @Test
    void staleHeartbeatConflicts() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 3).build());

        assertThatThrownBy(() -> directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 2)
                .setState(PresenceState.PRESENCE_STATE_SUSPECT)
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting update for presence for node 'node-1'");
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
    void expiredIdentitiesRequireAFresherIncarnationToRejoin() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1")
                .setLeaseExpiresAt(ClusterFixtures.ts(ClusterFixtures.T0.plusSeconds(15)))
                .build());

        clock.advance(Duration.ofSeconds(16));
        directory.sweep();
        assertThatThrownBy(() -> directory.registerProcessor(
                ClusterFixtures.processorBuilder("proc-1", "node-1").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not advance the registered epoch 1 seq 1");

        ProcessorAdvertisement reLeased = ClusterFixtures.processorBuilder("proc-1", "node-1")
                .setLeaseEpoch(2)
                .setSeq(1)
                .setAdvertisedAt(ClusterFixtures.ts(clock.instant()))
                .setLeaseExpiresAt(ClusterFixtures.ts(clock.instant().plusSeconds(60)))
                .build();
        assertThat(directory.registerProcessor(reLeased)).isEqualTo(ApplyOutcome.REGISTERED);

        clock.advance(Duration.ofSeconds(15));
        directory.sweep();
        assertThatThrownBy(() -> directory.register(ClusterFixtures.node("node-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not advance the registered epoch 1 seq 1");
        NodeAdvertisement restarted = ClusterFixtures.nodeBuilder("node-1", 2, 1)
                .setAdvertisedAt(ClusterFixtures.ts(clock.instant()))
                .build();
        assertThat(directory.register(restarted)).isEqualTo(ApplyOutcome.REGISTERED);
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
                .hasMessageContaining("conflicting update for capacity for node 'node-1'");
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
    void processorCapacityMustNameItsOwningNodeAndCurrentLease() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.register(ClusterFixtures.node("node-2"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());

        CapacityAdvertisement wrongOwner = ClusterFixtures.capacityBuilder("node-2", 1)
                .setProcessorId("proc-1")
                .build();
        assertThatThrownBy(() -> directory.updateCapacity(wrongOwner))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("belongs to 'node-1'");

        directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", 1)
                .setProcessorId("proc-1")
                .build());
        ProcessorAdvertisement moved = ClusterFixtures.processorBuilder("proc-1", "node-2")
                .setLeaseEpoch(2)
                .setSeq(1)
                .build();
        directory.registerProcessor(moved);
        assertThat(directory.processorCapacity("node-1", "proc-1")).isEmpty();

        CapacityAdvertisement staleLease = ClusterFixtures.capacityBuilder("node-2", 1)
                .setProcessorId("proc-1")
                .build();
        assertThatThrownBy(() -> directory.updateCapacity(staleLease))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source_epoch 1")
                .hasMessageContaining("current source epoch is 2");
    }

    @Test
    void eligibilityHonorsCapacityPayloadAndDrainingState() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1")
                .setMaxActiveSessions(2)
                .build());

        CapacityAdvertisement saturated = ClusterFixtures.capacityBuilder("node-1", 1)
                .setMaxInFlight(3)
                .setInFlight(3)
                .build();
        directory.updateCapacity(saturated);
        assertThat(directory.eligibleProcessors("", "", "")).isEmpty();

        directory.updateCapacity(saturated.toBuilder()
                .setSeq(2)
                .setInFlight(0)
                .setMaxActiveSessions(1)
                .setActiveSessions(0)
                .build());
        CapacityAdvertisement processorCapacity = ClusterFixtures.capacityBuilder("node-1", 1)
                .setProcessorId("proc-1")
                .setMaxActiveSessions(0)
                .setActiveSessions(2)
                .build();
        directory.updateCapacity(processorCapacity);
        assertThat(directory.eligibleProcessors("", "", "")).isEmpty();

        directory.updateCapacity(processorCapacity.toBuilder()
                .setSeq(2)
                .setInFlight(0)
                .setMaxActiveSessions(1)
                .setActiveSessions(0)
                .setMaxPayloadBytes(100)
                .build());
        assertThat(directory.eligibleProcessors("", "", "", 100)).hasSize(1);
        assertThat(directory.eligibleProcessors("", "", "", 101)).isEmpty();

        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 2)
                .setState(PresenceState.PRESENCE_STATE_DRAINING)
                .build());
        assertThat(directory.eligibleProcessors("", "", "", 100)).isEmpty();
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

    @Test
    void replayRestoresStateAndFencingTombstones() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", 1)
                .setProcessorId("proc-1")
                .build());
        clock.advance(Duration.ofSeconds(31));
        directory.sweep();

        List<ClusterEvent> log = directory.events();
        ClusterDirectory restored = ClusterDirectory.replay(
                ClusterFixtures.cluster(), log, clock);

        assertThat(restored.events()).containsExactlyElementsOf(log);
        assertThat(restored.snapshot().toByteArray()).isEqualTo(directory.snapshot().toByteArray());
        assertThat(restored.node("node-1")).isEmpty();
        assertThat(restored.processor("proc-1")).isEmpty();
        assertThatThrownBy(() -> restored.register(ClusterFixtures.node("node-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not advance the registered epoch 1 seq 1");

        NodeAdvertisement restarted = ClusterFixtures.nodeBuilder("node-1", 2, 1)
                .setAdvertisedAt(ClusterFixtures.ts(clock.instant()))
                .build();
        assertThat(restored.register(restarted)).isEqualTo(ApplyOutcome.REGISTERED);
    }

    @Test
    void replayRestoresCapacityAndArmsPresenceFromRegistration() {
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 2)
                .setState(PresenceState.PRESENCE_STATE_SUSPECT)
                .build());
        directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", 1).build());
        directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", 1)
                .setProcessorId("proc-1")
                .build());

        ClusterDirectory restored = ClusterDirectory.replay(
                ClusterFixtures.cluster(), directory.events(), clock);

        // Durable state comes back exactly. Presence does not, and must not: the heartbeat
        // that made this node SUSPECT was never written, so replay arms presence from the
        // registration instead. A node whose state matters restates it within one TTL, and
        // one that does not is swept, which is the behaviour a restart already relied on.
        assertThat(restored.nodeCapacity("node-1")).isPresent();
        assertThat(restored.processorCapacity("node-1", "proc-1")).isPresent();
        assertThat(directory.presence("node-1").orElseThrow().getState())
                .isEqualTo(PresenceState.PRESENCE_STATE_SUSPECT);
        assertThat(restored.presence("node-1").orElseThrow().getState())
                .isEqualTo(PresenceState.PRESENCE_STATE_ACTIVE);
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
