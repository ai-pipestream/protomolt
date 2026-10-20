package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryCheckpoint;
import ai.protomolt.proto.mesh.cluster.v1.FencingPosition;
import ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.NodeRecord;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The replay driver of the directory state machine: folds a durable event log, or a
 * checkpoint plus the events after it, back into a {@link ClusterDirectoryState} and returns
 * the directory over it. Replay emits no events and preserves the recorded sequences and
 * timestamps; the live driver ({@link ClusterDirectory}) is the only path that appends.
 *
 * <p>Replay and live apply share every gate in {@link ClusterTransitions}, with the two
 * deliberate tolerances the durable record requires: an expiry event may name a soft lease
 * renewal the log never recorded (same epoch, sequence at or past the registered one, nothing
 * else moved), and a heartbeated presence is rebuilt from the registration because heartbeats
 * are soft state. Everything else — fencing, epoch comparison, event ordering, registration,
 * expiry, and the persistent formats — is identical to the live path.
 */
final class ClusterEventReplay {

    private ClusterEventReplay() {
    }

    /**
     * Restores a directory's state from its complete event log. The log is validated before
     * any event is applied, the original event sequence and timestamps are preserved, and
     * fencing tombstones are rebuilt for expired nodes and processors, so the returned
     * directory can immediately accept new advertisements without admitting frames from an
     * incarnation that the log already superseded.
     */
    static ClusterDirectory replay(ClusterDescriptor cluster, List<ClusterEvent> eventLog,
            Clock clock) {
        ClusterValidation.validateEventLog(eventLog);
        ClusterValidation.validate(cluster);
        ClusterDirectoryState state = new ClusterDirectoryState();
        for (ClusterEvent event : eventLog) {
            apply(cluster, state, event);
            state.events.add(event);
            state.eventSeq = event.getSeq();
        }
        return new ClusterDirectory(cluster, clock, state);
    }

    /**
     * Restores a directory's state from a checkpoint and the events recorded after it. The
     * checkpoint stands in for every event up to its {@code snapshot_seq}, so the tail must
     * begin at the next sequence and is validated against that position rather than against
     * one. The result is exactly the state the complete log would have produced, fencing
     * tombstones included.
     */
    static ClusterDirectory restore(ClusterDescriptor cluster, DirectoryCheckpoint checkpoint,
            List<ClusterEvent> tail, Clock clock) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        ClusterSnapshot snapshot = checkpoint.getState();
        ClusterTransitions.require(snapshot.getCluster().equals(cluster),
                "checkpoint describes a different cluster identity than the directory");
        ClusterTransitions.require(snapshot.getFingerprint().equals(
                ClusterValidation.snapshotFingerprint(
                        snapshot.toBuilder().clearFingerprint().build())),
                "checkpoint state fingerprint does not commit to the checkpoint state");

        ClusterValidation.validate(cluster);
        ClusterDirectoryState state = new ClusterDirectoryState();
        for (NodeRecord record : snapshot.getNodesList()) {
            String nodeId = record.getAdvertisement().getNodeId();
            state.nodes.put(nodeId, record.getAdvertisement());
            state.presence.put(nodeId, record.getPresence());
            if (record.hasCapacity()) {
                state.nodeCapacity.put(nodeId, record.getCapacity());
            }
        }
        for (ProcessorAdvertisement processor : snapshot.getProcessorsList()) {
            state.processors.put(processor.getProcessorId(), processor);
        }
        for (CapacityAdvertisement capacity : snapshot.getCapacitiesList()) {
            state.processorCapacity.put(
                    ClusterDirectoryState.capacityKey(capacity.getNodeId(),
                            capacity.getProcessorId()), capacity);
        }
        for (FencingPosition position : checkpoint.getNodePositionsList()) {
            state.nodePositions.put(position.getId(),
                    new ClusterDirectoryState.Position(position.getEpoch(), position.getSeq()));
        }
        for (FencingPosition position : checkpoint.getProcessorPositionsList()) {
            state.processorPositions.put(position.getId(),
                    new ClusterDirectoryState.Position(position.getEpoch(), position.getSeq()));
        }
        state.eventSeq = snapshot.getSnapshotSeq();

        ClusterValidation.validateEventLog(tail, snapshot.getSnapshotSeq() + 1);
        for (ClusterEvent event : tail) {
            apply(cluster, state, event);
            state.events.add(event);
            state.eventSeq = event.getSeq();
        }
        return new ClusterDirectory(cluster, clock, state);
    }

    /** Folds one recorded event into the state, dispatching on its type. */
    private static void apply(ClusterDescriptor cluster, ClusterDirectoryState state,
            ClusterEvent event) {
        switch (event.getType()) {
            case CLUSTER_EVENT_TYPE_NODE_REGISTERED ->
                    nodeRegistration(cluster, state, event.getNode());
            case CLUSTER_EVENT_TYPE_NODE_EXPIRED -> nodeExpiry(state, event.getNode());
            case CLUSTER_EVENT_TYPE_PROCESSOR_REGISTERED ->
                    processorRegistration(state, event.getProcessor());
            case CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED ->
                    processorExpiry(state, event.getProcessor());
            case CLUSTER_EVENT_TYPE_PRESENCE_UPDATED -> presence(cluster, state,
                    event.getPresence());
            case CLUSTER_EVENT_TYPE_CAPACITY_UPDATED -> capacity(state, event.getCapacity());
            case CLUSTER_EVENT_TYPE_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("unsupported cluster event type at seq "
                            + event.getSeq());
        }
    }

    private static void nodeRegistration(ClusterDescriptor cluster, ClusterDirectoryState state,
            NodeAdvertisement advertisement) {
        ClusterTransitions.requireCluster(cluster, advertisement.getClusterId(),
                "replayed advertisement");
        String nodeId = advertisement.getNodeId();
        NodeAdvertisement existing = state.nodes.get(nodeId);
        if (existing == null) {
            ClusterTransitions.requireNewerThanTombstone(state.nodePositions.get(nodeId),
                    advertisement.getEpoch(), advertisement.getSeq(), "node '" + nodeId + "'");
        } else {
            ClusterTransitions.requireFresher(advertisement.getEpoch(), advertisement.getSeq(),
                    existing.getEpoch(), existing.getSeq(), "node '" + nodeId + "'");
            if (advertisement.getEpoch() > existing.getEpoch()) {
                ClusterTransitions.require(state.processors.values().stream()
                                .noneMatch(processor -> processor.getNodeId().equals(nodeId)),
                        "node '" + nodeId
                                + "' advanced epoch before its processors expired");
                state.nodeCapacity.remove(nodeId);
            }
        }
        boolean newEpoch = existing != null && advertisement.getEpoch() > existing.getEpoch();
        state.nodes.put(nodeId, advertisement);
        state.nodePositions.put(nodeId, new ClusterDirectoryState.Position(
                advertisement.getEpoch(), advertisement.getSeq()));
        state.presence.put(nodeId, existing == null || newEpoch
                ? ClusterTransitions.initialPresence(advertisement)
                : ClusterTransitions.mergePresence(state.presence.get(nodeId), advertisement));
    }

    /**
     * Replays a node expiry. The expiry names the advertisement the directory held when it
     * swept the node, and lease refreshes are soft state, so that advertisement may be a
     * renewal the log never saw: same epoch, a sequence at or past the recorded one, and
     * nothing else moved. The replay accepts exactly that and nothing looser, and it carries
     * the expiry's position into the fencing tombstone so a restarted directory fences the
     * dead incarnation from the same sequence the live one did.
     */
    private static void nodeExpiry(ClusterDirectoryState state, NodeAdvertisement advertisement) {
        String nodeId = advertisement.getNodeId();
        NodeAdvertisement registered = state.nodes.get(nodeId);
        ClusterTransitions.require(registered != null && (registered.equals(advertisement)
                        || (advertisement.getEpoch() == registered.getEpoch()
                        && advertisement.getSeq() >= registered.getSeq()
                        && ClusterTransitions.refreshOnly(registered, advertisement))),
                "node expiry at replay does not match registered node '" + nodeId + "'");
        ClusterTransitions.require(state.processors.values().stream()
                        .noneMatch(processor -> processor.getNodeId().equals(nodeId)),
                "node '" + nodeId + "' expired before its processors");
        state.nodes.remove(nodeId);
        state.presence.remove(nodeId);
        state.nodeCapacity.remove(nodeId);
        state.nodePositions.put(nodeId, new ClusterDirectoryState.Position(
                advertisement.getEpoch(), advertisement.getSeq()));
    }

    private static void processorRegistration(ClusterDirectoryState state,
            ProcessorAdvertisement advertisement) {
        String processorId = advertisement.getProcessorId();
        NodeAdvertisement node = state.nodes.get(advertisement.getNodeId());
        ClusterTransitions.require(node != null,
                "replayed processor '" + processorId + "' names unregistered node '"
                        + advertisement.getNodeId() + "'");
        ClusterTransitions.require(advertisement.getNodeEpoch() == node.getEpoch(),
                "replayed processor '" + processorId + "' carries stale node_epoch");
        ProcessorAdvertisement existing = state.processors.get(processorId);
        if (existing == null) {
            ClusterTransitions.requireNewerThanTombstone(state.processorPositions.get(processorId),
                    advertisement.getLeaseEpoch(), advertisement.getSeq(),
                    "processor '" + processorId + "'");
        } else {
            ClusterTransitions.requireFresher(advertisement.getLeaseEpoch(),
                    advertisement.getSeq(), existing.getLeaseEpoch(), existing.getSeq(),
                    "processor '" + processorId + "'");
            ClusterTransitions.require(existing.getNodeId().equals(advertisement.getNodeId())
                            || advertisement.getLeaseEpoch() > existing.getLeaseEpoch(),
                    "processor '" + processorId
                            + "' moved nodes without a newer lease_epoch");
            if (!existing.getNodeId().equals(advertisement.getNodeId())
                    || existing.getLeaseEpoch() != advertisement.getLeaseEpoch()) {
                state.processorCapacity.remove(ClusterDirectoryState.capacityKey(
                        existing.getNodeId(), processorId));
            }
        }
        state.processors.put(processorId, advertisement);
        state.processorPositions.put(processorId, new ClusterDirectoryState.Position(
                advertisement.getLeaseEpoch(), advertisement.getSeq()));
    }

    /**
     * Replays a processor expiry under the same rule as {@link #nodeExpiry}: the expired
     * advertisement may be a soft lease renewal of the registered one, and the tombstone takes
     * the expiry's position.
     */
    private static void processorExpiry(ClusterDirectoryState state,
            ProcessorAdvertisement advertisement) {
        String processorId = advertisement.getProcessorId();
        ProcessorAdvertisement registered = state.processors.get(processorId);
        ClusterTransitions.require(registered != null && (registered.equals(advertisement)
                        || (advertisement.getLeaseEpoch() == registered.getLeaseEpoch()
                        && advertisement.getSeq() >= registered.getSeq()
                        && ClusterTransitions.refreshOnly(registered, advertisement))),
                "processor expiry at replay does not match registered processor '"
                        + processorId + "'");
        state.processors.remove(processorId);
        state.processorCapacity.remove(ClusterDirectoryState.capacityKey(
                advertisement.getNodeId(), processorId));
        state.processorPositions.put(processorId, new ClusterDirectoryState.Position(
                advertisement.getLeaseEpoch(), advertisement.getSeq()));
    }

    private static void presence(ClusterDescriptor cluster, ClusterDirectoryState state,
            NodePresence record) {
        String nodeId = record.getNodeId();
        NodeAdvertisement node = state.nodes.get(nodeId);
        ClusterTransitions.require(node != null,
                "replayed presence names unregistered node '" + nodeId + "'");
        ClusterTransitions.requireCluster(cluster, record.getClusterId(), "replayed presence");
        ClusterTransitions.require(record.getNodeEpoch() == node.getEpoch(),
                "replayed presence for node '" + nodeId + "' carries stale node_epoch");
        NodePresence existing = state.presence.get(nodeId);
        ClusterTransitions.requireFresher(record.getNodeEpoch(), record.getHeartbeatSeq(),
                existing.getNodeEpoch(), existing.getHeartbeatSeq(),
                "presence for node '" + nodeId + "'");
        state.presence.put(nodeId, record);
    }

    private static void capacity(ClusterDirectoryState state, CapacityAdvertisement snapshot) {
        String nodeId = snapshot.getNodeId();
        NodeAdvertisement node = state.nodes.get(nodeId);
        ClusterTransitions.require(node != null,
                "replayed capacity names unregistered node '" + nodeId + "'");
        boolean nodeWide = snapshot.getProcessorId().isEmpty();
        long expectedEpoch;
        if (nodeWide) {
            expectedEpoch = node.getEpoch();
        } else {
            ProcessorAdvertisement processor = state.processors.get(snapshot.getProcessorId());
            ClusterTransitions.require(processor != null,
                    "replayed capacity names unregistered processor '"
                            + snapshot.getProcessorId() + "'");
            ClusterTransitions.require(processor.getNodeId().equals(nodeId),
                    "replayed capacity names the wrong node for processor '"
                            + snapshot.getProcessorId() + "'");
            expectedEpoch = processor.getLeaseEpoch();
        }
        ClusterTransitions.require(snapshot.getSourceEpoch() == expectedEpoch,
                "replayed capacity for node '" + nodeId + "' carries stale source_epoch");
        Map<String, CapacityAdvertisement> target = nodeWide
                ? state.nodeCapacity : state.processorCapacity;
        String key = nodeWide ? nodeId
                : ClusterDirectoryState.capacityKey(nodeId, snapshot.getProcessorId());
        CapacityAdvertisement existing = target.get(key);
        if (existing != null) {
            ClusterTransitions.requireFresher(snapshot.getSourceEpoch(), snapshot.getSeq(),
                    existing.getSourceEpoch(), existing.getSeq(),
                    "capacity for node '" + nodeId + "'");
        }
        target.put(key, snapshot);
    }
}
