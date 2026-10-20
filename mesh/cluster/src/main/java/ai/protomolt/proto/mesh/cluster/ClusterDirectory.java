package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEventType;
import ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryCheckpoint;
import ai.protomolt.proto.mesh.cluster.v1.FencingPosition;
import ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.NodeRecord;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The in-memory cluster directory: node and processor registration and refresh, heartbeat
 * presence, capacity snapshots, TTL expiry, canonical snapshots, and a replayable event
 * log for one cluster. Pure and single-threaded: no I/O, no threads, and all temporal
 * judgments run against the injected {@link Clock}, so tests drive expiry with a fake clock.
 *
 * <p><b>Update rules.</b> Every identity carries a fencing epoch and a per-epoch sequence
 * (nodes: {@code epoch}/{@code seq}; processors: {@code lease_epoch}/{@code seq}; presence:
 * {@code node_epoch}/{@code heartbeat_seq}; capacity:
 * {@code source_epoch}/{@code seq} per key). Re-applying the identical record is an idempotent
 * no-op. A changed record is accepted only from a strictly newer position: a higher epoch, or
 * the same epoch with a higher sequence. Expired identities retain fencing tombstones so a
 * delayed frame cannot revive an old incarnation. A changed record from a stale position is a
 * conflicting update and throws {@link IllegalArgumentException}.
 *
 * <p><b>Expiry.</b> A node is live while its presence record is ACTIVE and its
 * {@code expires_at} lies in the future. Registration arms presence from the advertisement's
 * {@code advertised_at + ttl}; heartbeats extend it. A processor is live while its
 * {@code lease_expires_at} lies in the future and its hosting node is live. {@link #sweep()}
 * removes expired processors, then expired (or GONE) nodes, cascading the node's remaining
 * processors, and emits one event per removal.
 *
 * <p><b>Snapshots.</b> {@link #snapshot()} orders nodes by node id, processors by processor
 * id, and processor-level capacity by (node id, processor id), so equal directory states
 * captured at the same time serialize to equal bytes and equal canonical fingerprints
 * ({@link ClusterValidation#snapshotFingerprint}).
 *
 * <p><b>Events.</b> Every durable change appends a {@link ClusterEvent} with a
 * directory-global, strictly increasing sequence; {@link #events()} returns the log in order.
 * Heartbeats and refresh-only re-advertisements (the same identity with only its lease
 * window and sequence moved) are soft state and append nothing, so an expiry event may
 * name a renewal the log never recorded; replay accepts exactly that difference.
 *
 * <p><b>Structure.</b> The directory is one state machine with two drivers over a single
 * {@link ClusterDirectoryState}: the live path (the methods below, which validate input and
 * append durable events) and the replay path ({@link ClusterEventReplay}, which folds the
 * durable record back without emitting). Both share the gates in {@link ClusterTransitions} —
 * fencing, epoch comparison, refresh classification, and presence arithmetic — so the two
 * drivers cannot drift apart.
 */
public final class ClusterDirectory {

    /**
     * What one apply call did to the directory.
     */
    public enum ApplyOutcome {
        /** The identity was not present; the record was inserted. */
        REGISTERED,
        /** The record replaced a different record from a newer position. */
        UPDATED,
        /** The record was identical to the registered one; nothing changed. */
        UNCHANGED
    }

    private final ClusterDescriptor cluster;
    private final Clock clock;
    private final ClusterDirectoryState state;

    /**
     * Creates the directory for one cluster.
     *
     * @param cluster the cluster identity; validated, fingerprint agreement included
     * @param clock the clock every temporal judgment runs against
     * @throws IllegalArgumentException when the cluster descriptor fails validation
     */
    public ClusterDirectory(ClusterDescriptor cluster, Clock clock) {
        this(cluster, clock, new ClusterDirectoryState());
    }

    /**
     * Creates the directory over a prepared state. The replay driver builds the state from the
     * durable record and hands it over here; the cluster identity is validated either way.
     */
    ClusterDirectory(ClusterDescriptor cluster, Clock clock, ClusterDirectoryState state) {
        ClusterValidation.validate(cluster);
        this.cluster = cluster;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.state = Objects.requireNonNull(state, "state");
    }

    /**
     * Restores a directory from its complete event log. Replay validates the log before applying
     * any event, preserves the original event sequence and timestamps, and rebuilds fencing
     * tombstones for expired nodes and processors. The returned directory can immediately accept
     * new advertisements without admitting frames from an incarnation that the log already
     * superseded.
     *
     * @param cluster the cluster identity the log belongs to
     * @param eventLog the complete, gap-free event log beginning at sequence 1
     * @param clock the clock for future liveness judgments and emitted events
     * @return the restored directory
     * @throws IllegalArgumentException when the log is invalid or cannot produce a coherent state
     */
    public static ClusterDirectory replay(ClusterDescriptor cluster, List<ClusterEvent> eventLog,
            Clock clock) {
        return ClusterEventReplay.replay(cluster, eventLog, clock);
    }

    /**
     * Restores a directory from a checkpoint and the events recorded after it. The
     * checkpoint stands in for every event up to its {@code snapshot_seq}, so the tail must
     * begin at the next sequence and is validated against that position rather than against
     * one. Restoring from {@link #checkpoint()} and the events after it produces exactly the
     * directory the complete log would have produced, fencing tombstones included.
     *
     * @param cluster the cluster identity the checkpoint and tail belong to
     * @param checkpoint the folded directory state
     * @param tail the events recorded after the checkpoint, gap-free and in order
     * @param clock the clock for future liveness judgments and emitted events
     * @return the restored directory
     * @throws IllegalArgumentException when the checkpoint names another cluster, or the tail
     *     is invalid or does not continue from the checkpoint
     */
    public static ClusterDirectory restore(ClusterDescriptor cluster,
            DirectoryCheckpoint checkpoint, List<ClusterEvent> tail, Clock clock) {
        return ClusterEventReplay.restore(cluster, checkpoint, tail, clock);
    }

    /**
     * Folds the directory into a checkpoint that can replace every event up to the current
     * sequence. Beyond the snapshot it carries the fencing tombstones, which the snapshot
     * cannot express because a tombstone outlives the identity it fences: dropping the
     * events without them would let a delayed frame from a superseded incarnation be
     * admitted after a restart.
     *
     * @return the folded directory state at the current event sequence
     */
    public DirectoryCheckpoint checkpoint() {
        DirectoryCheckpoint.Builder builder = DirectoryCheckpoint.newBuilder()
                .setState(snapshot());
        state.nodePositions.forEach((id, position) ->
                builder.addNodePositions(fencingPosition(id, position)));
        state.processorPositions.forEach((id, position) ->
                builder.addProcessorPositions(fencingPosition(id, position)));
        return builder.build();
    }

    private static FencingPosition fencingPosition(String id, ClusterDirectoryState.Position position) {
        return FencingPosition.newBuilder()
                .setId(id)
                .setEpoch(position.epoch())
                .setSeq(position.seq())
                .build();
    }

    /**
     * Registers or refreshes a node advertisement. Registration arms the node's presence from
     * the advertisement's TTL window. Re-applying the identical advertisement is a no-op; a
     * changed advertisement requires a strictly newer (epoch, seq) pair.
     *
     * @param advertisement the advertisement to apply
     * @return what the apply did
     * @throws IllegalArgumentException when the advertisement fails validation, names another
     *     cluster, or conflicts with the registered record
     */
    public ApplyOutcome register(NodeAdvertisement advertisement) {
        ClusterValidation.validate(advertisement);
        ClusterTransitions.requireCluster(cluster, advertisement.getClusterId(), "advertisement");
        String nodeId = advertisement.getNodeId();
        NodeAdvertisement existing = state.nodes.get(nodeId);
        if (existing == null) {
            ClusterTransitions.requireNewerThanTombstone(state.nodePositions.get(nodeId),
                    advertisement.getEpoch(), advertisement.getSeq(), "node '" + nodeId + "'");
            state.nodes.put(nodeId, advertisement);
            state.nodePositions.put(nodeId, new ClusterDirectoryState.Position(
                    advertisement.getEpoch(), advertisement.getSeq()));
            state.presence.put(nodeId, ClusterTransitions.initialPresence(advertisement));
            emit(ClusterEventType.CLUSTER_EVENT_TYPE_NODE_REGISTERED, nodeId, "",
                    b -> b.setNode(advertisement));
            return ApplyOutcome.REGISTERED;
        }
        if (existing.equals(advertisement)) {
            return ApplyOutcome.UNCHANGED;
        }
        ClusterTransitions.requireFresher(advertisement.getEpoch(), advertisement.getSeq(),
                existing.getEpoch(), existing.getSeq(), "node '" + nodeId + "'");
        boolean newEpoch = advertisement.getEpoch() > existing.getEpoch();
        if (newEpoch) {
            for (ProcessorAdvertisement processor : List.copyOf(state.processors.values())) {
                if (processor.getNodeId().equals(nodeId)) {
                    removeProcessor(processor.getProcessorId(), processor, null);
                }
            }
            state.nodeCapacity.remove(nodeId);
        }
        boolean refresh = !newEpoch && ClusterTransitions.refreshOnly(existing, advertisement);
        state.nodes.put(nodeId, advertisement);
        state.nodePositions.put(nodeId, new ClusterDirectoryState.Position(
                advertisement.getEpoch(), advertisement.getSeq()));
        state.presence.put(nodeId, newEpoch
                ? ClusterTransitions.initialPresence(advertisement)
                : ClusterTransitions.mergePresence(state.presence.get(nodeId), advertisement));
        if (refresh) {
            return ApplyOutcome.UPDATED;
        }
        emit(ClusterEventType.CLUSTER_EVENT_TYPE_NODE_REGISTERED, nodeId, "",
                b -> b.setNode(advertisement));
        return ApplyOutcome.UPDATED;
    }

    /**
     * Applies a heartbeat presence record for a registered node. The identical record is a
     * no-op; a changed record requires a strictly higher heartbeat sequence.
     *
     * <p><b>Presence is soft state and emits no event.</b> A heartbeat restates a fact the
     * node regenerates every few seconds and that expires on its own, so persisting it buys
     * nothing a restart could not rebuild by waiting. What it costs is the whole durable
     * write path on the cluster's most frequent call. The registration epoch is what fences
     * presence, and that stays durable: this method refuses any record whose
     * {@code node_epoch} disagrees with the registered advertisement, so a delayed frame
     * from a superseded incarnation cannot extend a node's life whether or not the heartbeat
     * that established the current epoch is still in the log.
     *
     * @param record the presence record to apply
     * @return what the apply did
     * @throws IllegalArgumentException when the record fails validation, names another cluster
     *     or an unregistered node, or carries a stale heartbeat sequence
     */
    public ApplyOutcome heartbeat(NodePresence record) {
        ClusterValidation.validate(record);
        ClusterTransitions.requireCluster(cluster, record.getClusterId(), "presence");
        String nodeId = record.getNodeId();
        ClusterTransitions.require(state.nodes.containsKey(nodeId),
                "heartbeat for unregistered node '" + nodeId + "'");
        NodeAdvertisement node = state.nodes.get(nodeId);
        ClusterTransitions.require(record.getNodeEpoch() == node.getEpoch(),
                "presence for node '" + nodeId + "' carries node_epoch "
                        + record.getNodeEpoch() + " but the registered epoch is "
                        + node.getEpoch());
        NodePresence existing = state.presence.get(nodeId);
        if (existing.equals(record)) {
            return ApplyOutcome.UNCHANGED;
        }
        ClusterTransitions.requireFresher(record.getNodeEpoch(), record.getHeartbeatSeq(),
                existing.getNodeEpoch(), existing.getHeartbeatSeq(),
                "presence for node '" + nodeId + "'");
        state.presence.put(nodeId, record);
        return ApplyOutcome.UPDATED;
    }

    /**
     * Registers or refreshes a processor advertisement. The hosting node must be registered.
     * Re-applying the identical advertisement is a no-op; a changed advertisement requires a
     * strictly newer (lease_epoch, seq) pair.
     *
     * @param advertisement the advertisement to apply
     * @return what the apply did
     * @throws IllegalArgumentException when the advertisement fails validation, names an
     *     unregistered node, or conflicts with the registered record
     */
    public ApplyOutcome registerProcessor(ProcessorAdvertisement advertisement) {
        ClusterValidation.validate(advertisement);
        NodeAdvertisement node = state.nodes.get(advertisement.getNodeId());
        ClusterTransitions.require(node != null,
                "processor '" + advertisement.getProcessorId() + "' names unregistered node '"
                        + advertisement.getNodeId() + "'");
        ClusterTransitions.require(advertisement.getNodeEpoch() == node.getEpoch(),
                "processor '" + advertisement.getProcessorId() + "' carries node_epoch "
                        + advertisement.getNodeEpoch() + " but node '"
                        + advertisement.getNodeId() + "' is at epoch " + node.getEpoch());
        String processorId = advertisement.getProcessorId();
        ProcessorAdvertisement existing = state.processors.get(processorId);
        if (existing == null) {
            ClusterTransitions.requireNewerThanTombstone(state.processorPositions.get(processorId),
                    advertisement.getLeaseEpoch(), advertisement.getSeq(),
                    "processor '" + processorId + "'");
            state.processors.put(processorId, advertisement);
            state.processorPositions.put(processorId, new ClusterDirectoryState.Position(
                    advertisement.getLeaseEpoch(), advertisement.getSeq()));
            emit(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_REGISTERED,
                    advertisement.getNodeId(), processorId, b -> b.setProcessor(advertisement));
            return ApplyOutcome.REGISTERED;
        }
        if (existing.equals(advertisement)) {
            return ApplyOutcome.UNCHANGED;
        }
        ClusterTransitions.requireFresher(advertisement.getLeaseEpoch(), advertisement.getSeq(),
                existing.getLeaseEpoch(), existing.getSeq(), "processor '" + processorId + "'");
        ClusterTransitions.require(existing.getNodeId().equals(advertisement.getNodeId())
                        || advertisement.getLeaseEpoch() > existing.getLeaseEpoch(),
                "processor '" + processorId
                        + "' may move nodes only under a newer lease_epoch");
        if (!existing.getNodeId().equals(advertisement.getNodeId())
                || existing.getLeaseEpoch() != advertisement.getLeaseEpoch()) {
            state.processorCapacity.remove(ClusterDirectoryState.capacityKey(
                    existing.getNodeId(), processorId));
        }
        boolean refresh = ClusterTransitions.refreshOnly(existing, advertisement);
        state.processors.put(processorId, advertisement);
        state.processorPositions.put(processorId, new ClusterDirectoryState.Position(
                advertisement.getLeaseEpoch(), advertisement.getSeq()));
        if (refresh) {
            return ApplyOutcome.UPDATED;
        }
        emit(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_REGISTERED,
                advertisement.getNodeId(), processorId, b -> b.setProcessor(advertisement));
        return ApplyOutcome.UPDATED;
    }

    /**
     * Applies a capacity snapshot for a registered node, or for one registered processor on
     * that node when {@code processor_id} is set. The identical snapshot is a no-op; a changed
     * snapshot requires a strictly higher sequence for its (node, processor) key.
     *
     * @param snapshot the snapshot to apply
     * @return what the apply did
     * @throws IllegalArgumentException when the snapshot fails validation, names an
     *     unregistered node or processor, or carries a stale sequence
     */
    public ApplyOutcome updateCapacity(CapacityAdvertisement snapshot) {
        ClusterValidation.validate(snapshot);
        String nodeId = snapshot.getNodeId();
        ClusterTransitions.require(state.nodes.containsKey(nodeId),
                "capacity for unregistered node '" + nodeId + "'");
        boolean nodeWide = snapshot.getProcessorId().isEmpty();
        long expectedEpoch;
        if (!nodeWide) {
            ProcessorAdvertisement processor = state.processors.get(snapshot.getProcessorId());
            ClusterTransitions.require(processor != null,
                    "capacity for unregistered processor '" + snapshot.getProcessorId() + "'");
            ClusterTransitions.require(processor.getNodeId().equals(nodeId),
                    "capacity for processor '" + snapshot.getProcessorId() + "' names node '"
                            + nodeId + "' but the processor belongs to '"
                            + processor.getNodeId() + "'");
            expectedEpoch = processor.getLeaseEpoch();
        } else {
            expectedEpoch = state.nodes.get(nodeId).getEpoch();
        }
        ClusterTransitions.require(snapshot.getSourceEpoch() == expectedEpoch,
                "capacity for node '" + nodeId + "' carries source_epoch "
                        + snapshot.getSourceEpoch() + " but the current source epoch is "
                        + expectedEpoch);
        Map<String, CapacityAdvertisement> map = nodeWide ? state.nodeCapacity : state.processorCapacity;
        String key = nodeWide ? nodeId
                : ClusterDirectoryState.capacityKey(nodeId, snapshot.getProcessorId());
        CapacityAdvertisement existing = map.get(key);
        if (existing == null) {
            map.put(key, snapshot);
            emit(ClusterEventType.CLUSTER_EVENT_TYPE_CAPACITY_UPDATED, nodeId,
                    snapshot.getProcessorId(), b -> b.setCapacity(snapshot));
            return ApplyOutcome.REGISTERED;
        }
        if (existing.equals(snapshot)) {
            return ApplyOutcome.UNCHANGED;
        }
        ClusterTransitions.requireFresher(snapshot.getSourceEpoch(), snapshot.getSeq(),
                existing.getSourceEpoch(), existing.getSeq(),
                "capacity for node '" + nodeId + "'");
        boolean refresh = ClusterTransitions.refreshOnly(existing, snapshot);
        map.put(key, snapshot);
        if (refresh) {
            return ApplyOutcome.UPDATED;
        }
        emit(ClusterEventType.CLUSTER_EVENT_TYPE_CAPACITY_UPDATED, nodeId,
                snapshot.getProcessorId(), b -> b.setCapacity(snapshot));
        return ApplyOutcome.UPDATED;
    }

    /**
     * Removes everything whose liveness has lapsed at the current clock instant: processors
     * whose lease expiry has passed, then nodes whose presence expired or went GONE, cascading
     * each swept node's remaining processors. Emits one event per removal and returns the
     * events this sweep appended.
     *
     * @return the expiry events this sweep emitted, in emission order
     */
    public List<ClusterEvent> sweep() {
        Instant now = clock.instant();
        List<ClusterEvent> emitted = new ArrayList<>();
        for (String processorId : List.copyOf(state.processors.keySet())) {
            ProcessorAdvertisement advertisement = state.processors.get(processorId);
            if (!ClusterTransitions.instant(advertisement.getLeaseExpiresAt()).isAfter(now)) {
                removeProcessor(processorId, advertisement, emitted);
            }
        }
        for (String nodeId : List.copyOf(state.presence.keySet())) {
            NodePresence record = state.presence.get(nodeId);
            boolean gone = record.getState() == PresenceState.PRESENCE_STATE_GONE
                    || !ClusterTransitions.instant(record.getExpiresAt()).isAfter(now);
            if (!gone) {
                continue;
            }
            NodeAdvertisement advertisement = state.nodes.get(nodeId);
            for (ProcessorAdvertisement processor : List.copyOf(state.processors.values())) {
                if (processor.getNodeId().equals(nodeId)) {
                    removeProcessor(processor.getProcessorId(), processor, emitted);
                }
            }
            state.nodes.remove(nodeId);
            state.presence.remove(nodeId);
            state.nodeCapacity.remove(nodeId);
            emitted.add(emit(ClusterEventType.CLUSTER_EVENT_TYPE_NODE_EXPIRED, nodeId, "",
                    b -> b.setNode(advertisement)));
        }
        return List.copyOf(emitted);
    }

    /**
     * Answers the eligibility query: which advertised processors can serve the given schema
     * with the given capability right now. A processor qualifies when its lease is unexpired,
     * its hosting node is live (presence ACTIVE and unexpired), it advertises the capability,
     * and its accepted schemas contain the exact schema identity. An empty type name matches
     * any schema set; an empty capability matches any capability set; an empty fingerprint
     * matches any fingerprint for the named type. Results order by processor id.
     *
     * @param typeName the fully qualified payload type to serve, or empty for any
     * @param descriptorFingerprint the canonical descriptor fingerprint, or empty for any
     * @param capability the required capability name, or empty for any
     * @return the eligible processor advertisements, ordered by processor id
     */
    public List<ProcessorAdvertisement> eligibleProcessors(String typeName,
            String descriptorFingerprint, String capability) {
        return eligibleProcessors(typeName, descriptorFingerprint, capability, 0);
    }

    /**
     * Answers the eligibility query and also enforces node and processor capacity. A payload size
     * of zero means the caller did not supply a size constraint.
     */
    public List<ProcessorAdvertisement> eligibleProcessors(String typeName,
            String descriptorFingerprint, String capability, long payloadBytes) {
        if (payloadBytes < 0) {
            throw new IllegalArgumentException("payloadBytes must not be negative");
        }
        Instant now = clock.instant();
        return state.processors.values().stream()
                .filter(p -> ClusterTransitions.instant(p.getLeaseExpiresAt()).isAfter(now))
                .filter(p -> isServing(p.getNodeId(), now))
                .filter(p -> p.getNodeEpoch() == state.nodes.get(p.getNodeId()).getEpoch())
                .filter(p -> hasCapacity(state.nodeCapacity.get(p.getNodeId()), payloadBytes))
                .filter(p -> hasProcessorCapacity(p, state.processorCapacity.get(
                        ClusterDirectoryState.capacityKey(p.getNodeId(), p.getProcessorId())),
                        payloadBytes))
                .filter(p -> capability.isEmpty() || p.getCapabilitiesList().contains(capability))
                .filter(p -> typeName.isEmpty() || p.getAcceptedSchemasList().stream()
                        .anyMatch(s -> s.getTypeName().equals(typeName)
                                && (descriptorFingerprint.isEmpty()
                                || s.getDescriptorFingerprint().equals(descriptorFingerprint))))
                .toList();
    }

    /**
     * Captures the deterministic point-in-time view: the cluster identity, the clock's
     * capture time, the current event position, and every live node, processor, and
     * processor-level capacity record in stable id order, sealed with the canonical
     * fingerprint.
     *
     * @return the snapshot, fingerprint included
     */
    public ClusterSnapshot snapshot() {
        ClusterSnapshot.Builder builder = ClusterSnapshot.newBuilder()
                .setCluster(cluster)
                .setCapturedAt(ClusterTransitions.timestamp(clock.instant()))
                .setSnapshotSeq(state.eventSeq);
        state.nodes.forEach((nodeId, advertisement) -> {
            NodeRecord.Builder record = NodeRecord.newBuilder()
                    .setAdvertisement(advertisement)
                    .setPresence(state.presence.get(nodeId));
            CapacityAdvertisement capacity = state.nodeCapacity.get(nodeId);
            if (capacity != null) {
                record.setCapacity(capacity);
            }
            builder.addNodes(record);
        });
        state.processors.values().forEach(builder::addProcessors);
        state.processorCapacity.values().forEach(builder::addCapacities);
        ClusterSnapshot unsigned = builder.build();
        return unsigned.toBuilder()
                .setFingerprint(ClusterValidation.snapshotFingerprint(unsigned))
                .build();
    }

    /** Returns the cluster this directory serves. */
    public ClusterDescriptor cluster() {
        return cluster;
    }

    /** Returns the registered advertisement for {@code nodeId}, when present. */
    public Optional<NodeAdvertisement> node(String nodeId) {
        return Optional.ofNullable(state.nodes.get(nodeId));
    }

    /** Returns the presence record for {@code nodeId}, when present. */
    public Optional<NodePresence> presence(String nodeId) {
        return Optional.ofNullable(state.presence.get(nodeId));
    }

    /** Returns the registered advertisement for {@code processorId}, when present. */
    public Optional<ProcessorAdvertisement> processor(String processorId) {
        return Optional.ofNullable(state.processors.get(processorId));
    }

    /** Returns the node-wide capacity snapshot for {@code nodeId}, when published. */
    public Optional<CapacityAdvertisement> nodeCapacity(String nodeId) {
        return Optional.ofNullable(state.nodeCapacity.get(nodeId));
    }

    /** Returns the capacity snapshot for one processor on one node, when published. */
    public Optional<CapacityAdvertisement> processorCapacity(String nodeId, String processorId) {
        return Optional.ofNullable(state.processorCapacity.get(
                ClusterDirectoryState.capacityKey(nodeId, processorId)));
    }

    /** Returns the event log in emission order. */
    public List<ClusterEvent> events() {
        return List.copyOf(state.events);
    }

    /**
     * Copies the directory, sharing nothing a later mutation could reach. Every value held
     * is an immutable message or record, so copying the maps is a true copy. This is how a
     * presence update produces a directory to install without replaying the event log: an
     * installed directory is never mutated, and soft state must not break that.
     *
     * @return an independent directory holding the same state at the same sequence
     */
    ClusterDirectory copy() {
        return new ClusterDirectory(cluster, clock, state.copy());
    }

    /**
     * Carries live soft state onto a directory rebuilt from the durable record. Presence
     * emits no events at all, and a lease refresh emits none either, so a replayed directory
     * knows only what the last durable change recorded. Without this, every durable mutation
     * would roll liveness and lease windows back to that and sweep identities that are
     * renewing normally.
     *
     * <p>A record is carried only when the rebuilt directory still holds the same identity at
     * the same epoch, and only when the live record differs from the rebuilt one by nothing
     * more than a refresh. That is the same fence the apply paths use, applied at the same
     * place: anything the durable record has superseded, or that differs in substance, is
     * left alone and the durable record wins.
     *
     * @param source the installed directory whose soft state to carry forward
     */
    void adoptSoftState(ClusterDirectory source) {
        source.state.presence.forEach((nodeId, record) -> {
            NodeAdvertisement node = state.nodes.get(nodeId);
            if (node == null || record.getNodeEpoch() != node.getEpoch()) {
                return;
            }
            NodePresence rebuilt = state.presence.get(nodeId);
            if (rebuilt == null || record.getHeartbeatSeq() >= rebuilt.getHeartbeatSeq()) {
                state.presence.put(nodeId, record);
            }
        });
        source.state.nodes.forEach((nodeId, live) -> {
            NodeAdvertisement rebuilt = state.nodes.get(nodeId);
            if (rebuilt == null || live.getEpoch() != rebuilt.getEpoch()
                    || live.getSeq() < rebuilt.getSeq()
                    || !ClusterTransitions.refreshOnly(rebuilt, live)) {
                return;
            }
            state.nodes.put(nodeId, live);
            state.nodePositions.put(nodeId,
                    new ClusterDirectoryState.Position(live.getEpoch(), live.getSeq()));
        });
        source.state.processors.forEach((processorId, live) -> {
            ProcessorAdvertisement rebuilt = state.processors.get(processorId);
            if (rebuilt == null || live.getLeaseEpoch() != rebuilt.getLeaseEpoch()
                    || live.getSeq() < rebuilt.getSeq()
                    || !ClusterTransitions.refreshOnly(rebuilt, live)) {
                return;
            }
            state.processors.put(processorId, live);
            state.processorPositions.put(processorId,
                    new ClusterDirectoryState.Position(live.getLeaseEpoch(), live.getSeq()));
        });
        adoptCapacity(source.state.nodeCapacity, state.nodeCapacity);
        adoptCapacity(source.state.processorCapacity, state.processorCapacity);
    }

    private static void adoptCapacity(Map<String, CapacityAdvertisement> source,
                                      Map<String, CapacityAdvertisement> target) {
        source.forEach((key, live) -> {
            CapacityAdvertisement rebuilt = target.get(key);
            if (rebuilt == null || live.getSourceEpoch() != rebuilt.getSourceEpoch()
                    || live.getSeq() < rebuilt.getSeq()
                    || !ClusterTransitions.refreshOnly(rebuilt, live)) {
                return;
            }
            target.put(key, live);
        });
    }

    private void removeProcessor(String processorId, ProcessorAdvertisement advertisement,
            List<ClusterEvent> emitted) {
        state.processors.remove(processorId);
        state.processorCapacity.remove(ClusterDirectoryState.capacityKey(
                advertisement.getNodeId(), processorId));
        ClusterEvent event = emit(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED,
                advertisement.getNodeId(), processorId, b -> b.setProcessor(advertisement));
        if (emitted != null) {
            emitted.add(event);
        }
    }

    private boolean isServing(String nodeId, Instant now) {
        NodePresence record = state.presence.get(nodeId);
        return record != null
                && record.getState() == PresenceState.PRESENCE_STATE_ACTIVE
                && ClusterTransitions.instant(record.getExpiresAt()).isAfter(now);
    }

    private ClusterEvent emit(ClusterEventType type, String nodeId, String processorId,
            Consumer<ClusterEvent.Builder> detail) {
        ClusterEvent.Builder builder = ClusterEvent.newBuilder()
                .setSeq(++state.eventSeq)
                .setOccurredAt(ClusterTransitions.timestamp(clock.instant()))
                .setType(type)
                .setNodeId(nodeId)
                .setProcessorId(processorId);
        detail.accept(builder);
        ClusterEvent event = builder.build();
        ClusterValidation.validate(event);
        state.events.add(event);
        return event;
    }

    private static boolean hasCapacity(CapacityAdvertisement capacity, long payloadBytes) {
        if (capacity == null) {
            return true;
        }
        boolean workAvailable = capacity.getMaxInFlight() == 0
                || capacity.getInFlight() < capacity.getMaxInFlight();
        boolean sessionsAvailable = capacity.getMaxActiveSessions() == 0
                || capacity.getActiveSessions() < capacity.getMaxActiveSessions();
        boolean payloadFits = payloadBytes == 0 || capacity.getMaxPayloadBytes() == 0
                || payloadBytes <= capacity.getMaxPayloadBytes();
        return workAvailable && sessionsAvailable && payloadFits;
    }

    private static boolean hasProcessorCapacity(ProcessorAdvertisement processor,
            CapacityAdvertisement capacity, long payloadBytes) {
        if (!hasCapacity(capacity, payloadBytes)) {
            return false;
        }
        return capacity == null || processor.getMaxActiveSessions() == 0
                || capacity.getActiveSessions() < processor.getMaxActiveSessions();
    }
}
