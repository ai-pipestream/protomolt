package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEventType;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.NodeRecord;
import ai.pipestream.proto.mesh.cluster.v1.PresenceState;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import com.google.protobuf.Timestamp;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
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
 * <p><b>Events.</b> Every applied change appends a {@link ClusterEvent} with a
 * directory-global, strictly increasing sequence; {@link #events()} returns the log in order.
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
    private final Map<String, NodeAdvertisement> nodes = new TreeMap<>();
    /** Highest accepted node positions, retained after expiry as fencing tombstones. */
    private final Map<String, Position> nodePositions = new TreeMap<>();
    private final Map<String, NodePresence> presence = new TreeMap<>();
    private final Map<String, CapacityAdvertisement> nodeCapacity = new TreeMap<>();
    private final Map<String, ProcessorAdvertisement> processors = new TreeMap<>();
    /** Highest accepted processor positions, retained after expiry as fencing tombstones. */
    private final Map<String, Position> processorPositions = new TreeMap<>();
    /** Keyed by {@code nodeId + "\n" + processorId}; newline cannot appear in a path-safe id. */
    private final Map<String, CapacityAdvertisement> processorCapacity = new TreeMap<>();
    private final List<ClusterEvent> events = new ArrayList<>();
    private long eventSeq;

    /**
     * Creates the directory for one cluster.
     *
     * @param cluster the cluster identity; validated, fingerprint agreement included
     * @param clock the clock every temporal judgment runs against
     * @throws IllegalArgumentException when the cluster descriptor fails validation
     */
    public ClusterDirectory(ClusterDescriptor cluster, Clock clock) {
        ClusterValidation.validate(cluster);
        this.cluster = cluster;
        this.clock = Objects.requireNonNull(clock, "clock");
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
        ClusterValidation.validateEventLog(eventLog);
        ClusterDirectory directory = new ClusterDirectory(cluster, clock);
        for (ClusterEvent event : eventLog) {
            directory.applyReplay(event);
            directory.events.add(event);
            directory.eventSeq = event.getSeq();
        }
        return directory;
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
        requireCluster(advertisement.getClusterId(), "advertisement");
        String nodeId = advertisement.getNodeId();
        NodeAdvertisement existing = nodes.get(nodeId);
        if (existing == null) {
            requireNewerThanTombstone(nodePositions.get(nodeId), advertisement.getEpoch(),
                    advertisement.getSeq(), "node '" + nodeId + "'");
            nodes.put(nodeId, advertisement);
            nodePositions.put(nodeId, new Position(advertisement.getEpoch(),
                    advertisement.getSeq()));
            presence.put(nodeId, initialPresence(advertisement));
            emit(ClusterEventType.CLUSTER_EVENT_TYPE_NODE_REGISTERED, nodeId, "",
                    b -> b.setNode(advertisement));
            return ApplyOutcome.REGISTERED;
        }
        if (existing.equals(advertisement)) {
            return ApplyOutcome.UNCHANGED;
        }
        requireFresher(advertisement.getEpoch(), advertisement.getSeq(),
                existing.getEpoch(), existing.getSeq(), "node '" + nodeId + "'");
        boolean newEpoch = advertisement.getEpoch() > existing.getEpoch();
        if (newEpoch) {
            for (ProcessorAdvertisement processor : List.copyOf(processors.values())) {
                if (processor.getNodeId().equals(nodeId)) {
                    removeProcessor(processor.getProcessorId(), processor, null);
                }
            }
            nodeCapacity.remove(nodeId);
        }
        nodes.put(nodeId, advertisement);
        nodePositions.put(nodeId, new Position(advertisement.getEpoch(),
                advertisement.getSeq()));
        presence.put(nodeId, newEpoch
                ? initialPresence(advertisement)
                : mergePresence(presence.get(nodeId), advertisement));
        emit(ClusterEventType.CLUSTER_EVENT_TYPE_NODE_REGISTERED, nodeId, "",
                b -> b.setNode(advertisement));
        return ApplyOutcome.UPDATED;
    }

    /**
     * Applies a heartbeat presence record for a registered node. The identical record is a
     * no-op; a changed record requires a strictly higher heartbeat sequence.
     *
     * @param record the presence record to apply
     * @return what the apply did
     * @throws IllegalArgumentException when the record fails validation, names another cluster
     *     or an unregistered node, or carries a stale heartbeat sequence
     */
    public ApplyOutcome heartbeat(NodePresence record) {
        ClusterValidation.validate(record);
        requireCluster(record.getClusterId(), "presence");
        String nodeId = record.getNodeId();
        require(nodes.containsKey(nodeId),
                "heartbeat for unregistered node '" + nodeId + "'");
        NodeAdvertisement node = nodes.get(nodeId);
        require(record.getNodeEpoch() == node.getEpoch(),
                "presence for node '" + nodeId + "' carries node_epoch "
                        + record.getNodeEpoch() + " but the registered epoch is "
                        + node.getEpoch());
        NodePresence existing = presence.get(nodeId);
        if (existing.equals(record)) {
            return ApplyOutcome.UNCHANGED;
        }
        requireFresher(record.getNodeEpoch(), record.getHeartbeatSeq(),
                existing.getNodeEpoch(), existing.getHeartbeatSeq(),
                "presence for node '" + nodeId + "'");
        presence.put(nodeId, record);
        emit(ClusterEventType.CLUSTER_EVENT_TYPE_PRESENCE_UPDATED, nodeId, "",
                b -> b.setPresence(record));
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
        NodeAdvertisement node = nodes.get(advertisement.getNodeId());
        require(node != null,
                "processor '" + advertisement.getProcessorId() + "' names unregistered node '"
                        + advertisement.getNodeId() + "'");
        require(advertisement.getNodeEpoch() == node.getEpoch(),
                "processor '" + advertisement.getProcessorId() + "' carries node_epoch "
                        + advertisement.getNodeEpoch() + " but node '"
                        + advertisement.getNodeId() + "' is at epoch " + node.getEpoch());
        String processorId = advertisement.getProcessorId();
        ProcessorAdvertisement existing = processors.get(processorId);
        if (existing == null) {
            requireNewerThanTombstone(processorPositions.get(processorId),
                    advertisement.getLeaseEpoch(), advertisement.getSeq(),
                    "processor '" + processorId + "'");
            processors.put(processorId, advertisement);
            processorPositions.put(processorId, new Position(advertisement.getLeaseEpoch(),
                    advertisement.getSeq()));
            emit(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_REGISTERED,
                    advertisement.getNodeId(), processorId, b -> b.setProcessor(advertisement));
            return ApplyOutcome.REGISTERED;
        }
        if (existing.equals(advertisement)) {
            return ApplyOutcome.UNCHANGED;
        }
        requireFresher(advertisement.getLeaseEpoch(), advertisement.getSeq(),
                existing.getLeaseEpoch(), existing.getSeq(), "processor '" + processorId + "'");
        require(existing.getNodeId().equals(advertisement.getNodeId())
                        || advertisement.getLeaseEpoch() > existing.getLeaseEpoch(),
                "processor '" + processorId
                        + "' may move nodes only under a newer lease_epoch");
        if (!existing.getNodeId().equals(advertisement.getNodeId())
                || existing.getLeaseEpoch() != advertisement.getLeaseEpoch()) {
            processorCapacity.remove(capacityKey(existing.getNodeId(), processorId));
        }
        processors.put(processorId, advertisement);
        processorPositions.put(processorId, new Position(advertisement.getLeaseEpoch(),
                advertisement.getSeq()));
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
        require(nodes.containsKey(nodeId),
                "capacity for unregistered node '" + nodeId + "'");
        boolean nodeWide = snapshot.getProcessorId().isEmpty();
        long expectedEpoch;
        if (!nodeWide) {
            ProcessorAdvertisement processor = processors.get(snapshot.getProcessorId());
            require(processor != null,
                    "capacity for unregistered processor '" + snapshot.getProcessorId() + "'");
            require(processor.getNodeId().equals(nodeId),
                    "capacity for processor '" + snapshot.getProcessorId() + "' names node '"
                            + nodeId + "' but the processor belongs to '"
                            + processor.getNodeId() + "'");
            expectedEpoch = processor.getLeaseEpoch();
        } else {
            expectedEpoch = nodes.get(nodeId).getEpoch();
        }
        require(snapshot.getSourceEpoch() == expectedEpoch,
                "capacity for node '" + nodeId + "' carries source_epoch "
                        + snapshot.getSourceEpoch() + " but the current source epoch is "
                        + expectedEpoch);
        Map<String, CapacityAdvertisement> map = nodeWide ? nodeCapacity : processorCapacity;
        String key = nodeWide ? nodeId : capacityKey(nodeId, snapshot.getProcessorId());
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
        requireFresher(snapshot.getSourceEpoch(), snapshot.getSeq(),
                existing.getSourceEpoch(), existing.getSeq(),
                "capacity for node '" + nodeId + "'");
        map.put(key, snapshot);
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
        for (String processorId : List.copyOf(processors.keySet())) {
            ProcessorAdvertisement advertisement = processors.get(processorId);
            if (!instant(advertisement.getLeaseExpiresAt()).isAfter(now)) {
                removeProcessor(processorId, advertisement, emitted);
            }
        }
        for (String nodeId : List.copyOf(presence.keySet())) {
            NodePresence record = presence.get(nodeId);
            boolean gone = record.getState() == PresenceState.PRESENCE_STATE_GONE
                    || !instant(record.getExpiresAt()).isAfter(now);
            if (!gone) {
                continue;
            }
            NodeAdvertisement advertisement = nodes.get(nodeId);
            for (ProcessorAdvertisement processor : List.copyOf(processors.values())) {
                if (processor.getNodeId().equals(nodeId)) {
                    removeProcessor(processor.getProcessorId(), processor, emitted);
                }
            }
            nodes.remove(nodeId);
            presence.remove(nodeId);
            nodeCapacity.remove(nodeId);
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
        return processors.values().stream()
                .filter(p -> instant(p.getLeaseExpiresAt()).isAfter(now))
                .filter(p -> isServing(p.getNodeId(), now))
                .filter(p -> p.getNodeEpoch() == nodes.get(p.getNodeId()).getEpoch())
                .filter(p -> hasCapacity(nodeCapacity.get(p.getNodeId()), payloadBytes))
                .filter(p -> hasProcessorCapacity(p, processorCapacity.get(
                        capacityKey(p.getNodeId(), p.getProcessorId())), payloadBytes))
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
                .setCapturedAt(timestamp(clock.instant()))
                .setSnapshotSeq(eventSeq);
        nodes.forEach((nodeId, advertisement) -> {
            NodeRecord.Builder record = NodeRecord.newBuilder()
                    .setAdvertisement(advertisement)
                    .setPresence(presence.get(nodeId));
            CapacityAdvertisement capacity = nodeCapacity.get(nodeId);
            if (capacity != null) {
                record.setCapacity(capacity);
            }
            builder.addNodes(record);
        });
        processors.values().forEach(builder::addProcessors);
        processorCapacity.values().forEach(builder::addCapacities);
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
        return Optional.ofNullable(nodes.get(nodeId));
    }

    /** Returns the presence record for {@code nodeId}, when present. */
    public Optional<NodePresence> presence(String nodeId) {
        return Optional.ofNullable(presence.get(nodeId));
    }

    /** Returns the registered advertisement for {@code processorId}, when present. */
    public Optional<ProcessorAdvertisement> processor(String processorId) {
        return Optional.ofNullable(processors.get(processorId));
    }

    /** Returns the node-wide capacity snapshot for {@code nodeId}, when published. */
    public Optional<CapacityAdvertisement> nodeCapacity(String nodeId) {
        return Optional.ofNullable(nodeCapacity.get(nodeId));
    }

    /** Returns the capacity snapshot for one processor on one node, when published. */
    public Optional<CapacityAdvertisement> processorCapacity(String nodeId, String processorId) {
        return Optional.ofNullable(processorCapacity.get(capacityKey(nodeId, processorId)));
    }

    /** Returns the event log in emission order. */
    public List<ClusterEvent> events() {
        return List.copyOf(events);
    }

    private void removeProcessor(String processorId, ProcessorAdvertisement advertisement,
            List<ClusterEvent> emitted) {
        processors.remove(processorId);
        processorCapacity.remove(capacityKey(advertisement.getNodeId(), processorId));
        ClusterEvent event = emit(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED,
                advertisement.getNodeId(), processorId, b -> b.setProcessor(advertisement));
        if (emitted != null) {
            emitted.add(event);
        }
    }

    private void applyReplay(ClusterEvent event) {
        switch (event.getType()) {
            case CLUSTER_EVENT_TYPE_NODE_REGISTERED -> replayNodeRegistration(event.getNode());
            case CLUSTER_EVENT_TYPE_NODE_EXPIRED -> replayNodeExpiry(event.getNode());
            case CLUSTER_EVENT_TYPE_PROCESSOR_REGISTERED ->
                    replayProcessorRegistration(event.getProcessor());
            case CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED ->
                    replayProcessorExpiry(event.getProcessor());
            case CLUSTER_EVENT_TYPE_PRESENCE_UPDATED -> replayPresence(event.getPresence());
            case CLUSTER_EVENT_TYPE_CAPACITY_UPDATED -> replayCapacity(event.getCapacity());
            case CLUSTER_EVENT_TYPE_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("unsupported cluster event type at seq "
                            + event.getSeq());
        }
    }

    private void replayNodeRegistration(NodeAdvertisement advertisement) {
        requireCluster(advertisement.getClusterId(), "replayed advertisement");
        String nodeId = advertisement.getNodeId();
        NodeAdvertisement existing = nodes.get(nodeId);
        if (existing == null) {
            requireNewerThanTombstone(nodePositions.get(nodeId), advertisement.getEpoch(),
                    advertisement.getSeq(), "node '" + nodeId + "'");
        } else {
            requireFresher(advertisement.getEpoch(), advertisement.getSeq(), existing.getEpoch(),
                    existing.getSeq(), "node '" + nodeId + "'");
            if (advertisement.getEpoch() > existing.getEpoch()) {
                require(processors.values().stream()
                                .noneMatch(processor -> processor.getNodeId().equals(nodeId)),
                        "node '" + nodeId
                                + "' advanced epoch before its processors expired");
                nodeCapacity.remove(nodeId);
            }
        }
        boolean newEpoch = existing != null && advertisement.getEpoch() > existing.getEpoch();
        nodes.put(nodeId, advertisement);
        nodePositions.put(nodeId, new Position(advertisement.getEpoch(), advertisement.getSeq()));
        presence.put(nodeId, existing == null || newEpoch
                ? initialPresence(advertisement)
                : mergePresence(presence.get(nodeId), advertisement));
    }

    private void replayNodeExpiry(NodeAdvertisement advertisement) {
        String nodeId = advertisement.getNodeId();
        require(advertisement.equals(nodes.get(nodeId)),
                "node expiry at replay does not match registered node '" + nodeId + "'");
        require(processors.values().stream()
                        .noneMatch(processor -> processor.getNodeId().equals(nodeId)),
                "node '" + nodeId + "' expired before its processors");
        nodes.remove(nodeId);
        presence.remove(nodeId);
        nodeCapacity.remove(nodeId);
    }

    private void replayProcessorRegistration(ProcessorAdvertisement advertisement) {
        String processorId = advertisement.getProcessorId();
        NodeAdvertisement node = nodes.get(advertisement.getNodeId());
        require(node != null,
                "replayed processor '" + processorId + "' names unregistered node '"
                        + advertisement.getNodeId() + "'");
        require(advertisement.getNodeEpoch() == node.getEpoch(),
                "replayed processor '" + processorId + "' carries stale node_epoch");
        ProcessorAdvertisement existing = processors.get(processorId);
        if (existing == null) {
            requireNewerThanTombstone(processorPositions.get(processorId),
                    advertisement.getLeaseEpoch(), advertisement.getSeq(),
                    "processor '" + processorId + "'");
        } else {
            requireFresher(advertisement.getLeaseEpoch(), advertisement.getSeq(),
                    existing.getLeaseEpoch(), existing.getSeq(),
                    "processor '" + processorId + "'");
            require(existing.getNodeId().equals(advertisement.getNodeId())
                            || advertisement.getLeaseEpoch() > existing.getLeaseEpoch(),
                    "processor '" + processorId
                            + "' moved nodes without a newer lease_epoch");
            if (!existing.getNodeId().equals(advertisement.getNodeId())
                    || existing.getLeaseEpoch() != advertisement.getLeaseEpoch()) {
                processorCapacity.remove(capacityKey(existing.getNodeId(), processorId));
            }
        }
        processors.put(processorId, advertisement);
        processorPositions.put(processorId, new Position(advertisement.getLeaseEpoch(),
                advertisement.getSeq()));
    }

    private void replayProcessorExpiry(ProcessorAdvertisement advertisement) {
        String processorId = advertisement.getProcessorId();
        require(advertisement.equals(processors.get(processorId)),
                "processor expiry at replay does not match registered processor '"
                        + processorId + "'");
        processors.remove(processorId);
        processorCapacity.remove(capacityKey(advertisement.getNodeId(), processorId));
    }

    private void replayPresence(NodePresence record) {
        String nodeId = record.getNodeId();
        NodeAdvertisement node = nodes.get(nodeId);
        require(node != null, "replayed presence names unregistered node '" + nodeId + "'");
        requireCluster(record.getClusterId(), "replayed presence");
        require(record.getNodeEpoch() == node.getEpoch(),
                "replayed presence for node '" + nodeId + "' carries stale node_epoch");
        NodePresence existing = presence.get(nodeId);
        requireFresher(record.getNodeEpoch(), record.getHeartbeatSeq(), existing.getNodeEpoch(),
                existing.getHeartbeatSeq(), "presence for node '" + nodeId + "'");
        presence.put(nodeId, record);
    }

    private void replayCapacity(CapacityAdvertisement snapshot) {
        String nodeId = snapshot.getNodeId();
        NodeAdvertisement node = nodes.get(nodeId);
        require(node != null, "replayed capacity names unregistered node '" + nodeId + "'");
        boolean nodeWide = snapshot.getProcessorId().isEmpty();
        long expectedEpoch;
        if (nodeWide) {
            expectedEpoch = node.getEpoch();
        } else {
            ProcessorAdvertisement processor = processors.get(snapshot.getProcessorId());
            require(processor != null,
                    "replayed capacity names unregistered processor '"
                            + snapshot.getProcessorId() + "'");
            require(processor.getNodeId().equals(nodeId),
                    "replayed capacity names the wrong node for processor '"
                            + snapshot.getProcessorId() + "'");
            expectedEpoch = processor.getLeaseEpoch();
        }
        require(snapshot.getSourceEpoch() == expectedEpoch,
                "replayed capacity for node '" + nodeId + "' carries stale source_epoch");
        Map<String, CapacityAdvertisement> target = nodeWide ? nodeCapacity : processorCapacity;
        String key = nodeWide ? nodeId : capacityKey(nodeId, snapshot.getProcessorId());
        CapacityAdvertisement existing = target.get(key);
        if (existing != null) {
            requireFresher(snapshot.getSourceEpoch(), snapshot.getSeq(),
                    existing.getSourceEpoch(), existing.getSeq(),
                    "capacity for node '" + nodeId + "'");
        }
        target.put(key, snapshot);
    }

    private boolean isServing(String nodeId, Instant now) {
        NodePresence record = presence.get(nodeId);
        return record != null
                && record.getState() == PresenceState.PRESENCE_STATE_ACTIVE
                && instant(record.getExpiresAt()).isAfter(now);
    }

    private NodePresence initialPresence(NodeAdvertisement advertisement) {
        return NodePresence.newBuilder()
                .setNodeId(advertisement.getNodeId())
                .setClusterId(advertisement.getClusterId())
                .setState(PresenceState.PRESENCE_STATE_ACTIVE)
                .setLastHeartbeatAt(advertisement.getAdvertisedAt())
                .setHeartbeatSeq(advertisement.getSeq())
                .setTtl(advertisement.getTtl())
                .setNodeEpoch(advertisement.getEpoch())
                .setExpiresAt(timestamp(instant(advertisement.getAdvertisedAt())
                        .plus(duration(advertisement.getTtl()))))
                .build();
    }

    private NodePresence mergePresence(NodePresence current, NodeAdvertisement advertisement) {
        Instant heartbeat = instant(advertisement.getAdvertisedAt());
        Instant advertisedExpiry = heartbeat.plus(duration(advertisement.getTtl()));
        Instant currentHeartbeat = instant(current.getLastHeartbeatAt());
        if (heartbeat.isAfter(currentHeartbeat)
                || (heartbeat.equals(currentHeartbeat)
                && advertisedExpiry.isAfter(instant(current.getExpiresAt())))) {
            return current.toBuilder()
                    .setLastHeartbeatAt(advertisement.getAdvertisedAt())
                    .setTtl(advertisement.getTtl())
                    .setExpiresAt(timestamp(advertisedExpiry))
                    .build();
        }
        return current;
    }

    private void requireCluster(String clusterId, String what) {
        require(clusterId.equals(cluster.getClusterId()),
                what + " names cluster '" + clusterId + "' but this directory serves cluster '"
                        + cluster.getClusterId() + "'");
    }

    private static void requireFresher(long epoch, long seq, long currentEpoch, long currentSeq,
            String identity) {
        require(epoch > currentEpoch || (epoch == currentEpoch && seq > currentSeq),
                "conflicting update for " + identity + ": epoch " + epoch + " seq " + seq
                        + " does not advance the registered epoch " + currentEpoch + " seq "
                        + currentSeq);
    }

    private ClusterEvent emit(ClusterEventType type, String nodeId, String processorId,
            Consumer<ClusterEvent.Builder> detail) {
        ClusterEvent.Builder builder = ClusterEvent.newBuilder()
                .setSeq(++eventSeq)
                .setOccurredAt(timestamp(clock.instant()))
                .setType(type)
                .setNodeId(nodeId)
                .setProcessorId(processorId);
        detail.accept(builder);
        ClusterEvent event = builder.build();
        ClusterValidation.validate(event);
        events.add(event);
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

    private static void requireNewerThanTombstone(Position position, long epoch, long seq,
            String identity) {
        if (position != null) {
            requireFresher(epoch, seq, position.epoch(), position.seq(), identity);
        }
    }

    private static String capacityKey(String nodeId, String processorId) {
        return nodeId + "\n" + processorId;
    }

    private static Instant instant(Timestamp value) {
        return Instant.ofEpochSecond(value.getSeconds(), value.getNanos());
    }

    private static java.time.Duration duration(com.google.protobuf.Duration value) {
        return java.time.Duration.ofSeconds(value.getSeconds(), value.getNanos());
    }

    private static Timestamp timestamp(Instant value) {
        return Timestamp.newBuilder()
                .setSeconds(value.getEpochSecond())
                .setNanos(value.getNano())
                .build();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private record Position(long epoch, long seq) {
    }
}
