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
 * presence, capacity snapshots, TTL expiry, deterministic snapshots, and a replayable event
 * log for one cluster. Pure and single-threaded: no I/O, no threads, and all temporal
 * judgments run against the injected {@link Clock}, so tests drive expiry with a fake clock.
 *
 * <p><b>Update rules.</b> Every identity carries a fencing epoch and a per-epoch sequence
 * (nodes: {@code epoch}/{@code seq}; processors: {@code lease_epoch}/{@code seq}; presence:
 * {@code heartbeat_seq}; capacity: {@code seq} per key). Re-applying the identical record is
 * an idempotent no-op. A changed record is accepted only from a strictly newer position: a
 * higher epoch, or the same epoch with a higher sequence. A changed record from a stale
 * position is a conflicting update and throws {@link IllegalArgumentException}.
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
 * serialize to equal bytes and equal canonical fingerprints
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
    private final Map<String, NodePresence> presence = new TreeMap<>();
    private final Map<String, CapacityAdvertisement> nodeCapacity = new TreeMap<>();
    private final Map<String, ProcessorAdvertisement> processors = new TreeMap<>();
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
            nodes.put(nodeId, advertisement);
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
        nodes.put(nodeId, advertisement);
        presence.put(nodeId, mergePresence(presence.get(nodeId), advertisement));
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
        NodePresence existing = presence.get(nodeId);
        if (existing.equals(record)) {
            return ApplyOutcome.UNCHANGED;
        }
        require(record.getHeartbeatSeq() > existing.getHeartbeatSeq(),
                "conflicting presence for node '" + nodeId + "': heartbeat_seq "
                        + record.getHeartbeatSeq() + " does not advance the registered "
                        + existing.getHeartbeatSeq());
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
        require(nodes.containsKey(advertisement.getNodeId()),
                "processor '" + advertisement.getProcessorId() + "' names unregistered node '"
                        + advertisement.getNodeId() + "'");
        String processorId = advertisement.getProcessorId();
        ProcessorAdvertisement existing = processors.get(processorId);
        if (existing == null) {
            processors.put(processorId, advertisement);
            emit(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_REGISTERED,
                    advertisement.getNodeId(), processorId, b -> b.setProcessor(advertisement));
            return ApplyOutcome.REGISTERED;
        }
        if (existing.equals(advertisement)) {
            return ApplyOutcome.UNCHANGED;
        }
        requireFresher(advertisement.getLeaseEpoch(), advertisement.getSeq(),
                existing.getLeaseEpoch(), existing.getSeq(), "processor '" + processorId + "'");
        processors.put(processorId, advertisement);
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
        if (!nodeWide) {
            require(processors.containsKey(snapshot.getProcessorId()),
                    "capacity for unregistered processor '" + snapshot.getProcessorId() + "'");
        }
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
        require(snapshot.getSeq() > existing.getSeq(),
                "conflicting capacity for node '" + nodeId + "': seq " + snapshot.getSeq()
                        + " does not advance the registered " + existing.getSeq());
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
        Instant now = clock.instant();
        return processors.values().stream()
                .filter(p -> instant(p.getLeaseExpiresAt()).isAfter(now))
                .filter(p -> isServing(p.getNodeId(), now))
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
        emitted.add(emit(ClusterEventType.CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED,
                advertisement.getNodeId(), processorId, b -> b.setProcessor(advertisement)));
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
                .setExpiresAt(timestamp(instant(advertisement.getAdvertisedAt())
                        .plus(duration(advertisement.getTtl()))))
                .build();
    }

    private NodePresence mergePresence(NodePresence current, NodeAdvertisement advertisement) {
        Instant heartbeat = instant(advertisement.getAdvertisedAt());
        Instant advertisedExpiry = heartbeat.plus(duration(advertisement.getTtl()));
        Instant expiry = advertisedExpiry.isAfter(instant(current.getExpiresAt()))
                ? advertisedExpiry : instant(current.getExpiresAt());
        Instant lastHeartbeat = heartbeat.isAfter(instant(current.getLastHeartbeatAt()))
                ? heartbeat : instant(current.getLastHeartbeatAt());
        return current.toBuilder()
                .setLastHeartbeatAt(timestamp(lastHeartbeat))
                .setExpiresAt(timestamp(expiry))
                .build();
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
        events.add(event);
        return event;
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
}
