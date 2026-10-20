package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The complete mutable state of one {@link ClusterDirectory}: the live records, the fencing
 * tombstones, and the durable event log with its next sequence. This is the state the single
 * directory state machine operates on; the rules that move it live with the two drivers,
 * {@link ClusterDirectory} (live apply, which emits) and {@link ClusterEventReplay} (replay,
 * which emits nothing), and the gates they share live in {@link ClusterTransitions}.
 *
 * <p>Nothing here enforces a rule: the holder names what the directory knows, and only the
 * drivers mutate it. The maps are id-ordered ({@link TreeMap}) so equal states serialize to
 * equal snapshots; {@link #events} is in emission order and {@link #eventSeq} is the sequence
 * of its last event (or the folded checkpoint's {@code snapshot_seq}).
 */
final class ClusterDirectoryState {

    final Map<String, NodeAdvertisement> nodes = new TreeMap<>();
    /** Highest accepted node positions, retained after expiry as fencing tombstones. */
    final Map<String, Position> nodePositions = new TreeMap<>();
    final Map<String, NodePresence> presence = new TreeMap<>();
    final Map<String, CapacityAdvertisement> nodeCapacity = new TreeMap<>();
    final Map<String, ProcessorAdvertisement> processors = new TreeMap<>();
    /** Highest accepted processor positions, retained after expiry as fencing tombstones. */
    final Map<String, Position> processorPositions = new TreeMap<>();
    /** Keyed by {@code nodeId + "\n" + processorId}; newline cannot appear in a path-safe id. */
    final Map<String, CapacityAdvertisement> processorCapacity = new TreeMap<>();
    final List<ClusterEvent> events = new ArrayList<>();
    long eventSeq;

    /**
     * Copies the state, sharing nothing a later mutation could reach. Every value held is an
     * immutable message or record, so copying the maps is a true copy. This is how a presence
     * update produces a directory to install without replaying the event log: an installed
     * directory is never mutated, and soft state must not break that.
     */
    ClusterDirectoryState copy() {
        ClusterDirectoryState copy = new ClusterDirectoryState();
        copy.nodes.putAll(nodes);
        copy.nodePositions.putAll(nodePositions);
        copy.presence.putAll(presence);
        copy.nodeCapacity.putAll(nodeCapacity);
        copy.processors.putAll(processors);
        copy.processorPositions.putAll(processorPositions);
        copy.processorCapacity.putAll(processorCapacity);
        copy.events.addAll(events);
        copy.eventSeq = eventSeq;
        return copy;
    }

    /** The map key for one processor's capacity on one node. */
    static String capacityKey(String nodeId, String processorId) {
        return nodeId + "\n" + processorId;
    }

    /** A fencing position: an epoch and the per-epoch sequence last accepted under it. */
    record Position(long epoch, long seq) {
    }
}
