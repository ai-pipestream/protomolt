package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Durable facade over the memory-resident {@link ClusterDirectory}. Each mutation is
 * applied to a replayed candidate, persisted as a complete event log, and only then
 * installed as the live directory. Failed writes cannot leak memory-only membership.
 */
public final class PersistentClusterDirectory {

    private final ClusterDescriptor cluster;
    private final Clock clock;
    private final ClusterEventRepository events;
    private ClusterDirectory current;

    /** Loads and replays any stored log for {@code cluster}. */
    public PersistentClusterDirectory(ClusterDescriptor cluster, Clock clock,
                                      ClusterEventRepository events) {
        ClusterValidation.validate(cluster);
        this.cluster = cluster;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.events = Objects.requireNonNull(events, "events");
        this.current = ClusterDirectory.replay(cluster,
                events.load(cluster).orElseGet(List::of), clock);
    }

    /** Registers or refreshes one node after the resulting event log is durable. */
    public synchronized ClusterDirectory.ApplyOutcome register(
            NodeAdvertisement advertisement) {
        return mutate(candidate -> candidate.register(advertisement));
    }

    /** Applies a node heartbeat after the resulting event log is durable. */
    public synchronized ClusterDirectory.ApplyOutcome heartbeat(NodePresence presence) {
        return mutate(candidate -> candidate.heartbeat(presence));
    }

    /** Registers or refreshes a processor after the resulting event log is durable. */
    public synchronized ClusterDirectory.ApplyOutcome registerProcessor(
            ProcessorAdvertisement advertisement) {
        return mutate(candidate -> candidate.registerProcessor(advertisement));
    }

    /** Applies capacity after the resulting event log is durable. */
    public synchronized ClusterDirectory.ApplyOutcome updateCapacity(
            CapacityAdvertisement capacity) {
        return mutate(candidate -> candidate.updateCapacity(capacity));
    }

    /** Sweeps expired identities after every emitted expiry event is durable. */
    public synchronized List<ClusterEvent> sweep() {
        return mutate(ClusterDirectory::sweep);
    }

    /** Returns eligible processors from the current memory projection. */
    public synchronized List<ProcessorAdvertisement> eligibleProcessors(
            String typeName, String descriptorFingerprint, String capability) {
        return current.eligibleProcessors(typeName, descriptorFingerprint, capability);
    }

    /** Returns capacity-aware eligible processors from the current memory projection. */
    public synchronized List<ProcessorAdvertisement> eligibleProcessors(
            String typeName, String descriptorFingerprint, String capability,
            long payloadBytes) {
        return current.eligibleProcessors(typeName, descriptorFingerprint,
                capability, payloadBytes);
    }

    /** Captures the current deterministic directory snapshot. */
    public synchronized ClusterSnapshot snapshot() {
        return current.snapshot();
    }

    /** Returns the registered node when present. */
    public synchronized Optional<NodeAdvertisement> node(String nodeId) {
        return current.node(nodeId);
    }

    /** Returns the current node presence when present. */
    public synchronized Optional<NodePresence> presence(String nodeId) {
        return current.presence(nodeId);
    }

    /** Returns the registered processor when present. */
    public synchronized Optional<ProcessorAdvertisement> processor(String processorId) {
        return current.processor(processorId);
    }

    /** Returns the node-level capacity record when present. */
    public synchronized Optional<CapacityAdvertisement> nodeCapacity(String nodeId) {
        return current.nodeCapacity(nodeId);
    }

    /** Returns the processor-level capacity record when present. */
    public synchronized Optional<CapacityAdvertisement> processorCapacity(
            String nodeId, String processorId) {
        return current.processorCapacity(nodeId, processorId);
    }

    /** Returns the complete durable event log. */
    public synchronized List<ClusterEvent> eventLog() {
        return current.events();
    }

    private <T> T mutate(Function<ClusterDirectory, T> operation) {
        ClusterDirectory candidate = ClusterDirectory.replay(
                cluster, current.events(), clock);
        int priorSize = candidate.events().size();
        T result = operation.apply(candidate);
        if (candidate.events().size() != priorSize) {
            events.save(cluster, candidate.events());
            current = candidate;
        }
        return result;
    }
}
