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
 *
 * <p><b>Readers never wait on the repository.</b> A mutation persists before it installs,
 * so the write path owns the durable round trip for as long as the repository takes.
 * Readers therefore do not share that lock: {@link #current} is volatile, an installed
 * directory is never mutated again, and each read takes one snapshot of the reference and
 * works entirely from it. Serializing reads behind writes is what starved
 * {@link #snapshot()} on a heartbeating cluster: a monitor makes no fairness promise, so a
 * steady stream of writers can hold it indefinitely against a waiting reader. A read may
 * observe the state from just before a concurrent commit, which is inherent to any
 * snapshot of a live directory and is exactly what {@code snapshot_seq} reports.
 */
public final class PersistentClusterDirectory {

    private final ClusterDescriptor cluster;
    private final Clock clock;
    private final ClusterEventRepository events;

    /** Serializes mutations, including their durable round trip. Never taken by a reader. */
    private final Object writeLock = new Object();

    /**
     * The installed directory. Written only under {@link #writeLock}, and only with a
     * candidate no other thread can reach, so the volatile write safely publishes a
     * directory that nothing mutates afterwards.
     */
    private volatile ClusterDirectory current;

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
    public ClusterDirectory.ApplyOutcome register(NodeAdvertisement advertisement) {
        return mutate(candidate -> candidate.register(advertisement));
    }

    /** Applies a node heartbeat after the resulting event log is durable. */
    public ClusterDirectory.ApplyOutcome heartbeat(NodePresence presence) {
        return mutate(candidate -> candidate.heartbeat(presence));
    }

    /** Registers or refreshes a processor after the resulting event log is durable. */
    public ClusterDirectory.ApplyOutcome registerProcessor(ProcessorAdvertisement advertisement) {
        return mutate(candidate -> candidate.registerProcessor(advertisement));
    }

    /** Applies capacity after the resulting event log is durable. */
    public ClusterDirectory.ApplyOutcome updateCapacity(CapacityAdvertisement capacity) {
        return mutate(candidate -> candidate.updateCapacity(capacity));
    }

    /** Sweeps expired identities after every emitted expiry event is durable. */
    public List<ClusterEvent> sweep() {
        return mutate(ClusterDirectory::sweep);
    }

    /** Returns eligible processors from the current memory projection. */
    public List<ProcessorAdvertisement> eligibleProcessors(
            String typeName, String descriptorFingerprint, String capability) {
        return current.eligibleProcessors(typeName, descriptorFingerprint, capability);
    }

    /** Returns capacity-aware eligible processors from the current memory projection. */
    public List<ProcessorAdvertisement> eligibleProcessors(
            String typeName, String descriptorFingerprint, String capability,
            long payloadBytes) {
        return current.eligibleProcessors(typeName, descriptorFingerprint,
                capability, payloadBytes);
    }

    /** Captures the current deterministic directory snapshot. */
    public ClusterSnapshot snapshot() {
        return current.snapshot();
    }

    /** Returns the registered node when present. */
    public Optional<NodeAdvertisement> node(String nodeId) {
        return current.node(nodeId);
    }

    /** Returns the current node presence when present. */
    public Optional<NodePresence> presence(String nodeId) {
        return current.presence(nodeId);
    }

    /** Returns the registered processor when present. */
    public Optional<ProcessorAdvertisement> processor(String processorId) {
        return current.processor(processorId);
    }

    /** Returns the node-level capacity record when present. */
    public Optional<CapacityAdvertisement> nodeCapacity(String nodeId) {
        return current.nodeCapacity(nodeId);
    }

    /** Returns the processor-level capacity record when present. */
    public Optional<CapacityAdvertisement> processorCapacity(String nodeId, String processorId) {
        return current.processorCapacity(nodeId, processorId);
    }

    /** Returns the complete durable event log. */
    public List<ClusterEvent> eventLog() {
        return current.events();
    }

    private <T> T mutate(Function<ClusterDirectory, T> operation) {
        synchronized (writeLock) {
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
}
