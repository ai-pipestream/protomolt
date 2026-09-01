package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.DirectoryCheckpoint;
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
 * <p><b>Heartbeats never reach the repository.</b> Presence is soft state: a node restates
 * it every few seconds and it expires on its own, so it is held in memory and persisted
 * only incidentally, when a fold happens to capture it. Every other change is durable. This
 * is what takes the cluster's most frequent call off the write path entirely, and it is why
 * the event log now grows with membership rather than with time. The price is that a
 * coordinator restart begins with whatever presence the last fold captured, which is
 * usually expired, so nodes are swept and re-register. That is the recovery the fleet
 * already performs, now on a predictable trigger rather than an accidental one.
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

    /**
     * How many events may accumulate beyond the last checkpoint before the log is folded.
     * Every durable mutation rewrites the whole retained log, so this bounds the work one
     * such mutation costs. Since presence became soft state the log grows only with
     * registrations, processor leases, capacity, and expiries, so folds are rare; the bound
     * remains because an unbounded log still ends at the events cap, past which nothing can
     * be persisted at all.
     */
    public static final int DEFAULT_RETAINED_EVENTS = 256;

    private final ClusterDescriptor cluster;
    private final Clock clock;
    private final ClusterEventRepository events;
    private final int retainedEvents;

    /** Serializes mutations, including their durable round trip. Never taken by a reader. */
    private final Object writeLock = new Object();

    /**
     * The checkpoint the retained events replay onto, or null while the log still holds its
     * complete history. Written only under {@link #writeLock}, alongside {@link #current}.
     */
    private volatile DirectoryCheckpoint checkpoint;

    /**
     * The installed directory. Written only under {@link #writeLock}, and only with a
     * candidate no other thread can reach, so the volatile write safely publishes a
     * directory that nothing mutates afterwards.
     */
    private volatile ClusterDirectory current;

    /** Loads and replays any stored directory for {@code cluster}. */
    public PersistentClusterDirectory(ClusterDescriptor cluster, Clock clock,
                                      ClusterEventRepository events) {
        this(cluster, clock, events, DEFAULT_RETAINED_EVENTS);
    }

    /**
     * Loads and replays any stored directory, folding the log once more than
     * {@code retainedEvents} events accumulate beyond the last checkpoint.
     *
     * @param retainedEvents how many events may accumulate before the log is folded; at
     *     least one
     */
    public PersistentClusterDirectory(ClusterDescriptor cluster, Clock clock,
                                      ClusterEventRepository events, int retainedEvents) {
        ClusterValidation.validate(cluster);
        if (retainedEvents < 1) {
            throw new IllegalArgumentException("retainedEvents must be at least one");
        }
        this.cluster = cluster;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.events = Objects.requireNonNull(events, "events");
        this.retainedEvents = retainedEvents;
        ClusterEventRepository.StoredDirectory stored = events.load(cluster).orElseGet(
                () -> ClusterEventRepository.StoredDirectory.of(List.of()));
        // The checkpoint has to be retained, not merely replayed. A mutation rebuilds its
        // candidate from this field plus the retained events, so dropping it here would
        // rebuild the next candidate from the tail alone and quietly discard every node the
        // fold accounts for.
        this.checkpoint = stored.compacted() ? stored.checkpoint() : null;
        this.current = restore(stored);
    }

    private ClusterDirectory restore(ClusterEventRepository.StoredDirectory stored) {
        return stored.compacted()
                ? ClusterDirectory.restore(cluster, stored.checkpoint(), stored.events(), clock)
                : ClusterDirectory.replay(cluster, stored.events(), clock);
    }

    /** Registers or refreshes one node after the resulting event log is durable. */
    public ClusterDirectory.ApplyOutcome register(NodeAdvertisement advertisement) {
        return mutate(candidate -> candidate.register(advertisement));
    }

    /**
     * Applies a node heartbeat to memory alone. Presence is soft state: it expires on its
     * own and the node restates it every few seconds, so it never reaches the repository and
     * a heartbeat costs no durable write. See {@link ClusterDirectory#heartbeat} for why the
     * registered epoch, which stays durable, is what fences it.
     */
    public ClusterDirectory.ApplyOutcome heartbeat(NodePresence presence) {
        return soften(candidate -> candidate.heartbeat(presence));
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

    /**
     * Returns the durable events retained beyond the last checkpoint. Before the first fold
     * this is the directory's complete history; afterwards the events before
     * {@link #checkpoint()} are folded into it rather than kept.
     */
    public List<ClusterEvent> eventLog() {
        return current.events();
    }

    /** The checkpoint the retained events replay onto, empty while none has been folded. */
    public Optional<DirectoryCheckpoint> checkpoint() {
        return Optional.ofNullable(checkpoint);
    }

    /**
     * Applies a change that produces no durable event, installing it without touching the
     * repository. The candidate is copied rather than replayed: there is nothing to persist,
     * so there is nothing to rebuild from, and copying keeps the rule that an installed
     * directory is never mutated, which is what lets readers run without a lock.
     *
     * <p>This still takes {@link #writeLock}, so a heartbeat can wait behind one durable
     * mutation and its repository round trip. That is bounded by the repository deadline and
     * is strictly better than what it replaced, where the heartbeat was itself that round
     * trip. Serializing here is what keeps the carry-over in {@link #mutate} exact: a
     * heartbeat cannot land between a rebuild reading live presence and the install that
     * replaces it, so none is ever silently dropped.
     */
    private ClusterDirectory.ApplyOutcome soften(
            Function<ClusterDirectory, ClusterDirectory.ApplyOutcome> operation) {
        synchronized (writeLock) {
            ClusterDirectory candidate = current.copy();
            int priorSize = candidate.events().size();
            ClusterDirectory.ApplyOutcome outcome = operation.apply(candidate);
            if (candidate.events().size() != priorSize) {
                throw new IllegalStateException(
                        "a soft directory change emitted "
                                + (candidate.events().size() - priorSize)
                                + " durable events, which this path drops on the floor; the "
                                + "mesh refuses to install a directory whose log it silently "
                                + "discarded");
            }
            if (outcome == ClusterDirectory.ApplyOutcome.UNCHANGED) {
                return outcome;
            }
            current = candidate;
            return outcome;
        }
    }

    private <T> T mutate(Function<ClusterDirectory, T> operation) {
        synchronized (writeLock) {
            ClusterDirectory candidate = restore(new ClusterEventRepository.StoredDirectory(
                    checkpoint, current.events()));
            // The rebuild knows only the presence that registration armed and the last fold
            // captured, because heartbeats emit nothing. Carrying the live records over is
            // what stops an unrelated mutation from rolling liveness back and sweeping nodes
            // that are heartbeating normally.
            candidate.adoptSoftState(current);
            int priorSize = candidate.events().size();
            T result = operation.apply(candidate);
            if (candidate.events().size() == priorSize) {
                // Nothing durable happened, but the operation may still have refreshed a
                // lease or presence, which lives only in memory. Installing the candidate is
                // how that survives; returning here would discard it and let the next sweep
                // take an identity that is renewing normally. There is nothing to persist,
                // so the repository is not touched.
                current = candidate;
                return result;
            }
            if (candidate.events().size() > retainedEvents) {
                // Fold first, then install the directory restored from the fold rather than
                // the candidate. The two hold the same state, but only the restored one has
                // the retained log the checkpoint was written with, so the next mutation
                // cannot replay events the checkpoint already accounts for.
                DirectoryCheckpoint folded = candidate.checkpoint();
                ClusterDirectory compacted =
                        ClusterDirectory.restore(cluster, folded, List.of(), clock);
                events.save(cluster,
                        new ClusterEventRepository.StoredDirectory(folded, List.of()));
                checkpoint = folded;
                current = compacted;
            } else {
                events.save(cluster, new ClusterEventRepository.StoredDirectory(
                        checkpoint, candidate.events()));
                current = candidate;
            }
            return result;
        }
    }
}
