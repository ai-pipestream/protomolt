package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.DirectoryCheckpoint;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Stores validated, restartable cluster directory state. */
public interface ClusterEventRepository {

    /** Loads the stored directory for the exact cluster identity, when one exists. */
    Optional<StoredDirectory> load(ClusterDescriptor cluster);

    /** Atomically replaces the stored directory for the exact cluster identity. */
    void save(ClusterDescriptor cluster, StoredDirectory directory);

    /**
     * What one cluster's durable directory consists of: the events retained for replay and,
     * once the log has been folded, the checkpoint they replay onto.
     *
     * <p>A directory that emits an event per heartbeat cannot keep its whole history: the
     * log grows without bound, every mutation rewrites all of it, and the events cap turns
     * into a wall the directory can never get past. Folding the prefix into a checkpoint
     * bounds all three while keeping sequences monotonic and never reused, so a client that
     * watches {@code snapshot_seq} to detect change is unaffected by compaction.
     *
     * @param checkpoint the folded state the events replay onto, or null when the log has
     *     never been compacted and the events are the complete history
     * @param events the retained events, in order and gap-free
     */
    record StoredDirectory(DirectoryCheckpoint checkpoint, List<ClusterEvent> events) {

        public StoredDirectory {
            events = List.copyOf(Objects.requireNonNull(events, "events"));
        }

        /** An uncompacted directory whose events are the complete history from sequence one. */
        public static StoredDirectory of(List<ClusterEvent> events) {
            return new StoredDirectory(null, events);
        }

        /** Whether a checkpoint stands in for the events before {@link #firstSeq()}. */
        public boolean compacted() {
            return checkpoint != null;
        }

        /** The sequence the first retained event must carry. */
        public long firstSeq() {
            return compacted() ? checkpoint.getState().getSnapshotSeq() + 1 : 1;
        }

        /** The highest sequence this directory accounts for, folded or retained. */
        public long lastSeq() {
            return events.isEmpty()
                    ? firstSeq() - 1
                    : events.get(events.size() - 1).getSeq();
        }
    }
}
