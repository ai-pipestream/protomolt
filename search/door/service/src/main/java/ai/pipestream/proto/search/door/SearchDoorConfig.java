package ai.pipestream.proto.search.door;

import java.nio.file.Path;
import java.util.Map;

/**
 * The door's configuration.
 *
 * @param grpcPort the external gRPC port; 0 picks a free port
 * @param indexDir the root index directory; each subject indexes in its own
 *        subdirectory
 * @param subjects the mapping subjects the door serves, keyed by subject
 *        name; at least one is required
 * @param snapshots commit-point snapshots of every subject's index, or
 *        {@code null} for none: with snapshots the store restores each
 *        subject on boot and snapshots after every commit and on close
 * @param readOnly a reader node: the door mounts only the query surface
 *        (no {@code SearchIndexService}), needs no document fetcher, and
 *        its snapshots, if any, must be restore-only
 */
public record SearchDoorConfig(
        int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
        IndexSnapshots snapshots, boolean readOnly) {

    /** Validates the configuration. */
    public SearchDoorConfig {
        if (indexDir == null) {
            throw new IllegalArgumentException("indexDir must not be null");
        }
        if (subjects == null || subjects.isEmpty()) {
            throw new IllegalArgumentException("at least one served mapping subject is required");
        }
        if (readOnly && snapshots != null && !snapshots.readOnly()) {
            throw new IllegalArgumentException("a read-only node must not write snapshots:"
                    + " construct its IndexSnapshots read-only");
        }
        subjects = Map.copyOf(subjects);
    }

    /** A writable configuration. */
    public SearchDoorConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
            IndexSnapshots snapshots) {
        this(grpcPort, indexDir, subjects, snapshots, false);
    }

    /** A writable configuration without snapshots. */
    public SearchDoorConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects) {
        this(grpcPort, indexDir, subjects, null, false);
    }
}
