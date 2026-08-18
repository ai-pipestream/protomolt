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
 * @param refreshSeconds how often a reader pulls newer snapshots into its
 *        live index; {@code 0} means restart-only, and a positive value
 *        demands a read-only node with snapshots — refresh is the
 *        reader's pull, the writer publishes on its commit cadence
 */
public record SearchDoorConfig(
        int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
        IndexSnapshots snapshots, boolean readOnly, long refreshSeconds) {

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
        if (refreshSeconds < 0) {
            throw new IllegalArgumentException("refreshSeconds must not be negative");
        }
        if (refreshSeconds > 0 && !readOnly) {
            throw new IllegalArgumentException("refresh is the reader's pull: a writable"
                    + " node publishes snapshots on its commit cadence instead");
        }
        if (refreshSeconds > 0 && snapshots == null) {
            throw new IllegalArgumentException(
                    "refreshSeconds needs snapshots to refresh from");
        }
        subjects = Map.copyOf(subjects);
    }

    /** A configuration without periodic refresh (readers restore on boot). */
    public SearchDoorConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
            IndexSnapshots snapshots, boolean readOnly) {
        this(grpcPort, indexDir, subjects, snapshots, readOnly, 0L);
    }

    /** A writable configuration. */
    public SearchDoorConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
            IndexSnapshots snapshots) {
        this(grpcPort, indexDir, subjects, snapshots, false, 0L);
    }

    /** A writable configuration without snapshots. */
    public SearchDoorConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects) {
        this(grpcPort, indexDir, subjects, null, false, 0L);
    }
}
