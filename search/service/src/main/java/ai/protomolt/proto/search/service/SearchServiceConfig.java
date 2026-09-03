package ai.protomolt.proto.search.service;

import ai.protomolt.proto.search.embedding.VectorizationPolicy;
import java.nio.file.Path;
import java.util.Map;

/**
 * The service's configuration.
 *
 * @param grpcPort the external gRPC port; 0 picks a free port
 * @param indexDir the root index directory; each subject indexes in its own
 *        subdirectory
 * @param subjects the mapping subjects the service serves, keyed by subject
 *        name; at least one is required
 * @param snapshots commit-point snapshots of every subject's index, or
 *        {@code null} for none: with snapshots the store restores each
 *        subject on boot and snapshots after every commit and on close
 * @param readOnly a reader node: the service mounts only the query surface
 *        (no {@code SearchIndexService}), needs no document fetcher, and
 *        its snapshots, if any, must be restore-only
 * @param refreshSeconds how often a reader pulls newer snapshots into its
 *        live index; {@code 0} means restart-only, and a positive value
 *        demands a read-only node with snapshots — refresh is the
 *        reader's pull, the writer publishes on its commit cadence
 */
public record SearchServiceConfig(
        int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
        IndexSnapshots snapshots, boolean readOnly, long refreshSeconds,
        VectorizationPolicy vectorization) {

    /** Validates the configuration. */
    public SearchServiceConfig {
        if (vectorization == null) {
            vectorization = VectorizationPolicy.unclassifiedOnly();
        }
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

    /** The same configuration with a vectorization policy. */
    public SearchServiceConfig vectorizing(VectorizationPolicy policy) {
        return new SearchServiceConfig(grpcPort, indexDir, subjects, snapshots, readOnly,
                refreshSeconds, policy);
    }

    /** A configuration vectorizing unclassified content only. */
    public SearchServiceConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
            IndexSnapshots snapshots, boolean readOnly, long refreshSeconds) {
        this(grpcPort, indexDir, subjects, snapshots, readOnly, refreshSeconds, null);
    }

    /** A configuration without periodic refresh (readers restore on boot). */
    public SearchServiceConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
            IndexSnapshots snapshots, boolean readOnly) {
        this(grpcPort, indexDir, subjects, snapshots, readOnly, 0L);
    }

    /** A writable configuration. */
    public SearchServiceConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
            IndexSnapshots snapshots) {
        this(grpcPort, indexDir, subjects, snapshots, false, 0L);
    }

    /** A writable configuration without snapshots. */
    public SearchServiceConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects) {
        this(grpcPort, indexDir, subjects, null, false, 0L);
    }
}
