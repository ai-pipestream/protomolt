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
 */
public record SearchDoorConfig(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects) {

    /** Validates the configuration. */
    public SearchDoorConfig {
        if (indexDir == null) {
            throw new IllegalArgumentException("indexDir must not be null");
        }
        if (subjects == null || subjects.isEmpty()) {
            throw new IllegalArgumentException("at least one served mapping subject is required");
        }
        subjects = Map.copyOf(subjects);
    }
}
