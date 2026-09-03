package ai.protomolt.proto.sources.publish;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Outcome of publishing a source set: one {@link FileOutcome} per file, in registration order.
 */
public record PublishResult(List<FileOutcome> outcomes) {

    public PublishResult {
        outcomes = List.copyOf(outcomes);
    }

    /** What happened to one file. */
    public enum Action {
        /** Registered for the first time. */
        CREATED,
        /** A new version was registered for an existing subject/artifact. */
        UPDATED,
        /** The registry already holds identical content; nothing was written. */
        UNCHANGED,
        /** Dry run: a write (create or update) would have happened. */
        WOULD_WRITE,
        /** Publishing this file failed; {@code message} carries the reason. */
        FAILED
    }

    /**
     * @param path    import path of the file
     * @param subject subject/artifact name it was (or would be) registered under
     * @param action  what happened
     * @param detail  version/id information or a failure message; may be empty
     */
    public record FileOutcome(String path, String subject, Action action, String detail) {
    }

    public long created() {
        return count(Action.CREATED);
    }

    public long updated() {
        return count(Action.UPDATED);
    }

    public long unchanged() {
        return count(Action.UNCHANGED);
    }

    public List<FileOutcome> failures() {
        return outcomes.stream().filter(o -> o.action() == Action.FAILED).toList();
    }

    /**
     * @throws SchemaPublishException when any file failed, summarizing every failure
     */
    public void throwIfFailed() throws SchemaPublishException {
        List<FileOutcome> failures = failures();
        if (!failures.isEmpty()) {
            throw new SchemaPublishException(failures.size() + " of " + outcomes.size()
                    + " files failed to publish: "
                    + failures.stream()
                            .map(f -> f.path() + " (" + f.detail() + ")")
                            .collect(Collectors.joining("; ")));
        }
    }

    private long count(Action action) {
        return outcomes.stream().filter(o -> o.action() == action).count();
    }
}
