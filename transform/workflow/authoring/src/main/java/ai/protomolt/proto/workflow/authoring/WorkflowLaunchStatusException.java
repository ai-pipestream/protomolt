package ai.protomolt.proto.workflow.authoring;

/** Sanitized classification for the scoped launch-status adapter. */
public final class WorkflowLaunchStatusException extends Exception {
    public enum Kind { INVALID_INPUT, CONFLICT, CORRUPT_EVIDENCE, UNAVAILABLE, DEADLINE }

    private final Kind kind;

    public WorkflowLaunchStatusException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() { return kind; }
}
