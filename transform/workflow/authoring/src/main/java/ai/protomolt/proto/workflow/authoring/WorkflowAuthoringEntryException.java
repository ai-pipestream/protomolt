package ai.protomolt.proto.workflow.authoring;

/** Sanitized outcome for the scoped authoring-entry adapter. */
public final class WorkflowAuthoringEntryException extends RuntimeException {
    public enum Kind { INVALID_INPUT, CONFLICT, INACTIVE, CORRUPT_EVIDENCE, UNAVAILABLE, DEADLINE }

    private final Kind kind;

    public WorkflowAuthoringEntryException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() { return kind; }
}
