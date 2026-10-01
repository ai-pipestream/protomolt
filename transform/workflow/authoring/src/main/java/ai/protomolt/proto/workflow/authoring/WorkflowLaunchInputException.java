package ai.protomolt.proto.workflow.authoring;

/** Sanitized classification for the eventual scoped launch-input adapter. */
public final class WorkflowLaunchInputException extends Exception {
    public enum Kind {
        INVALID_INPUT, INACTIVE, CORRUPT_EVIDENCE, INCOMPATIBLE_ARTIFACT, UNAVAILABLE, DEADLINE
    }

    private final Kind kind;

    public WorkflowLaunchInputException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() { return kind; }
}
