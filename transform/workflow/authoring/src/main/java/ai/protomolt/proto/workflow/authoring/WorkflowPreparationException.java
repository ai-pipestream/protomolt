package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationBinding;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailure;

/** Stable preparation outcome classification for the eventual transport adapter. */
public final class WorkflowPreparationException extends Exception {
    public enum Kind {
        INVALID_INPUT, PERMISSION_DENIED, INACTIVE, CONFLICT, TERMINAL_FAILED, CORRUPT_EVIDENCE,
        INVALID_UPSTREAM, UNAVAILABLE, DEADLINE
    }

    private final Kind kind;
    private final WorkflowPreparationBinding binding;
    private final String runId;
    private final WorkflowPreparationFailure failure;

    public WorkflowPreparationException(Kind kind, String message, Throwable cause) {
        this(kind, message, cause, null);
    }

    public WorkflowPreparationException(Kind kind, String message, Throwable cause,
            WorkflowPreparationFailure failure) {
        super(message, cause);
        this.kind = kind;
        this.failure = failure;
        this.binding = failure == null ? null : failure.getBinding();
        this.runId = failure == null ? "" : failure.getRunId();
    }

    public Kind kind() { return kind; }
    public WorkflowPreparationBinding binding() { return binding; }
    public String runId() { return runId; }
    public WorkflowPreparationFailure failure() { return failure; }
}
