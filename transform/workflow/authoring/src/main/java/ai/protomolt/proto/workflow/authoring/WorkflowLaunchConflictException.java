package ai.protomolt.proto.workflow.authoring;

/** A launch UUID is already bound to different content; retrying changed content is invalid. */
public final class WorkflowLaunchConflictException extends IllegalArgumentException {
    public WorkflowLaunchConflictException(String message) {
        super(message);
    }
}
