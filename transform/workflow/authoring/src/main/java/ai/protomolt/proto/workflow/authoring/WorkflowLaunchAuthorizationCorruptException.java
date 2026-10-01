package ai.protomolt.proto.workflow.authoring;

import java.io.IOException;

/** A present keyed launch record failed its format or identity checks. */
public final class WorkflowLaunchAuthorizationCorruptException extends IOException {
    public WorkflowLaunchAuthorizationCorruptException(String message) {
        super(message);
    }

    public WorkflowLaunchAuthorizationCorruptException(String message, Throwable cause) {
        super(message, cause);
    }
}
