package ai.protomolt.proto.workflow.authoring;

import java.io.IOException;

/** An immutable preparation identity or terminal outcome was reused differently. */
public final class WorkflowPreparationConflictException extends IOException {
    public WorkflowPreparationConflictException(String message) {
        super(message);
    }
}
