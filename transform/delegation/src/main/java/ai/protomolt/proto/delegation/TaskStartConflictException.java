package ai.protomolt.proto.delegation;

/** A task UUID already has a different or unbound first offer. */
public final class TaskStartConflictException extends IllegalStateException {
    public TaskStartConflictException(String message) {
        super(message);
    }
}
